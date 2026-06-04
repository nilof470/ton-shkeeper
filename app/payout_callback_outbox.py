from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone

import requests
from sqlalchemy import and_, or_

from .config import config
from .models import PayoutCallbackOutbox, db


STATUS_PENDING = "PENDING"
STATUS_DISPATCHING = "DISPATCHING"
STATUS_SENT = "SENT"
STATUS_FAILED = "FAILED"


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _to_dict(row):
    if row is None:
        return None
    return {
        "id": row.id,
        "symbol": row.symbol,
        "payload_json": row.payload_json,
        "status": row.status,
        "attempts": row.attempts,
        "next_attempt_at": row.next_attempt_at.isoformat() if row.next_attempt_at else None,
        "claimed_at": row.claimed_at.isoformat() if row.claimed_at else None,
        "claim_token": row.claim_token,
        "last_http_status": row.last_http_status,
        "last_error": row.last_error,
        "last_response_text": row.last_response_text,
        "sent_at": row.sent_at.isoformat() if row.sent_at else None,
    }


def create_payout_callback(data, symbol):
    row = PayoutCallbackOutbox(
        symbol=symbol,
        payload_json=_json(data),
        status=STATUS_PENDING,
        attempts=0,
        next_attempt_at=utc_now(),
    )
    db.session.add(row)
    db.session.commit()
    return row.id


def get_payout_callback(outbox_id):
    return _to_dict(PayoutCallbackOutbox.query.get(int(outbox_id)))


def _due_filter(now):
    claim_expired_before = now - timedelta(
        seconds=config["PAYOUT_CALLBACK_CLAIM_TTL_SEC"]
    )
    return and_(
        PayoutCallbackOutbox.attempts < config["PAYOUT_CALLBACK_MAX_ATTEMPTS"],
        or_(
            and_(
                PayoutCallbackOutbox.status == STATUS_PENDING,
                or_(
                    PayoutCallbackOutbox.next_attempt_at.is_(None),
                    PayoutCallbackOutbox.next_attempt_at <= now,
                ),
            ),
            and_(
                PayoutCallbackOutbox.status == STATUS_DISPATCHING,
                PayoutCallbackOutbox.claimed_at.isnot(None),
                PayoutCallbackOutbox.claimed_at <= claim_expired_before,
            ),
        ),
    )


def claim_payout_callback(outbox_id, claim_token=None):
    claim_token = claim_token or str(uuid.uuid4())
    now = utc_now()
    PayoutCallbackOutbox.query.filter(
        PayoutCallbackOutbox.id == int(outbox_id),
        _due_filter(now),
    ).update(
        {
            "status": STATUS_DISPATCHING,
            "claimed_at": now,
            "claim_token": claim_token,
            "updated_at": now,
        },
        synchronize_session=False,
    )
    db.session.commit()
    return _to_dict(PayoutCallbackOutbox.query.get(int(outbox_id)))


def claim_due_payout_callbacks(limit, claim_token=None):
    claim_token = claim_token or str(uuid.uuid4())
    now = utc_now()
    rows = (
        PayoutCallbackOutbox.query.with_entities(PayoutCallbackOutbox.id)
        .filter(_due_filter(now))
        .order_by(PayoutCallbackOutbox.next_attempt_at, PayoutCallbackOutbox.id)
        .limit(int(limit))
        .all()
    )
    ids = [row.id for row in rows]
    claimed = []
    for outbox_id in ids:
        row = claim_payout_callback(outbox_id, claim_token=claim_token)
        if (
            row
            and row["status"] == STATUS_DISPATCHING
            and row["claim_token"] == claim_token
        ):
            claimed.append(row)
    return claimed


def _update_after_attempt(
    outbox_id,
    *,
    claim_token,
    status,
    http_status=None,
    response_text=None,
    error=None,
):
    row = PayoutCallbackOutbox.query.get(int(outbox_id))
    if row is None:
        return None
    if row.status != STATUS_DISPATCHING or row.claim_token != claim_token:
        return _to_dict(row)
    row.status = status
    row.attempts += 1
    row.next_attempt_at = (
        utc_now() + timedelta(seconds=config["PAYOUT_CALLBACK_RETRY_DELAY_SEC"])
        if status == STATUS_PENDING
        else None
    )
    row.claimed_at = None
    row.claim_token = None
    row.last_http_status = http_status
    row.last_response_text = response_text[:1000] if response_text else None
    row.last_error = error
    row.updated_at = utc_now()
    if status == STATUS_SENT:
        row.sent_at = utc_now()
    db.session.commit()
    return _to_dict(row)


def dispatch_payout_callback(outbox_id, claim_token=None):
    row = PayoutCallbackOutbox.query.get(int(outbox_id))
    if row is None:
        return {"status": STATUS_FAILED, "error": "callback outbox row not found"}
    if row.status in (STATUS_SENT, STATUS_FAILED):
        return _to_dict(row)
    if row.status == STATUS_DISPATCHING:
        if not claim_token or row.claim_token != claim_token:
            return _to_dict(row)
    else:
        claim_token = claim_token or str(uuid.uuid4())
        claimed = claim_payout_callback(outbox_id, claim_token=claim_token)
        if not claimed or claimed["status"] != STATUS_DISPATCHING:
            return claimed
        row = PayoutCallbackOutbox.query.get(int(outbox_id))

    payload = json.loads(row.payload_json)
    try:
        response = requests.post(
            f'http://{config["SHKEEPER_HOST"]}/api/v1/payoutnotify/{row.symbol}',
            headers={"X-Shkeeper-Backend-Key": config["SHKEEPER_KEY"]},
            json=payload,
            timeout=config["PAYOUT_CALLBACK_TIMEOUT_SEC"],
        )
    except Exception as exc:
        next_attempts = int(row.attempts) + 1
        status = (
            STATUS_FAILED
            if next_attempts >= config["PAYOUT_CALLBACK_MAX_ATTEMPTS"]
            else STATUS_PENDING
        )
        return _update_after_attempt(
            outbox_id,
            claim_token=claim_token,
            status=status,
            error=str(exc),
        )

    http_status = getattr(response, "status_code", None)
    response_text = getattr(response, "text", "")
    sent = http_status is not None and 200 <= http_status < 300
    next_attempts = int(row.attempts) + 1
    status = (
        STATUS_SENT
        if sent
        else (
            STATUS_FAILED
            if next_attempts >= config["PAYOUT_CALLBACK_MAX_ATTEMPTS"]
            else STATUS_PENDING
        )
    )
    return _update_after_attempt(
        outbox_id,
        claim_token=claim_token,
        status=status,
        http_status=http_status,
        response_text=response_text,
        error=None if sent else f"HTTP {http_status}",
    )


def should_retry(row):
    return (
        row is not None
        and row["status"] == STATUS_PENDING
        and int(row["attempts"]) < config["PAYOUT_CALLBACK_MAX_ATTEMPTS"]
    )
