from __future__ import annotations

import hashlib
import hmac
import time
from functools import wraps
from urllib.parse import parse_qsl, urlencode

from flask import g, request
from sqlalchemy.exc import IntegrityError

from .config import config
from .models import PayoutAuthNonce, db
from .payout_observability import (
    payout_operation_from_request,
    record_payout_request_failed,
)


class PayoutAuthError(Exception):
    def __init__(self, message, *, code, status_code=401):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def canonical_query_string(query_string):
    pairs = parse_qsl(query_string.decode("utf-8"), keep_blank_values=True)
    return urlencode(sorted(pairs))


def body_sha256(body_bytes):
    return hashlib.sha256(body_bytes).hexdigest()


def signature_base(timestamp, nonce, method, path, query, body_bytes):
    return "\n".join(
        [
            str(timestamp),
            str(nonce),
            method.upper(),
            path,
            query,
            body_sha256(body_bytes),
        ]
    )


def _consumer_config(consumer):
    consumers = config.get("PAYOUT_CONSUMER_KEYS") or {}
    return consumers.get(consumer) or {}


def _secret_for(consumer, key_id):
    consumer_config = _consumer_config(consumer)
    keys = consumer_config.get("keys") if isinstance(consumer_config, dict) else None
    if isinstance(keys, dict) and key_id in keys:
        return str(keys[key_id])
    if consumer_config.get("key_id") == key_id and consumer_config.get("secret"):
        return str(consumer_config["secret"])
    return None


def _rail_allowed(consumer, rail):
    consumer_config = _consumer_config(consumer)
    rails = consumer_config.get("rails") if isinstance(consumer_config, dict) else None
    return rails is None or rail in rails


def _remember_nonce(consumer, key_id, nonce, timestamp):
    cutoff = int(time.time()) - int(config["PAYOUT_AUTH_MAX_AGE_SECONDS"])
    PayoutAuthNonce.query.filter(PayoutAuthNonce.timestamp < cutoff).delete()
    row = PayoutAuthNonce(
        consumer=consumer,
        key_id=key_id,
        nonce=nonce,
        timestamp=int(timestamp),
    )
    db.session.add(row)
    try:
        db.session.commit()
    except IntegrityError as exc:
        db.session.rollback()
        raise PayoutAuthError(
            "Payout request nonce was already used",
            code="PAYOUT_AUTH_REPLAY",
        ) from exc


def verify_payout_request(headers, body_bytes, *, method, path, query, rail):
    consumer = headers.get("X-Payout-Consumer")
    key_id = headers.get("X-Payout-Key-Id")
    timestamp = headers.get("X-Payout-Timestamp")
    nonce = headers.get("X-Payout-Nonce")
    signature = headers.get("X-Payout-Signature")
    if not all([consumer, key_id, timestamp, nonce, signature]):
        raise PayoutAuthError("Missing payout auth headers", code="PAYOUT_AUTH_MISSING")
    try:
        timestamp_int = int(timestamp)
    except ValueError as exc:
        raise PayoutAuthError("Invalid payout auth timestamp", code="PAYOUT_AUTH_TIMESTAMP") from exc

    if abs(int(time.time()) - timestamp_int) > int(config["PAYOUT_AUTH_MAX_AGE_SECONDS"]):
        raise PayoutAuthError("Payout auth timestamp is outside tolerance", code="PAYOUT_AUTH_TIMESTAMP")
    if not _rail_allowed(consumer, rail):
        raise PayoutAuthError("Consumer is not authorized for this rail", code="PAYOUT_CONSUMER_FORBIDDEN", status_code=403)

    secret = _secret_for(consumer, key_id)
    if not secret:
        raise PayoutAuthError("Unknown payout auth key", code="PAYOUT_AUTH_UNKNOWN_KEY")

    expected = hmac.new(
        secret.encode("utf-8"),
        signature_base(timestamp, nonce, method, path, query, body_bytes).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise PayoutAuthError("Invalid payout request signature", code="PAYOUT_AUTH_INVALID_SIGNATURE")

    _remember_nonce(consumer, key_id, nonce, timestamp_int)
    return consumer


def payout_auth_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        body = request.get_data(cache=True)
        rail = getattr(g, "symbol", "").upper()
        query = canonical_query_string(request.query_string)
        try:
            g.payout_consumer = verify_payout_request(
                request.headers,
                body,
                method=request.method,
                path=request.path,
                query=query,
                rail=rail,
            )
        except PayoutAuthError as exc:
            record_payout_request_failed(
                payout_operation_from_request(request.method, request.path),
                exc.code,
            )
            return {
                "status": "error",
                "code": exc.code,
                "message": str(exc),
            }, exc.status_code
        return view(*args, **kwargs)

    return wrapper
