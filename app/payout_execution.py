from __future__ import annotations

from contextlib import nullcontext
import base64
import json
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy.exc import (
    DBAPIError,
    IntegrityError,
    OperationalError,
    PendingRollbackError,
    SQLAlchemyError,
)

from .config import config
from .models import PayoutExecution, db
from .payout_contract import (
    PayoutContractError,
    canonical_payload,
    compact_json,
    sidecar_payload_hash,
)


STATE_RECEIVED = "RECEIVED"
STATE_VALIDATED = "VALIDATED"
STATE_SIGNING = "SIGNING"
STATE_SIGNED = "SIGNED"
STATE_BROADCASTING = "BROADCASTING"
STATE_BROADCASTED = "BROADCASTED"
STATE_CONFIRMING = "CONFIRMING"
STATE_CONFIRMED = "CONFIRMED"
STATE_FAILED_PRE_BROADCAST = "FAILED_PRE_BROADCAST"
STATE_FAILED_CHAIN_TERMINAL = "FAILED_CHAIN_TERMINAL"
STATE_RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"
UNSAFE_RECOVERY_STATES = (STATE_SIGNING, STATE_SIGNED, STATE_BROADCASTING)
NO_DOWNGRADE_STATES = (
    STATE_BROADCASTED,
    STATE_CONFIRMING,
    STATE_CONFIRMED,
    STATE_FAILED_PRE_BROADCAST,
    STATE_FAILED_CHAIN_TERMINAL,
    STATE_RECONCILIATION_REQUIRED,
)
TRANSIENT_DBAPI_NUMERIC_CODES = {
    1205,  # MySQL lock wait timeout
    1213,  # MySQL deadlock
    2006,  # MySQL server has gone away
    2013,  # MySQL lost connection
    2055,  # MySQL lost connection to server
}
TRANSIENT_DBAPI_SQLSTATES = {
    "40001",  # serialization failure
    "40P01",  # PostgreSQL deadlock
    "57P01",  # PostgreSQL admin shutdown
    "57P02",  # PostgreSQL crash shutdown
    "57P03",  # PostgreSQL cannot connect now
}


class PayoutExecutionError(Exception):
    def __init__(self, message, *, code, status_code=400):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def utc_now_iso():
    return (
        datetime.now(timezone.utc)
        .replace(tzinfo=None)
        .isoformat(timespec="microseconds")
        + "Z"
    )


def utc_now_naive():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_iso(value):
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(
        tzinfo=None
    )


def maybe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def _dbapi_error_codes(exc):
    orig = getattr(exc, "orig", None)
    if orig is None:
        return []
    codes = []
    for attr in ("errno", "pgcode", "sqlstate"):
        value = getattr(orig, attr, None)
        if value is not None:
            codes.append(value)
    if getattr(orig, "args", None):
        codes.append(orig.args[0])
    return codes


def is_transient_db_error(exc):
    if isinstance(exc, PendingRollbackError):
        return True
    if isinstance(exc, DBAPIError) and bool(
        getattr(exc, "connection_invalidated", False)
    ):
        return True
    if not isinstance(exc, OperationalError):
        return False
    for code in _dbapi_error_codes(exc):
        if code in TRANSIENT_DBAPI_NUMERIC_CODES:
            return True
        text_code = str(code)
        if text_code in TRANSIENT_DBAPI_SQLSTATES or text_code.startswith("08"):
            return True
    return False


class PayoutExecutionStore:
    @staticmethod
    def _jetton_master(endpoint_symbol):
        try:
            return config["TOKENS"][config["CURRENT_TON_NETWORK"]][endpoint_symbol][
                "master_address"
            ]
        except KeyError as exc:
            raise PayoutExecutionError(
                f"{endpoint_symbol} is not configured",
                code="PAYOUT_RAIL_NOT_CONFIGURED",
                status_code=400,
            ) from exc

    @classmethod
    def _validate_payload(cls, payload, *, authenticated_consumer, endpoint_symbol):
        try:
            canonical = canonical_payload(payload, endpoint_symbol=endpoint_symbol)
        except PayoutContractError as exc:
            raise PayoutExecutionError(
                str(exc),
                code=exc.code,
                status_code=exc.status_code,
            ) from exc

        if canonical["consumer"] != authenticated_consumer:
            raise PayoutExecutionError(
                "Request consumer does not match authenticated consumer",
                code="PAYOUT_CONSUMER_FORBIDDEN",
                status_code=403,
            )
        if (
            endpoint_symbol != "TON-USDT"
            or canonical["asset"] != "USDT"
            or canonical["network"] != "TON"
        ):
            raise PayoutExecutionError(
                "Payout request asset/network does not match TON-USDT endpoint",
                code="PAYOUT_RAIL_MISMATCH",
                status_code=400,
            )
        if not canonical["request_hash"]:
            raise PayoutExecutionError(
                "request_hash is required",
                code="MISSING_REQUEST_HASH",
                status_code=400,
            )
        expected_hash = sidecar_payload_hash(canonical)
        if payload.get("sidecar_payload_hash") != expected_hash:
            raise PayoutExecutionError(
                "sidecar_payload_hash does not match canonical TON payload",
                code="SIDECAR_PAYLOAD_HASH_MISMATCH",
                status_code=400,
            )
        return canonical

    @staticmethod
    def _row_to_status(row, *, status=None):
        canonical = json.loads(row.canonical_payload_json or "{}")
        return {
            "status": status or row.state,
            "execution_id": row.execution_id,
            "sidecar_execution_id": row.execution_id,
            "consumer": row.consumer,
            "external_id": row.external_id,
            "contract_version": canonical.get("contract_version"),
            "asset": canonical.get("asset"),
            "network": canonical.get("network"),
            "amount": canonical.get("amount"),
            "destination": canonical.get("destination"),
            "request_hash": row.request_hash,
            "sidecar_payload_hash": row.sidecar_payload_hash,
            "state": row.state,
            "state_version": row.state_version,
            "state_transition_id": row.state_transition_id,
            "state_updated_at": row.state_updated_at,
            "lease_owner": row.lease_owner,
            "lease_expires_at": row.lease_expires_at,
            "attempt_id": row.attempt_id,
            "source_wallet": row.source_wallet,
            "jetton_master": row.jetton_master,
            "jetton_wallet": row.jetton_wallet,
            "chain_id_or_network_id": row.chain_id_or_network_id,
            "masterchain_seqno": row.masterchain_seqno,
            "source_seqno": row.source_seqno,
            "valid_until": maybe_int(row.valid_until),
            "signed_boc_ref": row.signed_boc_ref,
            "signed_boc_hash": row.signed_boc_hash,
            "signed_boc_stored_at": row.signed_boc_stored_at,
            "message_hash": row.message_hash,
            "broadcast_provider": row.broadcast_provider,
            "broadcast_attempted_at": row.broadcast_attempted_at,
            "chain_check_metadata": json.loads(row.chain_check_metadata or "{}"),
            "payout_queue": row.payout_queue,
            "failure_class": row.failure_class,
            "error_code": row.error_code,
            "error_message": row.error_message,
            "reconciliation_required": bool(row.reconciliation_required),
            "message_hashes": json.loads(row.message_hashes_json or "[]"),
        }

    @staticmethod
    def _get_row(execution_id):
        return PayoutExecution.query.filter_by(execution_id=str(execution_id)).first()

    @classmethod
    def _transition(cls, row, state, **fields):
        updates = {
            "state": state,
            "state_version": row.state_version + 1,
            "state_transition_id": str(uuid.uuid4()),
            "state_updated_at": utc_now_iso(),
            **fields,
        }
        updated = PayoutExecution.query.filter_by(
            execution_id=row.execution_id,
            state_version=row.state_version,
        ).update(updates, synchronize_session=False)
        if updated != 1:
            db.session.rollback()
            raise PayoutExecutionError(
                "Payout execution state changed concurrently",
                code="PAYOUT_EXECUTION_CAS_CONFLICT",
                status_code=409,
            )
        db.session.commit()
        return cls._get_row(row.execution_id)

    @staticmethod
    def _message_hashes(row):
        try:
            hashes = json.loads(row.message_hashes_json or "[]")
        except (TypeError, ValueError):
            return []
        return hashes if isinstance(hashes, list) else []

    @classmethod
    def _has_unsafe_side_effect(cls, row):
        return any(
            [
                row.source_seqno is not None,
                row.signed_boc_ref,
                row.signed_boc_hash,
                row.message_hash,
                row.broadcast_attempted_at,
                cls._message_hashes(row),
            ]
        )

    @staticmethod
    def _lease_expired(row):
        expires_at = parse_iso(row.lease_expires_at)
        if expires_at is None:
            return True
        return expires_at <= utc_now_naive()

    @classmethod
    def _orphan_recovery_old_enough(cls, row):
        updated_at = parse_iso(row.state_updated_at)
        if updated_at is None:
            return False
        min_age = int(config["PAYOUT_EXECUTION_ORPHAN_RECOVERY_MIN_AGE_SEC"])
        return (utc_now_naive() - updated_at).total_seconds() >= min_age

    @classmethod
    def recover_stale_execution(cls, execution_id):
        row = cls._get_row(execution_id)
        if row is None:
            raise PayoutExecutionError(
                "Payout execution was not created",
                code="NO_EXECUTION_CREATED",
                status_code=404,
            )
        if row.state not in UNSAFE_RECOVERY_STATES:
            return cls._row_to_status(row)
        if not cls._lease_expired(row):
            return cls._row_to_status(row)
        if row.state in (STATE_SIGNED, STATE_BROADCASTING):
            row = cls._transition(
                row,
                STATE_RECONCILIATION_REQUIRED,
                failure_class="AMBIGUOUS",
                error_code=f"STALE_{row.state}_WITH_SIDE_EFFECT",
                error_message=(
                    f"Stale {row.state} cannot be automatically retried because "
                    "signing or broadcasting may already have happened"
                ),
                reconciliation_required=True,
            )
            return cls._row_to_status(row)
        if cls._has_unsafe_side_effect(row):
            row = cls._transition(
                row,
                STATE_RECONCILIATION_REQUIRED,
                failure_class="AMBIGUOUS",
                error_code="STALE_SIGNING_WITH_SIDE_EFFECT",
                error_message=(
                    "Stale SIGNING contains seqno, signed BOC, message hash, "
                    "or broadcast evidence and cannot be automatically retried"
                ),
                reconciliation_required=True,
            )
        else:
            row = cls._transition(
                row,
                STATE_RECEIVED,
                lease_owner=None,
                lease_expires_at=None,
                attempt_id=None,
                reconciliation_required=False,
            )
        return cls._row_to_status(row)

    @classmethod
    def recover_stale_signing(cls, execution_id):
        return cls.recover_stale_execution(execution_id)

    @classmethod
    def recover_task_owned_transient_failure(cls, execution_id, *, lease_owner):
        row = cls._get_row(execution_id)
        if row is None:
            return "raise"
        if row.state in NO_DOWNGRADE_STATES:
            return "raise"
        if row.state in (STATE_RECEIVED, STATE_VALIDATED):
            return "retry"
        if (
            row.state == STATE_SIGNING
            and row.lease_owner == lease_owner
            and not cls._has_unsafe_side_effect(row)
        ):
            cls._transition(
                row,
                STATE_RECEIVED,
                lease_owner=None,
                lease_expires_at=None,
                attempt_id=None,
                reconciliation_required=False,
            )
            return "retry"
        return "raise"

    @classmethod
    def recover_orphan_execution(cls, execution_id, *, authenticated_consumer, endpoint_symbol):
        if endpoint_symbol != "TON-USDT":
            raise PayoutExecutionError(
                "Payout request asset/network does not match TON-USDT endpoint",
                code="PAYOUT_RAIL_MISMATCH",
                status_code=400,
            )
        row = PayoutExecution.query.filter_by(
            execution_id=str(execution_id),
            consumer=authenticated_consumer,
        ).first()
        if row is None:
            raise PayoutExecutionError(
                "Payout execution was not created",
                code="NO_EXECUTION_CREATED",
                status_code=404,
            )
        recovered_stale_orphan = False
        if (
            row.state in UNSAFE_RECOVERY_STATES
            and cls._lease_expired(row)
            and cls._orphan_recovery_old_enough(row)
        ):
            try:
                cls.recover_stale_execution(row.execution_id)
            except PayoutExecutionError as exc:
                if exc.code != "PAYOUT_EXECUTION_CAS_CONFLICT":
                    raise
            row = cls._get_row(row.execution_id)
            recovered_stale_orphan = True
        recovery = {"attempted": True, "enqueued": False, "reason": None}
        if row.state not in (STATE_RECEIVED, STATE_VALIDATED):
            recovery["reason"] = "state_not_recoverable"
        elif cls._has_unsafe_side_effect(row):
            recovery["reason"] = "unsafe_evidence_exists"
        elif row.lease_owner and not cls._lease_expired(row):
            recovery["reason"] = "active_lease"
        elif not recovered_stale_orphan and not cls._orphan_recovery_old_enough(row):
            recovery["reason"] = "not_old_enough"
        else:
            try:
                cls.enqueue_execution(row.execution_id, row.payout_queue)
            except Exception as exc:
                raise PayoutExecutionError(
                    "Payout execution orphan recovery enqueue failed",
                    code="PAYOUT_EXECUTION_RECOVERY_ENQUEUE_FAILED",
                    status_code=503,
                ) from exc
            recovery["enqueued"] = True
            recovery["reason"] = "enqueued"
        status = cls._row_to_status(row)
        status["orphan_recovery"] = recovery
        return status

    @classmethod
    def _mark_seqno_reserved(cls, row, *, masterchain_seqno, source_seqno, valid_until):
        return cls._transition(
            row,
            STATE_SIGNING,
            masterchain_seqno=int(masterchain_seqno),
            source_seqno=int(source_seqno),
            valid_until=str(valid_until),
        )

    @classmethod
    def _mark_signed(cls, row, evidence):
        metadata = evidence.get("chain_check_metadata") or {}
        return cls._transition(
            row,
            STATE_SIGNED,
            signed_boc_ref=evidence["signed_boc_ref"],
            signed_boc_hash=evidence["signed_boc_hash"],
            signed_boc_stored_at=utc_now_iso(),
            message_hash=evidence["message_hash"],
            jetton_wallet=evidence.get("jetton_wallet"),
            valid_until=str(evidence.get("valid_until") or row.valid_until),
            chain_check_metadata=compact_json(metadata),
            message_hashes_json=compact_json([evidence["message_hash"]]),
        )

    @classmethod
    def _mark_broadcasting(cls, row, provider="toncenter"):
        return cls._transition(
            row,
            STATE_BROADCASTING,
            broadcast_provider=provider,
            broadcast_attempted_at=utc_now_iso(),
        )

    @staticmethod
    def _message_hash_candidates(value):
        if value is None:
            return set()
        text = str(value)
        candidates = {text}
        try:
            decoded = base64.b64decode(text, validate=True).hex()
            if decoded:
                candidates.add(decoded)
        except Exception:
            pass
        return candidates

    @classmethod
    def _verify_broadcast_result(cls, row, result):
        if not isinstance(result, dict):
            raise PayoutExecutionError(
                "TON broadcast result is missing message hash",
                code="BROADCAST_MESSAGE_HASH_MISSING",
                status_code=502,
            )
        result_hash = result.get("hash") or result.get("message_hash")
        if not result_hash:
            raise PayoutExecutionError(
                "TON broadcast result is missing message hash",
                code="BROADCAST_MESSAGE_HASH_MISSING",
                status_code=502,
            )
        expected = str(row.message_hash)
        candidates = {candidate.lower() for candidate in cls._message_hash_candidates(result_hash)}
        if expected.lower() not in candidates:
            raise PayoutExecutionError(
                "TON broadcast message hash does not match signed BOC",
                code="BROADCAST_MESSAGE_HASH_MISMATCH",
                status_code=502,
            )
        return result_hash

    @classmethod
    def _mark_broadcasted(cls, row, message_hash, result):
        metadata = json.loads(row.chain_check_metadata or "{}")
        metadata["broadcast_result"] = result
        return cls._transition(
            row,
            STATE_BROADCASTED,
            message_hash=message_hash,
            chain_check_metadata=compact_json(metadata),
            message_hashes_json=compact_json([message_hash]),
            reconciliation_required=False,
        )

    @classmethod
    def _refresh_chain_status(cls, row, *, coin=None):
        if row.state not in (STATE_BROADCASTED, STATE_CONFIRMING):
            return row
        from .payout_status import refresh_ton_usdt_confirmation

        try:
            result = refresh_ton_usdt_confirmation(row, coin=coin)
        except Exception as exc:
            result = {
                "state": STATE_CONFIRMING,
                "metadata": {
                    "confirmation_check": "TON_USDT_JETTON_TRANSFER",
                    "message_hash": row.message_hash,
                    "transfer_match": False,
                    "error": str(exc),
                },
            }
        metadata = json.loads(row.chain_check_metadata or "{}")
        metadata["confirmation"] = result["metadata"]
        metadata.update(result["metadata"])
        fields = {"chain_check_metadata": compact_json(metadata)}
        if result["state"] == STATE_CONFIRMED:
            fields.update(
                {
                    "message_hashes_json": compact_json([row.message_hash]),
                    "reconciliation_required": False,
                }
            )
        elif result["state"] == STATE_FAILED_CHAIN_TERMINAL:
            fields.update(
                {
                    "failure_class": result["failure_class"],
                    "error_code": result["error_code"],
                    "error_message": result["error_message"],
                    "reconciliation_required": False,
                }
            )
        return cls._transition(row, result["state"], **fields)

    @classmethod
    def _mark_failed_or_reconciliation(cls, execution_id, exc):
        row = cls._get_row(execution_id)
        if row is None:
            raise exc
        if row.state in NO_DOWNGRADE_STATES:
            return cls._row_to_status(row)
        if (
            isinstance(exc, PayoutExecutionError)
            and exc.code == "PAYOUT_EXECUTION_CAS_CONFLICT"
        ):
            return cls._row_to_status(row)
        if cls._has_unsafe_side_effect(row):
            row = cls._transition(
                row,
                STATE_RECONCILIATION_REQUIRED,
                failure_class="AMBIGUOUS",
                error_code=getattr(exc, "code", None) or "UNSAFE_EXECUTION_INTERRUPTED",
                error_message=str(exc),
                reconciliation_required=True,
            )
        else:
            row = cls._transition(
                row,
                STATE_FAILED_PRE_BROADCAST,
                failure_class="PREFLIGHT",
                error_code=getattr(exc, "code", None)
                or "EXECUTION_PRE_BROADCAST_FAILED",
                error_message=str(exc),
                reconciliation_required=False,
            )
        return cls._row_to_status(row)

    @classmethod
    def execute(cls, execution_id, *, coin, lock_factory=None, lease_owner=None):
        row = cls._get_row(execution_id)
        if row is None:
            raise PayoutExecutionError(
                "Payout execution was not created",
                code="NO_EXECUTION_CREATED",
                status_code=404,
            )
        try:
            if row.state in UNSAFE_RECOVERY_STATES:
                status = cls.recover_stale_execution(execution_id)
                row = cls._get_row(execution_id)
                if row.state in UNSAFE_RECOVERY_STATES:
                    return status
            if row.state == STATE_RECEIVED:
                row = cls._transition(row, STATE_VALIDATED)
            if row.state == STATE_VALIDATED:
                if config["PAYOUT_EXECUTION_PREFLIGHT_CHECKS_ENABLED"]:
                    from .payout_status import (
                        PayoutStatusError,
                        run_ton_usdt_preflight_checks,
                    )

                    try:
                        run_ton_usdt_preflight_checks(
                            json.loads(row.canonical_payload_json),
                            coin=coin,
                            worker_ready=lambda: True,
                        )
                    except PayoutStatusError as exc:
                        return cls._mark_failed_or_reconciliation(row.execution_id, exc)
                lease_expires_at = (
                    utc_now_naive()
                    + timedelta(seconds=int(config["PAYOUT_EXECUTION_LEASE_TTL_SEC"]))
                ).isoformat(timespec="microseconds") + "Z"
                row = cls._transition(
                    row,
                    STATE_SIGNING,
                    lease_owner=lease_owner or "payout-execution-worker",
                    lease_expires_at=lease_expires_at,
                    attempt_id=str(uuid.uuid4()),
                )
        except PayoutExecutionError as exc:
            if exc.code != "PAYOUT_EXECUTION_CAS_CONFLICT":
                raise
            row = cls._get_row(execution_id)
            return cls._row_to_status(row)
        except SQLAlchemyError as exc:
            if is_transient_db_error(exc):
                raise
            return cls._mark_failed_or_reconciliation(execution_id, exc)
        attempt_id = row.attempt_id
        if row.state not in (STATE_SIGNING, STATE_SIGNED, STATE_BROADCASTING):
            return cls._row_to_status(row)

        canonical = json.loads(row.canonical_payload_json)
        signed = None
        lock = lock_factory() if lock_factory else nullcontext()
        try:
            with lock:
                row = cls._get_row(execution_id)
                if row is None:
                    raise PayoutExecutionError(
                        "Payout execution was not created",
                        code="NO_EXECUTION_CREATED",
                        status_code=404,
                    )
                if (
                    row.state not in (STATE_SIGNING, STATE_SIGNED, STATE_BROADCASTING)
                    or row.attempt_id != attempt_id
                ):
                    return cls._row_to_status(row)
                if row.source_seqno is None:
                    masterchain_seqno = coin.toncenter.get_masterchain_head()
                    source_seqno = coin.toncenter.get_account_seqno(
                        coin.get_fee_deposit_account("raw")
                    )
                    valid_until = int(time.time()) + int(
                        config["TON_USDT_PAYOUT_VALID_UNTIL_CAP_SEC"]
                    )
                    row = cls._mark_seqno_reserved(
                        row,
                        masterchain_seqno=masterchain_seqno,
                        source_seqno=source_seqno,
                        valid_until=valid_until,
                    )
                if not row.signed_boc_hash:
                    signed = coin.build_signed_payout(
                        canonical,
                        row.source_seqno,
                        maybe_int(row.valid_until),
                    )
                    row = cls._mark_signed(row, coin.signed_payout_evidence(signed))
                else:
                    raise RuntimeError(
                        "Signed BOC evidence exists but signed artifact is not "
                        "available in worker memory"
                    )
                row = cls._mark_broadcasting(row)
                result = coin.broadcast_signed_payout(signed)
                cls._verify_broadcast_result(row, result)
                row = cls._mark_broadcasted(row, row.message_hash, result)
                return cls._row_to_status(row)
        except SQLAlchemyError as exc:
            if is_transient_db_error(exc):
                raise
            return cls._mark_failed_or_reconciliation(execution_id, exc)
        except Exception as exc:
            return cls._mark_failed_or_reconciliation(execution_id, exc)

    @staticmethod
    def enqueue_execution(execution_id, queue):
        from .tasks import execute_payout_execution

        return execute_payout_execution.apply_async(
            args=[str(execution_id)],
            headers={"payout_enqueued_at": utc_now_iso()},
            queue=queue,
        )

    @classmethod
    def _safe_recover_for_enqueue(cls, row):
        if (
            row.state in UNSAFE_RECOVERY_STATES
            and cls._lease_expired(row)
        ):
            try:
                cls.recover_stale_execution(row.execution_id)
            except PayoutExecutionError as exc:
                if exc.code != "PAYOUT_EXECUTION_CAS_CONFLICT":
                    raise
            return cls._get_row(row.execution_id)
        return row

    @classmethod
    def _enqueue_if_enabled(cls, row):
        if not config["PAYOUT_EXECUTION_AUTO_ENQUEUE_ENABLED"]:
            return row
        row = cls._safe_recover_for_enqueue(row)
        if row.state in (STATE_RECEIVED, STATE_VALIDATED):
            cls.enqueue_execution(row.execution_id, row.payout_queue)
        return row

    @classmethod
    def preflight(cls, payload, *, authenticated_consumer, endpoint_symbol):
        canonical = cls._validate_payload(
            payload,
            authenticated_consumer=authenticated_consumer,
            endpoint_symbol=endpoint_symbol,
        )
        if config["PAYOUT_EXECUTION_PREFLIGHT_CHECKS_ENABLED"]:
            from .payout_status import PayoutStatusError, run_ton_usdt_preflight_checks

            try:
                runtime = run_ton_usdt_preflight_checks(canonical)
            except PayoutStatusError as exc:
                raise PayoutExecutionError(
                    str(exc),
                    code=exc.code,
                    status_code=exc.status_code,
                ) from exc
        else:
            runtime = {}
        return {
            "status": "OK",
            "state": "PREFLIGHT_OK",
            "execution_id": canonical["execution_id"],
            "consumer": canonical["consumer"],
            "external_id": canonical["external_id"],
            "sidecar_payload_hash": payload["sidecar_payload_hash"],
            "payout_queue": canonical["payout_queue"],
            **runtime,
        }

    @classmethod
    def submit(cls, payload, *, authenticated_consumer, endpoint_symbol):
        canonical = cls._validate_payload(
            payload,
            authenticated_consumer=authenticated_consumer,
            endpoint_symbol=endpoint_symbol,
        )
        existing = PayoutExecution.query.filter_by(
            execution_id=canonical["execution_id"],
        ).first()
        if existing:
            if (
                existing.request_hash != canonical["request_hash"]
                or existing.sidecar_payload_hash != payload["sidecar_payload_hash"]
            ):
                raise PayoutExecutionError(
                    "Payout execution already exists with different payload",
                    code="PAYOUT_EXECUTION_CONFLICT",
                    status_code=409,
                )
            existing = cls._enqueue_if_enabled(existing)
            return cls._row_to_status(existing, status="ACCEPTED")

        consumer_existing = PayoutExecution.query.filter_by(
            consumer=canonical["consumer"],
            external_id=canonical["external_id"],
        ).first()
        if consumer_existing:
            if (
                consumer_existing.request_hash != canonical["request_hash"]
                or consumer_existing.sidecar_payload_hash != payload["sidecar_payload_hash"]
            ):
                raise PayoutExecutionError(
                    "Payout external_id already exists with different payload",
                    code="PAYOUT_EXECUTION_CONFLICT",
                    status_code=409,
                )
            consumer_existing = cls._enqueue_if_enabled(consumer_existing)
            return cls._row_to_status(consumer_existing, status="ACCEPTED")

        row = PayoutExecution(
            execution_id=canonical["execution_id"],
            consumer=canonical["consumer"],
            external_id=canonical["external_id"],
            request_hash=canonical["request_hash"],
            sidecar_payload_hash=payload["sidecar_payload_hash"],
            state=STATE_RECEIVED,
            state_version=1,
            state_transition_id=str(uuid.uuid4()),
            state_updated_at=utc_now_iso(),
            source_wallet=canonical["source_wallet"],
            jetton_master=cls._jetton_master(endpoint_symbol),
            jetton_wallet=None,
            chain_id_or_network_id=canonical["chain_id_or_network_id"],
            canonical_payload_json=compact_json(canonical),
            chain_check_metadata="{}",
            payout_queue=canonical["payout_queue"],
            reconciliation_required=False,
            message_hashes_json="[]",
        )
        db.session.add(row)
        try:
            db.session.commit()
        except IntegrityError as exc:
            db.session.rollback()
            raise PayoutExecutionError(
                "Payout execution already exists",
                code="PAYOUT_EXECUTION_CONFLICT",
                status_code=409,
            ) from exc
        row = cls._enqueue_if_enabled(row)
        return cls._row_to_status(row, status="ACCEPTED")

    @classmethod
    def status(cls, execution_id, *, authenticated_consumer, endpoint_symbol, coin=None):
        row = PayoutExecution.query.filter_by(
            execution_id=str(execution_id),
            consumer=authenticated_consumer,
        ).first()
        if not row:
            raise PayoutExecutionError(
                "Payout execution was not created",
                code="NO_EXECUTION_CREATED",
                status_code=404,
            )
        if row.state in UNSAFE_RECOVERY_STATES:
            try:
                cls.recover_stale_execution(execution_id)
            except PayoutExecutionError as exc:
                if exc.code != "PAYOUT_EXECUTION_CAS_CONFLICT":
                    raise
            row = cls._get_row(execution_id)
        try:
            row = cls._refresh_chain_status(row, coin=coin)
        except PayoutExecutionError as exc:
            if exc.code != "PAYOUT_EXECUTION_CAS_CONFLICT":
                raise
            row = cls._get_row(execution_id)
        return cls._row_to_status(row)
