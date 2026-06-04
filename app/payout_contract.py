from __future__ import annotations

import hashlib
import json
import uuid
from decimal import Decimal, InvalidOperation

from .config import config


TON_USDT_DECIMALS = Decimal("0.000001")
ALLOWED_PAYLOAD_FIELDS = frozenset(
    (
        "consumer",
        "execution_id",
        "external_id",
        "asset",
        "network",
        "amount",
        "destination",
        "contract_version",
        "request_hash",
        "sidecar_payload_hash",
        "source_wallet_ref",
        "payout_queue",
    )
)


class PayoutContractError(Exception):
    def __init__(self, message, *, code, status_code=400):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def compact_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def hash_payload(value):
    return hashlib.sha256(compact_json(value).encode("utf-8")).hexdigest()


def _canonical_amount(value):
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:
        raise PayoutContractError(
            "Invalid payout amount",
            code="INVALID_AMOUNT",
        ) from exc
    if not amount.is_finite():
        raise PayoutContractError(
            "Payout amount must be finite", code="INVALID_AMOUNT"
        )
    if amount <= 0:
        raise PayoutContractError(
            "Payout amount must be positive",
            code="INVALID_AMOUNT",
        )
    quantized = amount.quantize(TON_USDT_DECIMALS)
    if quantized != amount:
        raise PayoutContractError(
            "TON-USDT payout amount precision must not exceed 6 decimals",
            code="INVALID_AMOUNT_PRECISION",
        )
    return format(quantized, "f")


def deterministic_execution_id(consumer, external_id):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ton-usdt:{consumer}:{external_id}"))


def canonical_payload(payload, *, endpoint_symbol="TON-USDT", execution_id=None):
    unknown = sorted(set(payload) - ALLOWED_PAYLOAD_FIELDS)
    if unknown:
        raise PayoutContractError(
            "Payout execution request contains unsupported fields: "
            f"{', '.join(unknown)}. TON sidecar accepts only execution "
            "contract fields.",
            code="PAYOUT_EXECUTION_BAD_REQUEST",
        )
    consumer = str(payload.get("consumer") or "")
    external_id = str(payload.get("external_id") or "")
    if not consumer:
        raise PayoutContractError("consumer is required", code="MISSING_CONSUMER")
    if not external_id:
        raise PayoutContractError("external_id is required", code="MISSING_EXTERNAL_ID")

    resolved_execution_id = (
        str(execution_id or payload.get("execution_id") or "")
        or deterministic_execution_id(consumer, external_id)
    )
    asset = str(payload.get("asset") or "USDT").upper()
    network = str(payload.get("network") or "TON").upper()
    contract_version = str(
        payload.get("contract_version") or "usdt-payout-execution-v1"
    )
    destination = str(payload.get("destination") or "")
    if not destination:
        raise PayoutContractError("destination is required", code="MISSING_DESTINATION")

    return {
        "amount": _canonical_amount(payload.get("amount")),
        "asset": asset,
        "chain_id_or_network_id": network,
        "contract_version": contract_version,
        "consumer": consumer,
        "destination": destination,
        "execution_id": resolved_execution_id,
        "external_id": external_id,
        "network": network,
        "payout_queue": str(payload.get("payout_queue") or config["TON_USDT_PAYOUT_QUEUE"]),
        "request_hash": str(payload.get("request_hash") or ""),
        "source_wallet": str(payload.get("source_wallet_ref") or "fee_deposit"),
    }


def canonical_sidecar_hash_payload(canonical):
    return {
        "consumer": canonical["consumer"],
        "execution_id": canonical["execution_id"],
        "external_id": canonical["external_id"],
        "asset": canonical["asset"],
        "network": canonical["network"],
        "amount": canonical["amount"],
        "destination": canonical["destination"],
        "contract_version": canonical["contract_version"],
    }


def sidecar_payload_hash(canonical):
    return hash_payload(canonical_sidecar_hash_payload(canonical))
