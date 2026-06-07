from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation

from tonsdk.utils import Address

from . import celery
from .coin import Coin, is_valid_ton_address
from .config import config


class PayoutStatusError(Exception):
    def __init__(self, message, *, code, status_code=400):
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def ton_usdt_payout_worker_ready():
    try:
        responses = celery.control.inspect(
            timeout=config["TON_USDT_PAYOUT_QUEUE_READINESS_TIMEOUT_SEC"]
        ).active_queues()
    except Exception:
        return False
    if not responses:
        return False
    for queues in responses.values():
        for queue in queues or []:
            if queue.get("name") == config["TON_USDT_PAYOUT_QUEUE"]:
                return True
    return False


def run_ton_usdt_preflight_checks(canonical, *, coin=None, worker_ready=None):
    coin = coin or Coin("TON-USDT")
    worker_ready = worker_ready or ton_usdt_payout_worker_ready
    destination = canonical["destination"]
    amount = Decimal(canonical["amount"])
    if not is_valid_ton_address(destination):
        raise PayoutStatusError(
            "TON-USDT payout destination address is invalid",
            code="INVALID_DESTINATION",
            status_code=400,
        )

    try:
        jetton_balance = Decimal(str(coin.get_fee_deposit_jetton_balance()))
        source = coin.get_fee_deposit_account("public")
        fee = Decimal(str(coin.get_jetton_transaction_fee(source, destination, amount)))
        ton_balance = Decimal(str(coin.get_fee_deposit_coin_balance()))
    except Exception as exc:
        raise PayoutStatusError(
            f"Unable to run TON-USDT payout preflight checks: {exc}",
            code="PAYOUT_PREFLIGHT_UNAVAILABLE",
            status_code=503,
        ) from exc

    if jetton_balance < amount:
        raise PayoutStatusError(
            f"Insufficient fee-deposit Jetton balance: {jetton_balance} < {amount}",
            code="INSUFFICIENT_JETTON_BALANCE",
            status_code=409,
        )

    if ton_balance < fee:
        raise PayoutStatusError(
            f"Insufficient fee-deposit TON balance for Jetton fee: {ton_balance} < {fee}",
            code="INSUFFICIENT_TON_FEE_BALANCE",
            status_code=409,
        )

    if not worker_ready():
        raise PayoutStatusError(
            "TON-USDT payout worker is not ready",
            code="PAYOUT_WORKER_UNAVAILABLE",
            status_code=503,
        )

    return {
        "source_wallet": canonical["source_wallet"],
        "fee_deposit_ton_balance": str(ton_balance),
        "fee_deposit_jetton_balance": str(jetton_balance),
        "estimated_ton_fee": str(fee),
        "payout_queue": config["TON_USDT_PAYOUT_QUEUE"],
        "worker_ready": True,
    }


def _transfer_field(transfer, *names):
    for name in names:
        if name in transfer:
            return transfer[name]
    return None


def _raw_jetton_amount_to_decimal(raw_amount, decimals):
    try:
        return Decimal(str(raw_amount)) / Decimal(10) ** int(decimals)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PayoutStatusError(
            "Indexed Jetton transfer amount is invalid",
            code="INVALID_INDEXED_TRANSFER_AMOUNT",
            status_code=502,
        ) from exc


def _normalize_ton_address(address):
    if not address:
        return None
    try:
        parsed = Address(str(address))
        return f"{parsed.wc}:{parsed.hash_part.hex().upper()}"
    except Exception:
        return str(address)


def _expected_transfer_sources(row, coin):
    sources = {_normalize_ton_address(row.jetton_wallet)}
    try:
        sources.add(_normalize_ton_address(coin.get_fee_deposit_account("raw")))
    except Exception:
        pass
    try:
        sources.add(_normalize_ton_address(coin.get_fee_deposit_account("public")))
    except Exception:
        pass
    return {source for source in sources if source}


def _matches_transfer(row, transfer, coin):
    canonical = json.loads(row.canonical_payload_json)
    decimals = coin.toncenter.jetton_master_decimals(row.jetton_master)
    amount = _raw_jetton_amount_to_decimal(
        _transfer_field(transfer, "amount", "jetton_amount"),
        decimals,
    )
    expected_amount = Decimal(canonical["amount"])
    source = _transfer_field(transfer, "source", "sender", "from")
    destination = _transfer_field(transfer, "destination", "recipient", "to")
    jetton_master = _transfer_field(transfer, "jetton_master", "jetton")
    normalized_source = _normalize_ton_address(source)
    normalized_destination = _normalize_ton_address(destination)
    normalized_jetton_master = _normalize_ton_address(jetton_master)
    expected_sources = _expected_transfer_sources(row, coin)
    expected_destination = _normalize_ton_address(canonical["destination"])
    expected_jetton_master = _normalize_ton_address(row.jetton_master)
    return {
        "amount": str(amount),
        "expected_amount": str(expected_amount),
        "source": source,
        "normalized_source": normalized_source,
        "expected_source": row.jetton_wallet,
        "expected_sources": sorted(expected_sources),
        "destination": destination,
        "expected_destination": canonical["destination"],
        "normalized_destination": normalized_destination,
        "normalized_expected_destination": expected_destination,
        "jetton_master": jetton_master,
        "expected_jetton_master": row.jetton_master,
        "normalized_jetton_master": normalized_jetton_master,
        "normalized_expected_jetton_master": expected_jetton_master,
        "transfer_match": (
            amount == expected_amount
            and normalized_source in expected_sources
            and normalized_destination == expected_destination
            and normalized_jetton_master == expected_jetton_master
            and row.chain_id_or_network_id == "TON"
        ),
    }


def _confirmation_progress(row, coin, generic_tx):
    block_seqno = (
        generic_tx.get("mc_block_seqno")
        or generic_tx.get("masterchain_seqno")
        or row.masterchain_seqno
    )
    progress = {
        "confirmations": 0,
        "min_confirmations": config["TON_USDT_PAYOUT_MIN_CONFIRMATIONS"],
        "message_masterchain_seqno": block_seqno,
        "latest_masterchain_seqno": None,
    }
    if block_seqno is None:
        progress["confirmation_error"] = "message masterchain seqno is unavailable"
        return progress
    try:
        latest_seqno = coin.toncenter.get_masterchain_head()
    except Exception as exc:
        progress["confirmation_error"] = str(exc)
        return progress
    progress["latest_masterchain_seqno"] = latest_seqno
    progress["confirmations"] = max(int(latest_seqno) - int(block_seqno) + 1, 0)
    return progress


def _has_min_confirmations(progress):
    return progress["confirmations"] >= progress["min_confirmations"]


def refresh_ton_usdt_confirmation(row, *, coin=None):
    coin = coin or Coin("TON-USDT")
    metadata = {
        "confirmation_check": "TON_USDT_JETTON_TRANSFER",
        "message_hash": row.message_hash,
        "transfer_match": False,
        "confirmations": 0,
        "min_confirmations": config["TON_USDT_PAYOUT_MIN_CONFIRMATIONS"],
    }
    if not row.message_hash:
        metadata["error"] = "missing message_hash"
        return {"state": "CONFIRMING", "metadata": metadata}

    try:
        generic_tx = coin.toncenter.get_transaction_by_hash(row.message_hash)
        metadata["message_seen"] = True
        metadata["message_transaction_hash"] = generic_tx.get("hash")
        metadata.update(_confirmation_progress(row, coin, generic_tx))
    except Exception as exc:
        metadata["message_seen"] = False
        metadata["message_error"] = str(exc)

    try:
        transfer = coin.toncenter.get_jetton_transaction_by_hash(
            row.message_hash,
            row.jetton_master,
        )
    except Exception as exc:
        metadata["transfer_error"] = str(exc)
        return {"state": "CONFIRMING", "metadata": metadata}

    metadata.update(_matches_transfer(row, transfer, coin))
    if metadata["transfer_match"]:
        if not _has_min_confirmations(metadata):
            return {"state": "CONFIRMING", "metadata": metadata}
        return {"state": "CONFIRMED", "metadata": metadata}
    if _has_min_confirmations(metadata):
        return {
            "state": "FAILED_CHAIN_TERMINAL",
            "metadata": metadata,
            "failure_class": "CHAIN_TERMINAL",
            "error_code": "TON_USDT_TRANSFER_MISMATCH",
            "error_message": (
                "Confirmed TON message has indexed Jetton transfer evidence, "
                "but it does not match the expected payout"
            ),
        }
    return {"state": "CONFIRMING", "metadata": metadata}
