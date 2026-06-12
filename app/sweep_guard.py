import requests as rq

from .config import config
from .logging import logger


GUARDED_SWEEP_SYMBOLS = set()


def _symbol(symbol):
    return str(symbol or "").strip().upper()


def is_sweep_gate_active(symbol):
    return _symbol(symbol) in GUARDED_SWEEP_SYMBOLS


def is_sweep_allowed(symbol, address, txid=None):
    crypto = _symbol(symbol)
    if crypto not in GUARDED_SWEEP_SYMBOLS:
        return True

    payload = {
        "crypto": crypto,
        "network": "TON",
        "address": address,
    }
    if txid:
        payload["txid"] = txid

    try:
        response = rq.post(
            f'http://{config["SHKEEPER_HOST"]}/api/v1/sweep-eligibility',
            headers={"X-Shkeeper-Backend-Key": config["SHKEEPER_KEY"]},
            json=payload,
            timeout=config["AML_SWEEP_GATE_TIMEOUT_SEC"],
        )
        body = response.json()
    except Exception as exc:
        logger.warning(
            f"TON sweep guard failed closed for {crypto}/{address}: {exc}"
        )
        return False

    allowed = isinstance(body, dict) and body.get("decision") == "allow"
    if not allowed:
        logger.warning(
            f"TON sweep guard blocked {crypto}/{address}: response={body}"
        )
    return allowed
