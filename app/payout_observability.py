import re

from prometheus_client import Counter


METRIC_ERROR_CODE_RE = re.compile(r"^[A-Z0-9_:-]{1,80}$")
PAYOUT_API_OPERATIONS = {"preflight", "submit", "status"}

ton_payout_request_failed = Counter(
    "ton_payout_request_failed",
    "TON payout API requests rejected by operation and bounded error code.",
    ("operation", "code"),
)


def _metric_operation(operation):
    operation = str(operation or "").strip().lower()
    return operation if operation in PAYOUT_API_OPERATIONS else "other"


def _metric_error_code(code):
    if not code:
        return ""
    code = str(code).strip()
    if METRIC_ERROR_CODE_RE.match(code):
        return code
    return "OTHER"


def payout_operation_from_request(method, path):
    method = str(method or "").upper()
    path = str(path or "").rstrip("/")
    if method == "GET":
        return "status"
    if path.endswith("/preflight"):
        return "preflight"
    if method == "POST":
        return "submit"
    return "other"


def record_payout_request_failed(operation, code):
    ton_payout_request_failed.labels(
        operation=_metric_operation(operation),
        code=_metric_error_code(code),
    ).inc()


def clear_payout_request_metrics():
    ton_payout_request_failed.clear()
