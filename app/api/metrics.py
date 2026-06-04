import json
import re
from datetime import datetime, timezone

import prometheus_client
import redis
from prometheus_client import generate_latest, Gauge

from . import metrics_blueprint
from ..models import PayoutCallbackOutbox, PayoutExecution, Settings
from ..toncenterapi import Toncenterapi
from ..db_import import db


for collector in (
    prometheus_client.GC_COLLECTOR,
    prometheus_client.PLATFORM_COLLECTOR,
    prometheus_client.PROCESS_COLLECTOR,
):
    try:
        prometheus_client.REGISTRY.unregister(collector)
    except KeyError:
        pass


def get_all_metrics():
    toncenterapi = Toncenterapi()

    try:
        response = {}
        last_fullnode_block_number = toncenterapi.get_masterchain_head()
        response['last_fullnode_block_number'] = last_fullnode_block_number
        response['last_fullnode_block_timestamp'] = toncenterapi.get_block_timestamp(last_fullnode_block_number)
    
        pd = Settings.query.filter_by(name = 'last_block').first()
        last_checked_block_number = int(pd.value)
        response['ton_wallet_last_block'] = last_checked_block_number
        timestamp = toncenterapi.get_block_timestamp(last_checked_block_number)
        response['ton_wallet_last_block_timestamp'] = timestamp
        response['ton_fullnode_status'] = 1
        return response
    except:
        response['ton_fullnode_status'] = 0
        return response


ton_fullnode_status = Gauge('ton_fullnode_status', 'Connection status to ton fullnode')
ton_fullnode_last_block = Gauge('ton_fullnode_last_block', 'Last block loaded to the fullnode', )
ton_wallet_last_block = Gauge('ton_wallet_last_block', 'Last checked block ') 
ton_fullnode_last_block_timestamp = Gauge('ton_fullnode_last_block_timestamp', 'Last block timestamp loaded to the fullnode', )
ton_wallet_last_block_timestamp = Gauge('ton_wallet_last_block_timestamp', 'Last checked block timestamp')

ton_payout_execution_count = Gauge(
    "ton_payout_execution_count",
    "TON payout executions by sidecar state.",
    ("state", "reconciliation_required"),
)
ton_payout_non_terminal_oldest_age_seconds = Gauge(
    "ton_payout_non_terminal_oldest_age_seconds",
    "Age in seconds of the oldest non-terminal TON payout execution by state.",
    ("state",),
)
ton_payout_reconciliation_required_count = Gauge(
    "ton_payout_reconciliation_required_count",
    "TON payout executions currently requiring operator reconciliation.",
)
ton_payout_callback_outbox_backlog_count = Gauge(
    "ton_payout_callback_outbox_backlog_count",
    "Undelivered TON payout callback outbox events by status.",
    ("status",),
)
ton_payout_callback_outbox_oldest_age_seconds = Gauge(
    "ton_payout_callback_outbox_oldest_age_seconds",
    "Age in seconds of the oldest undelivered TON payout callback outbox event.",
    ("status",),
)
ton_payout_worker_ready = Gauge(
    "ton_payout_worker_ready",
    "Whether the dedicated TON-USDT payout worker is consuming its queue.",
    ("queue",),
)
ton_payout_broker_queue_depth = Gauge(
    "ton_payout_broker_queue_depth",
    "Redis broker list length for the dedicated TON-USDT payout queue. -1 means unavailable.",
    ("queue",),
)
ton_payout_broker_queue_oldest_age_seconds = Gauge(
    "ton_payout_broker_queue_oldest_age_seconds",
    "Age in seconds of the oldest queued TON-USDT broker item. 0 means empty, -1 means unavailable.",
    ("queue",),
)
ton_payout_hot_wallet_balance = Gauge(
    "ton_payout_hot_wallet_balance",
    "TON payout hot wallet Jetton balance. -1 means unavailable.",
    ("asset", "source_wallet"),
)
ton_payout_fee_wallet_balance = Gauge(
    "ton_payout_fee_wallet_balance",
    "TON payout fee wallet native TON balance. -1 means unavailable.",
    ("asset", "source_wallet"),
)
ton_payout_failure_count = Gauge(
    "ton_payout_failure_count",
    "TON payout executions with failure metadata by failure class and bounded error code.",
    ("state", "failure_class", "error_code"),
)

TERMINAL_PAYOUT_STATES = {
    "CONFIRMED",
    "FAILED_PRE_BROADCAST",
    "FAILED_CHAIN_TERMINAL",
}
PAYOUT_STATES = (
    "RECEIVED",
    "VALIDATED",
    "SIGNING",
    "SIGNED",
    "BROADCASTING",
    "BROADCASTED",
    "CONFIRMING",
    "CONFIRMED",
    "FAILED_PRE_BROADCAST",
    "FAILED_CHAIN_TERMINAL",
    "RECONCILIATION_REQUIRED",
)
RECONCILIATION_LABELS = ("false", "true")
UNDELIVERED_CALLBACK_STATUSES = ("PENDING", "RETRY", "DISPATCHING", "FAILED")
METRIC_ERROR_CODE_RE = re.compile(r"^[A-Z0-9_:-]{1,80}$")


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _age_seconds(now, value):
    parsed = _parse_datetime(value)
    if parsed is None:
        return 0
    return max(0, int((now - parsed).total_seconds()))


def _payout_enqueued_at_from_message(message):
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    payload = json.loads(message)
    headers = payload.get("headers") or {}
    return headers.get("payout_enqueued_at")


def _redis_queue_stats(redis_host, queue, now):
    try:
        client = redis.Redis.from_url(
            f"redis://{redis_host}",
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        depth = int(client.llen(queue))
        if depth <= 0:
            return depth, 0

        edge_messages = []
        edge_messages.extend(client.lrange(queue, 0, 0))
        edge_messages.extend(client.lrange(queue, -1, -1))
    except (redis.exceptions.RedisError, OSError, TypeError, ValueError):
        return -1, -1

    try:
        ages = []
        for message in edge_messages:
            enqueued_at = _payout_enqueued_at_from_message(message)
            if enqueued_at:
                ages.append(_age_seconds(now, enqueued_at))
    except (TypeError, ValueError, AttributeError, UnicodeError):
        return depth, -1
    if not ages:
        return depth, -1
    return depth, max(ages)


def _ton_fee_deposit_balances():
    from ..coin import Coin
    from ..models import Accounts

    account = Accounts.query.filter_by(type="fee_deposit").first()
    if account is None or not account.pub_address:
        raise RuntimeError("fee_deposit account is missing")
    coin = Coin("TON-USDT")
    return (
        coin.get_account_jetton_balance(account.pub_address),
        coin.get_ton_balance(account.pub_address),
    )


def _metric_number_or_unavailable(collector):
    try:
        return float(collector())
    except Exception:
        return -1


def _metric_error_code(error_code):
    if not error_code:
        return ""
    error_code = str(error_code).strip()
    if METRIC_ERROR_CODE_RE.match(error_code):
        return error_code
    return "OTHER"


def _update_wallet_balance_metrics():
    try:
        jetton_balance, ton_balance = _ton_fee_deposit_balances()
    except Exception:
        jetton_balance, ton_balance = -1, -1
    labels = {"source_wallet": "fee_deposit"}
    ton_payout_hot_wallet_balance.labels(asset="USDT", **labels).set(
        _metric_number_or_unavailable(lambda: jetton_balance)
    )
    ton_payout_fee_wallet_balance.labels(asset="TON", **labels).set(
        _metric_number_or_unavailable(lambda: ton_balance)
    )


def _clear_payout_metrics():
    ton_payout_failure_count.clear()
    for state in PAYOUT_STATES:
        for reconciliation_required in RECONCILIATION_LABELS:
            ton_payout_execution_count.labels(
                state=state,
                reconciliation_required=reconciliation_required,
            ).set(0)
        if state not in TERMINAL_PAYOUT_STATES:
            ton_payout_non_terminal_oldest_age_seconds.labels(state=state).set(0)
    ton_payout_reconciliation_required_count.set(0)
    for status in UNDELIVERED_CALLBACK_STATUSES:
        ton_payout_callback_outbox_backlog_count.labels(status=status).set(0)
        ton_payout_callback_outbox_oldest_age_seconds.labels(status=status).set(0)


def _update_worker_and_broker_metrics(now=None):
    from ..config import config
    from ..payout_status import ton_usdt_payout_worker_ready

    now = now or _utcnow()
    queue = config["TON_USDT_PAYOUT_QUEUE"]
    try:
        worker_ready = 1 if ton_usdt_payout_worker_ready() else 0
    except Exception:
        worker_ready = 0
    ton_payout_worker_ready.labels(queue=queue).set(worker_ready)
    depth, oldest_age = _redis_queue_stats(config["REDIS_HOST"], queue, now)
    ton_payout_broker_queue_depth.labels(queue=queue).set(depth)
    ton_payout_broker_queue_oldest_age_seconds.labels(queue=queue).set(oldest_age)
    _update_wallet_balance_metrics()


def update_payout_metrics(now=None):
    now = now or _utcnow()
    try:
        execution_rows = (
            db.session.query(
                PayoutExecution.state,
                PayoutExecution.reconciliation_required,
                db.func.count(PayoutExecution.execution_id),
                db.func.min(PayoutExecution.state_updated_at),
            )
            .group_by(PayoutExecution.state, PayoutExecution.reconciliation_required)
            .all()
        )
        reconciliation_count = (
            db.session.query(db.func.count(PayoutExecution.execution_id))
            .filter(PayoutExecution.reconciliation_required.is_(True))
            .scalar()
            or 0
        )
        callback_rows = (
            db.session.query(
                PayoutCallbackOutbox.status,
                db.func.count(PayoutCallbackOutbox.id),
                db.func.min(PayoutCallbackOutbox.created_at),
            )
            .filter(PayoutCallbackOutbox.status.in_(UNDELIVERED_CALLBACK_STATUSES))
            .group_by(PayoutCallbackOutbox.status)
            .all()
        )
        failure_rows = (
            db.session.query(
                PayoutExecution.state,
                PayoutExecution.failure_class,
                PayoutExecution.error_code,
                db.func.count(PayoutExecution.execution_id),
            )
            .filter(
                (PayoutExecution.failure_class.isnot(None))
                | (PayoutExecution.error_code.isnot(None))
            )
            .group_by(
                PayoutExecution.state,
                PayoutExecution.failure_class,
                PayoutExecution.error_code,
            )
            .all()
        )

        _clear_payout_metrics()

        for state, reconciliation_required, count, oldest_state_updated_at in execution_rows:
            reconciliation_label = "true" if reconciliation_required else "false"
            ton_payout_execution_count.labels(
                state=state,
                reconciliation_required=reconciliation_label,
            ).set(count)
            if state not in TERMINAL_PAYOUT_STATES:
                ton_payout_non_terminal_oldest_age_seconds.labels(state=state).set(
                    _age_seconds(now, oldest_state_updated_at)
                )

        ton_payout_reconciliation_required_count.set(reconciliation_count)

        for status, count, oldest_created_at in callback_rows:
            ton_payout_callback_outbox_backlog_count.labels(status=status).set(count)
            ton_payout_callback_outbox_oldest_age_seconds.labels(status=status).set(
                _age_seconds(now, oldest_created_at)
            )

        for state, failure_class, error_code, count in failure_rows:
            ton_payout_failure_count.labels(
                state=state or "",
                failure_class=failure_class or "",
                error_code=_metric_error_code(error_code),
            ).set(count)
    finally:
        _update_worker_and_broker_metrics(now=now)


@metrics_blueprint.get("/metrics")
def get_metrics():
    response = get_all_metrics()
    if response['ton_fullnode_status'] == 1:
        ton_fullnode_last_block.set(response['last_fullnode_block_number'])
        ton_fullnode_last_block_timestamp.set(response['last_fullnode_block_timestamp'])
        ton_wallet_last_block.set(response['ton_wallet_last_block'])
        ton_wallet_last_block_timestamp.set(response['ton_wallet_last_block_timestamp'])
        ton_fullnode_status.set(response['ton_fullnode_status'])
    else:
        ton_fullnode_status.set(response['ton_fullnode_status'])

    try:
        update_payout_metrics()
    except Exception:
        pass

    return generate_latest().decode()
