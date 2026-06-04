from contextlib import contextmanager
from contextvars import ContextVar

from celery.utils.log import get_task_logger
import redis
import redis.exceptions

from .config import config


logger = get_task_logger(__name__)
_fee_deposit_seqno_lock_depth = ContextVar(
    "fee_deposit_seqno_lock_depth",
    default=0,
)


class FeeDepositSeqnoLockError(Exception):
    code = "PAYOUT_SEQNO_LOCK_UNAVAILABLE"
    status_code = 503


@contextmanager
def fee_deposit_seqno_lock(reason=None):
    depth = _fee_deposit_seqno_lock_depth.get()
    token = _fee_deposit_seqno_lock_depth.set(depth + 1)
    if depth > 0:
        try:
            yield
        finally:
            _fee_deposit_seqno_lock_depth.reset(token)
        return

    client = redis.Redis.from_url(f"redis://{config['REDIS_HOST']}")
    lock = client.lock(
        "ton_usdt_fee_deposit_seqno",
        timeout=config["TON_USDT_PAYOUT_SEQNO_LOCK_TTL_SEC"],
        blocking_timeout=config["TON_USDT_PAYOUT_SEQNO_LOCK_WAIT_SEC"],
        thread_local=False,
    )
    try:
        acquired = lock.acquire(blocking=True)
    except redis.exceptions.RedisError as exc:
        _fee_deposit_seqno_lock_depth.reset(token)
        raise FeeDepositSeqnoLockError(
            "Unable to acquire TON fee-deposit seqno lock"
        ) from exc
    if not acquired:
        _fee_deposit_seqno_lock_depth.reset(token)
        raise FeeDepositSeqnoLockError(
            "Timed out waiting for TON fee-deposit seqno lock"
        )
    try:
        yield
    finally:
        try:
            lock.release()
        except redis.exceptions.RedisError:
            logger.warning(
                "TON fee-deposit seqno lock release failed: reason=%s",
                reason,
            )
        _fee_deposit_seqno_lock_depth.reset(token)


@contextmanager
def fee_deposit_seqno_guard_for_address(address, fee_deposit_address, reason=None):
    if address and fee_deposit_address and address == fee_deposit_address:
        with fee_deposit_seqno_lock(reason=reason):
            yield
    else:
        yield
