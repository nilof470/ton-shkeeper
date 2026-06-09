
import decimal
import time
import copy
from contextlib import contextmanager
import uuid

from celery.utils.log import get_task_logger
from sqlalchemy.exc import SQLAlchemyError

from . import celery
from .config import config, get_min_token_transfer_threshold
from .models import Accounts, db
from .coin import Coin, get_all_accounts
from .fee_deposit_seqno_guard import fee_deposit_seqno_lock
from .payout_callback_outbox import (
    claim_due_payout_callbacks,
    create_payout_callback,
    dispatch_payout_callback,
    should_retry,
)
from .utils import skip_if_running

logger = get_task_logger(__name__)


@contextmanager
def ton_usdt_payout_seqno_lock():
    with fee_deposit_seqno_lock(reason="ton-usdt-payout"):
        yield


def _db_retry_countdown(retries):
    return min(5 * (2 ** max(retries, 0)), 60)


def _cleanup_db_session():
    try:
        db.session.rollback()
    except Exception:
        logger.warning("TON payout task db rollback failed", exc_info=True)
    finally:
        db.session.remove()


def run_execute_payout_execution(task, execution_id):
    from .payout_execution import (
        PayoutExecutionError,
        PayoutExecutionStore,
        is_transient_db_error,
    )

    try:
        db.session.rollback()
    except Exception:
        logger.warning("TON payout task initial db rollback failed", exc_info=True)

    lease_owner = task.request.id
    try:
        PayoutExecutionStore.recover_task_owned_transient_failure(
            execution_id,
            lease_owner=lease_owner,
        )
    except PayoutExecutionError as exc:
        if exc.code != "PAYOUT_EXECUTION_CAS_CONFLICT":
            raise
    except SQLAlchemyError as exc:
        if not is_transient_db_error(exc):
            raise
        _cleanup_db_session()
        raise task.retry(
            exc=exc,
            countdown=_db_retry_countdown(getattr(task.request, "retries", 0)),
        )

    try:
        coin = Coin("TON-USDT")
        return PayoutExecutionStore.execute(
            execution_id,
            coin=coin,
            lock_factory=ton_usdt_payout_seqno_lock,
            lease_owner=lease_owner,
        )
    except SQLAlchemyError as exc:
        if not is_transient_db_error(exc):
            raise
        _cleanup_db_session()
        try:
            action = PayoutExecutionStore.recover_task_owned_transient_failure(
                execution_id,
                lease_owner=lease_owner,
            )
        except SQLAlchemyError as recovery_exc:
            if not is_transient_db_error(recovery_exc):
                raise
            _cleanup_db_session()
            action = "retry"
        if action == "retry":
            raise task.retry(
                exc=exc,
                countdown=_db_retry_countdown(getattr(task.request, "retries", 0)),
            )
        raise
    finally:
        db.session.remove()


@celery.task()
def make_multipayout(symbol, payout_list, fee):
    if symbol == config["COIN_SYMBOL"]:
        coint_inst = Coin(symbol)
        payout_results = coint_inst.make_multipayout_ton(payout_list, fee)
        queue_payout_callback(payout_results, symbol)
        return payout_results
    elif symbol in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys():
        token_inst = Coin(symbol)
        payout_results = token_inst.make_multipayout_jetton(payout_list, fee)
        queue_payout_callback(payout_results, symbol)
        return payout_results
    else:
        return [{"status": "error", 'msg': "Symbol is not in config"}]


@celery.task(bind=True, max_retries=5)
def execute_payout_execution(self, execution_id):
    return run_execute_payout_execution(self, execution_id)


def queue_payout_callback(data, symbol):
    try:
        outbox_id = create_payout_callback(data, symbol)
    except Exception as exc:
        logger.exception(
            "Shkeeper payout notification outbox write failed after payout "
            f"completed: symbol={symbol} error={exc}"
        )
        return None
    try:
        post_payout_results.delay(outbox_id)
    except Exception as exc:
        logger.warning(
            "Shkeeper payout notification task enqueue failed; outbox row "
            f"remains pending: outbox_id={outbox_id} error={exc}"
        )
    return outbox_id


@celery.task(bind=True)
def post_payout_results(self, outbox_id):
    result = dispatch_payout_callback(outbox_id, claim_token=self.request.id)
    if should_retry(result):
        logger.warning(
            "Shkeeper payout notification failed; outbox retry remains pending: "
            f"outbox_id={outbox_id} attempts={result['attempts']} "
            f"error={result['last_error']} next_attempt_at={result['next_attempt_at']}"
        )
    elif result and result.get("status") == "FAILED":
        logger.warning(
            "Shkeeper payout notification permanently failed: "
            f"outbox_id={outbox_id} attempts={result.get('attempts')} "
            f"error={result.get('last_error')}"
        )
    return result


@celery.task(bind=True)
def dispatch_due_payout_callbacks(self, limit=None):
    claim_token = self.request.id or f"payout-callback-sweep-{uuid.uuid4()}"
    rows = claim_due_payout_callbacks(
        limit or config["PAYOUT_CALLBACK_SWEEP_LIMIT"],
        claim_token=claim_token,
    )
    results = []
    for row in rows:
        results.append(dispatch_payout_callback(row["id"], claim_token=claim_token))
    return results


@celery.task()
def refresh_balances():
    updated = 0

    try:
        from app import create_app
        app = create_app()
        app.app_context().push()

        list_acccounts = get_all_accounts()
        for account in list_acccounts:
            try:
                pd = Accounts.query.filter_by(pub_address = account).first()
            except:
                db.session.rollback()
                raise Exception("There was exception during query to the database, try again later")
            coin_inst = Coin()
            acc_balance = coin_inst.get_ton_balance(account)
            if Accounts.query.filter_by(pub_address = account, crypto = config["COIN_SYMBOL"]).first():
                pd = Accounts.query.filter_by(pub_address = account, crypto = config["COIN_SYMBOL"]).first()
                pd.amount = acc_balance
                with app.app_context():
                    db.session.add(pd)
                    db.session.commit()
                    db.session.close()

            have_tokens = False

            for token in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys():
                token_inst = Coin(token)
                if Accounts.query.filter_by(pub_address = account, crypto = token).first():
                    pd = Accounts.query.filter_by(pub_address = account, crypto = token).first()
                    balance = decimal.Decimal(token_inst.get_account_jetton_balance(account))
                    pd.amount = balance

                    with app.app_context():
                        db.session.add(pd)
                        db.session.commit()
                        db.session.close()
                    if balance >= decimal.Decimal(get_min_token_transfer_threshold(token)):
                        have_tokens = copy.deepcopy(token)

            if have_tokens in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys():
                drain_account.delay(have_tokens, account)
            else:
                if acc_balance >= decimal.Decimal(config['MIN_TRANSFER_THRESHOLD']):
                    drain_account.delay(config["COIN_SYMBOL"], account)

            updated = updated + 1

            with app.app_context():
                db.session.add(pd)
                db.session.commit()
                db.session.close()

            if config['DELAY_BETWEEN_ACC_BALANCE_REFRESH'] > 0: # if set, delay between accounts balance refresh to avoid too many requests to fullnode in short time
                time.sleep(config['DELAY_BETWEEN_ACC_BALANCE_REFRESH'])
    finally:

        with app.app_context():
            db.session.remove()
            db.engine.dispose()

    return updated


@celery.task(bind=True)
@skip_if_running
def drain_account(self, symbol, account):
    logger.warning(f"Start draining from account {account} crypto {symbol}")
    # return False
    if symbol == config["COIN_SYMBOL"]:
        inst = Coin(symbol)
        destination = inst.get_fee_deposit_account('public')
        results = inst.drain_account(account, destination)
    elif symbol in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys():
        inst = Coin(symbol)
        destination = inst.get_fee_deposit_account('public')
        results = inst.drain_account(account, destination)
    else:
        raise Exception("Symbol is not in config")

    return results


@celery.task(bind=True)
@skip_if_running
def create_fee_deposit_account(self):
    logger.warning("Creating fee-deposit account")
    inst = Coin(config["COIN_SYMBOL"])
    inst.set_fee_deposit_account()
    return True


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    if config["PAYOUT_CALLBACK_SWEEP_ENABLED"]:
        sender.add_periodic_task(
            int(config["PAYOUT_CALLBACK_SWEEP_PERIOD_SEC"]),
            dispatch_due_payout_callbacks.s(),
        )
    sender.add_periodic_task(int(config['UPDATE_TOKEN_BALANCES_EVERY_SECONDS']), refresh_balances.s())
