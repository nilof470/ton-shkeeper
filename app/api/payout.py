from decimal import Decimal

from flask import g, request

from .. import celery
from ..tasks import make_multipayout 
from . import api

from ..coin import Coin
from ..config import config
from ..payout_auth import payout_auth_required
from ..payout_execution import PayoutExecutionError, PayoutExecutionStore
from ..payout_observability import record_payout_request_failed
from ..payout_status import ton_usdt_payout_worker_ready


PAYOUT_WORKER_UNAVAILABLE_CODE = "PAYOUT_WORKER_UNAVAILABLE"
PAYOUT_WORKER_UNAVAILABLE_MESSAGE = (
    "TON-USDT payout worker is not ready. Ensure ton-usdt-payouts consumes "
    "ton_usdt_payouts before retrying."
)


def _payout_body(execution_id=None):
    body = request.get_json(force=True) or {}
    if execution_id is not None:
        path_execution_id = str(execution_id)
        body_execution_id = str(body.get("execution_id") or "")
        if body_execution_id and body_execution_id != path_execution_id:
            raise PayoutExecutionError(
                "Path execution_id does not match request body",
                code="PAYOUT_EXECUTION_ID_MISMATCH",
                status_code=400,
            )
        body["execution_id"] = path_execution_id
    return body


def _payout_error_response(exc, operation):
    record_payout_request_failed(operation, exc.code)
    return {
        "status": "error",
        "code": exc.code,
        "message": str(exc),
    }, exc.status_code


def _payout_worker_unavailable_response():
    return {
        "status": "error",
        "code": PAYOUT_WORKER_UNAVAILABLE_CODE,
        "message": PAYOUT_WORKER_UNAVAILABLE_MESSAGE,
        "error": PAYOUT_WORKER_UNAVAILABLE_MESSAGE,
    }


def _legacy_payout_signature(payout_list):
    signature = make_multipayout.s(
        g.symbol,
        payout_list,
        Decimal(config['TON_TRANSACTION_FEE']),
    )
    if g.symbol == "TON-USDT":
        if not ton_usdt_payout_worker_ready():
            return None, (_payout_worker_unavailable_response(), 503)
        signature = signature.set(queue=config["TON_USDT_PAYOUT_QUEUE"])
    return signature, None


def _preflight_response(execution_id=None):
    try:
        return PayoutExecutionStore.preflight(
            _payout_body(execution_id=execution_id),
            authenticated_consumer=g.payout_consumer,
            endpoint_symbol=g.symbol,
        )
    except PayoutExecutionError as exc:
        return _payout_error_response(exc, "preflight")


def _submit_response(execution_id=None):
    try:
        return PayoutExecutionStore.submit(
            _payout_body(execution_id=execution_id),
            authenticated_consumer=g.payout_consumer,
            endpoint_symbol=g.symbol,
        ), 202
    except PayoutExecutionError as exc:
        return _payout_error_response(exc, "submit")


def _status_response(execution_id):
    try:
        return PayoutExecutionStore.status(
            execution_id,
            authenticated_consumer=g.payout_consumer,
            endpoint_symbol=g.symbol,
        )
    except PayoutExecutionError as exc:
        return _payout_error_response(exc, "status")


def _recover_orphan_response(execution_id):
    try:
        return PayoutExecutionStore.recover_orphan_execution(
            execution_id,
            authenticated_consumer=g.payout_consumer,
            endpoint_symbol=g.symbol,
        ), 202
    except PayoutExecutionError as exc:
        return _payout_error_response(exc, "recover_orphan")


@api.post("/payout/preflight")
@payout_auth_required
def payout_execution_preflight():
    return _preflight_response()


@api.post("/payout/submit")
@payout_auth_required
def payout_execution_submit():
    return _submit_response()


@api.get("/payout/status/<execution_id>")
@payout_auth_required
def payout_execution_status(execution_id):
    return _status_response(execution_id)


@api.post("/payout-executions/<execution_id>/preflight")
@payout_auth_required
def payout_execution_v1_preflight(execution_id):
    return _preflight_response(execution_id=execution_id)


@api.post("/payout-executions/<execution_id>")
@payout_auth_required
def payout_execution_v1_submit(execution_id):
    return _submit_response(execution_id=execution_id)


@api.get("/payout-executions/<execution_id>")
@payout_auth_required
def payout_execution_v1_status(execution_id):
    return _status_response(execution_id)


@api.post("/payout-executions/<execution_id>/recover-orphan")
@payout_auth_required
def payout_execution_v1_recover_orphan(execution_id):
    return _recover_orphan_response(execution_id)


@api.post('/calc-tx-fee/<decimal:amount>')
def calc_tx_fee(amount):
    if g.symbol == config["COIN_SYMBOL"]:
        coin_inst = Coin(config["COIN_SYMBOL"])
        fee = coin_inst.get_transaction_price()
        return {'accounts_num': 1,
                'fee': float(fee)}

    elif g.symbol in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys():
        token_instance = Coin(g.symbol)
        need_crypto = token_instance.get_jetton_transaction_fee()
        return {
            'accounts_num': 1,
            'fee': float(need_crypto),
        }
    else:
        return {'status': 'error', 'msg': 'unknown crypto' }

@api.post('/multipayout')
def multipayout():
    
    try:
        payout_list = request.get_json(force=True)
    except Exception as e:
        raise Exception(f"Bad JSON in payout list: {e}")

    if not payout_list:
            raise Exception("Payout list is empty!")

    for transfer in payout_list:
        try:
            transfer['amount'] = Decimal(transfer['amount'])
        except Exception as e:
            raise Exception(f"Bad amount in {transfer}: {e}")

        if transfer['amount'] <= 0:
            raise Exception(f"Payout amount should be a positive number: {transfer}")

    if g.symbol == config["COIN_SYMBOL"]:
        signature, error = _legacy_payout_signature(payout_list)
        if error:
            return error
        task = signature.apply_async()
        return{'task_id': task.id}
    elif  g.symbol in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys(): 
        signature, error = _legacy_payout_signature(payout_list)
        if error:
            return error
        task = signature.apply_async()
        return {'task_id': task.id}
    else:
        raise Exception(f"{g.symbol} is not defined in config, cannot make payout")
    
@api.post('/payout/<to>/<decimal:amount>')
def payout(to, amount):
    payout_list = [{ "dest": to, "amount": amount }]
    if g.symbol == config["COIN_SYMBOL"]:
        payout_list = [{ "dest": to, "amount": amount }]
        signature, error = _legacy_payout_signature(payout_list)
        if error:
            return error
        task = signature.apply_async()
        return {'task_id': task.id}
    elif  g.symbol in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys():
        signature, error = _legacy_payout_signature(payout_list)
        if error:
            return error
        task = signature.apply_async()
        return {'task_id': task.id}
    else:
        raise Exception(f"{g.symbol} is not defined in config, cannot make payout")

@api.post('/task/<id>')
def get_task(id):
    task = celery.AsyncResult(id)
    if isinstance(task.result, Exception):
        return {'status': task.status, 'result': str(task.result)}
    return {'status': task.status, 'result': task.result}
