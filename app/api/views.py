from decimal import Decimal
from flask import g

from ..config import config
from ..models import Settings
from ..coin import Coin, get_all_accounts, get_all_raw_accounts, get_pub_address_by_raw_address
from ..toncenterapi import Toncenterapi, from_nanotons
from ..logging import logger
from . import api


@api.post("/generate-address")
def generate_new_address():   
    coin_inst = Coin(g.symbol)
    pub_address = coin_inst.create_wallet('regular')
    return {'status': 'success', 'address': pub_address}


@api.post('/balance')
def get_balance():
    crypto_str = str(g.symbol)   
    if crypto_str == config["COIN_SYMBOL"]:
        inst = Coin(config["COIN_SYMBOL"])
        balance = inst.get_fee_deposit_coin_balance()
    else:
        if crypto_str in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys():
            token_instance = Coin(crypto_str)
            balance = token_instance.get_fee_deposit_jetton_balance()
        else:
            return {'status': 'error', 'msg': 'token is not defined in config'}
    return {'status': 'success', 'balance': balance}


@api.post('/status')
def get_status():
    toncenterapi = Toncenterapi()
    pd = Settings.query.filter_by(name = 'last_block').first()
    
    last_checked_block_number = int(pd.value)
    timestamp = toncenterapi.get_block_timestamp(last_checked_block_number)
    return {'status': 'success', 'last_block_timestamp': timestamp}


@api.post('/transaction/<txid>')
def get_transaction(txid):
    logger.warning(f"Checking the transaction {txid} for {g.symbol}")
    toncenterapi = Toncenterapi()
    related_transactions = []
    list_accounts = get_all_raw_accounts()
    if g.symbol == config["COIN_SYMBOL"]:
        try:
            transaction = toncenterapi.get_transaction_by_hash(txid)
            if transaction['description']['destroyed']:
                logger.warning(f'Failed transaction {transaction}, return empty list')
                return []
            out_msgs = transaction['out_msgs']
            if len(out_msgs) > 0:
                logger.warning(f'Transaction {txid} has out messages, it will be ignored')
                return []
            message = transaction['in_msg']
            if message['value'] is None:
                logger.warning(f'Transaction {txid} value is None, it will be ignored')
                return []
            block = transaction['mc_block_seqno']
            confirmations = int(toncenterapi.get_masterchain_head()) - int(block)
            logger.warning(f'Confirmations - {str(confirmations)}')
            # for message in messages:
            if message['bounced']:
                logger.warning(f'Message {message} in tx {txid} is bounced, so it will be ignored')
                return []
            if message['decoded_opcode'] == 'jetton_notify':
                logger.warning(f'Message {message} in tx {txid} is jetton transfer notify, so it will be ignored')
                return []
            if (message['destination'] in list_accounts) and (message['source'] in list_accounts):
                address = get_pub_address_by_raw_address(message['source'])
                category = 'internal'
            elif message['destination'] in list_accounts:
                address = get_pub_address_by_raw_address(message["destination"])
                category = 'receive'
            elif message['source'] in list_accounts:
                address = get_pub_address_by_raw_address(message['source'])
                category = 'send'
            else:
                pass
            amount = from_nanotons(int(message['value']))
            related_transactions.append([address, amount, confirmations, category])
        except Exception as e:
            logger.warning(f"Error while checking transaction {txid} - {e}")
            return {'status': 'error', 'msg': {e}}
    elif g.symbol in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys():
        jetton_master = config['TOKENS'][config["CURRENT_TON_NETWORK"]][g.symbol]['master_address']
        transaction = toncenterapi.get_jetton_transaction_by_hash(txid, jetton_master)
        if transaction['transaction_aborted']:
            logger.warning(f'aborted transaction {transaction}')
            return []
        raw_ton_transaction = toncenterapi.get_transaction_by_hash(txid)
        block = raw_ton_transaction['mc_block_seqno']
        confirmations = int(toncenterapi.get_masterchain_head()) - int(block)
        logger.warning(f'Confirmations - {str(confirmations)}')
        
        if (transaction['destination'] in list_accounts) and (transaction['source'] in list_accounts):
            address = get_pub_address_by_raw_address(transaction['source'])
            category = 'internal'
        elif transaction['destination'] in list_accounts:
            address = get_pub_address_by_raw_address(transaction["destination"])
            category = 'receive'
        elif transaction['source'] in list_accounts:                
            address = get_pub_address_by_raw_address(transaction['source'])
            category = 'send'
        amount = Decimal(int(transaction['amount'])) / 10 ** toncenterapi.jetton_master_decimals(jetton_master)
        related_transactions.append([address, amount, confirmations, category])
 
    else:
        logger.warning(f"Currency {g.symbol} is not defined in config")
        return {'status': 'error', 'msg': 'Currency is not defined in config'}
    if len(related_transactions) == 0:
        logger.warning(f"txid {txid} is not related to any known address for {g.symbol}")
    logger.warning(related_transactions)
    return related_transactions


@api.post('/dump')
def dump():
    w = Coin(config["COIN_SYMBOL"])
    all_wallets = w.get_dump()
    return all_wallets


@api.post('/fee-deposit-account')
def get_fee_deposit_account():
    token_instance = Coin(g.symbol)
    return {'account': token_instance.get_fee_deposit_account('public'), 
            'balance': token_instance.get_fee_deposit_coin_balance()}


@api.post('/get_all_addresses')
def get_all_addresses():
    all_addresses_list = get_all_accounts()   
    return all_addresses_list


    
