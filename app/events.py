import requests as rq
import time
import base64
from concurrent.futures import ThreadPoolExecutor

from .models import Settings, db
from .config import config
from .logging import logger
from .coin import get_all_raw_accounts, get_pub_address_by_raw_address
from .toncenterapi import Toncenterapi


def walletnotify_shkeeper(symbol, txid) -> bool:
    """Notify SHKeeper about transaction"""
    logger.warning(f"Notifying about {symbol}/{txid}")
    while True:
        try:
            r = rq.post(
                    f'http://{config["SHKEEPER_HOST"]}/api/v1/walletnotify/{symbol}/{txid}',
                    headers={'X-Shkeeper-Backend-Key': config['SHKEEPER_KEY']}).json()
            if r["status"] == "success":
                logger.warning(f"The notification about {symbol}/{txid} was successful")
                return True
            else:
                logger.warning(f"Failed to notify SHKeeper about {symbol}/{txid}, received response: {r}")
                time.sleep(5)
        except Exception as e:
            logger.warning(f'Shkeeper notification failed for {symbol}/{txid}: {e}')
            time.sleep(10)


def log_loop(last_checked_block, check_interval):
    from .tasks import drain_account
    from app import create_app
    app = create_app()
    app.app_context().push()

    toncenterapi = Toncenterapi()

    while True:
        last_block = toncenterapi.get_masterchain_head()
        if last_checked_block == '' or last_checked_block is None:
            last_checked_block = last_block
        list_accounts = set(get_all_raw_accounts()) 
        if last_checked_block > last_block:
            logger.exception(f'Last checked block {last_checked_block} is bigger than last block {last_block} in blockchain')
            time.sleep(check_interval) 
        elif last_checked_block == last_block - 2:
            pass
        elif (last_block - last_checked_block) > int(config['EVENTS_MIN_DIFF_TO_RUN_PARALLEL']):
            def check_in_parallel(block):
                try:
                    ton_start_time = time.time()
                    transactions = toncenterapi.get_all_transactions_by_masterchain_seqno(block)
                    for transaction in transactions:
                        if 'in_msg' in transaction.keys() and transaction['in_msg'] is not None:
                            message = transaction['in_msg']
                            if message['source'] != '' and message['destination'] != '':
                                if ((message['destination'] in list_accounts) or 
                                    (message['source'] in list_accounts)):
                                    walletnotify_shkeeper(config["COIN_SYMBOL"], base64.b64decode(transaction['hash']).hex())
                                if ((message['destination'] in list_accounts and message['source'] not in list_accounts) and 
                                    ((toncenterapi.get_masterchain_head() - block) < 400)):
                                    drain_account.delay(config["COIN_SYMBOL"], message['destination'])
                    ton_finish_time = time.time()

                    # Jetton section
                    for token in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys():
                        master_address = config['TOKENS'][config["CURRENT_TON_NETWORK"]][token]['master_address']
                        all_txs = toncenterapi.get_all_jetton_txs_by_masterchain_seqno(seqno=block, jetton_master=master_address)
                        for transaction in all_txs:
                            if ((transaction['destination'] in list_accounts) or 
                                (transaction['source'] in list_accounts)):
                                walletnotify_shkeeper(token,  base64.b64decode(transaction['transaction_hash']).hex())

                                if ((transaction['destination'] in list_accounts and 
                                     transaction['source'] not in list_accounts) and 
                                    ((toncenterapi.get_masterchain_head() - block) < 400)):
                                    drain_account.delay(token, transaction['destination'])
                   
                    block_ton_time = ton_finish_time - ton_start_time
                    block_jetton_time = time.time() - ton_finish_time
                    logger.warning(f"Сhecked block {block}. TON time: {block_ton_time:.2f}, Jetton time: {block_jetton_time:.2f}")

                except Exception as e:
                    logger.exception(f'Block {block}: Failed to scan: {e}')
                    return False
                return True
            
            with ThreadPoolExecutor(max_workers=int(config['EVENTS_MAX_THREADS_NUMBER'])) as executor:
                 while True:
                    blocks = []
                    try:
                        if last_block - last_checked_block < int(config['EVENTS_MIN_DIFF_TO_RUN_PARALLEL']):
                            break
                        for i in range(int(config['EVENTS_MAX_THREADS_NUMBER'])):
                            blocks.append(last_checked_block + 1 + i)
                        start_time = time.time()
                        results = list(executor.map(check_in_parallel, blocks))
                        logger.debug(f'Block chunk {blocks[0]} - {blocks[-1]} processed for {time.time() - start_time} seconds')
    
                        if all(results):
                            logger.debug(f"Commiting chunk {blocks[0]} - {blocks[-1]}")
                            last_checked_block = blocks[-1]
                            pd = Settings.query.filter_by(name = "last_block").first()
                            pd.value = last_checked_block
                            with app.app_context():
                                db.session.add(pd)
                                db.session.commit()
                                db.session.close()
                        else:
                            logger.info(f"Some blocks failed, retrying chunk {blocks[0]} - {blocks[-1]}")

                    except Exception as e:
                        sleep_sec = 60
                        logger.exception(f"Exception in main block scanner loop: {e}")
                        logger.warning(f"Waiting {sleep_sec} seconds before retry.")
                        time.sleep(sleep_sec)
        else:     
            logger.warning("Waiting for a new slots")
            time.sleep(check_interval) 
           

def events_listener():

    from app import create_app
    app = create_app()
    app.app_context().push()

    if (not Settings.query.filter_by(name = "last_block").first()) and (config['LAST_BLOCK_LOCKED'].lower() != 'true'):
        logger.warning("Changing last_block to a last block on a fullnode, because cannot get it in DB")
        toncenterapi = Toncenterapi()
        
        with app.app_context():
            db.session.add(Settings(name = "last_block", 
                                         value = toncenterapi.get_masterchain_head()))
            db.session.commit()
            db.session.close() 
            db.session.remove()
            db.engine.dispose()

    
    while True:
        try:
            pd = Settings.query.filter_by(name = "last_block").first()
            last_checked_block = int(pd.value)
            log_loop(last_checked_block, int(config["CHECK_NEW_BLOCK_EVERY_SECONDS"]))
        except Exception as e:
            sleep_sec = 60
            logger.exception(f"Exception in main block scanner loop: {e}")
            logger.warning(f"Waiting {sleep_sec} seconds before retry.")           
            time.sleep(sleep_sec)


