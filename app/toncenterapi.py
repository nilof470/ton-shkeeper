import base64
import time

import requests as rq
from decimal import Decimal

from .logging import logger
from .config import config


class Toncenterapi():
    
    def __init__(self, init=True):
        self.api_url = config["TONCENTER_API_URL"]
        self.api_key = config["TONCENTER_API_KEY"]
        self.indexer_url = config["TONCENTER_INDEXER_URL"]
        self.indexer_key = config["TONCENTER_INDEXER_KEY"]
        self.headers = {'accept': 'application/json'}

    def get_masterchain_head(self):
        response = rq.get(f'{self.api_url}/api/v2/getMasterchainInfo', 
                          params={'api_key': self.api_key},
                          headers = self.headers)
        response.raise_for_status()
        if response.json()['ok']:
            return response.json()['result']['last']['seqno']
        else:
            raise Exception ("Cannot get masterchain head block number")
        
    def get_block_header(self, 
                         seqno, 
                         workchain = config['WORKCHAIN'],
                         shard = str(config['SHARD'])):
        response = rq.get(f'{self.api_url}/api/v2/getBlockHeader', 
                          params={'api_key': self.api_key,
                                  'seqno': seqno,
                                  'workchain': workchain,
                                  'shard': shard},
                          headers = self.headers)
        response.raise_for_status()
        if response.json()['ok']:
            return response.json()
        else:
            raise Exception ("Cannot get masterchain head block number")
    
    def get_block_lts(self,
                      seqno,
                      workchain = config["WORKCHAIN"],
                      shard = str(config['SHARD'])):
        result = self.get_block_header(seqno, workchain, shard)
        result2 = self.get_block_header(seqno + 1, workchain, shard)
        if result['ok'] and 'start_lt' in result['result'].keys():
            start_lt = result['result']['start_lt']
        if result2['ok'] and 'start_lt' in result2['result'].keys():
            end_lt = result2['result']['start_lt']
            return {'start_lt': start_lt, 'end_lt': end_lt}
        else:            
            raise Exception (f"Cannot get block lts in {result}")
        
    def get_all_jetton_txs_by_masterchain_seqno(self, seqno=None, start_lt=None, end_lt=None, jetton_master=None):
        if seqno is not None and start_lt is None and end_lt is None:
            result = self.get_block_lts(seqno)
            start_lt = result['start_lt']
            end_lt = result['end_lt']

        end_transactions = False
        request_counter = 0 
        all_transactions = []

        while not end_transactions:
            response = rq.get(f'{self.indexer_url}/api/v3/jetton/transfers', 
                            params={'api_key': self.indexer_key,
                                    'jetton_master': jetton_master,
                                    'start_lt': start_lt,
                                    'end_lt': end_lt,
                                    'limit': config['GET_JETTON_TXS_LIMIT'],
                                    'offset': request_counter * config['GET_JETTON_TXS_LIMIT']
                                    },
                            headers = self.headers)
            response.raise_for_status()
            request_counter += 1
            if len(response.json()['jetton_transfers']) < config['GET_JETTON_TXS_LIMIT']:   
                end_transactions = True
            all_transactions.extend(response.json()['jetton_transfers'])

        return all_transactions

    def get_transaction_by_hash(self, hash):
        response = rq.get(f'{self.indexer_url}/api/v3/transactions', 
                          params={'api_key': self.indexer_key,
                                  'hash': hash,
                                  },
                          headers = self.headers)
        response.raise_for_status()
        if len(response.json()['transactions']) != 0:
            return response.json()['transactions'][0]
        else:
            # payout transactions get transaction by message hash 
            logger.warning(f"Cannot get transaction by hash {hash}, try to get transaction by message hash")
            response2 = rq.get(f'{self.indexer_url}/api/v3/transactionsByMessage', 
                          params={'api_key': self.indexer_key,
                                  'msg_hash': hash,
                                  },
                          headers = self.headers)
            response2.raise_for_status()
            if len(response2.json()['transactions']) > 0:
                response3 = rq.get(f'{self.indexer_url}/api/v3/adjacentTransactions', 
                          params={'api_key': self.indexer_key,
                                  'hash': response2.json()['transactions'][0]['hash'],
                                  },
                          headers = self.headers)

                return response3.json()['transactions'][0]
            else:
                raise Exception (f"Cannot get transaction by hash {hash}")            

    def get_jetton_transaction_by_hash(self, hash, jetton_master):
        tx_by_hash = self.get_transaction_by_hash(hash)
        tx_lt = int(tx_by_hash['lt'])
        logger.warning(f"transaction lt - {tx_lt}")
        tx_list = self.get_all_jetton_txs_by_masterchain_seqno(start_lt=tx_lt-3, end_lt=tx_lt+3, jetton_master=jetton_master)

        for tx in tx_list:
            if base64.b64decode(tx['transaction_hash']).hex() == hash:
                return tx
        logger.warning(f"Cannot get jetton transaction by hash {hash}, probably it is outgoing tx, try to get transaction by message hash")
        message_hash = tx_by_hash['hash']
        for tx in tx_list:
            if (tx['trace_id']) == message_hash:
                return tx
        raise Exception (f"Cannot get jetton transaction by hash {hash}")
            
    def get_masterchain_block_by_shardchain_block(self, block):
        response = rq.get(f'{self.indexer_url}/api/v3/blocks', 
                          params={'api_key': self.indexer_key,
                                  'workchain': block['workchain'],
                                  'shard': block['shard'],
                                  'seqno': block['seqno']},
                          headers = self.headers)
        response.raise_for_status()
        #logger.warning(response.text)
        return response.json()

    def get_all_transactions_by_masterchain_seqno(self, seqno):
        for i in range(3):
            try:
                response = rq.get(f'{self.indexer_url}/api/v3/transactionsByMasterchainBlock', 
                                  params={'api_key': self.indexer_key,
                                          'seqno': seqno,},
                                  headers = self.headers)
                response.raise_for_status()
                #logger.warning(f'{response.json()}')
                return response.json()['transactions']
            except Exception as e:
                logger.warning(f'Cannot get all transactions from {seqno} block, {e} wait 10 seconds')
                time.sleep(10)
        
    def get_block_timestamp(self, seqno):
        block = {'seqno': seqno,
                 'workchain': config['WORKCHAIN'],
                 'shard': str(config['SHARD']),}
        response = rq.get(f'{self.api_url}/api/v2/getBlockHeader', 
                          params={'api_key': self.api_key,
                                  'workchain': block['workchain'],
                                  'shard': block['shard'],
                                  'seqno': block['seqno']},
                          headers = self.headers)
        response.raise_for_status()
        return response.json()['result']['gen_utime']
    
    def get_account_balance(self, address):
        response = rq.get(f'{self.api_url}/api/v2/getAddressInformation', 
                      headers=self.headers,
                      params={'api_key': self.api_key,
                              'address': address})
        response.raise_for_status()
        return int(response.json()['result']['balance'])
    
    def get_account_jetton_balance(self, owner_address, jetton_master):
        response = rq.get(f'{self.api_url}/api/v3/jetton/wallets', 
                      headers=self.headers,
                      params={'api_key': self.api_key,
                              'owner_address': owner_address,
                              'jetton_address': jetton_master})
        response.raise_for_status()
        if len(response.json()['jetton_wallets']) > 0:
            jetton_master_raw_address = response.json()['jetton_wallets'][0]['jetton']
            decimals = int(response.json()['metadata'][jetton_master_raw_address]['token_info'][0]['extra']['decimals'])
            jetton_raw_amount = int(response.json()['jetton_wallets'][0]['balance'])
            result = Decimal(jetton_raw_amount) / Decimal(10**decimals)
        else:
            result = Decimal(0)
        return result

    def get_account_wallet_jetton_address(self, owner_address, jetton_master):
        response = rq.get(f'{self.api_url}/api/v3/jetton/wallets', 
                      headers=self.headers,
                      params={'api_key': self.api_key,
                              'owner_address': owner_address,
                              'jetton_address': jetton_master})
        response.raise_for_status()
        if len(response.json()['jetton_wallets']) > 0:
            jetton_wallet_raw_address = response.json()['jetton_wallets'][0]['address']
        else:
            raise Exception (f"Cannot get jetton wallet address in {response.text}")
        return jetton_wallet_raw_address
    
    def jetton_master_decimals(self, jetton_master):
        response = rq.get(f'{self.api_url}/api/v3/jetton/masters', 
                      headers=self.headers,
                      params={'api_key': self.api_key,
                              'address': jetton_master})
        response.raise_for_status()
        if len(response.json()['jetton_masters']) > 0:
            decimals = int(response.json()['jetton_masters'][0]['jetton_content']['decimals'])
            return int(decimals)
        else:
             raise Exception (f"Cannot get jetton master decimals in {response.text}")

    def get_account_state(self, address):
        response = rq.get(f'{self.api_url}/api/v2/getWalletInformation', 
                      headers=self.headers,
                      params={'api_key': self.api_key,
                              'address': address})
        response.raise_for_status()
        state = response.json()['result']['account_state']
        if state == 'empty' or state == 'uninit':
            return 'uninitialized'
        else:
            return state
        
    def get_account_seqno(self, address):
        response = rq.get(f'{self.api_url}/api/v2/getWalletInformation', 
                      headers=self.headers,
                      params={'api_key': self.api_key,
                              'address': address})
        response.raise_for_status()
        return response.json()['result']['seqno']
    
    def get_account_transactions(self, address, limit):
        response = rq.get(f'{self.indexer_url}/v1/getTransactionsByAddress', 
                      headers=self.headers,
                      params={'api_key': self.indexer_key,
                              'address': address,
                              'limit': int(limit)})
        response.raise_for_status()
        return response.json()
    
    def send_message(self, signed_boc):
        response = rq.post(f'{self.api_url}/api/v2/sendBoc',
                          json={"boc": signed_boc}, 
                          headers={'accept': 'application/json',
                                   'Content-Type': 'application/json'},
                          params={'api_key': self.api_key})
        response.raise_for_status()
        logger.warning(f'Sent message to the blockchain, {response.text}')
        return response.status_code
    
    def send_message_with_hash(self, signed_boc):
        response = rq.post(f'{self.api_url}/api/v2/sendBocReturnHash', 
                          json={"boc": signed_boc}, 
                          headers={'accept': 'application/json',
                                   'Content-Type': 'application/json'},
                          params={'api_key': self.api_key})
        response.raise_for_status()
        logger.warning(f'Sent message to the blockchain, {response.text}')
        result_json = response.json()
        if result_json['ok']:
            tx_id = result_json['result']['hash_norm']
            return tx_id
        else:
            return False


def from_nanotons(amount):
    return amount / 1_000_000_000


def to_nanotons(amount):
    return amount * 1_000_000_000