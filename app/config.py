import os
from decimal import Decimal


def str_to_bool(value):
    return str(value).lower() in ('1', 'true', 'yes', 'on')


config = {
    'TONCENTER_API_URL': str(os.environ.get('TONCENTER_API_URL', 'https://api.testnet.ton.shkeeper.io:8082')),
    'TONCENTER_API_KEY': str(os.environ.get('TONCENTER_API_KEY', ' ')),
    'TONCENTER_INDEXER_URL': str(os.environ.get('TONCENTER_INDEXER_URL', 'https://api.testnet.ton.shkeeper.io:8082')),
    'TONCENTER_INDEXER_KEY': str(os.environ.get('TONCENTER_INDEXER_KEY', ' ')),    
    'GET_JETTON_TXS_LIMIT': int(os.environ.get('GET_JETTON_TXS_LIMIT', '20')), #limit of transactions get from 1 request to toncenter indexer /api/v3/jetton/transfers , max 1000
    'COIN_SYMBOL':  str(os.environ.get('COIN_SYMBOL', 'TON')),
    'TON_TRANSACTION_FEE': Decimal(os.environ.get('TON_TRANSACTION_FEE', '0.006')), # in TON
    'JETTON_TRANSACTION_FEE': Decimal(os.environ.get('JETTON_TRANSACTION_FEE', '0.04')), # in TON
    'JETTON_TRANSACTION_NEED_BALANCE': Decimal(os.environ.get('JETTON_TRANSACTION_NEED_BALANCE', '0.05')), # in TON
    'TON_FEE_FACTOR': Decimal(os.environ.get('TON_FEE_FACTOR', '1.4')), # Factor to send more TON to cover storage fee on recepient side, 1.1 means send 10% more TON
    'SHARD':  str(os.environ.get('SHARD', '8000000000000000')),
    'WORKCHAIN': str(os.environ.get('WORKCHAIN', '-1')), # -1 for masterchain
    'EVENTS_MAX_THREADS_NUMBER': int(os.environ.get('EVENTS_MAX_THREADS_NUMBER', '3')),
    'EVENTS_MIN_DIFF_TO_RUN_PARALLEL': int(os.environ.get('EVENTS_MIN_DIFF_TO_RUN_PARALLEL', '10')),
    'SCAN_NATIVE_TON_EVENTS': str_to_bool(os.environ.get('SCAN_NATIVE_TON_EVENTS', 'true')),
    'FULLNODE_TIMEOUT': int(os.environ.get('FULLNODE_TIMEOUT', '60')),
    'CHECK_NEW_BLOCK_EVERY_SECONDS': int(os.environ.get('CHECK_NEW_BLOCK_EVERY_SECONDS', '2')),
    'CURRENT_TON_NETWORK': str(os.environ.get('CURRENT_TON_NETWORK','testnet')),
    'TOKENS': {
        'main': {
            'TON-USDT': {
                'master_address': 'EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs', 
            },
        },
        'testnet': {
            'TON-USDT': {
                'master_address': 'kQDXn-tVCycUFu1PrKI9R-hnk9lP6MxqSEbUjkWtkcmuWdvu',
            },
        },
    },   
    'DEBUG': os.environ.get('DEBUG', False),
    'LOGGING_LEVEL': os.environ.get('LOGGING_LEVEL', 'INFO'),
    'SQLALCHEMY_DATABASE_URI' : os.environ.get('SQLALCHEMY_DATABASE_URI', "mariadb+pymysql://root:shkeeper@mariadb/ton-shkeeper?charset=utf8mb4"),
    'UPDATE_TOKEN_BALANCES_EVERY_SECONDS': int(os.environ.get('UPDATE_TOKEN_BALANCES_EVERY_SECONDS', 3600)),
    'DELAY_BETWEEN_ACC_BALANCE_REFRESH': float(os.environ.get('DELAY_BETWEEN_ACC_BALANCE_REFRESH', 2)),  # in seconds, use float for miliseconds
    'API_USERNAME': os.environ.get('TON_USERNAME', 'shkeeper'),
    'API_PASSWORD': os.environ.get('TON_PASSWORD', 'shkeeper'),
    'SHKEEPER_KEY': os.environ.get('SHKEEPER_BACKEND_KEY', 'shkeeper'),
    'SHKEEPER_HOST': os.environ.get('SHKEEPER_HOST', 'shkeeper:5000'),
    'REDIS_HOST': os.environ.get('REDIS_HOST', 'localhost'),
    'TON_HOST': os.environ.get('TON_HOST', 'ton'),
    'MIN_TRANSFER_THRESHOLD': Decimal(os.environ.get('MIN_TRANSFER_THRESHOLD', '0.2')),
    'MIN_TOKEN_TRANSFER_THRESHOLD': Decimal(os.environ.get('MIN_TOKEN_TRANSFER_THRESHOLD', '0.5')), 
    'LAST_BLOCK_LOCKED': os.environ.get('LAST_BLOCK_LOCKED'),
}

def get_min_token_transfer_threshold(symbol):
    return config['TOKENS'][config['CURRENT_TON_NETWORK']][symbol].get('min_transfer_threshold', config['MIN_TOKEN_TRANSFER_THRESHOLD'])
