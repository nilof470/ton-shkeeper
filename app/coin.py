import base64
import hashlib
import json
import time

from decimal import Decimal
from flask import current_app as app
from tonsdk.contract.wallet import Wallets as TonWallets
from tonsdk.contract.wallet import WalletVersionEnum
from tonsdk.contract.token.ft import JettonWallet
from tonsdk.utils import Address
from tonsdk.utils import bytes_to_b64str


from .logging import logger
from .encryption import Encryption
from .config import config
from .models import Accounts, db, Wallets
from .fee_deposit_seqno_guard import (
    fee_deposit_seqno_guard_for_address,
    fee_deposit_seqno_lock,
)
#from .unlock_acc import get_account_password
from .toncenterapi import Toncenterapi, to_nanotons


def get_all_accounts():
        account_list = []
        tries = 3
        for i in range(tries):
            try:
                all_account_list = Accounts.query.all()
            except:
                if i < tries - 1: # i is zero indexed
                    db.session.rollback()
                    continue
                else:
                    db.session.rollback()
                    raise Exception("There was exception during query to the database, try again later")
            break
        for account in all_account_list:
            account_list.append(account.pub_address)
        return account_list


def get_all_raw_accounts():
    raw_account_list = []
    tries = 3
    for i in range(tries):
        try:
            all_account_list = Accounts.query.all()
        except:
            if i < tries - 1: # i is zero indexed
                db.session.rollback()
                continue
            else:
                db.session.rollback()
                raise Exception("There was exception during query to the database, try again later")
        break
    for account in all_account_list:
        raw_account_list.append(account.raw_address.upper())
    return raw_account_list


def get_pub_address_by_raw_address(in_raw_address):
    logger.warning(f'Seek for {in_raw_address}')
    tries = 3
    for i in range(tries):
        try:
            pd = Accounts.query.filter_by(raw_address = in_raw_address.lower()).first()
        except:
            if i < tries - 1: # i is zero indexed
                db.session.rollback()
                continue
            else:
                db.session.rollback()
                raise Exception("There was exception during query to the database, try again later")
    if not pd:
        logger.warning("Cannot find the pub-address using the raw address, return None")
        return None
    return pd.pub_address


def is_valid_ton_address(address: str) -> bool:
    try:
        Address(address)
        return True
    except ValueError:
        return False


class Coin:
    @staticmethod
    def _normalize_broadcast_hash(response_hash):
        text = str(response_hash).strip()
        if len(text) == 64 and all(c in "0123456789abcdefABCDEF" for c in text):
            return text.lower()
        try:
            decoded = base64.b64decode(text, validate=True)
            if len(decoded) == 32:
                return decoded.hex()
        except Exception:
            pass
        return text

    def __init__(self, symbol=config['COIN_SYMBOL'], init=True):
        self.symbol = symbol
        self.headers = {'accept': 'application/json'}
        self.toncenter = Toncenterapi()
        if self.symbol in config['TOKENS'][config['CURRENT_TON_NETWORK']].keys():
            self.jetton_master_address = config['TOKENS'][config['CURRENT_TON_NETWORK']][self.symbol]['master_address']


    def get_transaction_price(self):
        fee_ = self.get_transaction_fee(self.get_fee_deposit_account('public'),
                                       self.get_fee_deposit_account('public'),
                                       to_nanotons(0))
        return fee_

    def set_fee_deposit_account(self):
        self.create_wallet('fee_deposit')

    def get_transaction_fee(self, source_addr, dest_addr, amount):
        return config['TON_TRANSACTION_FEE']

    def get_jetton_transaction_fee(self, source_addr=None, dest_addr=None, amount=None):
        return config['JETTON_TRANSACTION_FEE']

    def get_fee_deposit_account(self, address_type):
        try:
            pd = Accounts.query.filter_by(type = "fee_deposit").first()
        except:
            db.session.rollback()
            raise Exception("There was exception during query to the database, try again later")
        if not pd:
            from .tasks import create_fee_deposit_account
            create_fee_deposit_account.delay()
            time.sleep(10)
        pd = Accounts.query.filter_by(type = "fee_deposit").first()
        if address_type == 'public':
            return pd.pub_address
        elif address_type == 'raw':
            return pd.raw_address
        else:
            raise Exception("Return address type is not defined")

    def get_ton_balance(self, address):
        nanoton_balance = self.toncenter.get_account_balance(address)
        balance = Decimal(nanoton_balance) / Decimal(1_000_000_000)
        return balance

    def get_nanoton_balance(self, address):
        nanoton_balance = self.toncenter.get_account_balance(address)
        return nanoton_balance

    def deploy_wallet(self, address):
        mnemonics = self.get_mnemonic_from_address(address)
        _mnemonics, _pub_k, _priv_k, wallet = TonWallets.from_mnemonics(mnemonics, WalletVersionEnum('v4r2'), 0)
        query = wallet.create_init_external_message()
        boc = bytes_to_b64str(query["message"].to_boc(False))
        #response = self.toncenter.send_message(boc) #send_message_with_hash
        response = self.toncenter.send_message_with_hash(boc)
        return response

    def get_fee_deposit_coin_balance(self):
        fee_deposit_account = self.get_fee_deposit_account('public')
        amount = self.get_ton_balance(fee_deposit_account)
        return amount

    def get_account_jetton_balance(self, account):
        amount = self.toncenter.get_account_jetton_balance(account, self.jetton_master_address)
        return amount

    def get_fee_deposit_jetton_balance(self):
        fee_deposit_account = self.get_fee_deposit_account('public')
        amount = self.get_account_jetton_balance(fee_deposit_account)
        return amount

    def build_signed_payout(self, canonical, source_seqno, valid_until):
        wallet_valid_until = int(time.time()) + 60
        fee_deposit_public = self.get_fee_deposit_account('public')
        fee_deposit_mnemonic = self.get_mnemonic_from_address(fee_deposit_public)
        _mnemonics, _pub_k, _priv_k, fee_deposit_wallet = TonWallets.from_mnemonics(
            fee_deposit_mnemonic,
            WalletVersionEnum('v4r2'),
            0,
        )
        amount = Decimal(canonical['amount'])
        destination = canonical['destination']
        decimals = self.toncenter.jetton_master_decimals(self.jetton_master_address)
        body = JettonWallet().create_transfer_body(
            to_address=Address(destination),
            jetton_amount=int(amount * Decimal(10) ** int(decimals)),
            response_address=Address(fee_deposit_public),
        )
        jetton_wallet = self.toncenter.get_account_wallet_jetton_address(
            fee_deposit_public,
            self.jetton_master_address,
        )
        fee_amount = Decimal(
            self.get_jetton_transaction_fee(
                fee_deposit_public,
                destination,
                amount,
            )
        )
        transfer = fee_deposit_wallet.create_transfer_message(
            to_addr=jetton_wallet,
            amount=to_nanotons(fee_amount),
            seqno=int(source_seqno),
            payload=body,
        )
        message = transfer['message']
        boc_bytes = message.to_boc(False)
        message_hash_bytes = message.bytes_hash()
        message_hash = (
            message_hash_bytes.hex()
            if hasattr(message_hash_bytes, 'hex')
            else str(message_hash_bytes)
        )
        return {
            'boc': bytes_to_b64str(boc_bytes),
            'message_hash': message_hash,
            'jetton_wallet': jetton_wallet,
            'valid_until': wallet_valid_until,
        }

    @staticmethod
    def signed_payout_evidence(signed):
        boc_bytes = str(signed['boc']).encode('utf-8')
        signed_boc_hash = hashlib.sha256(boc_bytes).hexdigest()
        return {
            'signed_boc_ref': f"not-retained:signed-ton-boc-sha256:{signed_boc_hash}",
            'signed_boc_hash': signed_boc_hash,
            'message_hash': signed['message_hash'],
            'jetton_wallet': signed['jetton_wallet'],
            'valid_until': signed['valid_until'],
            'chain_check_metadata': {
                'signed_boc_artifact_retention': 'NOT_RETAINED_SPENDABLE_BOC',
                'message_hash': signed['message_hash'],
                'jetton_wallet': signed['jetton_wallet'],
            },
        }

    def broadcast_signed_payout(self, signed):
        response_hash = self.toncenter.send_message_with_hash(signed['boc'])
        message_hash = self._normalize_broadcast_hash(response_hash)
        return {
            'ok': True,
            'hash': message_hash,
            'hash_base64': response_hash,
        }

    def initialize_account(self, account):
        if self.toncenter.get_account_state(account) == 'uninitialized':
            logger.warning(f"Account {account} is uninitialized, deploying it before payout")
            self.deploy_wallet(account)
            for i in range(5):
                if self.toncenter.get_account_state(account) != 'uninitialized':
                    return True
                time.sleep(2)
            if self.toncenter.get_account_state(account) == 'uninitialized':
                raise Exception(f"Cannot deploy {account} account")
        else:
            return True

    def get_all_balances(self):
        balances = {}
        try:
            pd = Accounts.query.filter_by(crypto = self.symbol,).all()
        except:
            db.session.rollback()
            raise Exception("There was exception during query to the database, try again later")
        if not pd:
            raise Exception(f"There is not any account with {self.symbol} crypto in database")
        else:
            for account in pd:
                if account.type != "fee_deposit":
                    balances.update({account.pub_address: Decimal(account.amount)})
            return balances

    def make_multipayout_ton(self, payout_list, fee,):
        message = ''
        send_mode = 1 # https://docs.ton.org/v3/documentation/smart-contracts/message-management/sending-messages#message-modes
        payout_results = []
        payout_list = payout_list
        fee = Decimal(fee)

        logger.warning(f'Start multipayout, payout list: {payout_list}')

        for payout in payout_list:
            if not is_valid_ton_address(payout['dest']):
                raise Exception(f"Address {payout['dest']} is not valid blockchain address")

        # Check if enouth funds for multipayout on account
        should_pay  = Decimal(0)
        for payout in payout_list:
            should_pay = should_pay + Decimal(payout['amount'])
            should_pay = should_pay + Decimal(self.get_transaction_fee(self.get_fee_deposit_account('public'),
                                                               payout['dest'],
                                                               payout['amount']))
        have_crypto = self.get_fee_deposit_coin_balance()
        if have_crypto < should_pay:
            raise Exception(f"Have not enough crypto on fee account, need {should_pay} have {have_crypto}")
        else:
            with fee_deposit_seqno_lock(reason="fee-deposit-ton-multipayout"):
                self.initialize_account(self.get_fee_deposit_account('public'))
                fee_deposit_mnemonic = self.get_mnemonic_from_address(self.get_fee_deposit_account('public'))
                mnemonics, _pub_k, _priv_k, fee_deposit_wallet  = TonWallets.from_mnemonics(
                    fee_deposit_mnemonic,
                    WalletVersionEnum('v4r2'), 0)
                fee_deposit_seqno = self.toncenter.get_account_seqno(self.get_fee_deposit_account('raw'))
                transaction_list = []
                for payout in payout_list:
                    transaction_list.append((payout, fee_deposit_wallet.create_transfer_message(
                        to_addr=payout['dest'],
                        amount=to_nanotons(float(payout['amount'])),
                        seqno=fee_deposit_seqno,
                        payload=message,
                        send_mode=send_mode
                    )))
                    fee_deposit_seqno  = fee_deposit_seqno + 1

                logger.warning('Start sending transactions from the list')
                for payout, transaction in transaction_list:
                    boc = bytes_to_b64str(transaction["message"].to_boc(False))
                    # old_acc_seqno = self.toncenter.get_account_seqno(self.get_fee_deposit_account('raw'))
                    tx_id = self.toncenter.send_message_with_hash(boc)
                    raw_txid =  base64.b64decode(tx_id).hex()
                    logger.warning(f'Message sent, hash: {tx_id}')

                    payout_results.append({
                        "dest": payout['dest'],
                        "amount": float(payout['amount']),
                        "status": "success",
                        "txids": [raw_txid],
                    })

                logger.warning(payout_results)
                return payout_results

    def make_multipayout_jetton(self, payout_list, fee,):
        message = ''
        payout_results = []
        payout_list = payout_list
        fee = Decimal(fee)

        logger.warning(f'Start jetton multipayout, payout list: {payout_list}')

        for payout in payout_list:
            if not is_valid_ton_address(payout['dest']):
                raise Exception(f"Address {payout['dest']} is not valid blockchain address")

        multipayout_sum = Decimal(0)
        for payout in payout_list:
            multipayout_sum = multipayout_sum + Decimal(payout['amount'])
        fee_deposit_jetton_balance = self.get_fee_deposit_jetton_balance()
        if multipayout_sum > fee_deposit_jetton_balance:
            raise Exception(f"Have not enough tokens on fee account, need {multipayout_sum} have {fee_deposit_jetton_balance}")

        should_pay_fee  = Decimal(0)
        for payout in payout_list:
            should_pay_fee = should_pay_fee + Decimal(self.get_jetton_transaction_fee(self.get_fee_deposit_account('public'),
                                                               payout['dest'],
                                                               payout['amount']))
        have_crypto = self.get_fee_deposit_coin_balance()
        if have_crypto < should_pay_fee:
            raise Exception(f"Have not enough crypto on fee account to cover fees, need {should_pay_fee} have {have_crypto}")
        else:
            with fee_deposit_seqno_lock(reason="fee-deposit-jetton-multipayout"):
                self.initialize_account(self.get_fee_deposit_account('public'))
                fee_deposit_mnemonic = self.get_mnemonic_from_address(self.get_fee_deposit_account('public'))
                mnemonics, _pub_k, _priv_k, fee_deposit_wallet  = TonWallets.from_mnemonics(
                    fee_deposit_mnemonic,
                    WalletVersionEnum('v4r2'), 0)
                fee_deposit_seqno = self.toncenter.get_account_seqno(self.get_fee_deposit_account('raw'))
                transaction_list = []
                for payout in payout_list:
                    body = JettonWallet().create_transfer_body(
                            to_address=Address(payout['dest']),
                            jetton_amount=int((payout['amount']) * 10**self.toncenter.jetton_master_decimals(self.jetton_master_address)),
                            response_address=Address(self.get_fee_deposit_account('public'))
                    )

                    fee_amount = Decimal(self.get_jetton_transaction_fee(self.get_fee_deposit_account('public'),
                                                                   payout['dest'],
                                                                   payout['amount']))


                    message = fee_deposit_wallet.create_transfer_message(to_addr=self.toncenter.get_account_wallet_jetton_address(self.get_fee_deposit_account('public'), self.jetton_master_address),
                                           amount=to_nanotons(fee_amount), # just for fee, real amount will be in payload
                                           seqno=int(fee_deposit_seqno),
                                           payload=body)
                    transaction_list.append((payout, message))
                    fee_deposit_seqno = fee_deposit_seqno + 1



                logger.warning('Start sending transactions from the list')

                for payout, transaction in transaction_list:
                    boc = bytes_to_b64str(transaction["message"].to_boc(False))
                    # old_acc_seqno = self.toncenter.get_account_seqno(self.get_fee_deposit_account('raw'))
                    tx_id = self.toncenter.send_message_with_hash(boc)
                    raw_txid =  base64.b64decode(tx_id).hex()
                    logger.warning(f'Message sent, hash: {tx_id}')

                    payout_results.append({
                        "dest": payout['dest'],
                        "amount": float(payout['amount']),
                        "status": "success",
                        "txids": [raw_txid],
                    })

                logger.warning(payout_results)
                return payout_results

    def drain_account(self, account, destination):
        drain_results = []
        message = ''
        send_mode = 128 # send all we have in account - fee

        if not is_valid_ton_address(destination):
            raise Exception(f"Address {destination} is not valid blockchain address")

        if not is_valid_ton_address(account):
            raise Exception(f"Address {account} is not valid blockchain address")

        if len(account) > 63:
            logger.warning(f"Account {account} is in raw format, try to get pub address")
            account = get_pub_address_by_raw_address(account)
            logger.warning(f"Pub address is {account}")

        if account == destination:
            logger.warning("Fee-deposit account, skip draining")
            return False

        if self.symbol == config["COIN_SYMBOL"]:

            have_crypto = self.get_ton_balance(account)

            if Decimal(config['MIN_TRANSFER_THRESHOLD']) > have_crypto:
                logger.warning(f"Balance {have_crypto} is lower than MIN_TRANSFER_THRESHOLD {Decimal(config['MIN_TRANSFER_THRESHOLD'])}, skip draining ")
                return False

            self.initialize_account(account)

            logger.warning(f'Start draining from {account} to {destination}')

            with fee_deposit_seqno_guard_for_address(
                account,
                self.get_fee_deposit_account('public'),
                reason="fee-deposit-ton-drain",
            ):
                account_mnemonic = self.get_mnemonic_from_address(account)
                mnemonics, _pub_k, _priv_k, account_wallet  = TonWallets.from_mnemonics(
                                                            account_mnemonic,
                                                            WalletVersionEnum('v4r2'), 0)
                account_seqno = self.toncenter.get_account_seqno(account)
                transaction = account_wallet.create_transfer_message(
                            to_addr=destination,
                            amount=to_nanotons(float(have_crypto)),
                            seqno=account_seqno,
                            payload=message,
                            send_mode=send_mode)
                boc = bytes_to_b64str(transaction["message"].to_boc(False))
                # old_acc_seqno = self.toncenter.get_account_seqno(account)
                tx_id = self.toncenter.send_message_with_hash(boc)
                raw_txid =  base64.b64decode(tx_id).hex()
            logger.warning(f'Message sent, hash: {raw_txid}')

            drain_results.append({
                "dest": destination,
                "amount": float(have_crypto),
                "status": "success",
                "txids": [raw_txid],
            })

            logger.warning(drain_results)
            return drain_results

        elif self.symbol in config['TOKENS'][config["CURRENT_TON_NETWORK"]].keys():

            logger.warning(f'Start draining {self.symbol} from {account} to {destination}')

            have_tokens = self.get_account_jetton_balance(account)

            if Decimal(config['MIN_TOKEN_TRANSFER_THRESHOLD']) > have_tokens:
                logger.warning(f"Token balance {have_tokens} is lower than MIN_TOKEN_TRANSFER_THRESHOLD {Decimal(config['MIN_TOKEN_TRANSFER_THRESHOLD'])}, skip draining ")
                return False

            fee_amount = Decimal(self.get_jetton_transaction_fee(account,
                                                                 destination,
                                                                 have_tokens))

            ton_balance = self.get_ton_balance(account)
            need_ton_balance = config['JETTON_TRANSACTION_NEED_BALANCE']

            if ton_balance < need_ton_balance:
                logger.warning(f"Have not enough TON on account {account} to cover fee, have {ton_balance}, need {need_ton_balance}")
                need_send = Decimal(need_ton_balance) - Decimal(ton_balance)
                if self.get_fee_deposit_coin_balance() < (need_send + config['TON_TRANSACTION_FEE']):
                    raise Exception(f"Have not enough TON on fee-deposit account to cover fee, need {need_send + config['TON_TRANSACTION_FEE']} have {self.get_fee_deposit_coin_balance()}")
                else:
                    self.make_multipayout_ton([{"dest": account, "amount": float(need_send)}], config['TON_TRANSACTION_FEE'])

            time.sleep(10) # wait for TON transfer to be processed, otherwise deploy wallet will fail due to not enough balance to pay fee
            self.initialize_account(account)

            with fee_deposit_seqno_guard_for_address(
                account,
                self.get_fee_deposit_account('public'),
                reason="fee-deposit-jetton-drain",
            ):
                account_mnemonic = self.get_mnemonic_from_address(account)
                mnemonics, _pub_k, _priv_k, account_wallet  = TonWallets.from_mnemonics(
                                                            account_mnemonic,
                                                            WalletVersionEnum('v4r2'), 0)
                account_seqno = self.toncenter.get_account_seqno(account)

                body = JettonWallet().create_transfer_body(
                            to_address=Address(destination),
                            jetton_amount=int((have_tokens) * 10**self.toncenter.jetton_master_decimals(self.jetton_master_address)),
                            response_address=Address(self.get_fee_deposit_account('public'))
                    )
                message = account_wallet.create_transfer_message(to_addr=self.toncenter.get_account_wallet_jetton_address(account, self.jetton_master_address),
                                           amount=to_nanotons(fee_amount), # just for fee, real amount will be in payload
                                           seqno=int(account_seqno),
                                           payload=body)

                boc = bytes_to_b64str(message["message"].to_boc(False))
                tx_id = self.toncenter.send_message_with_hash(boc)
                raw_txid =  base64.b64decode(tx_id).hex()
            logger.warning(f'Message sent, hash: {raw_txid}')

            drain_results.append({
                "dest": destination,
                "amount": float(have_tokens),
                "status": "success",
                "txids": [raw_txid],
            })
            logger.warning(drain_results)
            return drain_results

    def get_mnemonic_from_address(self, address):
        tries = 3
        for i in range(tries):
            try:
                pd = Wallets.query.filter_by(pub_address = address).first()
            except:
                if i < tries - 1: # i is zero indexed
                    db.session.rollback()
                    continue
                else:
                    db.session.rollback()
                    raise Exception("There was exception during query to the database, try again later")
            break
        return json.loads(Encryption.decrypt(pd.mnemonic))

    def get_dump(self):
        logger.warning('Start dumping wallets')
        all_wallets = {}
        tries = 3
        for i in range(tries):
            try:
                pd = Wallets.query.all()
            except:
                if i < tries - 1: # i is zero indexed
                    db.session.rollback()
                    continue
                else:
                    db.session.rollback()
                    raise Exception("There was exception during query to the database, try again later")
            break
        for wallet in pd:
            all_wallets.update({wallet.pub_address: {'public_address': wallet.pub_address,
                                                    'mnemonic_phrase': json.loads(Encryption.decrypt(wallet.mnemonic))}})
        return all_wallets

    def save_wallet_to_db(self, _pub_address, _raw_address, mnemonic, addr_type):
        logger.warning(f'Saving {addr_type} wallet {_pub_address} to DB')
        crypto_str = self.symbol
        e = Encryption
        try:
            with app.app_context():
                db.session.add(Wallets(pub_address = _pub_address,
                                       raw_address = _raw_address,
                                       mnemonic = e.encrypt(json.dumps(mnemonic)),
                                       type = addr_type,
                                       ))
                db.session.add(Accounts(pub_address = _pub_address,
                                        raw_address = _raw_address,
                                        crypto = crypto_str,
                                        amount = 0,
                                        type = addr_type,
                                        ))
                db.session.commit()
                db.session.close()
                db.engine.dispose()
        finally:
            with app.app_context():
                db.session.remove()
                db.engine.dispose()

        logger.info(f'Wallet, {_pub_address} has been added to DB')


    def create_wallet(self, addr_type):
        mnemonic, pub_k, priv_k, wallet = TonWallets.create(WalletVersionEnum.v4r2, workchain=0)
        pub_address = wallet.address.to_string(True, True, False)
        raw_address = wallet.address.to_string(False, False, False)
        self.save_wallet_to_db(pub_address, raw_address, mnemonic, addr_type)
        return pub_address
