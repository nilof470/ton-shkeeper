from __future__ import annotations

from decimal import Decimal
import os
import unittest
from unittest.mock import patch

from tests.test_payout_execution_contract import (
    CONSUMER,
    DESTINATION,
    KEY_ID,
    SECRET,
    TEST_DATABASE,
    payload,
    reset_modules,
)


class FakePreflightCoin:
    def __init__(self, *, jetton_balance="100", ton_balance="1", fee="0.04"):
        self.jetton_balance = Decimal(jetton_balance)
        self.ton_balance = Decimal(ton_balance)
        self.fee = Decimal(fee)

    def get_fee_deposit_account(self, address_type):
        return {"public": "EQFEEDEPOSIT", "raw": "0:fee-deposit-raw"}[address_type]

    def get_fee_deposit_jetton_balance(self):
        return self.jetton_balance

    def get_fee_deposit_coin_balance(self):
        return self.ton_balance

    def get_jetton_transaction_fee(self, source_addr=None, dest_addr=None, amount=None):
        return self.fee


class FakeStatusToncenter:
    def __init__(
        self,
        *,
        transfer=None,
        generic_tx=None,
        decimals=6,
        latest_masterchain_seqno=66018445,
    ):
        self.transfer = transfer
        self.generic_tx = generic_tx or {
            "hash": "generic-message-tx",
            "mc_block_seqno": 66018441,
        }
        self.decimals = decimals
        self.latest_masterchain_seqno = latest_masterchain_seqno

    def get_transaction_by_hash(self, _message_hash):
        return self.generic_tx

    def get_masterchain_head(self):
        return self.latest_masterchain_seqno

    def get_jetton_transaction_by_hash(self, _message_hash, _jetton_master):
        if self.transfer is None:
            raise Exception("jetton transfer not indexed yet")
        return self.transfer

    def jetton_master_decimals(self, _jetton_master):
        return self.decimals


class FakeStatusCoin:
    def __init__(self, toncenter):
        self.toncenter = toncenter


class TonPayoutStatusConfirmationTests(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TEST_DATABASE):
            os.unlink(TEST_DATABASE)

        from app.config import config

        config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{TEST_DATABASE}"
        config["PAYOUT_CONSUMER_KEYS"] = {
            CONSUMER: {
                "rails": ["TON-USDT"],
                "keys": {KEY_ID: SECRET},
            }
        }
        config["PAYOUT_AUTH_MAX_AGE_SECONDS"] = 300
        config["TON_USDT_PAYOUT_QUEUE"] = "ton_usdt_payouts"
        config["PAYOUT_EXECUTION_AUTO_ENQUEUE_ENABLED"] = False
        config["PAYOUT_EXECUTION_PREFLIGHT_CHECKS_ENABLED"] = True
        config["TON_USDT_PAYOUT_MIN_CONFIRMATIONS"] = 1
        reset_modules()

        from app import create_app
        from app.db_import import db
        import werkzeug

        if not hasattr(werkzeug, "__version__"):
            werkzeug.__version__ = "3"

        self.app = create_app()
        self.app.config.update(TESTING=True)
        self.db = db
        with self.app.app_context():
            db.drop_all()
            db.create_all()

        self.store_module = __import__("app.payout_execution", fromlist=["PayoutExecutionStore"])
        self.status_module = __import__("app.payout_status", fromlist=["run_ton_usdt_preflight_checks"])

    def tearDown(self):
        with self.app.app_context():
            self.db.session.remove()
            self.db.drop_all()
            self.db.engine.dispose()
        if os.path.exists(TEST_DATABASE):
            os.unlink(TEST_DATABASE)

    def canonical(self, **overrides):
        from app.payout_contract import canonical_payload

        return canonical_payload(payload(**overrides), endpoint_symbol="TON-USDT")

    def create_broadcasted_execution(self, **fields):
        with self.app.app_context():
            created = self.store_module.PayoutExecutionStore.submit(
                payload(),
                authenticated_consumer=CONSUMER,
                endpoint_symbol="TON-USDT",
            )
            self.execution_id = created["execution_id"]
            from app.models import PayoutExecution

            row = PayoutExecution.query.filter_by(execution_id=self.execution_id).first()
            defaults = {
                "state": "BROADCASTED",
                "message_hash": "message-hash-100",
                "message_hashes_json": '["message-hash-100"]',
                "jetton_wallet": "0:jetton-wallet",
                "valid_until": "600",
                "source_seqno": 100,
                "masterchain_seqno": 66018441,
            }
            defaults.update(fields)
            for key, value in defaults.items():
                setattr(row, key, value)
            self.db.session.commit()
            return row

    def test_preflight_rejects_invalid_destination(self):
        with patch.object(self.status_module, "is_valid_ton_address", return_value=False):
            with self.assertRaises(self.status_module.PayoutStatusError) as raised:
                self.status_module.run_ton_usdt_preflight_checks(
                    self.canonical(),
                    coin=FakePreflightCoin(),
                    worker_ready=lambda: True,
                )

        self.assertEqual(raised.exception.code, "INVALID_DESTINATION")

    def test_preflight_rejects_insufficient_jetton_balance(self):
        with patch.object(self.status_module, "is_valid_ton_address", return_value=True):
            with self.assertRaises(self.status_module.PayoutStatusError) as raised:
                self.status_module.run_ton_usdt_preflight_checks(
                    self.canonical(amount="12.000000"),
                    coin=FakePreflightCoin(jetton_balance="1"),
                    worker_ready=lambda: True,
                )

        self.assertEqual(raised.exception.code, "INSUFFICIENT_JETTON_BALANCE")

    def test_preflight_rejects_insufficient_ton_fee_balance(self):
        with patch.object(self.status_module, "is_valid_ton_address", return_value=True):
            with self.assertRaises(self.status_module.PayoutStatusError) as raised:
                self.status_module.run_ton_usdt_preflight_checks(
                    self.canonical(),
                    coin=FakePreflightCoin(ton_balance="0.001", fee="0.04"),
                    worker_ready=lambda: True,
                )

        self.assertEqual(raised.exception.code, "INSUFFICIENT_TON_FEE_BALANCE")

    def test_preflight_maps_provider_error_to_unavailable(self):
        class BrokenCoin(FakePreflightCoin):
            def get_fee_deposit_jetton_balance(self):
                raise RuntimeError("toncenter down")

        with patch.object(self.status_module, "is_valid_ton_address", return_value=True):
            with self.assertRaises(self.status_module.PayoutStatusError) as raised:
                self.status_module.run_ton_usdt_preflight_checks(
                    self.canonical(),
                    coin=BrokenCoin(),
                    worker_ready=lambda: True,
                )

        self.assertEqual(raised.exception.code, "PAYOUT_PREFLIGHT_UNAVAILABLE")
        self.assertEqual(raised.exception.status_code, 503)

    def test_preflight_rejects_unavailable_worker(self):
        with patch.object(self.status_module, "is_valid_ton_address", return_value=True):
            with self.assertRaises(self.status_module.PayoutStatusError) as raised:
                self.status_module.run_ton_usdt_preflight_checks(
                    self.canonical(),
                    coin=FakePreflightCoin(),
                    worker_ready=lambda: False,
                )

        self.assertEqual(raised.exception.code, "PAYOUT_WORKER_UNAVAILABLE")
        self.assertEqual(raised.exception.status_code, 503)

    def test_status_stays_confirming_without_matching_jetton_transfer(self):
        self.create_broadcasted_execution()
        coin = FakeStatusCoin(FakeStatusToncenter(transfer=None))

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.status(
                self.execution_id,
                authenticated_consumer=CONSUMER,
                endpoint_symbol="TON-USDT",
                coin=coin,
            )

        self.assertEqual(status["state"], "CONFIRMING")
        self.assertFalse(status["chain_check_metadata"]["transfer_match"])
        self.assertEqual(status["message_hash"], "message-hash-100")

    def test_status_confirms_only_matching_jetton_transfer(self):
        self.create_broadcasted_execution()
        transfer = {
            "source": "0:jetton-wallet",
            "destination": DESTINATION,
            "amount": "12345678",
            "jetton_master": "kQDXn-tVCycUFu1PrKI9R-hnk9lP6MxqSEbUjkWtkcmuWdvu",
            "transaction_hash": "message-hash-100",
        }
        coin = FakeStatusCoin(FakeStatusToncenter(transfer=transfer, decimals=6))

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.status(
                self.execution_id,
                authenticated_consumer=CONSUMER,
                endpoint_symbol="TON-USDT",
                coin=coin,
            )

        self.assertEqual(status["state"], "CONFIRMED")
        self.assertTrue(status["chain_check_metadata"]["transfer_match"])
        self.assertGreaterEqual(status["chain_check_metadata"]["confirmations"], 1)
        self.assertEqual(status["message_hashes"], ["message-hash-100"])

    def test_matching_jetton_transfer_waits_for_min_confirmations(self):
        from app.config import config

        config["TON_USDT_PAYOUT_MIN_CONFIRMATIONS"] = 5
        self.create_broadcasted_execution()
        transfer = {
            "source": "0:jetton-wallet",
            "destination": DESTINATION,
            "amount": "12345678",
            "jetton_master": "kQDXn-tVCycUFu1PrKI9R-hnk9lP6MxqSEbUjkWtkcmuWdvu",
            "transaction_hash": "message-hash-100",
        }
        coin = FakeStatusCoin(
            FakeStatusToncenter(
                transfer=transfer,
                decimals=6,
                latest_masterchain_seqno=66018442,
            )
        )

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.status(
                self.execution_id,
                authenticated_consumer=CONSUMER,
                endpoint_symbol="TON-USDT",
                coin=coin,
            )

        self.assertEqual(status["state"], "CONFIRMING")
        self.assertTrue(status["chain_check_metadata"]["transfer_match"])
        self.assertEqual(status["chain_check_metadata"]["confirmations"], 2)
        self.assertEqual(status["chain_check_metadata"]["min_confirmations"], 5)

    def test_indexed_mismatched_jetton_transfer_fails_terminally(self):
        self.create_broadcasted_execution()
        transfer = {
            "source": "0:someone-else",
            "destination": DESTINATION,
            "amount": "12345678",
            "jetton_master": "kQDXn-tVCycUFu1PrKI9R-hnk9lP6MxqSEbUjkWtkcmuWdvu",
            "transaction_hash": "message-hash-100",
        }
        coin = FakeStatusCoin(FakeStatusToncenter(transfer=transfer, decimals=6))

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.status(
                self.execution_id,
                authenticated_consumer=CONSUMER,
                endpoint_symbol="TON-USDT",
                coin=coin,
            )

        self.assertEqual(status["state"], "FAILED_CHAIN_TERMINAL")
        self.assertEqual(status["error_code"], "TON_USDT_TRANSFER_MISMATCH")
        self.assertFalse(status["chain_check_metadata"]["transfer_match"])


if __name__ == "__main__":
    unittest.main()
