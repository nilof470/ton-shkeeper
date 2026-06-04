import os
import unittest

from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

from tests.test_payout_execution_contract import (
    CONSUMER,
    KEY_ID,
    SECRET,
    reset_modules,
)


class TonPayoutExecutionSchemaTests(unittest.TestCase):
    def setUp(self):
        self.test_database = (
            "/private/tmp/"
            f"ton-shkeeper-payout-execution-schema-{self._testMethodName}.db"
        )
        if os.path.exists(self.test_database):
            os.unlink(self.test_database)

        from app.config import config

        config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{self.test_database}"
        config["PAYOUT_CONSUMER_KEYS"] = {
            CONSUMER: {
                "rails": ["TON-USDT"],
                "keys": {KEY_ID: SECRET},
            }
        }
        config["TON_USDT_PAYOUT_QUEUE"] = "ton_usdt_payouts"
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

    def tearDown(self):
        with self.app.app_context():
            self.db.session.remove()
            self.db.drop_all()
            self.db.engine.dispose()
        if os.path.exists(self.test_database):
            os.unlink(self.test_database)

    def test_payout_execution_tables_constraints_and_indexes_are_created(self):
        with self.app.app_context():
            inspector = inspect(self.db.engine)

            self.assertIn("payout_execution", inspector.get_table_names())
            self.assertIn("payout_auth_nonce", inspector.get_table_names())
            self.assertIn("payout_callback_outbox", inspector.get_table_names())
            self.assertEqual(
                inspector.get_pk_constraint("payout_execution")["constrained_columns"],
                ["execution_id"],
            )

            unique_constraints = {
                tuple(item["column_names"])
                for item in inspector.get_unique_constraints("payout_execution")
            }
            self.assertIn(("consumer", "external_id"), unique_constraints)

            indexes = {
                tuple(item["column_names"])
                for item in inspector.get_indexes("payout_execution")
            }
            self.assertIn(("state", "state_updated_at"), indexes)
            self.assertIn(("reconciliation_required", "state_updated_at"), indexes)

            columns = {column["name"] for column in inspector.get_columns("payout_execution")}
            for column in [
                "request_hash",
                "sidecar_payload_hash",
                "state_version",
                "source_wallet",
                "jetton_master",
                "source_seqno",
                "valid_until",
                "signed_boc_ref",
                "signed_boc_hash",
                "message_hash",
                "broadcast_attempted_at",
                "chain_check_metadata",
                "reconciliation_required",
            ]:
                self.assertIn(column, columns)

    def test_consumer_external_id_unique_constraint_rejects_duplicate_submit_key(self):
        from app.models import PayoutExecution

        def row(execution_id):
            return PayoutExecution(
                execution_id=execution_id,
                consumer=CONSUMER,
                external_id="WD-1",
                request_hash=f"request-hash-{execution_id}",
                sidecar_payload_hash=f"sidecar-hash-{execution_id}",
                state="RECEIVED",
                state_version=1,
                state_transition_id=f"transition-{execution_id}",
                state_updated_at="2026-06-04T00:00:00.000000Z",
                source_wallet="fee_deposit",
                jetton_master="jetton-master",
                chain_id_or_network_id="TON",
                canonical_payload_json="{}",
                chain_check_metadata="{}",
                payout_queue="ton_usdt_payouts",
                reconciliation_required=False,
                message_hashes_json="[]",
            )

        with self.app.app_context():
            self.db.session.add(row("execution-1"))
            self.db.session.add(row("execution-2"))
            with self.assertRaises(IntegrityError):
                self.db.session.commit()
            self.db.session.rollback()


if __name__ == "__main__":
    unittest.main()
