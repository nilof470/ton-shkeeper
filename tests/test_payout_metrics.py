from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
import json
import os
import unittest

import prometheus_client

from tests.test_payout_execution_contract import (
    CONSUMER,
    KEY_ID,
    SECRET,
    reset_modules,
)


TEST_DATABASE = "/private/tmp/ton-shkeeper-payout-metrics.db"


class FakeRedis:
    def __init__(self, depth, messages=None):
        self.depth = depth
        self.messages = messages or []

    def llen(self, queue):
        return self.depth

    def lrange(self, queue, start, end):
        if start == 0 and end == 0:
            return self.messages[:1]
        if start == -1 and end == -1:
            return self.messages[-1:]
        return self.messages[start : end + 1]


def redis_message(enqueued_at):
    return json.dumps({"headers": {"payout_enqueued_at": enqueued_at}}).encode("utf-8")


class TonPayoutMetricsTests(unittest.TestCase):
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

        import app.api.metrics as metrics
        import app.payout_status as payout_status

        self.metrics = metrics
        self.payout_status = payout_status
        self.original_worker_ready = payout_status.ton_usdt_payout_worker_ready
        self.original_redis_from_url = metrics.redis.Redis.from_url
        self.original_fee_deposit_balances = metrics._ton_fee_deposit_balances
        payout_status.ton_usdt_payout_worker_ready = lambda: True
        metrics.redis.Redis.from_url = lambda *args, **kwargs: FakeRedis(0)
        metrics._ton_fee_deposit_balances = lambda: (
            Decimal("234.567"),
            Decimal("12.345"),
        )
        metrics._clear_payout_metrics()

    def tearDown(self):
        self.payout_status.ton_usdt_payout_worker_ready = self.original_worker_ready
        self.metrics.redis.Redis.from_url = self.original_redis_from_url
        self.metrics._ton_fee_deposit_balances = self.original_fee_deposit_balances
        self.metrics._clear_payout_metrics()
        with self.app.app_context():
            self.db.session.remove()
            self.db.drop_all()
            self.db.engine.dispose()
        if os.path.exists(TEST_DATABASE):
            os.unlink(TEST_DATABASE)

    def insert_execution(
        self,
        execution_id,
        state,
        updated_at,
        reconciliation_required=False,
        failure_class=None,
        error_code=None,
    ):
        from app.models import PayoutExecution

        self.db.session.add(
            PayoutExecution(
                execution_id=execution_id,
                consumer=CONSUMER,
                external_id=f"WD-{execution_id}",
                request_hash=f"request-{execution_id}",
                sidecar_payload_hash=f"sidecar-{execution_id}",
                state=state,
                state_version=1,
                state_transition_id=f"transition-{execution_id}",
                state_updated_at=updated_at,
                source_wallet="fee_deposit",
                jetton_master="jetton-master",
                chain_id_or_network_id="TON",
                canonical_payload_json="{}",
                chain_check_metadata="{}",
                payout_queue="ton_usdt_payouts",
                reconciliation_required=reconciliation_required,
                failure_class=failure_class,
                error_code=error_code,
            )
        )
        self.db.session.commit()

    def insert_callback(self, status, created_at):
        from app.models import PayoutCallbackOutbox

        self.db.session.add(
            PayoutCallbackOutbox(
                symbol="TON-USDT",
                payload_json="[]",
                status=status,
                attempts=1,
                created_at=created_at,
                updated_at=created_at,
            )
        )
        self.db.session.commit()

    def test_payout_metrics_expose_execution_outbox_and_worker_readiness(self):
        now = datetime(2026, 6, 4, 12, 0, 0)
        with self.app.app_context():
            self.insert_execution(
                "created-1",
                "RECEIVED",
                (now - timedelta(minutes=10)).isoformat() + "Z",
            )
            self.insert_execution(
                "created-2",
                "RECEIVED",
                (now - timedelta(minutes=5)).isoformat() + "Z",
            )
            self.insert_execution(
                "recon-1",
                "RECONCILIATION_REQUIRED",
                (now - timedelta(minutes=30)).isoformat() + "Z",
                reconciliation_required=True,
            )
            self.insert_execution(
                "confirmed-1",
                "CONFIRMED",
                (now - timedelta(hours=2)).isoformat() + "Z",
            )
            self.insert_callback("RETRY", now - timedelta(minutes=20))
            self.metrics.redis.Redis.from_url = lambda *args, **kwargs: FakeRedis(
                4,
                messages=[
                    redis_message((now - timedelta(minutes=4)).isoformat() + "Z"),
                    redis_message((now - timedelta(minutes=11)).isoformat() + "Z"),
                ],
            )

            self.metrics.update_payout_metrics(now=now)

        text = prometheus_client.generate_latest().decode()
        self.assertIn(
            'ton_payout_execution_count{reconciliation_required="false",state="RECEIVED"} 2.0',
            text,
        )
        self.assertIn(
            'ton_payout_non_terminal_oldest_age_seconds{state="RECEIVED"} 600.0',
            text,
        )
        self.assertIn("ton_payout_reconciliation_required_count 1.0", text)
        self.assertIn(
            'ton_payout_callback_outbox_backlog_count{status="RETRY"} 1.0',
            text,
        )
        self.assertIn(
            'ton_payout_callback_outbox_oldest_age_seconds{status="RETRY"} 1200.0',
            text,
        )
        self.assertIn('ton_payout_worker_ready{queue="ton_usdt_payouts"} 1.0', text)
        self.assertIn(
            'ton_payout_broker_queue_depth{queue="ton_usdt_payouts"} 4.0',
            text,
        )
        self.assertIn(
            'ton_payout_broker_queue_oldest_age_seconds{queue="ton_usdt_payouts"} 660.0',
            text,
        )
        self.assertIn(
            'ton_payout_hot_wallet_balance{asset="USDT",source_wallet="fee_deposit"} 234.567',
            text,
        )
        self.assertIn(
            'ton_payout_fee_wallet_balance{asset="TON",source_wallet="fee_deposit"} 12.345',
            text,
        )
        self.assertNotIn(
            'ton_payout_non_terminal_oldest_age_seconds{state="CONFIRMED"}',
            text,
        )

    def test_payout_failure_metrics_bound_error_code_labels(self):
        now = datetime(2026, 6, 4, 12, 0, 0)
        with self.app.app_context():
            self.insert_execution(
                "failed-preflight",
                "FAILED_PRE_BROADCAST",
                (now - timedelta(minutes=5)).isoformat() + "Z",
                failure_class="PREFLIGHT",
                error_code="INSUFFICIENT_JETTON_BALANCE",
            )
            self.insert_execution(
                "failed-weird-error",
                "FAILED_PRE_BROADCAST",
                (now - timedelta(minutes=4)).isoformat() + "Z",
                failure_class="PREFLIGHT",
                error_code="destination EQsecret leaked",
            )
            self.metrics.update_payout_metrics(now=now)

        text = prometheus_client.generate_latest().decode()
        self.assertIn(
            'ton_payout_failure_count{error_code="INSUFFICIENT_JETTON_BALANCE",failure_class="PREFLIGHT",state="FAILED_PRE_BROADCAST"} 1.0',
            text,
        )
        self.assertIn(
            'ton_payout_failure_count{error_code="OTHER",failure_class="PREFLIGHT",state="FAILED_PRE_BROADCAST"} 1.0',
            text,
        )
        self.assertNotIn("EQsecret", text)

    def test_broker_queue_depth_fails_open_when_redis_is_unavailable(self):
        self.metrics.redis.Redis.from_url = lambda *args, **kwargs: (
            _ for _ in ()
        ).throw(self.metrics.redis.exceptions.ConnectionError("redis down"))

        with self.app.app_context():
            self.metrics.update_payout_metrics(now=datetime(2026, 6, 4, 12, 0, 0))

        text = prometheus_client.generate_latest().decode()
        self.assertIn(
            'ton_payout_broker_queue_depth{queue="ton_usdt_payouts"} -1.0',
            text,
        )
        self.assertIn(
            'ton_payout_broker_queue_oldest_age_seconds{queue="ton_usdt_payouts"} -1.0',
            text,
        )

    def test_wallet_balance_metrics_fail_open_when_balance_collection_fails(self):
        self.metrics._ton_fee_deposit_balances = lambda: (_ for _ in ()).throw(
            RuntimeError("balance unavailable")
        )

        with self.app.app_context():
            self.metrics.update_payout_metrics(now=datetime(2026, 6, 4, 12, 0, 0))

        text = prometheus_client.generate_latest().decode()
        self.assertIn(
            'ton_payout_hot_wallet_balance{asset="USDT",source_wallet="fee_deposit"} -1.0',
            text,
        )
        self.assertIn(
            'ton_payout_fee_wallet_balance{asset="TON",source_wallet="fee_deposit"} -1.0',
            text,
        )

    def test_broker_queue_age_fails_open_when_message_is_unparseable(self):
        self.metrics.redis.Redis.from_url = lambda *args, **kwargs: FakeRedis(
            2,
            messages=[b"\xff"],
        )

        with self.app.app_context():
            self.metrics.update_payout_metrics(now=datetime(2026, 6, 4, 12, 0, 0))

        text = prometheus_client.generate_latest().decode()
        self.assertIn(
            'ton_payout_broker_queue_depth{queue="ton_usdt_payouts"} 2.0',
            text,
        )
        self.assertIn(
            'ton_payout_broker_queue_oldest_age_seconds{queue="ton_usdt_payouts"} -1.0',
            text,
        )

    def test_worker_and_queue_metrics_survive_db_collection_failure(self):
        now = datetime(2026, 6, 4, 12, 0, 0)
        with self.app.app_context():
            self.insert_execution(
                "recon-snapshot",
                "RECONCILIATION_REQUIRED",
                now - timedelta(minutes=30),
                reconciliation_required=True,
            )
            self.metrics.update_payout_metrics(now=now)

        original_query = self.metrics.db.session.query
        self.metrics.db.session.query = lambda *args, **kwargs: (
            _ for _ in ()
        ).throw(RuntimeError("db down"))
        self.metrics.redis.Redis.from_url = lambda *args, **kwargs: FakeRedis(
            5,
            messages=[
                redis_message((now - timedelta(minutes=3)).isoformat() + "Z"),
            ],
        )
        try:
            with self.app.app_context():
                with self.assertRaises(RuntimeError):
                    self.metrics.update_payout_metrics(
                        now=now + timedelta(minutes=1)
                    )
        finally:
            self.metrics.db.session.query = original_query

        text = prometheus_client.generate_latest().decode()
        self.assertIn(
            'ton_payout_execution_count{reconciliation_required="true",state="RECONCILIATION_REQUIRED"} 1.0',
            text,
        )
        self.assertIn('ton_payout_worker_ready{queue="ton_usdt_payouts"} 1.0', text)
        self.assertIn(
            'ton_payout_broker_queue_depth{queue="ton_usdt_payouts"} 5.0',
            text,
        )
        self.assertIn(
            'ton_payout_broker_queue_oldest_age_seconds{queue="ton_usdt_payouts"} 240.0',
            text,
        )

    def test_enqueue_execution_sets_broker_age_header(self):
        from app import tasks
        from app.payout_execution import PayoutExecutionStore

        calls = []

        class FakeTask:
            def apply_async(self, **kwargs):
                calls.append(kwargs)
                return "task-result"

        original_task = tasks.execute_payout_execution
        tasks.execute_payout_execution = FakeTask()
        try:
            result = PayoutExecutionStore.enqueue_execution("exec-1", "queue-a")
        finally:
            tasks.execute_payout_execution = original_task

        self.assertEqual(result, "task-result")
        self.assertEqual(calls[0]["args"], ["exec-1"])
        self.assertEqual(calls[0]["queue"], "queue-a")
        self.assertIn("payout_enqueued_at", calls[0]["headers"])


if __name__ == "__main__":
    unittest.main()
