from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests.test_payout_execution_contract import (
    CONSUMER,
    KEY_ID,
    SECRET,
    TEST_DATABASE,
    reset_modules,
)


DESTINATION = "EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c"


class FakeCoin:
    def __init__(self, _symbol):
        self.symbol = _symbol

    def make_multipayout_jetton(self, payout_list, _fee):
        return [
            {
                "dest": payout["dest"],
                "amount": float(payout["amount"]),
                "status": "success",
                "txids": ["message-hash-1"],
            }
            for payout in payout_list
        ]


class FakeSignature:
    def __init__(self, name, args, calls):
        self.name = name
        self.args = args
        self.calls = calls
        self.options = {}

    def set(self, **kwargs):
        self.options.update(kwargs)
        return self

    def apply_async(self):
        self.calls.append(self)
        return SimpleNamespace(id="task-1")


class TonPayoutCallbackOutboxTests(unittest.TestCase):
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
        config["PAYOUT_CALLBACK_MAX_ATTEMPTS"] = 3
        config["PAYOUT_CALLBACK_RETRY_DELAY_SEC"] = 1
        config["PAYOUT_CALLBACK_TIMEOUT_SEC"] = 1
        config["PAYOUT_CALLBACK_SWEEP_LIMIT"] = 10
        config["PAYOUT_CALLBACK_CLAIM_TTL_SEC"] = 60
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
        if os.path.exists(TEST_DATABASE):
            os.unlink(TEST_DATABASE)

    def test_post_payout_results_records_failure_without_sleep_loop(self):
        outbox = __import__("app.payout_callback_outbox", fromlist=["create_payout_callback"])
        tasks = __import__("app.tasks", fromlist=["post_payout_results"])

        with self.app.app_context():
            outbox_id = outbox.create_payout_callback(
                [{"dest": DESTINATION, "status": "success", "txids": ["message-hash-1"]}],
                "TON-USDT",
            )
            with patch.object(
                outbox.requests,
                "post",
                side_effect=RuntimeError("shkeeper unavailable"),
            ):
                with patch.object(
                    tasks.time,
                    "sleep",
                    side_effect=AssertionError("callback task must not sleep-loop"),
                ):
                    result = tasks.post_payout_results.run(outbox_id)

            stored = outbox.get_payout_callback(outbox_id)

        self.assertEqual(result["status"], "PENDING")
        self.assertEqual(stored["status"], "PENDING")
        self.assertEqual(stored["attempts"], 1)
        self.assertEqual(stored["last_error"], "shkeeper unavailable")
        self.assertIsNotNone(stored["next_attempt_at"])

    def test_queue_payout_callback_keeps_outbox_when_enqueue_fails(self):
        outbox = __import__("app.payout_callback_outbox", fromlist=["get_payout_callback"])
        tasks = __import__("app.tasks", fromlist=["queue_payout_callback"])

        with self.app.app_context():
            with patch.object(
                tasks.post_payout_results,
                "delay",
                side_effect=RuntimeError("redis unavailable"),
            ):
                outbox_id = tasks.queue_payout_callback(
                    [{"dest": DESTINATION, "status": "success", "txids": ["message-hash-1"]}],
                    "TON-USDT",
                )
            stored = outbox.get_payout_callback(outbox_id)

        self.assertEqual(stored["status"], "PENDING")
        self.assertEqual(stored["symbol"], "TON-USDT")

    def test_due_dispatcher_recovers_pending_callback(self):
        outbox = __import__("app.payout_callback_outbox", fromlist=["get_payout_callback"])
        tasks = __import__("app.tasks", fromlist=["dispatch_due_payout_callbacks"])

        with self.app.app_context():
            with patch.object(
                tasks.post_payout_results,
                "delay",
                side_effect=RuntimeError("redis unavailable"),
            ):
                outbox_id = tasks.queue_payout_callback(
                    [{"dest": DESTINATION, "status": "success", "txids": ["message-hash-1"]}],
                    "TON-USDT",
                )

            response = SimpleNamespace(status_code=200, text="accepted")
            with patch.object(outbox.requests, "post", return_value=response):
                results = tasks.dispatch_due_payout_callbacks.run(limit=10)
            stored = outbox.get_payout_callback(outbox_id)

        self.assertEqual(stored["status"], "SENT")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "SENT")

    def test_due_dispatcher_does_not_let_active_claims_starve_due_rows(self):
        outbox = __import__("app.payout_callback_outbox", fromlist=["get_payout_callback"])
        tasks = __import__("app.tasks", fromlist=["dispatch_due_payout_callbacks"])
        from app.models import PayoutCallbackOutbox

        with self.app.app_context():
            active_id = outbox.create_payout_callback(
                [{"dest": DESTINATION, "status": "success", "txids": ["active"]}],
                "TON-USDT",
            )
            due_id = outbox.create_payout_callback(
                [{"dest": DESTINATION, "status": "success", "txids": ["due"]}],
                "TON-USDT",
            )
            active = PayoutCallbackOutbox.query.get(active_id)
            active.status = "DISPATCHING"
            active.claimed_at = outbox.utc_now()
            active.claim_token = "active-worker"
            active.next_attempt_at = outbox.utc_now()
            self.db.session.commit()

            response = SimpleNamespace(status_code=200, text="accepted")
            with patch.object(outbox.requests, "post", return_value=response):
                results = tasks.dispatch_due_payout_callbacks.run(limit=1)

            active_stored = outbox.get_payout_callback(active_id)
            due_stored = outbox.get_payout_callback(due_id)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], due_id)
        self.assertEqual(active_stored["status"], "DISPATCHING")
        self.assertEqual(active_stored["claim_token"], "active-worker")
        self.assertEqual(due_stored["status"], "SENT")

    def test_due_dispatcher_reclaims_expired_claim(self):
        outbox = __import__("app.payout_callback_outbox", fromlist=["get_payout_callback"])
        tasks = __import__("app.tasks", fromlist=["dispatch_due_payout_callbacks"])
        from app.models import PayoutCallbackOutbox

        with self.app.app_context():
            outbox_id = outbox.create_payout_callback(
                [{"dest": DESTINATION, "status": "success", "txids": ["expired"]}],
                "TON-USDT",
            )
            row = PayoutCallbackOutbox.query.get(outbox_id)
            row.status = "DISPATCHING"
            row.claimed_at = outbox.utc_now() - timedelta(seconds=61)
            row.claim_token = "dead-worker"
            self.db.session.commit()

            response = SimpleNamespace(status_code=200, text="accepted")
            with patch.object(outbox.requests, "post", return_value=response):
                results = tasks.dispatch_due_payout_callbacks.run(limit=1)
            stored = outbox.get_payout_callback(outbox_id)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "SENT")
        self.assertEqual(stored["status"], "SENT")

    def test_make_multipayout_records_outbox_and_finishes_before_notification(self):
        outbox = __import__("app.payout_callback_outbox", fromlist=["get_payout_callback"])
        tasks = __import__("app.tasks", fromlist=["make_multipayout"])

        scheduled = []
        original_coin = tasks.Coin
        original_post_task = tasks.post_payout_results
        try:
            tasks.Coin = FakeCoin
            tasks.post_payout_results = SimpleNamespace(
                delay=lambda outbox_id: scheduled.append(outbox_id)
            )
            with self.app.app_context():
                result = tasks.make_multipayout.run(
                    "TON-USDT",
                    [{"dest": DESTINATION, "amount": Decimal("1.25")}],
                    Decimal("0.04"),
                )
                stored = outbox.get_payout_callback(scheduled[0])
        finally:
            tasks.Coin = original_coin
            tasks.post_payout_results = original_post_task

        self.assertEqual(result[0]["status"], "success")
        self.assertEqual(len(scheduled), 1)
        self.assertEqual(stored["status"], "PENDING")
        self.assertIn("message-hash-1", stored["payload_json"])

    def test_make_multipayout_returns_success_when_outbox_write_fails_after_transfer(self):
        tasks = __import__("app.tasks", fromlist=["make_multipayout"])
        queue_callback = tasks.make_multipayout.run.__globals__["queue_payout_callback"]

        original_coin = tasks.Coin
        original_create = queue_callback.__globals__["create_payout_callback"]
        try:
            tasks.Coin = FakeCoin
            queue_callback.__globals__["create_payout_callback"] = (
                lambda *args, **kwargs: (_ for _ in ()).throw(
                    RuntimeError("database locked")
                )
            )
            with self.app.app_context():
                result = tasks.make_multipayout.run(
                    "TON-USDT",
                    [{"dest": DESTINATION, "amount": Decimal("1.25")}],
                    Decimal("0.04"),
                )
        finally:
            tasks.Coin = original_coin
            queue_callback.__globals__["create_payout_callback"] = original_create

        self.assertEqual(result[0]["status"], "success")
        self.assertEqual(result[0]["txids"], ["message-hash-1"])

    def test_api_routes_ton_usdt_legacy_multipayout_to_dedicated_queue(self):
        from flask import Flask, g

        payout_module = __import__("app.api.payout", fromlist=["multipayout"])
        calls = []
        original_make_multipayout = payout_module.make_multipayout
        original_worker_ready = payout_module.ton_usdt_payout_worker_ready
        try:
            payout_module.make_multipayout = SimpleNamespace(
                s=lambda *args: FakeSignature("make_multipayout", args, calls)
            )
            payout_module.ton_usdt_payout_worker_ready = lambda: True
            app = Flask(__name__)
            with app.test_request_context(
                "/TON-USDT/multipayout",
                method="POST",
                json=[{"dest": DESTINATION, "amount": "1.25"}],
            ):
                g.symbol = "TON-USDT"
                result = payout_module.multipayout()
        finally:
            payout_module.make_multipayout = original_make_multipayout
            payout_module.ton_usdt_payout_worker_ready = original_worker_ready

        self.assertEqual(result, {"task_id": "task-1"})
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].options, {"queue": "ton_usdt_payouts"})

    def test_api_rejects_ton_usdt_legacy_payout_when_worker_missing(self):
        from flask import Flask, g

        payout_module = __import__("app.api.payout", fromlist=["payout"])
        calls = []
        original_make_multipayout = payout_module.make_multipayout
        original_worker_ready = payout_module.ton_usdt_payout_worker_ready
        try:
            payout_module.make_multipayout = SimpleNamespace(
                s=lambda *args: FakeSignature("make_multipayout", args, calls)
            )
            payout_module.ton_usdt_payout_worker_ready = lambda: False
            app = Flask(__name__)
            with app.test_request_context(
                f"/TON-USDT/payout/{DESTINATION}/1.25",
                method="POST",
            ):
                g.symbol = "TON-USDT"
                payload, status_code = payout_module.payout(DESTINATION, Decimal("1.25"))
        finally:
            payout_module.make_multipayout = original_make_multipayout
            payout_module.ton_usdt_payout_worker_ready = original_worker_ready

        self.assertEqual(status_code, 503)
        self.assertEqual(payload["code"], "PAYOUT_WORKER_UNAVAILABLE")
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
