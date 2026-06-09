from __future__ import annotations

import os
import unittest
from unittest.mock import Mock, patch

from sqlalchemy.exc import OperationalError, PendingRollbackError

from tests.test_payout_execution_contract import (
    CONSUMER,
    KEY_ID,
    SECRET,
    TEST_DATABASE,
    payload,
    reset_modules,
)


class RetryRequested(Exception):
    pass


class FakeRequest:
    id = "task-123"
    retries = 0


class FakeTask:
    request = FakeRequest()

    def __init__(self):
        self.retry_call = None

    def retry(self, *, exc, countdown):
        self.retry_call = {"exc": exc, "countdown": countdown}
        raise RetryRequested()


def transient_operational_error():
    return OperationalError(
        "select 1",
        {},
        RuntimeError("server has gone away"),
        connection_invalidated=True,
    )


class TonPayoutExecutionTaskRetryTests(unittest.TestCase):
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
        config["PAYOUT_EXECUTION_PREFLIGHT_CHECKS_ENABLED"] = False
        config["PAYOUT_EXECUTION_AUTO_ENQUEUE_ENABLED"] = False
        reset_modules()
        import sys

        sys.modules.pop("app.tasks", None)

        from app import create_app
        from app.db_import import db
        import werkzeug

        if not hasattr(werkzeug, "__version__"):
            werkzeug.__version__ = "3"

        self.app = create_app()
        self.app.config.update(TESTING=True)
        self.ctx = self.app.app_context()
        self.ctx.push()
        self.db = db
        db.drop_all()
        db.create_all()

    def tearDown(self):
        self.db.session.remove()
        self.db.drop_all()
        self.db.engine.dispose()
        self.ctx.pop()
        if os.path.exists(TEST_DATABASE):
            os.unlink(TEST_DATABASE)

    def set_execution_fields(self, execution_id, **fields):
        from app.models import PayoutExecution

        row = PayoutExecution.query.filter_by(execution_id=execution_id).first()
        self.assertIsNotNone(row)
        for key, value in fields.items():
            setattr(row, key, value)
        self.db.session.commit()

    def test_operational_error_rolls_back_removes_session_and_retries_when_store_allows_retry(self):
        from app import tasks
        from app.models import db

        exc = transient_operational_error()
        fake_task = FakeTask()

        with patch("app.tasks.Coin", return_value=Mock()):
            with patch("app.payout_execution.PayoutExecutionStore.execute", side_effect=exc):
                with patch(
                    "app.payout_execution.PayoutExecutionStore.recover_task_owned_transient_failure",
                    return_value="retry",
                ) as recover:
                    with patch.object(
                        db.session,
                        "rollback",
                        wraps=db.session.rollback,
                    ) as rollback:
                        with patch.object(
                            db.session,
                            "remove",
                            wraps=db.session.remove,
                        ) as remove:
                            with self.assertRaises(RetryRequested):
                                tasks.run_execute_payout_execution(fake_task, "30")

        self.assertEqual(
            recover.call_args_list,
            [
                (("30",), {"lease_owner": "task-123"}),
                (("30",), {"lease_owner": "task-123"}),
            ],
        )
        self.assertIs(fake_task.retry_call["exc"], exc)
        self.assertEqual(fake_task.retry_call["countdown"], 5)
        self.assertGreaterEqual(rollback.call_count, 1)
        self.assertGreaterEqual(remove.call_count, 1)

    def test_pending_rollback_error_retries_after_session_cleanup(self):
        from app import tasks
        from app.models import db

        exc = PendingRollbackError(
            "Can't reconnect until invalid transaction is rolled back"
        )
        fake_task = FakeTask()

        with patch("app.tasks.Coin", return_value=Mock()):
            with patch("app.payout_execution.PayoutExecutionStore.execute", side_effect=exc):
                with patch(
                    "app.payout_execution.PayoutExecutionStore.recover_task_owned_transient_failure",
                    return_value="retry",
                ):
                    with patch.object(
                        db.session,
                        "rollback",
                        wraps=db.session.rollback,
                    ) as rollback:
                        with patch.object(
                            db.session,
                            "remove",
                            wraps=db.session.remove,
                        ) as remove:
                            with self.assertRaises(RetryRequested):
                                tasks.run_execute_payout_execution(fake_task, "30")

        self.assertIs(fake_task.retry_call["exc"], exc)
        self.assertEqual(fake_task.retry_call["countdown"], 5)
        self.assertGreaterEqual(rollback.call_count, 1)
        self.assertGreaterEqual(remove.call_count, 1)

    def test_transient_db_error_is_not_retried_when_store_detects_unsafe_evidence(self):
        from app import tasks

        exc = transient_operational_error()
        fake_task = FakeTask()

        with patch("app.tasks.Coin", return_value=Mock()):
            with patch("app.payout_execution.PayoutExecutionStore.execute", side_effect=exc):
                with patch(
                    "app.payout_execution.PayoutExecutionStore.recover_task_owned_transient_failure",
                    return_value="raise",
                ):
                    with self.assertRaises(OperationalError):
                        tasks.run_execute_payout_execution(fake_task, "30")

        self.assertIsNone(fake_task.retry_call)

    def test_retry_attempt_recovers_same_owner_signing_before_execute(self):
        from app import tasks
        from app.payout_execution import PayoutExecutionStore

        accepted = PayoutExecutionStore.submit(
            payload(external_id="WD-same-owner-signing"),
            authenticated_consumer=CONSUMER,
            endpoint_symbol="TON-USDT",
        )
        execution_id = accepted["execution_id"]
        self.set_execution_fields(
            execution_id,
            state="SIGNING",
            lease_owner="task-123",
            lease_expires_at="2999-01-01T00:00:00.000000Z",
            attempt_id="attempt-1",
        )
        fake_task = FakeTask()

        with patch("app.tasks.Coin", return_value=Mock()):
            with patch(
                "app.payout_execution.PayoutExecutionStore.execute",
                return_value={"status": "OK"},
            ) as execute:
                result = tasks.run_execute_payout_execution(fake_task, execution_id)

        row = PayoutExecutionStore._get_row(execution_id)
        self.assertEqual(result, {"status": "OK"})
        self.assertEqual(row.state, "RECEIVED")
        self.assertIsNone(row.lease_owner)
        self.assertIsNone(row.lease_expires_at)
        self.assertIsNone(row.attempt_id)
        execute.assert_called_once()

    def test_first_row_load_operational_error_is_retried_without_mutating_execution(self):
        from app import tasks
        from app.payout_execution import PayoutExecutionStore

        accepted = PayoutExecutionStore.submit(
            payload(external_id="WD-first-row-load"),
            authenticated_consumer=CONSUMER,
            endpoint_symbol="TON-USDT",
        )
        execution_id = accepted["execution_id"]
        exc = transient_operational_error()
        fake_task = FakeTask()
        original_get_row = PayoutExecutionStore._get_row
        calls = {"count": 0}

        def flaky_get_row(execution_id):
            calls["count"] += 1
            if calls["count"] == 1:
                raise exc
            return original_get_row(execution_id)

        with patch("app.tasks.Coin", return_value=Mock()):
            with patch.object(PayoutExecutionStore, "_get_row", side_effect=flaky_get_row):
                with self.assertRaises(RetryRequested):
                    tasks.run_execute_payout_execution(fake_task, execution_id)

        self.assertIs(fake_task.retry_call["exc"], exc)
        self.assertEqual(calls["count"], 1)
        row = PayoutExecutionStore._get_row(execution_id)
        self.assertEqual(row.state, "RECEIVED")
