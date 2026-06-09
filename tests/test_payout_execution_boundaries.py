from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
import os
import time
import unittest
from unittest.mock import patch

from sqlalchemy.exc import OperationalError

from tests.test_payout_execution_contract import (
    CONSUMER,
    DESTINATION,
    KEY_ID,
    SECRET,
    TEST_DATABASE,
    payload,
    reset_modules,
)


class DbapiErrorWithCode(Exception):
    pass


class FakeToncenter:
    def __init__(self):
        self.masterchain_head = 66018441
        self.seqno = 100

    def get_masterchain_head(self):
        return self.masterchain_head

    def get_account_seqno(self, _address):
        return self.seqno


class FakeCoin:
    def __init__(self, *, broadcast_error=None, broadcast_hash=None):
        self.toncenter = FakeToncenter()
        self.client = "ton-client"
        self.broadcast_error = broadcast_error
        self.broadcast_hash = broadcast_hash
        self.events = []

    def get_fee_deposit_account(self, address_type):
        return {
            "public": "EQFEEDEPOSIT",
            "raw": "0:fee-deposit-raw",
        }[address_type]

    def build_signed_payout(self, canonical, source_seqno, valid_until):
        self.events.append(
            (
                "build",
                canonical["destination"],
                canonical["amount"],
                source_seqno,
                valid_until,
            )
        )
        return {
            "boc": f"signed-boc:{source_seqno}:60",
            "message_hash": f"message-hash-{source_seqno}",
            "jetton_wallet": "0:jetton-wallet",
            "valid_until": 60,
        }

    def signed_payout_evidence(self, signed):
        return {
            "signed_boc_ref": f"not-retained:signed-ton-boc-sha256:{signed['message_hash']}",
            "signed_boc_hash": f"signed-boc-hash-{signed['message_hash']}",
            "message_hash": signed["message_hash"],
            "jetton_wallet": signed["jetton_wallet"],
            "valid_until": signed["valid_until"],
            "chain_check_metadata": {
                "signed_boc_artifact_retention": "NOT_RETAINED_SPENDABLE_BOC",
                "message_hash": signed["message_hash"],
            },
        }

    def broadcast_signed_payout(self, signed):
        self.events.append(("broadcast", signed["message_hash"]))
        if self.broadcast_error:
            raise self.broadcast_error
        return {"ok": True, "hash": self.broadcast_hash or signed["message_hash"]}


@contextmanager
def null_lock():
    yield


def recording_lock(events):
    @contextmanager
    def lock():
        events.append("lock-enter")
        try:
            yield
        finally:
            events.append("lock-exit")

    return lock


def failing_lock(exc):
    @contextmanager
    def lock():
        raise exc
        yield

    return lock


class TonPayoutExecutionBoundaryTests(unittest.TestCase):
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
        config["PAYOUT_EXECUTION_ORPHAN_RECOVERY_MIN_AGE_SEC"] = 60
        config["PAYOUT_EXECUTION_LEASE_TTL_SEC"] = 300
        config["TON_USDT_PAYOUT_VALID_UNTIL_CAP_SEC"] = 60
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

    def tearDown(self):
        with self.app.app_context():
            self.db.session.remove()
            self.db.drop_all()
            self.db.engine.dispose()
        if os.path.exists(TEST_DATABASE):
            os.unlink(TEST_DATABASE)

    def submit(self, **overrides):
        with self.app.app_context():
            return self.store_module.PayoutExecutionStore.submit(
                payload(**overrides),
                authenticated_consumer=CONSUMER,
                endpoint_symbol="TON-USDT",
            )

    def set_execution_fields(self, **fields):
        from app.models import PayoutExecution

        with self.app.app_context():
            row = PayoutExecution.query.filter_by(execution_id=self.execution_id).first()
            for key, value in fields.items():
                setattr(row, key, value)
            self.db.session.commit()

    def get_execution(self):
        from app.models import PayoutExecution

        with self.app.app_context():
            return PayoutExecution.query.filter_by(execution_id=self.execution_id).first()

    def create_execution(self):
        response = self.submit()
        self.execution_id = response["execution_id"]
        return response

    def test_transient_db_error_classifier_accepts_known_disconnect_code(self):
        exc = OperationalError(
            "select 1",
            {},
            DbapiErrorWithCode(2006, "MySQL server has gone away"),
        )

        self.assertTrue(self.store_module.is_transient_db_error(exc))

    def test_transient_db_error_classifier_rejects_generic_operational_error(self):
        exc = OperationalError(
            "select 1",
            {},
            DbapiErrorWithCode(1146, "table does not exist"),
        )

        self.assertFalse(self.store_module.is_transient_db_error(exc))

    def test_execute_persists_seqno_signed_boc_and_broadcast_markers(self):
        self.create_execution()
        coin = FakeCoin()

        with self.app.app_context():
            with patch.object(self.store_module.time, "time", return_value=0):
                status = self.store_module.PayoutExecutionStore.execute(
                    self.execution_id,
                    coin=coin,
                    lock_factory=null_lock,
                    lease_owner="worker-1",
                )

        self.assertEqual(status["state"], "BROADCASTED")
        self.assertEqual(status["source_seqno"], 100)
        self.assertEqual(status["masterchain_seqno"], 66018441)
        self.assertEqual(status["valid_until"], 60)
        self.assertEqual(status["message_hash"], "message-hash-100")
        self.assertEqual(status["jetton_wallet"], "0:jetton-wallet")
        self.assertTrue(status["signed_boc_ref"].startswith("not-retained:"))
        self.assertEqual(status["broadcast_provider"], "toncenter")
        self.assertEqual(status["message_hashes"], ["message-hash-100"])
        self.assertEqual(
            coin.events,
            [
                ("build", DESTINATION, "12.345678", 100, 60),
                ("broadcast", "message-hash-100"),
            ],
        )

    def test_execute_holds_seqno_lock_around_sign_and_broadcast(self):
        self.create_execution()
        coin = FakeCoin()
        lock_events = []

        with self.app.app_context():
            with patch.object(self.store_module.time, "time", return_value=0):
                status = self.store_module.PayoutExecutionStore.execute(
                    self.execution_id,
                    coin=coin,
                    lock_factory=recording_lock(lock_events),
                    lease_owner="worker-1",
                )

        self.assertEqual(status["state"], "BROADCASTED")
        self.assertEqual(lock_events, ["lock-enter", "lock-exit"])
        self.assertEqual(
            coin.events,
            [
                ("build", DESTINATION, "12.345678", 100, 60),
                ("broadcast", "message-hash-100"),
            ],
        )

    def test_seqno_lock_failure_keeps_machine_readable_error_code(self):
        from app.fee_deposit_seqno_guard import FeeDepositSeqnoLockError

        self.create_execution()
        coin = FakeCoin()

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.execute(
                self.execution_id,
                coin=coin,
                lock_factory=failing_lock(
                    FeeDepositSeqnoLockError(
                        "Timed out waiting for TON fee-deposit seqno lock"
                    )
                ),
                lease_owner="worker-1",
            )

        self.assertEqual(status["state"], "FAILED_PRE_BROADCAST")
        self.assertEqual(status["failure_class"], "PREFLIGHT")
        self.assertEqual(status["error_code"], "PAYOUT_SEQNO_LOCK_UNAVAILABLE")
        self.assertFalse(status["reconciliation_required"])
        self.assertEqual(coin.events, [])

    def test_execute_rechecks_preflight_before_seqno_side_effects(self):
        from app.config import config
        from app.payout_status import PayoutStatusError

        self.create_execution()
        coin = FakeCoin()
        config["PAYOUT_EXECUTION_PREFLIGHT_CHECKS_ENABLED"] = True

        with patch(
            "app.payout_status.run_ton_usdt_preflight_checks",
            side_effect=PayoutStatusError(
                "no funds",
                code="INSUFFICIENT_JETTON_BALANCE",
                status_code=409,
            ),
        ):
            with self.app.app_context():
                status = self.store_module.PayoutExecutionStore.execute(
                    self.execution_id,
                    coin=coin,
                    lock_factory=null_lock,
                    lease_owner="worker-1",
                )

        self.assertEqual(status["state"], "FAILED_PRE_BROADCAST")
        self.assertEqual(status["failure_class"], "PREFLIGHT")
        self.assertEqual(coin.events, [])

    def test_stale_signing_without_side_effects_is_safe_to_retry(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            lease_expires_at="2026-01-01T00:00:00.000000Z",
            lease_owner="dead-worker",
        )

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.recover_stale_signing(
                self.execution_id
            )

        self.assertEqual(status["state"], "RECEIVED")
        self.assertIsNone(status["lease_owner"])

    def test_active_signing_lease_is_not_recovered(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            lease_expires_at="2999-01-01T00:00:00.000000Z",
            lease_owner="worker-active",
        )

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.recover_stale_signing(
                self.execution_id
            )

        self.assertEqual(status["state"], "SIGNING")
        self.assertEqual(status["lease_owner"], "worker-active")

    def test_stale_signing_with_seqno_requires_reconciliation(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            lease_expires_at="2026-01-01T00:00:00.000000Z",
            source_seqno=100,
        )

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.recover_stale_signing(
                self.execution_id
            )

        self.assertEqual(status["state"], "RECONCILIATION_REQUIRED")
        self.assertEqual(status["error_code"], "STALE_SIGNING_WITH_SIDE_EFFECT")

    def test_stale_signing_with_signed_boc_requires_reconciliation(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            lease_expires_at="2026-01-01T00:00:00.000000Z",
            signed_boc_ref="signed-ref",
            signed_boc_hash="signed-hash",
        )

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.recover_stale_signing(
                self.execution_id
            )

        self.assertEqual(status["state"], "RECONCILIATION_REQUIRED")

    def test_stale_signing_with_broadcast_marker_requires_reconciliation(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            lease_expires_at="2026-01-01T00:00:00.000000Z",
            broadcast_attempted_at="2026-01-01T00:00:01.000000Z",
        )

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.recover_stale_signing(
                self.execution_id
            )

        self.assertEqual(status["state"], "RECONCILIATION_REQUIRED")

    def test_status_recovers_stale_signed_to_reconciliation(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNED",
            lease_expires_at="2026-01-01T00:00:00.000000Z",
            source_seqno=100,
            valid_until="600",
            signed_boc_ref="not-retained:signed-ton-boc-sha256:abc",
            signed_boc_hash="abc",
            message_hash="message-hash-100",
        )

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.status(
                self.execution_id,
                authenticated_consumer=CONSUMER,
                endpoint_symbol="TON-USDT",
            )

        self.assertEqual(status["state"], "RECONCILIATION_REQUIRED")
        self.assertTrue(status["reconciliation_required"])
        self.assertEqual(status["error_code"], "STALE_SIGNED_WITH_SIDE_EFFECT")

    def test_broadcast_timeout_requires_reconciliation_without_retry(self):
        self.create_execution()
        coin = FakeCoin(broadcast_error=TimeoutError("sendBoc timeout"))

        with self.app.app_context():
            with patch.object(self.store_module.time, "time", return_value=0):
                status = self.store_module.PayoutExecutionStore.execute(
                    self.execution_id,
                    coin=coin,
                    lock_factory=null_lock,
                    lease_owner="worker-1",
                )

        self.assertEqual(status["state"], "RECONCILIATION_REQUIRED")
        self.assertEqual(status["failure_class"], "AMBIGUOUS")
        self.assertEqual(status["error_code"], "UNSAFE_EXECUTION_INTERRUPTED")
        self.assertEqual(coin.events.count(("broadcast", "message-hash-100")), 1)

    def test_broadcast_hash_mismatch_requires_reconciliation(self):
        self.create_execution()
        coin = FakeCoin(broadcast_hash="unexpected-message-hash")

        with self.app.app_context():
            with patch.object(self.store_module.time, "time", return_value=0):
                status = self.store_module.PayoutExecutionStore.execute(
                    self.execution_id,
                    coin=coin,
                    lock_factory=null_lock,
                    lease_owner="worker-1",
                )

        self.assertEqual(status["state"], "RECONCILIATION_REQUIRED")
        self.assertEqual(status["failure_class"], "AMBIGUOUS")
        self.assertEqual(status["error_code"], "BROADCAST_MESSAGE_HASH_MISMATCH")
        self.assertEqual(status["message_hash"], "message-hash-100")
        self.assertIsNotNone(status["broadcast_attempted_at"])
        self.assertTrue(status["reconciliation_required"])

    def test_signed_evidence_without_worker_artifact_is_not_rebroadcast(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNED",
            source_seqno=100,
            valid_until="600",
            signed_boc_ref="not-retained:signed-ton-boc-sha256:abc",
            signed_boc_hash="abc",
            message_hash="message-hash-100",
        )
        coin = FakeCoin()

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.execute(
                self.execution_id,
                coin=coin,
                lock_factory=null_lock,
                lease_owner="worker-2",
            )

        self.assertEqual(status["state"], "RECONCILIATION_REQUIRED")
        self.assertEqual(status["failure_class"], "AMBIGUOUS")
        self.assertEqual(coin.events, [])
        self.assertTrue(status["reconciliation_required"])

    def test_execute_reloads_row_after_lock_before_side_effects(self):
        self.create_execution()
        coin = FakeCoin()

        @contextmanager
        def racing_lock():
            self.set_execution_fields(
                state="BROADCASTED",
                source_seqno=100,
                signed_boc_ref="not-retained:signed-ton-boc-sha256:abc",
                signed_boc_hash="abc",
                message_hash="message-hash-100",
                message_hashes_json='["message-hash-100"]',
            )
            yield

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.execute(
                self.execution_id,
                coin=coin,
                lock_factory=racing_lock,
                lease_owner="worker-1",
            )

        self.assertEqual(status["state"], "BROADCASTED")
        self.assertEqual(status["message_hash"], "message-hash-100")
        self.assertEqual(coin.events, [])

    def test_execute_returns_current_state_when_pre_lock_transition_hits_cas(self):
        self.create_execution()
        coin = FakeCoin()
        store = self.store_module.PayoutExecutionStore
        exc = self.store_module.PayoutExecutionError(
            "Payout execution state changed concurrently",
            code="PAYOUT_EXECUTION_CAS_CONFLICT",
            status_code=409,
        )

        def racing_transition(cls, row, state, **fields):
            if state == "VALIDATED":
                self.set_execution_fields(
                    state="SIGNING",
                    state_version=2,
                    lease_owner="worker-other",
                    lease_expires_at="2999-01-01T00:00:00.000000Z",
                    attempt_id="attempt-other",
                )
                raise exc
            raise AssertionError("race test should only touch first transition")

        with self.app.app_context():
            with patch.object(store, "_transition", classmethod(racing_transition)):
                status = store.execute(
                    self.execution_id,
                    coin=coin,
                    lock_factory=null_lock,
                    lease_owner="worker-1",
                )

        self.assertEqual(status["state"], "SIGNING")
        self.assertEqual(status["lease_owner"], "worker-other")
        self.assertEqual(coin.events, [])

    def test_cas_conflict_returns_current_state_without_downgrading_terminal(self):
        self.create_execution()
        self.set_execution_fields(
            state="BROADCASTED",
            source_seqno=100,
            signed_boc_ref="not-retained:signed-ton-boc-sha256:abc",
            signed_boc_hash="abc",
            message_hash="message-hash-100",
            message_hashes_json='["message-hash-100"]',
        )

        with self.app.app_context():
            exc = self.store_module.PayoutExecutionError(
                "Payout execution state changed concurrently",
                code="PAYOUT_EXECUTION_CAS_CONFLICT",
                status_code=409,
            )
            status = self.store_module.PayoutExecutionStore._mark_failed_or_reconciliation(
                self.execution_id,
                exc,
            )

        self.assertEqual(status["state"], "BROADCASTED")
        self.assertEqual(status["message_hash"], "message-hash-100")
        self.assertFalse(status["reconciliation_required"])

    def test_failed_handler_does_not_downgrade_broadcasted_after_worker_race(self):
        self.create_execution()
        self.set_execution_fields(
            state="BROADCASTED",
            source_seqno=100,
            signed_boc_ref="not-retained:signed-ton-boc-sha256:abc",
            signed_boc_hash="abc",
            message_hash="message-hash-100",
            message_hashes_json='["message-hash-100"]',
        )

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore._mark_failed_or_reconciliation(
                self.execution_id,
                RuntimeError("stale worker"),
            )

        self.assertEqual(status["state"], "BROADCASTED")
        self.assertEqual(status["message_hash"], "message-hash-100")
        self.assertFalse(status["reconciliation_required"])

    def test_status_returns_current_row_when_stale_recovery_hits_cas_conflict(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            lease_expires_at="2026-01-01T00:00:00.000000Z",
        )
        store = self.store_module.PayoutExecutionStore
        exc = self.store_module.PayoutExecutionError(
            "Payout execution state changed concurrently",
            code="PAYOUT_EXECUTION_CAS_CONFLICT",
            status_code=409,
        )

        def racing_recovery(execution_id):
            self.set_execution_fields(
                state="RECEIVED",
                lease_owner=None,
                lease_expires_at=None,
                attempt_id=None,
            )
            raise exc

        with self.app.app_context():
            with patch.object(store, "recover_stale_execution", side_effect=racing_recovery):
                status = store.status(
                    self.execution_id,
                    authenticated_consumer=CONSUMER,
                    endpoint_symbol="TON-USDT",
                )

        self.assertEqual(status["state"], "RECEIVED")
        self.assertIsNone(status["lease_owner"])

    def test_active_signing_execute_does_not_steal_lease(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            lease_expires_at="2999-01-01T00:00:00.000000Z",
            lease_owner="worker-active",
        )
        coin = FakeCoin()

        with self.app.app_context():
            status = self.store_module.PayoutExecutionStore.execute(
                self.execution_id,
                coin=coin,
                lock_factory=null_lock,
                lease_owner="worker-2",
            )

        self.assertEqual(status["state"], "SIGNING")
        self.assertEqual(coin.events, [])

    def test_duplicate_submit_auto_reenqueues_safe_existing_execution(self):
        from app.config import config

        first = self.create_execution()
        config["PAYOUT_EXECUTION_AUTO_ENQUEUE_ENABLED"] = True
        calls = []

        with patch.object(
            self.store_module.PayoutExecutionStore,
            "enqueue_execution",
            side_effect=lambda execution_id, queue: calls.append((execution_id, queue)),
        ):
            second = self.submit()

        self.assertEqual(first["execution_id"], second["execution_id"])
        self.assertEqual(calls, [(self.execution_id, "ton_usdt_payouts")])

    def test_task_owned_transient_failure_retries_received_without_mutation(self):
        self.create_execution()

        with self.app.app_context():
            action = self.store_module.PayoutExecutionStore.recover_task_owned_transient_failure(
                self.execution_id,
                lease_owner="task-1",
            )

        row = self.get_execution()
        self.assertEqual(action, "retry")
        self.assertEqual(row.state, "RECEIVED")
        self.assertIsNone(row.lease_owner)
        self.assertIsNone(row.attempt_id)
        self.assertFalse(row.reconciliation_required)

    def test_task_owned_transient_failure_does_not_retry_received_with_hash_list(self):
        self.create_execution()
        self.set_execution_fields(message_hashes_json='["message-hash-present"]')

        with self.app.app_context():
            action = self.store_module.PayoutExecutionStore.recover_task_owned_transient_failure(
                self.execution_id,
                lease_owner="task-1",
            )

        row = self.get_execution()
        self.assertEqual(action, "raise")
        self.assertEqual(row.state, "RECEIVED")
        self.assertEqual(row.message_hashes_json, '["message-hash-present"]')

    def test_task_owned_transient_failure_resets_signing_without_unsafe_evidence(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            lease_owner="task-1",
            lease_expires_at="2999-01-01T00:00:00.000000Z",
            attempt_id="attempt-1",
        )

        with self.app.app_context():
            action = self.store_module.PayoutExecutionStore.recover_task_owned_transient_failure(
                self.execution_id,
                lease_owner="task-1",
            )

        row = self.get_execution()
        self.assertEqual(action, "retry")
        self.assertEqual(row.state, "RECEIVED")
        self.assertIsNone(row.lease_owner)
        self.assertIsNone(row.lease_expires_at)
        self.assertIsNone(row.attempt_id)
        self.assertFalse(row.reconciliation_required)

    def test_task_owned_transient_failure_does_not_retry_signing_with_seqno(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            lease_owner="task-1",
            lease_expires_at="2999-01-01T00:00:00.000000Z",
            attempt_id="attempt-1",
            source_seqno=101,
        )

        with self.app.app_context():
            action = self.store_module.PayoutExecutionStore.recover_task_owned_transient_failure(
                self.execution_id,
                lease_owner="task-1",
            )

        row = self.get_execution()
        self.assertEqual(action, "raise")
        self.assertEqual(row.state, "SIGNING")
        self.assertEqual(row.source_seqno, 101)
        self.assertEqual(row.lease_owner, "task-1")

    def test_task_owned_transient_failure_does_not_steal_other_worker_signing(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            lease_owner="task-other",
            lease_expires_at="2999-01-01T00:00:00.000000Z",
            attempt_id="attempt-other",
        )

        with self.app.app_context():
            action = self.store_module.PayoutExecutionStore.recover_task_owned_transient_failure(
                self.execution_id,
                lease_owner="task-1",
            )

        row = self.get_execution()
        self.assertEqual(action, "raise")
        self.assertEqual(row.state, "SIGNING")
        self.assertEqual(row.lease_owner, "task-other")
        self.assertEqual(row.attempt_id, "attempt-other")

    def test_status_is_read_only_for_received_orphan(self):
        self.create_execution()
        self.set_execution_fields(state_updated_at="2026-01-01T00:00:00.000000Z")
        store = self.store_module.PayoutExecutionStore

        with self.app.app_context():
            with patch.object(store, "enqueue_execution") as enqueue:
                status = store.status(
                    self.execution_id,
                    authenticated_consumer=CONSUMER,
                    endpoint_symbol="TON-USDT",
                )

        self.assertEqual(status["state"], "RECEIVED")
        enqueue.assert_not_called()

    def test_recover_orphan_reenqueues_received_without_unsafe_evidence(self):
        self.create_execution()
        self.set_execution_fields(state_updated_at="2026-01-01T00:00:00.000000Z")
        store = self.store_module.PayoutExecutionStore

        with self.app.app_context():
            with patch.object(store, "enqueue_execution") as enqueue:
                status = store.recover_orphan_execution(
                    self.execution_id,
                    authenticated_consumer=CONSUMER,
                    endpoint_symbol="TON-USDT",
                )

        row = self.get_execution()
        self.assertEqual(status["state"], "RECEIVED")
        self.assertEqual(status["orphan_recovery"]["enqueued"], True)
        self.assertEqual(row.state, "RECEIVED")
        enqueue.assert_called_once_with(self.execution_id, row.payout_queue)

    def test_recover_orphan_does_not_reenqueue_fresh_received(self):
        self.create_execution()
        store = self.store_module.PayoutExecutionStore

        with self.app.app_context():
            with patch.object(store, "enqueue_execution") as enqueue:
                status = store.recover_orphan_execution(
                    self.execution_id,
                    authenticated_consumer=CONSUMER,
                    endpoint_symbol="TON-USDT",
                )

        self.assertEqual(status["state"], "RECEIVED")
        self.assertEqual(status["orphan_recovery"]["enqueued"], False)
        self.assertEqual(status["orphan_recovery"]["reason"], "not_old_enough")
        enqueue.assert_not_called()

    def test_recover_orphan_does_not_reenqueue_active_lease(self):
        self.create_execution()
        self.set_execution_fields(
            state="VALIDATED",
            state_updated_at="2026-01-01T00:00:00.000000Z",
            lease_owner="worker-active",
            lease_expires_at="2999-01-01T00:00:00.000000Z",
        )
        store = self.store_module.PayoutExecutionStore

        with self.app.app_context():
            with patch.object(store, "enqueue_execution") as enqueue:
                status = store.recover_orphan_execution(
                    self.execution_id,
                    authenticated_consumer=CONSUMER,
                    endpoint_symbol="TON-USDT",
                )

        self.assertEqual(status["state"], "VALIDATED")
        self.assertEqual(status["orphan_recovery"]["enqueued"], False)
        self.assertEqual(status["orphan_recovery"]["reason"], "active_lease")
        enqueue.assert_not_called()

    def test_recover_orphan_does_not_reenqueue_when_unsafe_evidence_exists(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            state_updated_at="2026-01-01T00:00:00.000000Z",
            source_seqno=101,
            message_hashes_json='["message-hash-present"]',
            lease_owner=None,
            lease_expires_at=None,
        )
        store = self.store_module.PayoutExecutionStore

        with self.app.app_context():
            with patch.object(store, "enqueue_execution") as enqueue:
                status = store.recover_orphan_execution(
                    self.execution_id,
                    authenticated_consumer=CONSUMER,
                    endpoint_symbol="TON-USDT",
                )

        self.assertEqual(status["state"], "SIGNING")
        self.assertEqual(status["orphan_recovery"]["enqueued"], False)
        self.assertEqual(status["orphan_recovery"]["reason"], "unsafe_evidence_exists")
        enqueue.assert_not_called()
        row = self.get_execution()
        self.assertEqual(row.state, "SIGNING")
        self.assertEqual(row.source_seqno, 101)
        self.assertFalse(row.reconciliation_required)

    def test_recover_orphan_reenqueues_stale_signing_without_unsafe_evidence(self):
        self.create_execution()
        self.set_execution_fields(
            state="SIGNING",
            state_updated_at="2026-01-01T00:00:00.000000Z",
            lease_owner="worker-expired",
            lease_expires_at="2026-01-01T00:01:00.000000Z",
            attempt_id="attempt-1",
        )
        store = self.store_module.PayoutExecutionStore

        with self.app.app_context():
            with patch.object(store, "enqueue_execution") as enqueue:
                status = store.recover_orphan_execution(
                    self.execution_id,
                    authenticated_consumer=CONSUMER,
                    endpoint_symbol="TON-USDT",
                )

        row = self.get_execution()
        self.assertEqual(status["state"], "RECEIVED")
        self.assertEqual(status["orphan_recovery"]["enqueued"], True)
        self.assertEqual(row.state, "RECEIVED")
        self.assertIsNone(row.lease_owner)
        self.assertIsNone(row.lease_expires_at)
        self.assertIsNone(row.attempt_id)
        enqueue.assert_called_once_with(self.execution_id, row.payout_queue)

    def test_recover_orphan_does_not_reenqueue_when_message_hash_list_exists(self):
        self.create_execution()
        self.set_execution_fields(
            state="RECEIVED",
            state_updated_at="2026-01-01T00:00:00.000000Z",
            message_hashes_json='["message-hash-present"]',
        )
        store = self.store_module.PayoutExecutionStore

        with self.app.app_context():
            with patch.object(store, "enqueue_execution") as enqueue:
                status = store.recover_orphan_execution(
                    self.execution_id,
                    authenticated_consumer=CONSUMER,
                    endpoint_symbol="TON-USDT",
                )

        self.assertEqual(status["state"], "RECEIVED")
        self.assertEqual(status["orphan_recovery"]["enqueued"], False)
        self.assertEqual(status["orphan_recovery"]["reason"], "unsafe_evidence_exists")
        enqueue.assert_not_called()



class TonPayoutCoinPrimitiveTests(unittest.TestCase):
    def test_coin_builds_signed_payout_and_verifies_broadcast_hash(self):
        import base64
        from app.coin import Coin

        class FakeMessage:
            def to_boc(self, _has_idx):
                return b"signed-boc"

            def bytes_hash(self):
                return bytes.fromhex("11" * 32)

        class FakeWallet:
            def __init__(self):
                self.calls = []

            def create_transfer_message(self, **kwargs):
                self.calls.append(kwargs)
                return {"message": FakeMessage()}

        class FakeToncenterForCoin:
            def jetton_master_decimals(self, _master):
                return 6

            def get_account_wallet_jetton_address(self, _owner, _master):
                return "EQJETTONWALLET"

            def send_message_with_hash(self, _boc):
                return base64.b64encode(bytes.fromhex("11" * 32)).decode("ascii")

        class FakeJettonWallet:
            def create_transfer_body(self, **_kwargs):
                return "jetton-body"

        wallet = FakeWallet()
        coin = Coin("TON-USDT")
        coin.toncenter = FakeToncenterForCoin()
        coin.jetton_master_address = "EQJETTONMASTER"
        coin.get_fee_deposit_account = lambda address_type: {
            "public": DESTINATION,
            "raw": "0:fee-deposit",
        }[address_type]
        coin.get_mnemonic_from_address = lambda _address: ["word"] * 24
        coin.get_jetton_transaction_fee = lambda *_args: Decimal("0.05")

        with patch("app.coin.TonWallets.from_mnemonics", return_value=(None, None, None, wallet)):
            with patch("app.coin.JettonWallet", return_value=FakeJettonWallet()):
                with patch("app.coin.time.time", return_value=0):
                    signed = coin.build_signed_payout(payload(), 7, 600)

        self.assertEqual(signed["message_hash"], "11" * 32)
        self.assertEqual(signed["jetton_wallet"], "EQJETTONWALLET")
        self.assertEqual(wallet.calls[0]["seqno"], 7)
        self.assertEqual(signed["valid_until"], 60)

        evidence = coin.signed_payout_evidence(signed)
        self.assertEqual(evidence["message_hash"], "11" * 32)
        self.assertTrue(evidence["signed_boc_ref"].startswith("not-retained:"))

        result = coin.broadcast_signed_payout(signed)
        self.assertEqual(result["hash"], "11" * 32)

    def test_coin_accepts_hex_broadcast_hash_without_base64_decoding(self):
        from app.coin import Coin

        class FakeToncenterForCoin:
            def send_message_with_hash(self, _boc):
                return "22" * 32

        coin = Coin("TON-USDT")
        coin.toncenter = FakeToncenterForCoin()

        result = coin.broadcast_signed_payout({"boc": "signed-boc"})

        self.assertEqual(result["hash"], "22" * 32)
        self.assertEqual(result["hash_base64"], "22" * 32)



if __name__ == "__main__":
    unittest.main()
