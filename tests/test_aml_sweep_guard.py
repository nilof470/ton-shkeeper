import unittest
from unittest.mock import patch

from app import events, sweep_guard, tasks
from app.utils import skip_if_running


ADDRESS = "UQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class FakeInspect:
    def active(self):
        return {}


class FakeDrainTask:
    def __init__(self):
        self.calls = []

    def delay(self, *args, **kwargs):
        self.calls.append((args, kwargs))


class TonAmlSweepGuardTests(unittest.TestCase):
    def test_ton_usdt_is_not_strictly_guarded_until_aml_provider_coverage_exists(self):
        with patch.object(sweep_guard.rq, "post") as post:
            self.assertTrue(
                sweep_guard.is_sweep_allowed(
                    "TON-USDT",
                    ADDRESS,
                    txid="ton-tx-1",
                )
            )

        post.assert_not_called()

    def test_guarded_symbol_posts_backend_eligibility_request_with_txid(self):
        calls = []

        def post(url, **kwargs):
            calls.append((url, kwargs))
            return FakeResponse({"decision": "allow"})

        with patch.object(sweep_guard, "GUARDED_SWEEP_SYMBOLS", {"TON-USDT"}):
            with patch.object(sweep_guard.rq, "post", side_effect=post):
                allowed = sweep_guard.is_sweep_allowed(
                    "TON-USDT",
                    ADDRESS,
                    txid="ton-tx-1",
                )

        self.assertTrue(allowed)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "http://shkeeper:5000/api/v1/sweep-eligibility")
        self.assertEqual(
            calls[0][1]["headers"],
            {"X-Shkeeper-Backend-Key": "shkeeper"},
        )
        self.assertEqual(
            calls[0][1]["json"],
            {
                "crypto": "TON-USDT",
                "network": "TON",
                "address": ADDRESS,
                "txid": "ton-tx-1",
            },
        )
        self.assertEqual(calls[0][1]["timeout"], 5)

    def test_guarded_symbol_fails_closed_for_non_allow_and_errors(self):
        for payload in (
            {"decision": "wait"},
            {"decision": "block"},
            {"decision": "ALLOW"},
            {"status": "success"},
        ):
            with self.subTest(payload=payload):
                with patch.object(sweep_guard, "GUARDED_SWEEP_SYMBOLS", {"TON-USDT"}):
                    with patch.object(
                        sweep_guard.rq, "post", return_value=FakeResponse(payload)
                    ):
                        self.assertFalse(
                            sweep_guard.is_sweep_allowed("TON-USDT", ADDRESS)
                        )

        with patch.object(sweep_guard, "GUARDED_SWEEP_SYMBOLS", {"TON-USDT"}):
            with patch.object(
                sweep_guard.rq, "post", side_effect=RuntimeError("timeout")
            ):
                self.assertFalse(sweep_guard.is_sweep_allowed("TON-USDT", ADDRESS))

    def test_live_event_enqueue_passes_txid_only_after_allow(self):
        drain_task = FakeDrainTask()

        with patch.object(events, "is_sweep_allowed", return_value=True) as allowed:
            result = events.enqueue_drain_if_sweep_allowed(
                "TON-USDT",
                ADDRESS,
                "ton-tx-1",
                drain_task,
            )

        self.assertTrue(result)
        allowed.assert_called_once_with("TON-USDT", ADDRESS, txid="ton-tx-1")
        self.assertEqual(
            drain_task.calls,
            [(("TON-USDT", ADDRESS), {"txid": "ton-tx-1"})],
        )

    def test_live_event_enqueue_does_not_queue_without_allow(self):
        drain_task = FakeDrainTask()

        with patch.object(events, "is_sweep_allowed", return_value=False):
            result = events.enqueue_drain_if_sweep_allowed(
                "TON-USDT",
                ADDRESS,
                "ton-tx-1",
                drain_task,
            )

        self.assertFalse(result)
        self.assertEqual(drain_task.calls, [])

    def test_drain_task_stops_before_coin_when_guard_denies(self):
        with patch.object(
            tasks.drain_account.app.control, "inspect", return_value=FakeInspect()
        ):
            with patch.object(tasks, "is_sweep_allowed", return_value=False) as guard:
                with patch.object(tasks, "Coin") as coin:
                    result = tasks.drain_account.run(
                        "TON-USDT",
                        ADDRESS,
                        txid="ton-tx-1",
                    )

        self.assertFalse(result)
        guard.assert_called_once_with("TON-USDT", ADDRESS, txid="ton-tx-1")
        coin.assert_not_called()

    def test_drain_task_ignores_txid_for_running_task_dedupe(self):
        calls = []

        def drain_account(self, symbol, account, txid=None):
            calls.append((symbol, account, txid))
            return True

        drain_account.__module__ = "app.tasks"
        wrapped = skip_if_running(drain_account)

        class InspectWithActiveDrain:
            def active(self):
                return {
                    "worker-1": [
                        {
                            "name": "app.tasks.drain_account",
                            "args": ["TON-USDT", ADDRESS],
                            "kwargs": {},
                            "id": "running-task",
                        }
                    ]
                }

        class FakeSelf:
            request = type("Request", (), {"id": "new-task"})()
            app = type(
                "App",
                (),
                {
                    "control": type(
                        "Control",
                        (),
                        {"inspect": lambda self: InspectWithActiveDrain()},
                    )()
                },
            )()

        result = wrapped(FakeSelf(), "TON-USDT", ADDRESS, txid="other-txid")

        self.assertIsNone(result)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
