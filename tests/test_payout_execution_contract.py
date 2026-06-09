from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import unittest
from urllib.parse import urlencode
from unittest.mock import patch

import prometheus_client


TEST_DATABASE = "/private/tmp/ton-shkeeper-payout-execution-contract.db"
CONSUMER = "grither-pay"
KEY_ID = "key-1"
SECRET = "test-secret"
DESTINATION = "EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c"


def reset_modules():
    import sys

    for module_name in [
        "app.payout_auth",
        "app.payout_contract",
        "app.payout_execution",
        "app.api.payout",
        "app.api.views",
        "app.api",
    ]:
        sys.modules.pop(module_name, None)


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def body_hash(body_bytes):
    return hashlib.sha256(body_bytes).hexdigest()


def sign_request(method, path, body_bytes, *, timestamp=None, nonce="nonce-1", query=""):
    timestamp = str(timestamp or int(time.time()))
    signature_base = "\n".join(
        [
            timestamp,
            nonce,
            method.upper(),
            path,
            query,
            body_hash(body_bytes),
        ]
    )
    signature = hmac.new(
        SECRET.encode("utf-8"),
        signature_base.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "X-Payout-Consumer": CONSUMER,
        "X-Payout-Key-Id": KEY_ID,
        "X-Payout-Timestamp": timestamp,
        "X-Payout-Nonce": nonce,
        "X-Payout-Signature": signature,
    }


def payload(**overrides):
    from app.payout_contract import canonical_payload, sidecar_payload_hash

    value = {
        "consumer": CONSUMER,
        "external_id": "WD-1",
        "asset": "USDT",
        "network": "TON",
        "destination": DESTINATION,
        "amount": "12.345678",
        "contract_version": "usdt-payout-execution-v1",
        "request_hash": "request-hash-1",
    }
    value.update(overrides)
    canonical = canonical_payload(value, endpoint_symbol="TON-USDT")
    value["sidecar_payload_hash"] = sidecar_payload_hash(canonical)
    return value


class TonPayoutExecutionContractTests(unittest.TestCase):
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
        reset_modules()

        from app import create_app
        from app.db_import import db
        import werkzeug

        if not hasattr(werkzeug, "__version__"):
            werkzeug.__version__ = "3"

        self.app = create_app()
        self.app.config.update(TESTING=True)
        self.client = self.app.test_client()
        self.db = db
        from app.payout_observability import clear_payout_request_metrics

        clear_payout_request_metrics()
        with self.app.app_context():
            db.drop_all()
            db.create_all()

    def test_sidecar_payload_hash_matches_shared_shkeeper_contract(self):
        from app.payout_contract import canonical_payload, sidecar_payload_hash

        base = canonical_payload(payload(), endpoint_symbol="TON-USDT")
        changed = dict(
            base,
            source_wallet="dedicated-wallet",
            payout_queue="other_queue",
            request_hash="other-request-hash",
        )

        self.assertEqual(sidecar_payload_hash(base), sidecar_payload_hash(changed))

    def tearDown(self):
        with self.app.app_context():
            self.db.session.remove()
            self.db.drop_all()
            self.db.engine.dispose()
        if os.path.exists(TEST_DATABASE):
            os.unlink(TEST_DATABASE)

    def post_json(self, path, value, *, method="POST", nonce="nonce-1", query=None):
        body = canonical_json(value).encode("utf-8")
        query_string = urlencode(query or {})
        request_path = path if not query_string else f"{path}?{query_string}"
        headers = sign_request(
            method,
            path,
            body,
            nonce=nonce,
            query=query_string,
        )
        return self.client.open(
            request_path,
            method=method,
            data=body,
            headers=headers,
            content_type="application/json",
        )

    def submit_v1_execution(self, execution_id):
        response = self.post_json(
            f"/TON-USDT/payout-executions/{execution_id}",
            payload(execution_id=execution_id),
            nonce=f"submit-{execution_id}",
        )
        self.assertEqual(response.status_code, 202)
        return response.get_json()

    def set_execution_fields(self, execution_id, **fields):
        from app.models import PayoutExecution

        with self.app.app_context():
            row = PayoutExecution.query.filter_by(execution_id=execution_id).first()
            self.assertIsNotNone(row)
            for key, value in fields.items():
                setattr(row, key, value)
            self.db.session.commit()

    def test_preflight_accepts_signed_consumer_request(self):
        response = self.post_json("/TON-USDT/payout/preflight", payload())

        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data["status"], "OK")
        self.assertEqual(data["state"], "PREFLIGHT_OK")
        self.assertEqual(data["consumer"], CONSUMER)
        self.assertEqual(data["external_id"], "WD-1")
        self.assertEqual(data["payout_queue"], "ton_usdt_payouts")

    def test_preflight_accepts_canonical_nested_consumer_key(self):
        from app.config import config

        config["PAYOUT_CONSUMER_KEYS"] = {
            CONSUMER: {
                KEY_ID: {
                    "secret": SECRET,
                    "rails": ["TON-USDT"],
                }
            }
        }

        response = self.post_json(
            "/TON-USDT/payout/preflight",
            payload(),
            nonce="nested-key",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["state"], "PREFLIGHT_OK")

    def test_submit_creates_execution_and_status_schema(self):
        submit = self.post_json("/TON-USDT/payout/submit", payload())

        self.assertEqual(submit.status_code, 202)
        accepted = submit.get_json()
        self.assertEqual(accepted["status"], "ACCEPTED")
        self.assertEqual(accepted["state"], "RECEIVED")
        execution_id = accepted["execution_id"]

        status_body = b""
        status_headers = sign_request(
            "GET",
            f"/TON-USDT/payout/status/{execution_id}",
            status_body,
            nonce="status-1",
        )
        status = self.client.get(
            f"/TON-USDT/payout/status/{execution_id}",
            headers=status_headers,
        )

        self.assertEqual(status.status_code, 200)
        data = status.get_json()
        required_fields = [
            "execution_id",
            "sidecar_execution_id",
            "consumer",
            "external_id",
            "contract_version",
            "asset",
            "network",
            "amount",
            "destination",
            "request_hash",
            "sidecar_payload_hash",
            "state",
            "state_version",
            "state_transition_id",
            "state_updated_at",
            "source_wallet",
            "jetton_master",
            "jetton_wallet",
            "chain_id_or_network_id",
            "masterchain_seqno",
            "source_seqno",
            "valid_until",
            "signed_boc_ref",
            "signed_boc_hash",
            "signed_boc_stored_at",
            "message_hash",
            "broadcast_provider",
            "broadcast_attempted_at",
            "chain_check_metadata",
            "failure_class",
            "error_code",
            "error_message",
            "reconciliation_required",
        ]
        for field in required_fields:
            self.assertIn(field, data)
        self.assertEqual(data["consumer"], CONSUMER)
        self.assertEqual(data["external_id"], "WD-1")

    def test_duplicate_same_payload_is_idempotent(self):
        first = self.post_json("/TON-USDT/payout/submit", payload(), nonce="nonce-1")
        second = self.post_json("/TON-USDT/payout/submit", payload(), nonce="nonce-2")

        self.assertEqual(first.status_code, 202)
        self.assertEqual(second.status_code, 202)
        self.assertEqual(
            first.get_json()["execution_id"],
            second.get_json()["execution_id"],
        )

    def test_duplicate_changed_payload_is_rejected(self):
        first = self.post_json("/TON-USDT/payout/submit", payload(), nonce="nonce-1")
        changed = payload(amount="13.000000", request_hash="request-hash-2")
        second = self.post_json(
            "/TON-USDT/payout/submit",
            changed,
            nonce="nonce-2",
        )

        self.assertEqual(first.status_code, 202)
        self.assertEqual(second.status_code, 409)
        self.assertEqual(second.get_json()["code"], "PAYOUT_EXECUTION_CONFLICT")

    def test_non_finite_amounts_are_rejected_cleanly(self):
        for raw_amount in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(raw_amount=raw_amount):
                value = payload()
                value["amount"] = raw_amount
                value["sidecar_payload_hash"] = "not-used-for-invalid-amount"

                response = self.post_json(
                    "/TON-USDT/payout/preflight", value, nonce=raw_amount
                )

                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.get_json()["code"], "INVALID_AMOUNT")

    def test_tampered_body_signature_is_rejected(self):
        body = canonical_json(payload()).encode("utf-8")
        headers = sign_request("POST", "/TON-USDT/payout/submit", body)
        tampered = payload(amount="13.000000")

        response = self.client.post(
            "/TON-USDT/payout/submit",
            data=canonical_json(tampered).encode("utf-8"),
            headers=headers,
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["code"], "PAYOUT_AUTH_INVALID_SIGNATURE")
        text = prometheus_client.generate_latest().decode()
        self.assertIn(
            'ton_payout_request_failed_total{code="PAYOUT_AUTH_INVALID_SIGNATURE",operation="submit"} 1.0',
            text,
        )

    def test_replayed_nonce_is_rejected(self):
        first = self.post_json("/TON-USDT/payout/preflight", payload(), nonce="same")
        second = self.post_json("/TON-USDT/payout/preflight", payload(), nonce="same")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 401)
        self.assertEqual(second.get_json()["code"], "PAYOUT_AUTH_REPLAY")

    def test_expired_nonce_is_purged_before_remembering_new_nonce(self):
        from app.models import PayoutAuthNonce

        with self.app.app_context():
            self.db.session.add(
                PayoutAuthNonce(
                    consumer=CONSUMER,
                    key_id=KEY_ID,
                    nonce="expired",
                    timestamp=int(time.time()) - 1_000,
                )
            )
            self.db.session.commit()

        response = self.post_json(
            "/TON-USDT/payout/preflight",
            payload(),
            nonce="fresh",
        )

        self.assertEqual(response.status_code, 200)
        with self.app.app_context():
            self.assertIsNone(PayoutAuthNonce.query.filter_by(nonce="expired").first())
            self.assertIsNotNone(PayoutAuthNonce.query.filter_by(nonce="fresh").first())

    def test_method_and_path_are_bound_to_signature(self):
        value = payload()
        body = canonical_json(value).encode("utf-8")
        headers = sign_request("POST", "/TON-USDT/payout/preflight", body)

        response = self.client.post(
            "/TON-USDT/payout/submit",
            data=body,
            headers=headers,
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["code"], "PAYOUT_AUTH_INVALID_SIGNATURE")

    def test_query_string_is_bound_to_signature(self):
        value = payload()
        response = self.post_json(
            "/TON-USDT/payout/preflight",
            value,
            query={"probe": "1"},
        )
        self.assertEqual(response.status_code, 200)

        body = canonical_json(value).encode("utf-8")
        headers = sign_request("POST", "/TON-USDT/payout/preflight", body, nonce="q2")
        mismatched = self.client.post(
            "/TON-USDT/payout/preflight?probe=1",
            data=body,
            headers=headers,
            content_type="application/json",
        )

        self.assertEqual(mismatched.status_code, 401)

    def test_wrong_consumer_body_is_rejected(self):
        wrong = payload(consumer="other-service")
        response = self.post_json("/TON-USDT/payout/submit", wrong)

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["code"], "PAYOUT_CONSUMER_FORBIDDEN")

    def test_wrong_rail_body_is_rejected_before_creation(self):
        wrong = payload(asset="TON", network="TON")
        response = self.post_json("/TON-USDT/payout/submit", wrong)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "PAYOUT_RAIL_MISMATCH")
        text = prometheus_client.generate_latest().decode()
        self.assertIn(
            'ton_payout_request_failed_total{code="PAYOUT_RAIL_MISMATCH",operation="submit"} 1.0',
            text,
        )

    def test_sidecar_payload_hash_mismatch_is_rejected(self):
        value = payload()
        value["sidecar_payload_hash"] = "wrong"
        response = self.post_json("/TON-USDT/payout/submit", value)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "SIDECAR_PAYLOAD_HASH_MISMATCH")

    def test_unknown_execution_fields_are_rejected(self):
        value = payload()
        value["unsupported_alpha"] = "value-1"
        value["unsupported_beta"] = "value-2"
        response = self.post_json("/TON-USDT/payout/submit", value)

        self.assertEqual(response.status_code, 400)
        data = response.get_json()
        self.assertEqual(data["code"], "PAYOUT_EXECUTION_BAD_REQUEST")
        self.assertIn("unsupported_alpha", data["message"])
        self.assertIn("unsupported_beta", data["message"])

    def test_shkeeper_v1_routes_are_supported_without_legacy_basic_auth(self):
        execution_id = "ton-v1-execution-1"
        value = payload(execution_id=execution_id)

        preflight = self.post_json(
            f"/TON-USDT/payout-executions/{execution_id}/preflight",
            value,
            nonce="nonce-v1-preflight",
        )
        submit = self.post_json(
            f"/TON-USDT/payout-executions/{execution_id}",
            value,
            nonce="nonce-v1-submit",
        )
        status_path = f"/TON-USDT/payout-executions/{execution_id}"
        status = self.client.get(
            status_path,
            headers=sign_request(
                "GET",
                status_path,
                b"",
                nonce="nonce-v1-status",
            ),
        )

        self.assertEqual(preflight.status_code, 200)
        self.assertEqual(submit.status_code, 202)
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.get_json()["execution_id"], execution_id)


    def test_recover_orphan_accepts_signed_request(self):
        execution_id = "recover-orphan-old"
        self.submit_v1_execution(execution_id)
        self.set_execution_fields(
            execution_id,
            state_updated_at="2026-01-01T00:00:00.000000Z",
        )
        path = f"/TON-USDT/payout-executions/{execution_id}/recover-orphan"

        with patch("app.payout_execution.PayoutExecutionStore.enqueue_execution") as enqueue:
            response = self.post_json(path, {}, nonce="recover-old")

        self.assertEqual(response.status_code, 202)
        data = response.get_json()
        self.assertEqual(data["state"], "RECEIVED")
        self.assertEqual(
            data["orphan_recovery"],
            {"attempted": True, "enqueued": True, "reason": "enqueued"},
        )
        enqueue.assert_called_once_with(execution_id, "ton_usdt_payouts")

    def test_recover_orphan_requires_payout_auth(self):
        response = self.client.post(
            "/TON-USDT/payout-executions/missing/recover-orphan",
            data=canonical_json({}).encode("utf-8"),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["code"], "PAYOUT_AUTH_MISSING")

    def test_recover_orphan_unknown_execution_returns_signed_error_shape(self):
        response = self.post_json(
            "/TON-USDT/payout-executions/missing/recover-orphan",
            {},
            nonce="recover-missing",
        )

        self.assertEqual(response.status_code, 404)
        data = response.get_json()
        self.assertEqual(data["status"], "error")
        self.assertEqual(data["code"], "NO_EXECUTION_CREATED")
        self.assertIn("message", data)

    def test_recover_orphan_fresh_execution_does_not_enqueue(self):
        execution_id = "recover-orphan-fresh"
        self.submit_v1_execution(execution_id)
        path = f"/TON-USDT/payout-executions/{execution_id}/recover-orphan"

        with patch("app.payout_execution.PayoutExecutionStore.enqueue_execution") as enqueue:
            response = self.post_json(path, {}, nonce="recover-fresh")

        self.assertEqual(response.status_code, 202)
        data = response.get_json()
        self.assertFalse(data["orphan_recovery"]["enqueued"])
        self.assertEqual(data["orphan_recovery"]["reason"], "not_old_enough")
        enqueue.assert_not_called()

    def test_recover_orphan_active_lease_does_not_enqueue(self):
        execution_id = "recover-orphan-lease"
        self.submit_v1_execution(execution_id)
        self.set_execution_fields(
            execution_id,
            state="VALIDATED",
            state_updated_at="2026-01-01T00:00:00.000000Z",
            lease_owner="worker-active",
            lease_expires_at="2999-01-01T00:00:00.000000Z",
        )
        path = f"/TON-USDT/payout-executions/{execution_id}/recover-orphan"

        with patch("app.payout_execution.PayoutExecutionStore.enqueue_execution") as enqueue:
            response = self.post_json(path, {}, nonce="recover-active-lease")

        self.assertEqual(response.status_code, 202)
        data = response.get_json()
        self.assertFalse(data["orphan_recovery"]["enqueued"])
        self.assertEqual(data["orphan_recovery"]["reason"], "active_lease")
        enqueue.assert_not_called()

    def test_recover_orphan_unsafe_evidence_does_not_enqueue(self):
        execution_id = "recover-orphan-unsafe"
        self.submit_v1_execution(execution_id)
        self.set_execution_fields(
            execution_id,
            state="RECEIVED",
            state_updated_at="2026-01-01T00:00:00.000000Z",
            message_hashes_json='["message-hash-present"]',
        )
        path = f"/TON-USDT/payout-executions/{execution_id}/recover-orphan"

        with patch("app.payout_execution.PayoutExecutionStore.enqueue_execution") as enqueue:
            response = self.post_json(path, {}, nonce="recover-unsafe")

        self.assertEqual(response.status_code, 202)
        data = response.get_json()
        self.assertFalse(data["orphan_recovery"]["enqueued"])
        self.assertEqual(data["orphan_recovery"]["reason"], "unsafe_evidence_exists")
        enqueue.assert_not_called()

    def test_recover_orphan_enqueue_failure_returns_http_error(self):
        execution_id = "recover-orphan-enqueue-error"
        self.submit_v1_execution(execution_id)
        self.set_execution_fields(
            execution_id,
            state_updated_at="2026-01-01T00:00:00.000000Z",
        )
        path = f"/TON-USDT/payout-executions/{execution_id}/recover-orphan"

        with patch(
            "app.payout_execution.PayoutExecutionStore.enqueue_execution",
            side_effect=RuntimeError("broker unavailable"),
        ):
            response = self.post_json(path, {}, nonce="recover-enqueue-error")

        self.assertEqual(response.status_code, 503)
        data = response.get_json()
        self.assertEqual(data["status"], "error")
        self.assertEqual(data["code"], "PAYOUT_EXECUTION_RECOVERY_ENQUEUE_FAILED")

    def test_recover_orphan_rejects_non_ton_usdt_route(self):
        from app.config import config

        config["PAYOUT_CONSUMER_KEYS"][CONSUMER]["rails"] = ["TON-USDT", "TON"]
        execution_id = "recover-orphan-wrong-rail"
        self.submit_v1_execution(execution_id)
        self.set_execution_fields(
            execution_id,
            state_updated_at="2026-01-01T00:00:00.000000Z",
        )

        with patch("app.payout_execution.PayoutExecutionStore.enqueue_execution") as enqueue:
            response = self.post_json(
                f"/TON/payout-executions/{execution_id}/recover-orphan",
                {},
                nonce="recover-wrong-rail",
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "PAYOUT_RAIL_MISMATCH")
        enqueue.assert_not_called()

    def test_v1_path_execution_id_mismatch_is_rejected(self):
        value = payload(execution_id="body-id")
        response = self.post_json(
            "/TON-USDT/payout-executions/path-id/preflight",
            value,
            nonce="nonce-v1-mismatch",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "PAYOUT_EXECUTION_ID_MISMATCH")


if __name__ == "__main__":
    unittest.main()
