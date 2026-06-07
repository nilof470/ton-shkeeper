import unittest
import base64
from unittest.mock import patch

from app import toncenterapi


class FakeResponse:
    def __init__(self, status_code, url, payload=None, text=""):
        self.status_code = status_code
        self.url = url
        self._payload = payload or {}
        self.text = text

    @property
    def ok(self):
        return 200 <= self.status_code < 400

    def json(self):
        return self._payload


class ToncenterErrorClassificationTest(unittest.TestCase):
    def test_masks_api_key_in_url(self):
        url = (
            "https://toncenter.com/api/v3/transactionsByMasterchainBlock"
            "?api_key=TEST_SECRET&seqno=66018441"
        )

        masked = toncenterapi.mask_toncenter_secret(url)

        self.assertIn("api_key=***MASKED***", masked)
        self.assertNotIn("TEST_SECRET", masked)

    def test_404_transactions_by_masterchain_block_is_transient(self):
        self.assertTrue(
            toncenterapi.is_transient_toncenter_error(
                endpoint="transactionsByMasterchainBlock",
                status_code=404,
            )
        )

    def test_404_other_endpoint_is_not_automatically_transient(self):
        self.assertFalse(
            toncenterapi.is_transient_toncenter_error(
                endpoint="getAddressInformation",
                status_code=404,
            )
        )

    def test_429_and_5xx_are_transient(self):
        for status_code in (429, 500, 502, 503, 504):
            self.assertTrue(
                toncenterapi.is_transient_toncenter_error(
                    endpoint="getMasterchainInfo",
                    status_code=status_code,
                )
            )

    def test_toncenter_request_uses_bounded_timeout(self):
        response = FakeResponse(
            status_code=200,
            url="https://toncenter.com/api/v2/getMasterchainInfo",
            payload={"ok": True},
        )

        with patch.object(toncenterapi.rq, "request", return_value=response) as request:
            toncenterapi.toncenter_request(
                "getMasterchainInfo",
                "GET",
                "https://toncenter.com/api/v2/getMasterchainInfo",
            )

        self.assertEqual(toncenterapi.TONCENTER_TIMEOUT, request.call_args.kwargs["timeout"])

    def test_transactions_by_masterchain_block_404_raises_transient_after_retries(self):
        response = FakeResponse(
            status_code=404,
            url=(
                "https://toncenter.com/api/v3/transactionsByMasterchainBlock"
                "?api_key=TEST_SECRET&seqno=66018441"
            ),
            text="not found",
        )
        client = toncenterapi.Toncenterapi()

        with patch.object(toncenterapi.rq, "request", return_value=response) as request:
            with patch.object(toncenterapi, "sleep_before_retry"):
                with self.assertRaises(toncenterapi.ToncenterTransientError) as raised:
                    client.get_all_transactions_by_masterchain_seqno(66018441)

        self.assertEqual(3, request.call_count)
        self.assertIn("api_key=***MASKED***", str(raised.exception))
        self.assertNotIn("TEST_SECRET", str(raised.exception))

    def test_broadcast_send_boc_return_hash_timeout_is_not_retried(self):
        client = toncenterapi.Toncenterapi()

        with patch.object(
            toncenterapi.rq,
            "request",
            side_effect=toncenterapi.rq.Timeout("response lost after submit"),
        ) as request:
            with patch.object(toncenterapi, "sleep_before_retry"):
                with self.assertRaises(toncenterapi.ToncenterTransientError):
                    client.send_message_with_hash("SIGNED_BOC")

        self.assertEqual(1, request.call_count)

    def test_send_message_with_hash_prefers_signed_message_hash_field(self):
        message_hash = base64.b64encode(bytes.fromhex("64" * 32)).decode()
        normalized_hash = base64.b64encode(bytes.fromhex("c1" * 32)).decode()
        response = FakeResponse(
            status_code=200,
            url="https://toncenter.com/api/v2/sendBocReturnHash",
            payload={
                "ok": True,
                "result": {
                    "hash": message_hash,
                    "hash_norm": normalized_hash,
                },
            },
        )
        client = toncenterapi.Toncenterapi()

        with patch.object(toncenterapi.rq, "request", return_value=response):
            result = client.send_message_with_hash("SIGNED_BOC")

        self.assertEqual(message_hash, result)

    def test_broadcast_send_boc_timeout_is_not_retried(self):
        client = toncenterapi.Toncenterapi()

        with patch.object(
            toncenterapi.rq,
            "request",
            side_effect=toncenterapi.rq.Timeout("response lost after submit"),
        ) as request:
            with patch.object(toncenterapi, "sleep_before_retry"):
                with self.assertRaises(toncenterapi.ToncenterTransientError):
                    client.send_message("SIGNED_BOC")

        self.assertEqual(1, request.call_count)

    def test_transaction_by_hash_message_fallback_uses_adjacent_transactions_wrapper(self):
        client = toncenterapi.Toncenterapi()
        responses = [
            FakeResponse(
                status_code=200,
                url="https://toncenter.com/api/v3/transactions",
                payload={"transactions": []},
            ),
            FakeResponse(
                status_code=200,
                url="https://toncenter.com/api/v3/transactionsByMessage",
                payload={"transactions": [{"hash": "MSG_HASH"}]},
            ),
            FakeResponse(
                status_code=200,
                url="https://toncenter.com/api/v3/adjacentTransactions",
                payload={"transactions": [{"hash": "ADJACENT_HASH"}]},
            ),
        ]

        with patch.object(toncenterapi.rq, "request", side_effect=responses) as request:
            transaction = client.get_transaction_by_hash("ORIGINAL_HASH")

        self.assertEqual({"hash": "ADJACENT_HASH"}, transaction)
        self.assertEqual(3, request.call_count)
        self.assertIn("adjacentTransactions", request.call_args.args[1])

    def test_response_error_message_masks_api_key_in_body(self):
        response = FakeResponse(
            status_code=500,
            url="https://toncenter.com/api/v3/test?api_key=URL_SECRET",
            text="proxy echoed https://toncenter.com/api/v3/test?api_key=BODY_SECRET",
        )

        message = toncenterapi._response_error_message("test", response)

        self.assertIn("api_key=***MASKED***", message)
        self.assertNotIn("URL_SECRET", message)
        self.assertNotIn("BODY_SECRET", message)


if __name__ == "__main__":
    unittest.main()
