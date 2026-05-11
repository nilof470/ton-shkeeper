import unittest
from unittest.mock import patch

from app import events


class BlockScanResultTest(unittest.TestCase):
    def test_native_ton_failure_blocks_checkpoint_when_enabled(self):
        result = events.BlockScanResult(
            block=66018441,
            native_ton=events.SCAN_TRANSIENT_FAILURE,
            jettons=events.SCAN_OK,
        )

        with patch.dict(events.config, {"SCAN_NATIVE_TON_EVENTS": True}):
            self.assertFalse(result.can_advance_checkpoint())

    def test_native_ton_failure_does_not_block_checkpoint_when_disabled(self):
        result = events.BlockScanResult(
            block=66018441,
            native_ton=events.SCAN_TRANSIENT_FAILURE,
            jettons=events.SCAN_OK,
        )

        with patch.dict(events.config, {"SCAN_NATIVE_TON_EVENTS": False}):
            self.assertTrue(result.can_advance_checkpoint())

    def test_jetton_failure_blocks_checkpoint_even_when_native_ton_disabled(self):
        result = events.BlockScanResult(
            block=66018441,
            native_ton=events.SCAN_OK,
            jettons=events.SCAN_TRANSIENT_FAILURE,
        )

        with patch.dict(events.config, {"SCAN_NATIVE_TON_EVENTS": False}):
            self.assertFalse(result.can_advance_checkpoint())


if __name__ == "__main__":
    unittest.main()
