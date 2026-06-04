from contextlib import contextmanager
from decimal import Decimal
import base64
import importlib
import unittest


FEE_DEPOSIT = "EQFEEDEPOSIT"
FEE_DEPOSIT_RAW = "0:fee-deposit"
DESTINATION = "EQDESTINATION"
DESTINATION_2 = "EQDESTINATION2"


class FakeRedisLock:
    def __init__(self, events):
        self.events = events

    def acquire(self, blocking=True):
        self.events.append(("redis-acquire", blocking))
        return True

    def release(self):
        self.events.append(("redis-release",))


class FakeRedis:
    def __init__(self, events):
        self.events = events

    def lock(self, name, **kwargs):
        self.events.append(("redis-lock", name, kwargs))
        return FakeRedisLock(self.events)


class FakeMessage:
    def __init__(self, events):
        self.events = events

    def to_boc(self, _has_idx):
        self.events.append(("to-boc",))
        return b"boc"


class FakeWallet:
    def __init__(self, events):
        self.events = events

    def create_transfer_message(self, **kwargs):
        self.events.append(("create-transfer", kwargs["seqno"], kwargs["to_addr"]))
        return {"message": FakeMessage(self.events)}


class FakeJettonWallet:
    def __init__(self, events):
        self.events = events

    def create_transfer_body(self, **kwargs):
        self.events.append(("jetton-body", kwargs["to_address"], kwargs["jetton_amount"]))
        return "jetton-body"


class FakeToncenter:
    def __init__(self, events):
        self.events = events

    def get_account_seqno(self, address):
        self.events.append(("seqno", address))
        return 100

    def send_message_with_hash(self, boc):
        self.events.append(("send", boc))
        return base64.b64encode(b"tx").decode("ascii")

    def jetton_master_decimals(self, _master):
        return 6

    def get_account_wallet_jetton_address(self, account, _master):
        self.events.append(("jetton-wallet", account))
        return "jetton-wallet"


class FeeDepositSeqnoGuardTests(unittest.TestCase):
    def test_fee_deposit_seqno_lock_is_reentrant(self):
        guard = importlib.import_module("app.fee_deposit_seqno_guard")
        events = []
        original_from_url = guard.redis.Redis.from_url
        original_host = guard.config["REDIS_HOST"]
        guard.config["REDIS_HOST"] = "redis.local"
        guard.redis.Redis.from_url = lambda *_args, **_kwargs: FakeRedis(events)
        try:
            with guard.fee_deposit_seqno_lock(reason="outer"):
                with guard.fee_deposit_seqno_lock(reason="inner"):
                    events.append(("inside", guard._fee_deposit_seqno_lock_depth.get()))
        finally:
            guard.redis.Redis.from_url = original_from_url
            guard.config["REDIS_HOST"] = original_host

        self.assertEqual(
            [event[0] for event in events],
            ["redis-lock", "redis-acquire", "inside", "redis-release"],
        )
        self.assertEqual(events[2], ("inside", 2))

    def patch_coin_module(self, coin_module, events):
        originals = {
            "guard": coin_module.fee_deposit_seqno_lock,
            "from_mnemonics": coin_module.TonWallets.from_mnemonics,
            "is_valid": coin_module.is_valid_ton_address,
            "to_nanotons": coin_module.to_nanotons,
            "bytes_to_b64str": coin_module.bytes_to_b64str,
            "jetton_wallet": coin_module.JettonWallet,
            "address": coin_module.Address,
        }

        @contextmanager
        def fake_guard(reason=None):
            events.append(("guard-enter", reason))
            try:
                yield
            finally:
                events.append(("guard-exit", reason))

        coin_module.fee_deposit_seqno_lock = fake_guard
        coin_module.TonWallets.from_mnemonics = lambda *_args, **_kwargs: (
            [],
            None,
            None,
            FakeWallet(events),
        )
        coin_module.is_valid_ton_address = lambda _address: True
        coin_module.to_nanotons = lambda value: int(Decimal(str(value)) * Decimal("1000000000"))
        coin_module.bytes_to_b64str = lambda _boc: "boc64"
        coin_module.JettonWallet = lambda: FakeJettonWallet(events)
        coin_module.Address = lambda address: address
        return originals

    def restore_coin_module(self, coin_module, originals):
        coin_module.fee_deposit_seqno_lock = originals["guard"]
        coin_module.TonWallets.from_mnemonics = originals["from_mnemonics"]
        coin_module.is_valid_ton_address = originals["is_valid"]
        coin_module.to_nanotons = originals["to_nanotons"]
        coin_module.bytes_to_b64str = originals["bytes_to_b64str"]
        coin_module.JettonWallet = originals["jetton_wallet"]
        coin_module.Address = originals["address"]

    def fake_coin(self, coin_module, symbol, events):
        coin = coin_module.Coin.__new__(coin_module.Coin)
        coin.symbol = symbol
        coin.jetton_master_address = "jetton-master"
        coin.toncenter = FakeToncenter(events)
        coin.get_fee_deposit_account = lambda address_type: {
            "public": FEE_DEPOSIT,
            "raw": FEE_DEPOSIT_RAW,
        }[address_type]
        coin.get_fee_deposit_coin_balance = lambda: Decimal("100")
        coin.get_fee_deposit_jetton_balance = lambda: Decimal("100")
        coin.get_transaction_fee = lambda *_args, **_kwargs: Decimal("0.01")
        coin.get_jetton_transaction_fee = lambda *_args, **_kwargs: Decimal("0.04")
        coin.initialize_account = lambda account: events.append(("initialize", account)) or True
        coin.get_mnemonic_from_address = lambda account: events.append(
            ("mnemonic", account)
        ) or []
        return coin

    def test_native_fee_deposit_multipayout_uses_seqno_guard_before_send(self):
        coin_module = importlib.import_module("app.coin")
        events = []
        originals = self.patch_coin_module(coin_module, events)
        try:
            coin = self.fake_coin(coin_module, "TON", events)
            result = coin.make_multipayout_ton(
                [
                    {"dest": DESTINATION, "amount": Decimal("1")},
                    {"dest": DESTINATION_2, "amount": Decimal("2")},
                ],
                Decimal("0.006"),
            )
        finally:
            self.restore_coin_module(coin_module, originals)

        self.assertEqual([item["dest"] for item in result], [DESTINATION, DESTINATION_2])
        self.assertEqual([item["amount"] for item in result], [1.0, 2.0])
        self.assertEqual([item["status"] for item in result], ["success", "success"])
        self.assertIn(("guard-enter", "fee-deposit-ton-multipayout"), events)
        self.assertLess(
            events.index(("guard-enter", "fee-deposit-ton-multipayout")),
            events.index(("seqno", FEE_DEPOSIT_RAW)),
        )
        self.assertLess(
            events.index(("send", "boc64")),
            events.index(("guard-exit", "fee-deposit-ton-multipayout")),
        )

    def test_jetton_fee_deposit_multipayout_uses_seqno_guard_before_send(self):
        coin_module = importlib.import_module("app.coin")
        events = []
        originals = self.patch_coin_module(coin_module, events)
        try:
            coin = self.fake_coin(coin_module, "TON-USDT", events)
            result = coin.make_multipayout_jetton(
                [
                    {"dest": DESTINATION, "amount": Decimal("1")},
                    {"dest": DESTINATION_2, "amount": Decimal("2")},
                ],
                Decimal("0.006"),
            )
        finally:
            self.restore_coin_module(coin_module, originals)

        self.assertEqual([item["dest"] for item in result], [DESTINATION, DESTINATION_2])
        self.assertEqual([item["amount"] for item in result], [1.0, 2.0])
        self.assertEqual([item["status"] for item in result], ["success", "success"])
        self.assertIn(("guard-enter", "fee-deposit-jetton-multipayout"), events)
        self.assertLess(
            events.index(("guard-enter", "fee-deposit-jetton-multipayout")),
            events.index(("seqno", FEE_DEPOSIT_RAW)),
        )
        self.assertLess(
            events.index(("send", "boc64")),
            events.index(("guard-exit", "fee-deposit-jetton-multipayout")),
        )

if __name__ == "__main__":
    unittest.main()
