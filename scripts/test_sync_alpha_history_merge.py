import importlib.util
import pathlib
import sys
import types
import unittest
from datetime import datetime, timezone

# Keep unit tests stdlib-only: the functions under test do not need a real R2 client.
botocore = types.ModuleType("botocore")
botocore_config = types.ModuleType("botocore.config")
class DummyConfig:
    def __init__(self, *args, **kwargs):
        pass
botocore_config.Config = DummyConfig
botocore.config = botocore_config
sys.modules.setdefault("botocore", botocore)
sys.modules.setdefault("botocore.config", botocore_config)

boto3 = types.ModuleType("boto3")
boto3.client = lambda *args, **kwargs: None
sys.modules.setdefault("boto3", boto3)

HERE = pathlib.Path(__file__).resolve().parent
MODULE_PATH = HERE / "sync_alpha_history.py"
spec = importlib.util.spec_from_file_location("sync_alpha_history", MODULE_PATH)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def event(**overrides):
    base = {
        "project_name": "Example",
        "symbol": "ABC",
        "event_type": "grab",
        "points_threshold": "200",
        "amount_per_user": "100",
        "contract_address": "0xAbCd",
        "chain_id": "56",
        "event_time": "2026-09-20T18:00:00+00:00",
        "status": "ended",
        "phase": 1,
        "completed": True,
        "spot_listed": False,
        "futures_listed": False,
        "pretge": False,
        "source_channel": "historical",
    }
    base.update(overrides)
    return base


class AlphaHistoryMergeTests(unittest.TestCase):
    def test_identity_normalizes_evm_contract_and_phase(self):
        a = event(contract_address="0xABCD", phase=1)
        b = event(contract_address="0xabcd", phase="1")
        self.assertEqual(mod.event_identity(a), mod.event_identity(b))

    def test_multi_round_same_contract_stays_distinct(self):
        a = event(event_time="2026-09-20T18:00:00+00:00", phase=1)
        b = event(event_time="2026-09-21T18:00:00+00:00", phase=2)
        self.assertNotEqual(mod.event_identity(a), mod.event_identity(b))

    def test_existing_enrichment_is_preserved(self):
        existing = event(
            project_name="",
            listing_price={"vwap": 1.23, "max_since": {"price": 2.0}},
            spot_listing_price={"vwap": 1.80},
            air_number=7,
            completed=True,
        )
        incoming = event(
            project_name="Filled Name",
            listing_price={"vwap": 999},
            spot_listing_price={"vwap": 999},
            completed=True,
            spot_listed=True,
        )
        merged = mod.merge_existing(existing, incoming)
        self.assertEqual(merged["project_name"], "Filled Name")
        self.assertEqual(merged["listing_price"], existing["listing_price"])
        self.assertEqual(merged["spot_listing_price"], existing["spot_listing_price"])
        self.assertEqual(merged["air_number"], 7)
        self.assertTrue(merged["spot_listed"])

    def test_all_accepts_completed_event_even_if_not_ended(self):
        existing = []
        incoming = [event(status="live", completed=True)]
        merged, stats, added = mod.merge_catalog(
            existing, incoming, require_ended=False
        )
        self.assertEqual(len(merged), 1)
        self.assertEqual(stats["added"], 1)
        self.assertEqual(len(added), 1)

    def test_history_requires_completed_and_ended(self):
        incoming = [
            event(symbol="ENDED", contract_address="0x01", status="ended", completed=True),
            event(symbol="LIVE", contract_address="0x02", status="live", completed=True),
            event(symbol="INC", contract_address="0x03", status="ended", completed=False),
        ]
        merged, stats, added = mod.merge_catalog(
            [], incoming, require_ended=True
        )
        self.assertEqual([x["symbol"] for x in merged], ["ENDED"])
        self.assertEqual(stats["added"], 1)
        self.assertEqual(stats["skipped_not_ended"], 1)
        self.assertEqual(stats["skipped_incomplete"], 1)
        self.assertEqual(len(added), 1)

    def test_merge_never_removes_existing_rows(self):
        existing = [
            event(symbol="OLD1", contract_address="0x11"),
            event(symbol="OLD2", contract_address="0x22"),
        ]
        merged, stats, _ = mod.merge_catalog(
            existing,
            [event(symbol="NEW", contract_address="0x33")],
            require_ended=True,
        )
        self.assertGreaterEqual(len(merged), len(existing))
        self.assertEqual(stats["existing"], 2)
        self.assertEqual(stats["added"], 1)

    def test_source_has_no_competition_dependency(self):
        source = MODULE_PATH.read_text(encoding="utf-8").lower()
        self.assertNotIn("competition", source)
        self.assertNotIn("coai", source)


if __name__ == "__main__":
    unittest.main()
