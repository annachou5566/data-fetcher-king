import copy
import importlib.util
import pathlib
import sys
import types
import unittest

HERE = pathlib.Path(__file__).resolve().parent

# Stub dependencies imported by sync_listing_prices.py for pure unit tests.
dotenv = types.ModuleType("dotenv")
dotenv.load_dotenv = lambda *a, **k: None
sys.modules.setdefault("dotenv", dotenv)

boto3 = types.ModuleType("boto3")
boto3.client = lambda *a, **k: None
sys.modules.setdefault("boto3", boto3)

botocore = types.ModuleType("botocore")
botocore_config = types.ModuleType("botocore.config")
class DummyConfig:
    def __init__(self, *a, **k):
        pass
botocore_config.Config = DummyConfig
botocore.config = botocore_config
sys.modules.setdefault("botocore", botocore)
sys.modules.setdefault("botocore.config", botocore_config)

requests = types.ModuleType("requests")
sys.modules.setdefault("requests", requests)

fetch_alpha = types.ModuleType("fetch_alpha")
fetch_alpha.PROXY_WORKER_URL = None
sys.modules.setdefault("fetch_alpha", fetch_alpha)

spec_lp = importlib.util.spec_from_file_location("sync_listing_prices", HERE / "sync_listing_prices.py")
lp = importlib.util.module_from_spec(spec_lp)
spec_lp.loader.exec_module(lp)
sys.modules["sync_listing_prices"] = lp

spec = importlib.util.spec_from_file_location("gap", HERE / "enrich_alpha_history_gap.py")
gap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gap)


def target_row():
    return {
        "symbol": "KII",
        "event_time": "2026-08-14T13:00:00+00:00",
        "contract_address": "0xeec6574eabba52bac3f0277f2cd5ac7e67197886",
        "project_name": "KII",
        "listing_price": None,
    }


def all_target_rows():
    rows = []
    for symbol, event_time, contract in sorted(gap.TARGETS):
        rows.append({
            "symbol": symbol,
            "event_time": event_time,
            "contract_address": contract,
            "project_name": symbol,
            "listing_price": None,
        })
    return rows


class GapPriceTests(unittest.TestCase):
    def test_target_set_is_exactly_12(self):
        self.assertEqual(len(gap.TARGETS), 12)

    def test_apply_price_result_only_sets_allowed_fields(self):
        row = target_row()
        before_keys = set(row)
        result = {
            "vwap": 1.25,
            "open": 1.1,
            "close": 1.3,
            "max_since": {"price": 2.0, "date": "2026-08-20"},
        }
        self.assertTrue(gap.apply_price_result(row, result))
        self.assertEqual(row["listing_price"], result)
        self.assertEqual(row["ath_since_listing_price"], 2.0)
        self.assertEqual(row["ath_since_listing_date"], "2026-08-20")
        changed_keys = set(row) - before_keys
        self.assertTrue(changed_keys.issubset(set(gap.ALLOWED_TARGET_FIELDS)))

    def test_non_target_guard_detects_change(self):
        targets = all_target_rows()
        other = {"symbol": "OLD", "event_time": "2026-01-01T00:00:00+00:00", "contract_address": "0xold", "x": 1}
        before = [copy.deepcopy(x) for x in targets] + [copy.deepcopy(other)]
        after = [copy.deepcopy(x) for x in targets] + [copy.deepcopy(other)]
        self.assertTrue(gap.verify_non_target_unchanged(before, after))
        after[-1]["x"] = 2
        self.assertFalse(gap.verify_non_target_unchanged(before, after))

    def test_target_scope_rejects_unapproved_field_change(self):
        before = [target_row()]
        after = [copy.deepcopy(before[0])]
        after[0]["project_name"] = "changed"
        with self.assertRaises(RuntimeError):
            gap.verify_target_field_scope(before, after)

    def test_source_avoids_broad_maintenance_paths(self):
        src = (HERE / "enrich_alpha_history_gap.py").read_text(encoding="utf-8")
        self.assertNotIn("apply_alpha_status(", src)
        self.assertNotIn("invalidate_at_risk_listing_prices(", src)
        self.assertNotIn("invalidate_wrong_tge_dates(", src)
        self.assertNotIn("invalidate_multi_round_listing_prices(", src)
        self.assertNotIn("enrich_spot_listing_prices(", src)


    def test_agg_historical_fallback_anchors_with_end_time(self):
        old_api = lp.API_AGG_KLINES
        old_fetch = getattr(lp.fa, "fetch_smart", None)
        old_safe = getattr(lp.fa, "safe_float", None)
        old_official = lp.fetch_alpha_trade_klines_official
        seen = []
        try:
            lp.API_AGG_KLINES = "https://example.invalid/agg-klines"
            lp.fetch_alpha_trade_klines_official = lambda *a, **k: None
            lp.fa.safe_float = lambda value: float(value or 0)

            day_ms = int(lp.datetime.strptime("2026-09-04", "%Y-%m-%d").timestamp() * 1000)
            trade_ms = day_ms + 7 * 3600 * 1000

            def fake_fetch(url, retries=1):
                seen.append(url)
                if "interval=5m" in url:
                    return {"data": {"klineInfos": [
                        [trade_ms, "0.030", "0.032", "0.029", "0.031", "100", trade_ms + 299999],
                    ]}}
                if "interval=1d" in url and "endTime=" in url:
                    return {"data": {"klineInfos": [
                        [day_ms, "0.030", "0.038", "0.029", "0.031", "1000", day_ms + 86399999],
                    ]}}
                if "interval=1d" in url:
                    return {"data": {"klineInfos": [
                        [day_ms, "0.030", "0.038", "0.029", "0.031", "1000", day_ms + 86399999],
                        [day_ms + 86400000, "0.031", "0.050", "0.030", "0.045", "1200", day_ms + 2 * 86400000 - 1],
                    ]}}
                return None

            lp.fa.fetch_smart = fake_fetch
            result = lp.fetch_listing_price(
                "8453",
                "0x001aad84c21a5cd4d696c56d44866e9703c43f77",
                "2026-09-04",
                alpha_id="ALPHA_TEST",
                alpha_listing_time_ms=trade_ms,
            )
            self.assertIsNotNone(result)
            self.assertGreater(result["vwap"], 0)
            self.assertTrue(any("interval=1d" in u and "endTime=" in u for u in seen))
            self.assertTrue(any("interval=5m" in u and "endTime=" in u for u in seen))
            self.assertFalse(any("startTime=" in u for u in seen if "example.invalid/agg-klines" in u))
            self.assertEqual(result["max_since"]["price"], 0.045)
        finally:
            lp.API_AGG_KLINES = old_api
            lp.fetch_alpha_trade_klines_official = old_official
            if old_fetch is None:
                if hasattr(lp.fa, "fetch_smart"):
                    delattr(lp.fa, "fetch_smart")
            else:
                lp.fa.fetch_smart = old_fetch
            if old_safe is None:
                if hasattr(lp.fa, "safe_float"):
                    delattr(lp.fa, "safe_float")
            else:
                lp.fa.safe_float = old_safe


if __name__ == "__main__":
    unittest.main()
