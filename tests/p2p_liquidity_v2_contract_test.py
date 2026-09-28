import importlib.util
import math
import pathlib
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "scripts" / "fetch_p2p.py").read_text(encoding="utf-8")
WORKFLOW = (ROOT / ".github" / "workflows" / "fetch_p2p.yml").read_text(encoding="utf-8")
MIGRATE = (ROOT / "scripts" / "migrate_p2p_history.py").read_text(encoding="utf-8")

spec = importlib.util.spec_from_file_location("fetch_p2p_v2", ROOT / "scripts" / "fetch_p2p.py")
p2p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p2p)


def ad(merchant, *, price=25_000, max_fiat=25_000_000, dynamic_max_fiat=None,
       available=5_000, min_fiat=100_000, order_count=100, rate=0.99, ad_id=None):
    return {
        "price": price,
        "minFiat": min_fiat,
        "maxFiat": max_fiat,
        "dynamicMaxFiat": dynamic_max_fiat,
        "availableCrypto": available,
        "payTypes": ["BANK"],
        "merchant": merchant,
        "merchantId": merchant,
        "adId": ad_id or f"ad-{merchant}-{max_fiat}-{available}",
        "providerOrderCount": order_count,
        "providerCompletionRate": rate,
    }


class FakeResponse:
    def __init__(self, body, status=200):
        self._body = body
        self.status_code = status

    def json(self):
        return self._body


class BybitSession:
    def __init__(self):
        self.calls = []

    def post(self, url, json=None, timeout=None):
        body = dict(json or {})
        self.calls.append(body)
        item = {
            "currencyId": "VND",
            "tokenId": body["tokenId"],
            "side": body["side"],
            "price": "25000",
            "minAmount": "100000",
            "maxAmount": "25000000",
            "lastQuantity": "5000",
            "accountId": "0",
            "userMaskId": f"mask-{body['side']}",
            "id": f"bybit-{body['side']}",
            "nickName": "Bybit Merchant",
            "payments": ["1"],
            "recentOrderNum": "100",
            "recentExecuteRate": "99",
            "createDate": "2026-09-28",
        }
        return FakeResponse({"result": {"count": 1, "items": [item]}})


class P2PLiquidityV2ContractTest(unittest.TestCase):
    def test_methodology_is_explicit_and_all_is_excluded(self):
        self.assertEqual(p2p.LIQUIDITY_V2_METHODOLOGY, "p2p-liquidity-v2-r2")
        self.assertEqual(p2p.LIQUIDITY_V2_MAKER_BUY_CAP_CRYPTO["USDT"], 10_000)
        self.assertEqual(p2p.LIQUIDITY_V2_MAKER_BUY_CAP_CRYPTO["USDC"], 10_000)
        self.assertEqual(p2p.LIQUIDITY_V2_QUALIFIED_PROVIDERS, ("binance", "bybit"))
        self.assertEqual(
            p2p.LIQUIDITY_V2_EXCLUDED_PROVIDERS["okx"],
            "provider_completeness_not_proven",
        )
        self.assertEqual(p2p.LIQUIDITY_V2_AGGREGATE_STATUS, "EXCLUDED")
        self.assertIn("cross_provider_identity", p2p.LIQUIDITY_V2_AGGREGATE_REASON)

    def test_r2_capacity_is_side_aware_and_sell_is_capped_per_merchant(self):
        ads = [
            ad("m1", max_fiat=50_000_000, available=7_000, ad_id="a1"),     # 2,000
            ad("m1", max_fiat=125_000_000, available=8_000, ad_id="a2"),    # 5,000; wins merchant
            ad("m2", max_fiat=500_000_000, available=25_000, ad_id="a3"),   # 20,000
        ]
        buy = p2p.build_liquidity_v2_record("binance", "USDT", "BUY", ads, 123)
        sell = p2p.build_liquidity_v2_record("binance", "USDT", "SELL", ads, 123)
        self.assertEqual(buy["capacity_crypto"], 25_000)
        self.assertEqual(sell["capacity_crypto"], 15_000)
        self.assertEqual(buy["qualified_merchant_count"], 2)
        self.assertEqual(sell["qualified_merchant_count"], 2)
        self.assertEqual(buy["qualified_ad_count"], 3)
        self.assertEqual(sell["qualified_ad_count"], 3)
        self.assertIsNone(buy["maker_buy_cap_crypto"])
        self.assertEqual(sell["maker_buy_cap_crypto"], 10_000)
        self.assertEqual(
            buy["capacity_policy"],
            "side_aware_dynamic_order_maker_buy_cap10k_v2",
        )
        self.assertEqual(
            sell["capacity_policy"],
            "side_aware_dynamic_order_maker_buy_cap10k_v2",
        )

    def test_r2_prefers_dynamic_max_fiat_without_changing_static_market_field(self):
        rows = [
            ad(
                "m1",
                max_fiat=250_000_000,
                dynamic_max_fiat=50_000_000,
                available=20_000,
            )
        ]
        buy = p2p.build_liquidity_v2_record("binance", "USDT", "BUY", rows, 1)
        sell = p2p.build_liquidity_v2_record("binance", "USDT", "SELL", rows, 1)
        self.assertEqual(buy["capacity_crypto"], 2_000)
        self.assertEqual(sell["capacity_crypto"], 2_000)

    def test_same_order_and_completion_floor_controls_capacity(self):
        low = [ad("m1", order_count=9, rate=0.84)]
        high = [ad("m1", order_count=10, rate=0.85)]
        a = p2p.build_liquidity_v2_record("bybit", "USDT", "BUY", low, 1)
        b = p2p.build_liquidity_v2_record("bybit", "USDT", "BUY", high, 1)
        self.assertEqual(a["capacity_crypto"], 0)
        self.assertGreater(b["capacity_crypto"], 0)
        self.assertEqual(
            b["qualification_policy"],
            "provider_orders_gte10_completion_gte0_85_v1",
        )

    def test_invalid_capacity_inputs_fail_closed_per_ad(self):
        rows = [
            ad("valid"),
            ad("", available=5_000),
            ad("no-inventory", available=0),
            ad("no-max", max_fiat=0),
            ad("bad-price", price=1),
        ]
        out = p2p.build_liquidity_v2_record("bybit", "USDT", "SELL", rows, 1)
        self.assertEqual(out["qualified_ad_count"], 1)
        self.assertEqual(out["qualified_merchant_count"], 1)
        self.assertGreater(out["capacity_crypto"], 0)

    def test_okx_liquidity_is_excluded_after_completeness_gate(self):
        self.assertNotIn("OKX_LIQUIDITY_URL", SOURCE)
        self.assertNotIn("def fetch_okx_v2_side", SOURCE)
        self.assertIn('"okx": "provider_completeness_not_proven"', SOURCE)


    def test_binance_normalizer_captures_dynamic_max_for_r2(self):
        item = {
            "price": "25000",
            "minSingleTransAmount": "100000",
            "maxSingleTransAmount": "250000000",
            "dynamicMaxSingleTransAmount": "50000000",
            "surplusAmount": "20000",
            "advNo": "ad-1",
            "advertiser": {
                "userNo": "merchant-1",
                "nickName": "Merchant",
                "monthOrderCount": 100,
                "monthFinishRate": 0.99,
            },
        }
        with patch.object(p2p, "fetch_binance_ads_page", return_value=([item], 1, True)):
            stats, ads = p2p.fetch_binance_side(object(), "USDT", "BUY")
        self.assertTrue(stats["v2_required_fields_complete"])
        self.assertEqual(len(ads), 1)
        self.assertEqual(ads[0]["maxFiat"], 250000000)
        self.assertEqual(ads[0]["dynamicMaxFiat"], 50000000)

    def test_binance_required_field_gap_excludes_v2_side(self):
        item = {
            "price": "25000",
            "minSingleTransAmount": "100000",
            # maxSingleTransAmount intentionally missing
            "surplusAmount": "5000",
            "advNo": "ad-1",
            "advertiser": {
                "userNo": "merchant-1",
                "nickName": "Merchant",
                "monthOrderCount": 100,
                "monthFinishRate": 0.99,
            },
        }
        with patch.object(p2p, "fetch_binance_ads_page", return_value=([item], 1, True)):
            stats, ads = p2p.fetch_binance_side(object(), "USDT", "BUY")
        self.assertEqual(ads, [])
        self.assertEqual(stats["v2_invalid_ad_count"], 1)
        self.assertFalse(stats["v2_required_fields_complete"])


    def test_bybit_maps_taker_buy_to_maker_sell_one_and_sell_to_buy_zero(self):
        buy_session = BybitSession()
        stats_buy, ads_buy = p2p.fetch_bybit_v2_side(buy_session, "USDC", "BUY")
        self.assertFalse(stats_buy["is_partial"])
        self.assertEqual(buy_session.calls[0]["side"], "1")
        self.assertEqual(ads_buy[0]["merchantId"], "mask-1")
        self.assertEqual(ads_buy[0]["paymentIds"], ["1"])
        self.assertEqual(ads_buy[0]["payTypes"], [])

        sell_session = BybitSession()
        stats_sell, _ = p2p.fetch_bybit_v2_side(sell_session, "USDC", "SELL")
        self.assertFalse(stats_sell["is_partial"])
        self.assertEqual(sell_session.calls[0]["side"], "0")

    def test_market_keeps_legacy_binance_shape_while_v2_stays_in_daily_records(self):
        def binance_side(_session, asset, side):
            stats = {
                "liquidity_verified": 100.0,
                "liquidity_unverified": 50.0,
                "liquidity_total": 150.0,
                "merchant_count_verified": 1,
                "merchant_count_unverified": 1,
                "merchant_count_total": 2,
                "ad_count_raw": 1,
                "market_ad_count": 1,
                "reported_ad_count": 1,
                "v2_required_fields_complete": True,
                "is_partial": False,
            }
            return stats, [ad(f"binance-{asset}-{side}", ad_id=f"b-{asset}-{side}")]

        def bybit_side(_session, asset, side):
            stats = {
                "reported_ad_count": 1,
                "ad_count_raw": 1,
                "market_ad_count": 1,
                "pages_fetched": 1,
                "required_fields_complete": True,
                "is_partial": False,
            }
            return stats, [ad(f"bybit-{asset}-{side}", ad_id=f"y-{asset}-{side}")]

        with patch.object(p2p, "fetch_binance_side", side_effect=binance_side), \
             patch.object(p2p, "fetch_bybit_v2_side", side_effect=bybit_side):
            records, market = p2p.build_liquidity_and_market(object(), 123)

        self.assertIsNotNone(market)
        self.assertTrue(market["complete"])
        self.assertEqual(market["schema_version"], 1)
        self.assertIn("assets", market)
        self.assertNotIn("providers", market)
        self.assertNotIn("liquidity_v2", market)
        v2 = [r for r in records if r.get("record_type") == "liquidity_v2_snapshot"]
        self.assertEqual(len(v2), 8)
        self.assertEqual({r["exchange"] for r in v2}, {"binance", "bybit"})
        self.assertTrue(all(r["methodology_version"] == "p2p-liquidity-v2-r2" for r in v2))
        sell = [r for r in v2 if r["side"] == "SELL"]
        buy = [r for r in v2 if r["side"] == "BUY"]
        self.assertTrue(all(r["maker_buy_cap_crypto"] == 10_000 for r in sell))
        self.assertTrue(all(r["maker_buy_cap_crypto"] is None for r in buy))
        self.assertTrue(all(r["aggregate_eligible"] is False for r in v2))

    def test_partial_bybit_side_is_excluded_without_killing_binance_market(self):
        def binance_side(_session, asset, side):
            return {
                "liquidity_verified": 100.0,
                "liquidity_unverified": 0.0,
                "liquidity_total": 100.0,
                "merchant_count_verified": 1,
                "merchant_count_unverified": 0,
                "merchant_count_total": 1,
                "ad_count_raw": 1,
                "market_ad_count": 1,
                "reported_ad_count": 1,
                "v2_required_fields_complete": True,
                "is_partial": False,
            }, [ad(f"b-{asset}-{side}", ad_id=f"b-{asset}-{side}")]

        def bybit_side(_session, asset, side):
            if asset == "USDC" and side == "SELL":
                return {"reported_ad_count": 10, "pages_fetched": 1, "required_fields_complete": True, "is_partial": True}, []
            return {"reported_ad_count": 1, "pages_fetched": 1, "required_fields_complete": True, "is_partial": False}, [
                ad(f"y-{asset}-{side}", ad_id=f"y-{asset}-{side}")
            ]

        with patch.object(p2p, "fetch_binance_side", side_effect=binance_side), \
             patch.object(p2p, "fetch_bybit_v2_side", side_effect=bybit_side):
            records, market = p2p.build_liquidity_and_market(object(), 123)

        self.assertIsNotNone(market)
        self.assertTrue(market["complete"])
        blocked = [
            r for r in records
            if r.get("record_type") == "liquidity_v2_snapshot"
            and r.get("exchange") == "bybit"
            and r.get("asset") == "USDC"
            and r.get("side") == "SELL"
        ]
        self.assertEqual(blocked, [])

    def test_required_field_gap_excludes_bybit_side(self):
        class BadBybitSession:
            def post(self, url, json=None, timeout=None):
                body = dict(json or {})
                item = {
                    "currencyId": "VND",
                    "tokenId": body["tokenId"],
                    "side": body["side"],
                    "price": "25000",
                    "minAmount": "100000",
                    "maxAmount": "25000000",
                    # lastQuantity intentionally missing
                    "accountId": "merchant",
                    "id": "bad-ad",
                    "recentOrderNum": "100",
                    "recentExecuteRate": "99",
                }
                return FakeResponse({"result": {"count": 1, "items": [item]}})

        stats, ads = p2p.fetch_bybit_v2_side(BadBybitSession(), "USDT", "BUY")
        self.assertEqual(ads, [])
        self.assertEqual(stats["invalid_ad_count"], 1)
        self.assertFalse(stats["required_fields_complete"])

    def test_v1_history_is_preserved_and_v2_is_prospective_only(self):
        self.assertIn('"record_type": "liquidity_snapshot"', SOURCE)
        self.assertIn('"record_type": "liquidity_v2_snapshot"', SOURCE)
        self.assertNotIn("liquidity_v2_snapshot", MIGRATE)
        self.assertIn("Historical v1 imbalance remains Binance-only", SOURCE)

    def test_existing_storage_owners_and_legacy_scheduler_are_preserved(self):
        self.assertIn('R2_KEY_LEGACY   = "p2p-data.json"', SOURCE)
        self.assertIn('R2_DAILY_PREFIX = "p2p-snapshots/"', SOURCE)
        self.assertIn('R2_MANIFEST_KEY = "p2p-snapshots/_manifest.json"', SOURCE)
        self.assertIn('R2_MARKET_KEY   = "p2p-snapshots/_market-latest.json"', SOURCE)
        self.assertNotIn("LIQUIDITY_V2_R2_KEY", SOURCE)
        self.assertNotIn("LIQUIDITY_V2_BUCKET", SOURCE)
        self.assertIn('P2P_WRITE_LEGACY", "1"', SOURCE)
        self.assertIn("P2P_WRITE_LEGACY:     '1'", WORKFLOW)
        self.assertIn("cron: '*/10 * * * *'", WORKFLOW)

    def test_no_all_record_is_emitted(self):
        self.assertNotIn('"exchange": "all"', SOURCE.lower())
        self.assertNotIn('"exchange": "ALL"', SOURCE)
        sample = p2p.build_liquidity_v2_record("bybit", "USDC", "BUY", [ad("m")], 1)
        self.assertEqual(sample["aggregate_status"], "EXCLUDED")
        self.assertFalse(sample["aggregate_eligible"])


if __name__ == "__main__":
    unittest.main()
