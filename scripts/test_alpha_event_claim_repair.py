"""No-network legacy Claim recovery tests: never fake an unverifiable Peak."""
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import alpha_event_claim_repair as repair
import sync_listing_prices as pricing

EVENT = "2026-02-10T20:00:00Z"
EVENT_MS = int(datetime.fromisoformat(EVENT.replace("Z","+00:00")).timestamp()*1000)

def legacy(name="ESP", spot=True, claim=.07645998824904926, peak=.0720195, event=EVENT):
    return {
        "symbol":name, "event_time":event,"contract_address":"0xabc",
        "spot_listed":spot, "spot_listed_at":"2026-02-12" if spot else None,
        "amount_per_user":"2520",
        "listing_price":{"vwap":claim,"date":"2026-02-12","max_since":{"price":peak,"date":"2026-02-12"}},
        "ath_since_listing_price":peak,
    }

def exact(vwap=.0726, quote=5000, day="2026-02-12"):
    return {
        "vwap":vwap, "date":day, "event_start_ms":EVENT_MS,
        "claim_method":"event_5m_quote_vwap", "claim_quote_usd":quote,
        "claim_source":"binance_spot_1m",
        "max_since":{"price":.07,"date":day},
    }

def candle(t,price,volume=10,quote=True):
    row=[str(t),str(price),str(price),str(price),str(price),
         str(volume),str(t+59999),str(price*volume if quote else 0),10,"0","0","0"]
    return row

class LegacyPricingRepair(unittest.TestCase):
    def test_anomalous_spot_claim_corrected_by_real_5m_trades(self):
        e=legacy()
        result=repair.repair_legacy_claims([e],fetcher=lambda _:exact())
        self.assertEqual(result["repaired"],1)
        self.assertEqual(result["legacy_below_claim_remaining"],0)
        self.assertAlmostEqual(e["listing_price"]["vwap"],.0726)
        self.assertEqual(e["listing_price"]["max_since"],{"price":.0726,"date":"2026-02-12"})
        self.assertEqual(e["ath_since_listing_price"],.0726)
        self.assertEqual(e["listing_price"]["claim_method"],repair.EXACT_CLAIM_METHOD)

    def test_preserves_verifiable_legacy_peak_above_new_claim(self):
        e=legacy(peak=.20)
        repair.repair_legacy_claims([e],fetcher=lambda _:exact(vwap=.072))
        self.assertEqual(e["listing_price"]["max_since"]["price"],.2)
        self.assertEqual(e["listing_price"]["peak_quality"],"pending_liquidity_scan")

    def test_stale_or_unverified_claim_never_floored(self):
        e=legacy()
        old=dict(e["listing_price"])
        r=repair.repair_legacy_claims([e],fetcher=lambda _:{"vwap":.07,"date":"2026-02-12"})
        self.assertEqual(r["held"],1)
        self.assertEqual(e["listing_price"]["max_since"],old["max_since"])
        self.assertEqual(e["listing_price"]["vwap"],old["vwap"])

    def test_no_data_preserves_old_prices_and_throttles_retry(self):
        e=legacy()
        now=datetime(2026,11,16,tzinfo=timezone.utc)
        r=repair.repair_legacy_claims([e],fetcher=lambda _:None,now=now)
        self.assertEqual((r["held"],r["repaired"]),(1,0))
        self.assertEqual(repair.candidates([e],now=now),[])
        self.assertEqual(len(repair.candidates([e],now=now.replace(day=18))),1)

    def test_contract_and_campaign_must_match(self):
        e=legacy()
        e["event_time"]="2026-01-01T20:00:00Z"
        result=repair.repair_legacy_claims([e],fetcher=lambda _:exact())
        self.assertEqual(result["held"],1)
        self.assertNotIn("claim_method",e["listing_price"])

    def test_exact_claim_not_queued_again(self):
        e=legacy()
        repair.apply_replacement(e,exact())
        self.assertEqual(repair.candidates([e]),[])

    def test_contradictory_rows_prioritized_over_healthy_legacy(self):
        x=legacy("OK",spot=False,peak=.5)
        y=legacy("BAD",spot=False,peak=.01)
        z=legacy("SPOT",spot=True,peak=.01)
        self.assertEqual([e["symbol"] for e in repair.candidates([x,y,z])],
                         ["SPOT","BAD","OK"])

    def test_evidence_read_only_until_valid(self):
        e=legacy()
        existing={"cursor_ms":EVENT_MS,"best_price":.08}
        e["listing_price"]["_peak_liquidity_v1"]=existing
        repair.repair_legacy_claims([e],fetcher=lambda _:exact(vwap=.07))
        self.assertEqual(e["listing_price"]["_peak_liquidity_v1"],existing)

    def test_exact_trade_vwap_requires_positive_quote(self):
        t=EVENT_MS
        good=[candle(t,.07,volume=100),candle(t+60000,.09,volume=10)]
        v,q=pricing._compute_exact_trade_vwap(good,t,t+300000)
        self.assertAlmostEqual(v,(7+.9)/110)
        self.assertAlmostEqual(q,7.9)
        bad=[candle(t,.07,quote=False)]
        self.assertEqual(pricing._compute_exact_trade_vwap(bad,t,t+300000),(None,0.0))

    def test_spot_fallback_requires_contract_verified_alpha_identity(self):
        e=legacy("ESP")
        stamp=int(datetime(2026,2,12,tzinfo=timezone.utc).timestamp()*1000)
        with patch.object(pricing,"_get_alpha_info",return_value=("ALPHA_693",stamp)), \
             patch.object(pricing,"fetch_listing_price",return_value=None), \
             patch.object(pricing,"_ALPHA_STATUS_MAP",{"ESP":{"listingCex":True}}), \
             patch.object(pricing,"fetch_event_price_public_spot",return_value={**exact(),"open":.07,"close":.07}) as spot:
            _,candidate=pricing._process_one(e,1,1)
        self.assertIsNotNone(candidate)
        self.assertEqual(spot.call_args.kwargs["spot_start_ms"],stamp)

        with patch.object(pricing,"_get_alpha_info",return_value=(None,None)), \
             patch.object(pricing,"fetch_listing_price",return_value=None), \
             patch.object(pricing,"_ALPHA_STATUS_MAP",{"ESP":{"listingCex":True}}), \
             patch.object(pricing,"fetch_event_price_public_spot") as spot:
            _,candidate=pricing._process_one(e,1,1)
        self.assertIsNone(candidate)
        spot.assert_not_called()

    def test_new_exact_claim_has_claim_as_peak_floor(self):
        candidate=exact(vwap=.08)
        candidate["max_since"]={"price":.06,"date":"2026-02-12"}
        result=pricing._normalize_verified_claim_peak(candidate)
        self.assertEqual(result["max_since"]["price"],.08)
        self.assertEqual(result["peak_quality"],"pending_liquidity_scan")

        unverified=dict(candidate)
        unverified.pop("claim_method")
        result=pricing._normalize_verified_claim_peak(unverified)
        self.assertEqual(result["max_since"]["price"],.06)

    def test_spot_event_window_uses_verified_listing_time(self):
        opened=EVENT_MS+2*86400000
        bars=[candle(opened+60_000,.07,volume=10),
              candle(opened+120_000,.09,volume=10)]
        def fake_spot(symbol,interval,start_ms=None,**kwargs):
            if interval=="1m":
                self.assertEqual(start_ms,opened)
                return bars
            return []
        with patch.object(pricing,"_binance_public_klines",side_effect=fake_spot):
            p=pricing.fetch_event_price_public_spot("ESP",EVENT_MS,spot_start_ms=opened)
        self.assertEqual(p["claim_method"],repair.EXACT_CLAIM_METHOD)
        self.assertAlmostEqual(p["vwap"],.08)
        self.assertEqual(p["date"],"2026-02-12")
        self.assertEqual(p["event_start_ms"],EVENT_MS)
        self.assertEqual(p["max_since"]["price"],p["vwap"])

if __name__=="__main__":
    unittest.main()