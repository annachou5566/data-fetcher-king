"""Offline, no-R2/no-network tests of historic contract-fenced Spot Peak recovery."""
import copy
import unittest
from datetime import datetime, timezone

from alpha_event_spot_allowlist import SPOT_MISSING_PEAK_ALLOWLIST
from alpha_event_spot_backfill import (
    DAY_MS, REPAIR_KEY, _identity, _recover_one,
    repair_missing_spot_peaks,
)
from alpha_event_peak_liquidity import WINDOW_MS
from alpha_events_completion_only_guard import (
    _no_price_erasure, ensure_all_contains_existing_history,
    enforce_no_unverified_history_admission,
    keep_existing_history_without_unverified_admission,
)

LISTED = int(datetime(2026, 1, 22, tzinfo=timezone.utc).timestamp() * 1000)
NOW = LISTED + 2 * DAY_MS + 18 * 3600_000


def bar(ts, price, quote=2000, trades=100, high=None):
    high = high if high is not None else price * 1.02
    base = quote / price
    return [ts, price, high, price * .98, price,
            base, ts + 299999, quote, trades, "0", "0", "0"]


def event(symbol="SENT"):
    ref = SPOT_MISSING_PEAK_ALLOWLIST[symbol]
    return {
        "symbol": symbol, "chain_id": ref["chain"],
        "contract_address": ref["contract"], "event_type": "tge",
        "spot_listed": True, "event_time": "2026-01-19T20:00:00Z",
        "spot_listed_at": ref["first_spot_day"],
        "spot_listing_price": {"vwap": .026933, "date": ref["first_spot_day"]},
        "listing_price": {"vwap": .026933, "date": ref["first_spot_day"],
                          "max_since": None},
        "amount_per_user": 11849,
        "ath_since_listing_price": None,
    }


class FakeMarket:
    def __init__(self):
        self.calls = []
        self.fail_first = False
        self.drop_day = False
        self.spoof_first = False
        self.wicked = False

        self.five = {}
        # Listing day: first trade at 13:00, as announced by Binance.
        self.five[LISTED] = [
            bar(LISTED + i * WINDOW_MS,
                .025 if i == 156 else .026,
                quote=12500 if i == 156 else 2000)
            for i in range(156, 288)
        ]
        self.five[LISTED + DAY_MS] = [
            bar(LISTED + DAY_MS + i * WINDOW_MS,
                .0474 if i == 14 else .030,
                quote=300000 if i == 14 else 2000,
                trades=3000 if i == 14 else 100,
                high=100.0 if self.wicked and i == 13 else None)
            for i in range(288)
        ]
        self.five[LISTED + 2 * DAY_MS] = [
            bar(LISTED + 2 * DAY_MS + i * WINDOW_MS,.02)
            for i in range(288)
        ]

    def fetch(self, sym, interval, start, end, limit):
        self.calls.append((sym, interval, start, end, limit))
        if interval == "1m":
            return [
                [LISTED + 13*3600_000 + i*60000, .025, .0251, .0249,
                 .025, 100000, 0, 2500, 100, 0, 0, 0]
                for i in range(10)
            ]
        if interval == "1d" and start == 0:
            return [[LISTED + (DAY_MS if self.spoof_first else 0),
                     .026, .03, .021, .026, 200, 0, 1000000, 99]]
        if interval == "1d":
            days = []
            for day, bars in self.five.items():
                if self.drop_day and day == LISTED + DAY_MS:
                    continue
                if day < start or (end is not None and day > end):
                    continue
                hi = max(float(b[2]) for b in bars)
                total = sum(float(b[7]) for b in bars)
                days.append([day, 0, hi, .01, .02, 1, 0, total, 100])
            return days[:limit]
        if interval == "5m":
            if self.fail_first:
                return None
            which = (start // DAY_MS) * DAY_MS
            rows = self.five.get(which, [])
            return [b for b in rows if start <= b[0] <= end][:limit]
        raise AssertionError("Unexpected interval " + interval)


class HistoricalSpotBackfillTests(unittest.TestCase):
    def test_real_archived_price_guard_preserves_existing_retry_receipts(self):
        # Production SENT preimage has BOTH legacy retry fields. Removing
        # either one triggered WA_AE_COMPLETION_ONLY_HOLD_ALL_ARCHIVED_PRICE_ERASURE
        # and prevented all 24 recovered peaks from reaching R2.
        row = event()
        row["listing_price"]["source"] = "public_spot"
        row["listing_price"]["open"] = .011
        row["listing_price"]["close"] = .02493
        row["listing_price"]["_price_retry_after_utc"] = "2026-11-16T12:07:53+00:00"
        row["listing_price"]["_claim_5m_repair_retry_after_utc"] = "2026-11-17T09:45:58+00:00"
        old = copy.deepcopy(row)
        repaired = repair_missing_spot_peaks(
            [row], fetch=FakeMarket().fetch, now_ms=NOW)
        self.assertEqual(repaired["recovered"], 1)
        self.assertTrue(_no_price_erasure(old, row))
        self.assertEqual(row["listing_price"]["_price_retry_after_utc"],
                         old["listing_price"]["_price_retry_after_utc"])
        self.assertEqual(row["listing_price"]["_claim_5m_repair_retry_after_utc"],
                         old["listing_price"]["_claim_5m_repair_retry_after_utc"])
        self.assertEqual(ensure_all_contains_existing_history([row], [old]), 1)
        safe, withheld = keep_existing_history_without_unverified_admission(
            [old], [row])
        self.assertEqual(withheld, 0)
        self.assertEqual(safe, [row])
        self.assertEqual(enforce_no_unverified_history_admission([old], safe), 1)

    def test_exactly_24_frozen_legacy_contracts(self):
        self.assertEqual(len(SPOT_MISSING_PEAK_ALLOWLIST), 25)
        self.assertEqual(SPOT_MISSING_PEAK_ALLOWLIST["SENT"]["chain"], "1")
        self.assertEqual(SPOT_MISSING_PEAK_ALLOWLIST["SENT"]["first_spot_day"],
                         "2026-01-22")

    def test_official_robo_contradiction_recovered_from_real_spot_shape(self):
        # Official Binance 2026-03-04 listing notice attests BSC contract,
        # ROBO/USDT and the genuine first Spot trading date. In R2 the
        # 20:00 UTC Alpha reward event used a legacy Claim above daily Peak.
        from alpha_events_completion_only_guard import _no_price_erasure
        from alpha_event_spot_backfill import CLAIM_METHOD
        day=int(datetime(2026,3,4,tzinfo=timezone.utc).timestamp()*1000)
        instant=day+20*3600_000
        now=day+2*DAY_MS+2*3600_000
        e=event("ROBO")
        e["event_time"]="2026-03-04T20:00:00+00:00"
        e["spot_listing_price"]={"vwap":0.04978564719035632,"date":"2026-03-04"}
        e["listing_price"]={"vwap":0.04978564719035632,
                            "date":"2026-03-04",
                            "_price_retry_after_utc":"2026-11-16T10:00:00+00:00",
                            "max_since":{"price":0.04636187,"date":"2026-03-04"}}
        e["amount_per_user"]=600
        before=copy.deepcopy(e)

        def market(symbol, interval, start, end, limit):
            if symbol!="ROBO":
                return None
            if interval=="1d" and start==0:
                return [[day,.042,.06,.04,.041,1,0,1000,100]]
            if interval=="1m":
                return [[instant+i*60000,.043,.044,.042,.043,
                         1000/.043,instant+i*60000+59999,1000,120,0,0,0]
                        for i in range(12)]
            if interval=="1d":
                return [[day+j*DAY_MS,.04,high,.03,.04,1,0,quote,100]
                        for j,(high,quote) in enumerate([
                         (.045,90*2000),(.061,288*2000),(.039,1000)])]
            if interval=="5m":
                day_begin=(start//DAY_MS)*DAY_MS
                if day_begin==day:
                    # Binance started ROBO at 16:30 UTC.
                    return [bar(day+i*WINDOW_MS,.041,quote=2000)
                            for i in range(198,288)]
                if day_begin==day+DAY_MS:
                    return [bar(day+DAY_MS+i*WINDOW_MS,
                                .05 if i==15 else .044,quote=2000)
                            for i in range(288)]
                return []
            return None

        result=repair_missing_spot_peaks([e],fetch=market,now_ms=now)
        self.assertEqual(result["eligible"],1)
        self.assertEqual(result["updated"],1)
        self.assertEqual(result["recovered"],0)
        self.assertEqual(result["complete_asof"],1)
        self.assertTrue(_no_price_erasure(before,e))
        self.assertEqual(e["listing_price"]["claim_method"],CLAIM_METHOD)
        self.assertAlmostEqual(e["listing_price"]["vwap"],.043)
        self.assertAlmostEqual(e["listing_price"]["max_since"]["price"],.05)
        self.assertEqual(e["listing_price"]["max_since"]["date"],"2026-03-05")
        self.assertEqual(e["listing_price"]["peak_scope"],"spot_since_verified_listing")
        self.assertEqual(e["listing_price"]["_price_retry_after_utc"],
                         before["listing_price"]["_price_retry_after_utc"])
        # Next run only continues its own verified Spot checkpoint.
        next_run=repair_missing_spot_peaks([e],fetch=market,now_ms=now)
        self.assertEqual(next_run["recovered"],0)
        self.assertEqual(next_run["complete_asof"],1)

    def test_robo_contract_and_listing_mismatch_fail_closed(self):
        e=event("ROBO")
        e["event_time"]="2026-03-04T20:00:00+00:00"
        e["listing_price"]["max_since"]={"price":.03,"date":"2026-03-04"}
        for field,value in (("contract_address","0x"+"1"*40),
                            ("chain_id","8453"),
                            ("spot_listed_at","2026-03-05")):
            bad=copy.deepcopy(e)
            bad[field]=value
            self.assertIsNone(_identity(bad))
            def forbidden(*args,**kwargs):
                raise AssertionError("Binance fetch on unverified identity")
            self.assertEqual(repair_missing_spot_peaks(
                [bad],fetch=forbidden,now_ms=NOW)["eligible"],0)

    def test_price_evidence_audit_is_read_only_and_distinguishes_legacy(self):
        import sync_listing_prices as price
        before=[
          {"symbol":"MISSING","listing_price":None},
          {"symbol":"OLD","listing_price":{"vwap":.4,
                      "max_since":{"price":.3},
                      "_alpha_legacy_repair_v1":{"last_status":"alpha_1m_unavailable"}}},
          {"symbol":"EXTREME","listing_price":{"vwap":.01,
                      "max_since":{"price":.9}}},
          {"symbol":"PROVEN","listing_price":{"vwap":.01,
                      "max_since":{"price":.9},
                      "peak_method":"highest_qualified_5m_quote_vwap"}},
        ]
        snapshot=copy.deepcopy(before)
        messages=[]
        out=price._report_price_anomalies(before,
                 emit=lambda *args,**kwargs: messages.append(args[0]))
        self.assertEqual(before,snapshot)
        self.assertEqual(out["missing_claim"],["MISSING"])
        self.assertEqual(out["peak_below_claim"],
                         [("OLD","alpha_1m_unavailable")])
        self.assertEqual(out["high_ratio_legacy_unverified"],["EXTREME"])
        self.assertIn("peak_below_claim=1",messages[0])

    def test_pin_contract_chain_and_listing_day(self):
        e = event()
        self.assertIsNotNone(_identity(e))
        for bad in (
            {"contract_address": "0x" + "0"*40},
            {"chain_id": "56"},
            {"spot_listed_at": "2026-01-23"},
            {"spot_listing_price": {"vwap": 0, "date": "2026-01-22"}},
            {"spot_listed": False},
        ):
            candidate = {**e, **bad}
            self.assertIsNone(_identity(candidate),bad)

    def test_reject_recycled_ticker_and_do_not_fetch(self):
        e = event()
        e["contract_address"] = "0x" + "a"*40
        m = FakeMarket()
        result = repair_missing_spot_peaks([e],fetch=m.fetch,now_ms=NOW)
        self.assertEqual(result["eligible"],0)
        self.assertEqual(m.calls,[])
        self.assertIsNone(e["listing_price"]["max_since"])

    def test_sent_claim_5m_and_liquidity_peak_verified(self):
        m = FakeMarket()
        e = event()
        yes,status,was_missing = _recover_one(
            e,fetch=m.fetch,now_ms=NOW,max_candidate_days=10)
        self.assertTrue(yes)
        self.assertTrue(was_missing)
        self.assertEqual(status,"spot_complete_asof")
        self.assertAlmostEqual(e["listing_price"]["vwap"],.025)
        self.assertAlmostEqual(e["listing_price"]["max_since"]["price"],.0474)
        self.assertEqual(e["listing_price"]["max_since"]["date"],"2026-01-23")
        self.assertEqual(e["ath_since_listing_price"],.0474)
        self.assertEqual(e["listing_price"][REPAIR_KEY]["pending_days"],0)
        # The day's high is a pruning bound only, never published as price.
        self.assertEqual(len([x for x in m.calls if x[1]=="5m"]),1)

    def test_provider_failure_does_not_touch_good_old_record(self):
        m = FakeMarket()
        m.spoof_first = True
        e = event()
        before = copy.deepcopy(e)
        yes,status,missing = _recover_one(
            e,fetch=m.fetch,now_ms=NOW,max_candidate_days=10)
        self.assertFalse(yes)
        self.assertEqual(status,"ticker_first_day_mismatch")
        self.assertEqual(e,before)

    def test_no_daily_coverage_never_fabricates_peak(self):
        m = FakeMarket()
        m.drop_day = True
        e = event()
        yes,status,missing = _recover_one(
            e,fetch=m.fetch,now_ms=NOW,max_candidate_days=10)
        self.assertFalse(yes)
        self.assertEqual(status,"spot_daily_coverage_unavailable")
        self.assertIsNone(e["listing_price"]["max_since"])

    def test_no_5m_data_publishes_only_claim_lower_bound_as_partial(self):
        m=FakeMarket()
        m.fail_first=True
        e=event()
        yes,status,missing=_recover_one(e,fetch=m.fetch,now_ms=NOW,max_candidate_days=10)
        self.assertTrue(yes)
        self.assertEqual(status,"spot_partial")
        self.assertAlmostEqual(e["listing_price"]["max_since"]["price"],.025)
        self.assertEqual(e["listing_price"]["peak_quality"],"partial_spot_history")
        self.assertGreater(e["listing_price"][REPAIR_KEY]["pending_days"],0)

    def test_legacy_spot_claim_outside_day_range_does_not_borrow_it(self):
        e=event()
        e["listing_price"]["vwap"]=10000
        e["spot_listing_price"]["vwap"]=10000
        m=FakeMarket()
        yes,_,_=_recover_one(e,fetch=m.fetch,now_ms=NOW,max_candidate_days=10)
        self.assertTrue(yes)
        self.assertAlmostEqual(e["listing_price"]["vwap"],.025)
        self.assertLess(e["listing_price"]["max_since"]["price"],1)

    def test_replay_is_idempotent_and_monotonic(self):
        e=event()
        m=FakeMarket()
        repair_missing_spot_peaks([e],fetch=m.fetch,now_ms=NOW)
        first=e["listing_price"]["max_since"]["price"]
        r=repair_missing_spot_peaks([e],fetch=m.fetch,now_ms=NOW)
        self.assertEqual(r["recovered"],0)
        self.assertAlmostEqual(e["listing_price"]["max_since"]["price"],first)
        self.assertEqual(r["updated"],1)

    def test_unlisted_events_and_other_fields_remain_unchanged(self):
        e=event()
        untouched={"symbol":"OTHER","contract_address":"0x1234",
                   "listing_price":{"vwap":.1,"max_since":None}}
        before=copy.deepcopy(untouched)
        r=repair_missing_spot_peaks([e,untouched],fetch=FakeMarket().fetch,now_ms=NOW)
        self.assertEqual(r["recovered"],1)
        self.assertEqual(untouched,before)

    def test_price_upper_bound_cannot_be_a_wick(self):
        m=FakeMarket()
        m.five[LISTED + DAY_MS][13][2]=1000.0
        # The first day close data reports a spurious high at 5m wick.
        e=event()
        yes,_,_=_recover_one(e,fetch=m.fetch,now_ms=NOW,max_candidate_days=10)
        self.assertTrue(yes)
        self.assertAlmostEqual(e["listing_price"]["max_since"]["price"],.0474)

    def test_partial_budget_stores_progress_without_reset(self):
        m=FakeMarket()
        e=event()
        yes,status,_=_recover_one(e,fetch=m.fetch,now_ms=NOW,max_candidate_days=0)
        self.assertTrue(yes)
        self.assertEqual(status,"spot_partial")
        self.assertAlmostEqual(e["listing_price"]["max_since"]["price"],.025)
        self.assertGreater(e["listing_price"][REPAIR_KEY]["pending_days"],0)
        yes,status,_=_recover_one(e,fetch=m.fetch,now_ms=NOW,max_candidate_days=10)
        self.assertTrue(yes)
        self.assertAlmostEqual(e["listing_price"]["max_since"]["price"],.0474)

    def test_recovery_never_updates_claim_if_spot_quote_unavailable(self):
        m=FakeMarket()
        def no_claim(sym,interval,start,end,limit):
            return None if interval=="1m" else m.fetch(sym,interval,start,end,limit)
        e=event()
        before=copy.deepcopy(e)
        yes,status,_=_recover_one(e,fetch=no_claim,now_ms=NOW,max_candidate_days=10)
        self.assertFalse(yes)
        self.assertEqual(status,"spot_claim_1m_unavailable")
        self.assertEqual(e,before)


if __name__=="__main__":
    unittest.main()