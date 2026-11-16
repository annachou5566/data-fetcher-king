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
    def test_exactly_24_frozen_legacy_contracts(self):
        self.assertEqual(len(SPOT_MISSING_PEAK_ALLOWLIST), 24)
        self.assertEqual(SPOT_MISSING_PEAK_ALLOWLIST["SENT"]["chain"], "1")
        self.assertEqual(SPOT_MISSING_PEAK_ALLOWLIST["SENT"]["first_spot_day"],
                         "2026-01-22")

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