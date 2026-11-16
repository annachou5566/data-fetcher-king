"""Offline regression tests for incremental event-anchored liquidity Peak.

No HTTP, R2, or other production side effects.
"""
import copy
import unittest
from datetime import datetime, timezone

from alpha_event_peak_liquidity import (
    METHOD, STATE_KEY, bar_vwap, qualify_bar, quote_floor, refresh_liquidity_peaks
)

def ms(s):
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)

EVENT = ms("2026-10-09T09:00:00Z")
DAY = 86400000


def bar(t, vwap, quote=1000, trades=10, high=None):
    high = vwap if high is None else high
    base = quote / vwap
    return [str(t), str(vwap), str(high), str(vwap * .999), str(vwap),
            str(base), str(t + 299999), str(quote), str(trades), "0", "0", "0"]


def event(id="ae:01", anchor="2026-10-09T09:00:00Z", peak=0.1, amount="200"):
    return {
        "id": id, "symbol": "BTR", "contract_address": "0xabc",
        "event_time": anchor, "amount": amount,
        "listing_price": {
            "vwap": 0.1, "date": anchor[:10],
            "max_since": {"price": peak, "date": anchor[:10]},
        },
    }


def run(rows, *, now_ms=EVENT + 2 * DAY, alpha=None, spot=None,
        info=None, resolve=None, max_events=12, pages=1):
    calls = []
    def fetch_alpha(aid, start, end, limit):
        calls.append(("alpha",start,end))
        return alpha(aid,start,end,limit) if alpha else [bar(start, 0.4)]
    def fetch_spot(sym, start, end, limit):
        calls.append(("spot",start,end))
        return spot(sym,start,end,limit) if spot else [bar(start, 0.6)]
    def get_info(sym, addr):
        return info(sym,addr) if info else ("ALPHA_777", EVENT - 5 * DAY)
    r = refresh_liquidity_peaks(
        rows, now_ms=now_ms, event_ms=lambda e: ms(e["event_time"]),
        alpha_info=get_info, fetch_alpha=fetch_alpha, fetch_spot=fetch_spot,
        spot_date_resolver=resolve, max_events=max_events,
        pages_per_event=pages)
    return r, calls


class LiquidityPeak(unittest.TestCase):
    def test_quote_vwap_ignores_wick_high(self):
        row = bar(EVENT, 0.50, quote=1000, trades=20, high=200.0)
        self.assertAlmostEqual(bar_vwap(row)[0], 0.50)
        self.assertAlmostEqual(qualify_bar(row, 100), 0.50)

    def test_one_trade_or_dust_quote_rejected(self):
        self.assertIsNone(qualify_bar(bar(EVENT, 999, quote=5, trades=1), 100))
        self.assertIsNone(qualify_bar(bar(EVENT, 999, quote=500, trades=1), 100))
        self.assertIsNone(qualify_bar(bar(EVENT, 999, quote=5, trades=200), 100))

    def test_five_times_sale_notional_floor(self):
        self.assertEqual(quote_floor(event(amount="200"), 0.1), 100)
        self.assertEqual(quote_floor(event(amount="10000"), 2), 100000)

    def test_old_peak_below_new_vwap_gets_updated(self):
        e = event(peak=0.3)
        result, calls = run([e], alpha=lambda aid,s,t,l: [bar(s, 0.4)])
        self.assertEqual(result["advanced"],1)
        self.assertAlmostEqual(e["listing_price"]["max_since"]["price"], 0.4)
        self.assertEqual(e["listing_price"]["peak_method"], METHOD)
        self.assertEqual(e["ath_since_listing_price"], 0.4)
        self.assertEqual(e["listing_price"][STATE_KEY]["status"], "caught_up")

    def test_very_low_volume_wick_cannot_be_peak(self):
        e = event(peak=0.3)
        run([e],alpha=lambda a,s,t,l:[bar(s,1000,quote=1,trades=1),bar(s+300000,0.5)])
        self.assertEqual(e["listing_price"]["max_since"]["price"],0.5)

    def test_partial_scan_retains_existing_peak_until_complete(self):
        e=event(peak=2.0)
        r,_=run([e],now_ms=EVENT + 10 * DAY, alpha=lambda a,s,t,l:[bar(s,0.5)])
        self.assertEqual(e["listing_price"]["max_since"]["price"],2)
        self.assertNotIn("peak_method", e["listing_price"])
        self.assertEqual(e["listing_price"][STATE_KEY]["status"], "partial")

    def test_full_scan_can_correct_old_daily_close_downwards(self):
        e=event(peak=2.0)
        # All historical 5m windows are verified by the supplied archive.
        run([e], now_ms=EVENT + DAY, alpha=lambda a,s,t,l:[bar(s,0.5)])
        self.assertEqual(e["listing_price"]["max_since"]["price"],0.5)

    def test_provider_failure_no_false_data(self):
        e=event()
        before=copy.deepcopy(e["listing_price"]["max_since"])
        result,_=run([e],alpha=lambda a,s,t,l:None)
        self.assertEqual(result["held"],1)
        self.assertEqual(e["listing_price"]["max_since"],before)
        self.assertEqual(e["listing_price"][STATE_KEY]["cursor_ms"],EVENT)

    def test_nonmatching_contract_never_scans_ticker(self):
        e=event()
        r,calls=run([e],info=lambda sym,addr:(None,None))
        self.assertEqual(r["eligible"],0)
        self.assertEqual(calls,[])
        self.assertNotIn(STATE_KEY,e["listing_price"])

    def test_alpha_to_spot_transition_scans_both_without_reset(self):
        e=event(peak=0.1)
        e["is_spot_listed"]=True
        e["spot_listed_at"]="2026-10-10"
        alpha=lambda aid,s,t,l:[bar(s,.6)]
        spot=lambda sym,s,t,l:[bar(s,.8)]
        r,calls=run([e],now_ms=EVENT+2*DAY,pages=3,alpha=alpha,spot=spot)
        self.assertEqual([v[0] for v in calls],["alpha","spot"])
        self.assertEqual(e["listing_price"]["max_since"]["price"],.8)
        self.assertEqual(e["listing_price"][STATE_KEY]["status"],"caught_up")

    def test_repeat_events_independent_anchors(self):
        e1=event("one",peak=0.2)
        e2=event("two","2026-10-10T09:00:00Z",peak=.3)
        r,calls=run([e1,e2],now_ms=EVENT+3*DAY,
            alpha=lambda aid,s,t,l:[bar(s,.4)])
        self.assertEqual(r["selected"],2)
        self.assertNotEqual(e1["listing_price"][STATE_KEY]["anchor_ms"],
                            e2["listing_price"][STATE_KEY]["anchor_ms"])

    def test_missing_spot_day_does_not_starve_other_events(self):
        missing = event(id="a", anchor="2026-10-09T09:00:00Z")
        missing["is_spot_listed"] = True
        healthy = event(id="b", anchor="2026-10-10T09:00:00Z")
        events = [missing, healthy]
        first, _ = run(events, resolve=lambda *args: None, max_events=1)
        self.assertEqual(first["held"], 1)
        self.assertIn(STATE_KEY, missing["listing_price"])
        second, _ = run(events, max_events=1)
        self.assertEqual(second["selected"], 1)
        self.assertIn(STATE_KEY, healthy["listing_price"])

    def test_oldest_checked_first_fair_rotation(self):
        events=[event(id=f"e{i}") for i in range(3)]
        run(events,max_events=1)
        done=[e for e in events if STATE_KEY in e["listing_price"]]
        self.assertEqual(len(done),1)
        run(events,max_events=1)
        done=[e for e in events if STATE_KEY in e["listing_price"]]
        self.assertEqual(len(done),2)


if __name__=="__main__":
    unittest.main()