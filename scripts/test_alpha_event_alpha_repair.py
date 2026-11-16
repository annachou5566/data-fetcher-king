"""Offline regression tests for original Alpha exact-contract Claim and Peak."""
import copy
import unittest
from datetime import datetime,timezone
from unittest.mock import patch

import alpha_event_alpha_repair as core
from alpha_events_completion_only_guard import (
    _no_price_erasure, keep_existing_history_without_unverified_admission,
    ensure_all_contains_existing_history
)

DAY=86400000
BASE=int(datetime(2026,8,3,tzinfo=timezone.utc).timestamp()*1000)
EVENT=BASE+12*3600000
NOW=BASE+3*DAY+11*3600000


def row(t,p,quote=2000,trades=100,high=None,minute=False):
    base=quote/p
    length=60000 if minute else 300000
    return [t,str(p),str(high if high is not None else p*1.01),str(p*.99),str(p),
            str(base),t+length-1,str(quote),trades,0,0,0]


def event(claim=.065,peak=.061,dt='2026-08-03T12:00:00+00:00'):
    return {
        "symbol":"TEST","event_time":dt,"event_type":"grab",
        "contract_address":"0x"+"a"*40,"chain_id":"56",
        "listing_price":{"vwap":claim,"date":"2026-08-03",
                         "max_since":{"price":peak,"date":"2026-08-03"},
                         "_price_retry_after_utc":"2026-11-16T11:00:00+00:00",
                         "_claim_5m_repair_retry_after_utc":"2026-11-17T11:00:00+00:00"},
        "ath_since_listing_price":peak
    }


class Market:
    def __init__(self):
        self.calls=[]
        self.none_1m=False
        self.none_daily=False
        self.none_5m=False
        self.huge_wick=False
        self.shift_first=0
    def fetch(self,aid,interval,start,end,limit):
        self.calls.append((aid,interval,start,end,limit))
        if aid!="ALPHA_999":
            return None
        if interval=="1m":
            if self.none_1m:return None
            return [row(EVENT+self.shift_first+i*60000,.05 if i<5 else .08,
                        quote=2000,trades=100,minute=True)
                    for i in range(15)]
        if interval=="1d":
            if self.none_daily:return None
            out=[]
            for i in range(4):
                day=BASE+i*DAY
                val=.09 if i==1 else .06
                if i==1 and self.huge_wick:val=300
                out.append(row(day,.05,quote=288000,high=val))
            return out
        if interval=="5m":
            if self.none_5m:return None
            day=(start//DAY)*DAY
            return [
                row(day+i*300000,.08 if i==100 else .045,
                    quote=1000,trades=30,
                    high=300 if (self.huge_wick and i==110) else .085)
                for i in range(288)
            ]
        return None


class DirectAlphaTests(unittest.TestCase):
    def test_uses_volume_weighted_first_five_minutes_not_candle_wick(self):
        m=Market()
        x,reason=core.verified_claim(event(),"ALPHA_999",BASE,m.fetch)
        self.assertEqual(reason,"ok")
        self.assertAlmostEqual(x["vwap"],.05)
        self.assertAlmostEqual(x["quote"],10000)
        self.assertEqual(x["trades"],500)

    def test_missing_alpha_1m_holds_without_inventing_claim(self):
        m=Market();m.none_1m=True
        e=event();before=copy.deepcopy(e)
        self.assertEqual(core.recover_one(e,"ALPHA_999",BASE,
                                         fetch=m.fetch,now_ms=NOW)[0],False)
        self.assertEqual(e,before)

    def test_large_daily_wick_is_not_reported_as_qualified_peak(self):
        m=Market();m.huge_wick=True
        e=event()
        ok,status=core.recover_one(e,"ALPHA_999",BASE,fetch=m.fetch,now_ms=NOW)
        self.assertTrue(ok)
        self.assertLess(e["listing_price"]["max_since"]["price"],1)

    def test_invalid_event_clock_hold(self):
        e=event(dt='not a date')
        ok,_=core.recover_one(e,"ALPHA_999",BASE,
                              fetch=Market().fetch,now_ms=NOW)
        self.assertFalse(ok)

    def test_old_shorthand_utc_event_clock(self):
        e=event(dt='2026-08-03 12:00:00+00')
        self.assertEqual(core.event_ms(e),EVENT)

    def test_archive_erasure_guard_retains_both_old_retry_receipts(self):
        e=event()
        previous=copy.deepcopy(e)
        ok,status=core.recover_one(e,"ALPHA_999",BASE,
                                   fetch=Market().fetch,now_ms=NOW)
        self.assertTrue(ok)
        self.assertTrue(_no_price_erasure(previous,e))
        self.assertEqual(
            ensure_all_contains_existing_history([e],[previous]),1)
        safe,held=keep_existing_history_without_unverified_admission(
            [previous],[e])
        self.assertEqual(held,0)
        self.assertEqual(safe,[e])

    def test_daily_missing_partial_peak_still_based_on_verified_claim(self):
        m=Market();m.none_daily=True
        e=event(claim=.07,peak=.03)
        ok,status=core.recover_one(e,"ALPHA_999",BASE,
                                   fetch=m.fetch,now_ms=NOW)
        self.assertTrue(ok)
        self.assertAlmostEqual(e["listing_price"]["vwap"],.05)
        self.assertAlmostEqual(e["listing_price"]["max_since"]["price"],.05)
        self.assertEqual(e["listing_price"]["peak_quality"],"partial_alpha_history")

    def test_missing_claim_can_gain_proven_1m_price_without_legacy_reset(self):
        e=event()
        e["listing_price"]=None
        e["ath_since_listing_price"]=None
        r=core.repair_exact_alpha_legacy([e],
            exact_info=lambda e:("ALPHA_999",BASE),
            fetch=Market().fetch,now_ms=NOW)
        self.assertEqual(r["repaired"],1)
        self.assertAlmostEqual(e["listing_price"]["vwap"],.05)
        self.assertGreaterEqual(e["listing_price"]["max_since"]["price"],.05)

    def test_failed_provider_retries_have_bounded_wait_and_no_erasure(self):
        e=event()
        m=Market();m.none_1m=True
        t=core.repair_exact_alpha_legacy([e],
            exact_info=lambda e:("ALPHA_999",BASE),
            fetch=m.fetch,now_ms=NOW)
        self.assertEqual(t["held"],1)
        self.assertEqual(e["listing_price"]["vwap"],.065)
        self.assertTrue(e["listing_price"][core.RECOVERY_KEY]["retry_after"])
        again=core.repair_exact_alpha_legacy([e],
            exact_info=lambda e:("ALPHA_999",BASE),
            fetch=m.fetch,now_ms=NOW)
        self.assertEqual(again["selected"],0)

    def test_partial_history_continues_on_six_hour_retry_not_every_cron(self):
        e=event()
        m=Market()
        m.none_daily=True
        one=core.repair_exact_alpha_legacy([e],
            exact_info=lambda e:("ALPHA_999",BASE),fetch=m.fetch,now_ms=NOW)
        self.assertEqual(one["repaired"],1)
        self.assertEqual(e["listing_price"]["peak_quality"],"partial_alpha_history")
        self.assertTrue(core.is_candidate(e))
        two=core.repair_exact_alpha_legacy([e],
            exact_info=lambda e:("ALPHA_999",BASE),fetch=m.fetch,now_ms=NOW)
        self.assertEqual(two["selected"],0)
        three=core.repair_exact_alpha_legacy([e],
            exact_info=lambda e:("ALPHA_999",BASE),fetch=m.fetch,
            now_ms=NOW+7*3600000)
        self.assertEqual(three["selected"],1)
        self.assertEqual(three["repaired"],1)

    def test_delayed_first_actual_trade_uses_1m_volume_not_5m_wick(self):
        class Delayed(Market):
            def fetch(self,aid,interval,start,end,limit):
                if interval=="1m" and start==EVENT:
                    return [row(EVENT+i*60000,.05,quote=0,
                                trades=0,minute=True)
                            for i in range(15)]
                if interval=="5m" and start==EVENT+15*60000:
                    return [row(EVENT+25*60000,.04,quote=2500,
                                trades=200,high=5.0)]
                if interval=="1m" and start==EVENT+25*60000:
                    return [row(EVENT+25*60000+i*60000,.04,
                                quote=3000,trades=400,minute=True)
                            for i in range(5)]
                return super().fetch(aid,interval,start,end,limit)
        e=event()
        c,status=core.verified_claim(e,"ALPHA_999",BASE,Delayed().fetch)
        self.assertEqual(status,"ok")
        self.assertAlmostEqual(c["vwap"],.04)
        self.assertEqual(c["first_ms"],EVENT+25*60000)

    def test_mismatch_contract_never_fetches(self):
        e=event()
        m=Market()
        x=core.repair_exact_alpha_legacy([e],
            exact_info=lambda e:(None,None),
            fetch=m.fetch,now_ms=NOW)
        self.assertEqual(x["eligible"],0)
        self.assertEqual(m.calls,[])

    def test_old_peak_above_verified_floor_is_preserved_if_partial(self):
        e=event(claim=1.3,peak=1.0)
        m=Market();m.none_daily=True
        ok,status=core.recover_one(e,"ALPHA_999",BASE,
                                   fetch=m.fetch,now_ms=NOW)
        self.assertTrue(ok)
        self.assertAlmostEqual(e["listing_price"]["max_since"]["price"],1.0)
        self.assertEqual(status,"alpha_partial_old_peak_preserved")

    def test_cannot_presume_complete_history_if_delisted_recent_daily_missing(self):
        e=event()
        m=Market()
        # Market returns only a 4-day period, but NOW is 20 days later.
        now=NOW+20*DAY
        ok,status=core.recover_one(e,"ALPHA_999",BASE,
                                   fetch=m.fetch,now_ms=now)
        self.assertTrue(ok)
        self.assertNotEqual(status,"alpha_complete_asof")

    def test_contract_identity_index_catches_reused_and_conflicting_symbols(self):
        import sync_listing_prices as sync
        items=[
            {"symbol":"TEST","contractAddress":"0x"+"a"*40,
             "chainId":"56","alphaId":"ALPHA_999","listingTime":BASE},
            {"symbol":"TEST","contractAddress":"0x"+"b"*40,
             "chainId":"56","alphaId":"ALPHA_1000","listingTime":BASE},
        ]
        idx=sync._verified_alpha_identity_index(items)
        with patch.object(sync,"_ALPHA_BY_EXACT_TOKEN",idx):
            self.assertEqual(sync._get_alpha_info_exact(event()),("ALPHA_999",BASE))
            e=event();e["contract_address"]="0x"+"c"*40
            self.assertEqual(sync._get_alpha_info_exact(e),(None,None))
        idx2=sync._verified_alpha_identity_index(items+[
            {**items[0],"alphaId":"ALPHA_111"}])
        self.assertNotIn(("TEST","evm:"+"a"*40,"56"),idx2)

if __name__=="__main__":
    unittest.main()