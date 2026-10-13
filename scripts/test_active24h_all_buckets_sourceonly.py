import unittest
from datetime import datetime, timedelta, timezone
from history24h_cutover import (plan, plan_qualified_active24h, recover_stale_pending, CutoverHold)

FIRST='2026-10-10T00:00:00.000Z'
NOW=datetime(2026,10,11,tzinfo=timezone.utc)
CID='ae:0123456789abcdef01234567'
E={'id':CID,'symbol':'SYN','event_time':'2026-10-12T00:00:00.000Z',
   'event_type':'airdrop','status':'PREDICTED','firstActiveAt':FIRST}
BASE=[{'symbol':'OLD','event_time':'2025-01-01T00:00:00.000Z','status':'ended',
       'listing_price':{'vwap':10,'max_since':{'price':20,'date':'2025-01-03'}},
       'spot_listing_price':{'vwap':8}}]

class AllActiveFirstClock(unittest.TestCase):
    def p(self, row=E, now=NOW):
        return plan_qualified_active24h(plan(BASE,BASE,[],now),[row],now)
    def test_upcoming_bucket_same_first_active_contract(self):
        a=self.p();self.assertEqual(a['pending24h_promoted'],[CID]);self.assertEqual(a['history'][0],BASE[0])
    def test_live_with_qualified_later_firstLive_clock(self):
        a=self.p({**E,'firstLiveAt':'2026-10-10T12:00:00.000Z'})
        self.assertEqual(a['history'][-1]['firstActiveAt'],FIRST)
        self.assertEqual(a['history'][-1]['firstLiveAt'],'2026-10-10T12:00:00.000Z')
    def test_live_clock_cannot_precede_first_active(self):
        with self.assertRaisesRegex(CutoverHold,'LIVE_CLOCK_CONFLICT'):
            self.p({**E,'firstLiveAt':'2026-10-09T23:00:00.000Z'})
    def test_live_clock_cannot_be_in_future(self):
        with self.assertRaisesRegex(CutoverHold,'LIVE_CLOCK_CONFLICT'):
            self.p({**E,'firstLiveAt':'2026-10-12T23:00:00.000Z'})
    def test_cross_bucket_duplicate_rejected(self):
        with self.assertRaisesRegex(CutoverHold,'DUPLICATE_ID'):
            plan_qualified_active24h(plan(BASE,BASE,[],NOW),[E,E],NOW)
    def test_earlier_than_firstActive_24_hours_preserved(self):
        v=self.p(now=NOW-timedelta(milliseconds=1))
        self.assertEqual(v['pending24h_promoted'],[])
    def test_prices_and_idless_legacy_unchanged(self):
        v=self.p();self.assertEqual(v['history'][0],BASE[0]);self.assertEqual(v['all'][0],BASE[0])
    def test_replay_reuses_exact_first_active_and_price(self):
        v=self.p();q=plan_qualified_active24h(v,[E],NOW+timedelta(days=1))
        self.assertEqual(q['history'],v['history']);self.assertEqual(q['all'],v['all'])
    def test_missing_first_active_not_guessed_from_event_time(self):
        r={k:v for k,v in E.items() if k!='firstActiveAt'}
        v=self.p(r);self.assertEqual(v['pending24h_promoted'],[])

if __name__=='__main__':unittest.main()
