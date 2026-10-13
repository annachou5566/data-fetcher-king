"""Source-only clock-precedence regression: old Live clock must never retire a firstActiveAt-less event."""
import unittest
from datetime import datetime, timezone
from history24h_cutover import plan, plan_qualified_active24h, CutoverHold

NOW=datetime(2026,10,13,0,0,tzinfo=timezone.utc)
CID='ae:0123456789abcdef01234567'
BASE=[{'symbol':'OLD','event_time':'2025-05-01T00:00:00.000Z',
       'status':'ended','listing_price':{'vwap':1.25},'spot_listing_price':{'vwap':1.1}}]
LEGACY={'id':CID,'symbol':'SYN','event_type':'airdrop',
        'event_time':'2026-10-14T00:00:00.000Z', 'status':'live',
        'firstLiveAt':'2026-10-10T00:00:00.000Z'}

class FirstActiveClockPrecedence(unittest.TestCase):
    def proposed(self,row):
        return plan_qualified_active24h(plan(BASE,BASE,[],NOW),[row],NOW)
    def test_legacy_live_sole_clock_does_not_prospectively_retire(self):
        result=self.proposed(LEGACY)
        self.assertEqual(result['pending24h_promoted'],[])
        self.assertEqual(result['history'],BASE)
    def test_mixed_legacy_plan_would_retire_early_and_must_be_bypassed(self):
        old=plan(BASE,BASE,[LEGACY],NOW)
        self.assertEqual(old['promoted'],[CID])
        prospective=self.proposed(LEGACY)
        self.assertEqual(prospective['pending24h_promoted'],[])
    def test_first_active_age_controls_even_if_live_is_recent(self):
        row={**LEGACY,'firstActiveAt':'2026-10-10T00:00:00.000Z',
             'firstLiveAt':'2026-10-12T23:00:00.000Z'}
        result=self.proposed(row)
        self.assertEqual(result['pending24h_promoted'],[CID])
        self.assertEqual(result['history'][-1]['firstActiveAt'],row['firstActiveAt'])
        self.assertEqual(result['history'][0],BASE[0])
    def test_live_before_first_active_denied(self):
        row={**LEGACY,'firstActiveAt':'2026-10-11T00:00:00.000Z'}
        with self.assertRaisesRegex(CutoverHold,'LIVE_CLOCK_CONFLICT'):
            self.proposed(row)

if __name__=='__main__':unittest.main()
