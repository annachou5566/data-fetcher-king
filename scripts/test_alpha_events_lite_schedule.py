import copy
import unittest
from datetime import datetime,timezone
from alpha_events_lite_schedule import plan_scheduled_archive,ScheduledArchiveHold
CID='ae:'+'a'*24
BASE={'id':CID,'symbol':'ABC','event_time':'2026-11-01T00:00:00.000Z',
      'event_type':None,'status':'PREDICTED','firstActiveAt':'2026-10-30T00:00:00.000Z'}
NOW=datetime(2026,11,7,tzinfo=timezone.utc)
def run(a=None,h=None,p=None,public=None,now=NOW):
    if a is None:a=[{'id':None,'symbol':'OLD','event_time':'2024-01-01T00:00:00.000Z','listing_price':{'vwap':1}}]
    if h is None:h=copy.deepcopy(a)
    if p is None:p=[copy.deepcopy(BASE)]
    if public is None:public=copy.deepcopy(p)
    return plan_scheduled_archive(a,h,[{'key':'alpha-events/pending.json','rows':p},
        {'key':'alpha-events/upcoming.json','rows':[]},{'key':'alpha-events/live.json','rows':[]}],public,now)
class TestSchedule(unittest.TestCase):
    def test_overdue_autopromoted_but_unverified(self):
        p=run()
        self.assertEqual(len(p['all']),2);self.assertEqual(p['all'],p['history'])
        self.assertEqual(p['promoted'],[CID]);self.assertEqual(p['history'][-1]['status'],'expired_unverified')
        self.assertFalse(p['history'][-1]['completion_verified']);self.assertFalse(p['retire_live'])
    def test_replay_no_duplicate(self):
        p=run();again=run(p['all'],p['history']);self.assertEqual(p['all'],again['all'])
    def test_future_remains_active(self):
        p=run(p=[{**BASE,'event_time':'2026-12-01T00:00:00.000Z'}]);self.assertFalse(p['promoted'])
    def test_invalid_date_does_not_archive(self):
        p=run(p=[{**BASE,'event_time':'n/a'}]);self.assertFalse(p['promoted'])
    def test_missing_public_pair_holds(self):
        with self.assertRaisesRegex(ScheduledArchiveHold,'PUBLIC_ACTIVE_DIVERGENCE'):run(public=[])
    def test_duplicate_active_holds(self):
        with self.assertRaisesRegex(ScheduledArchiveHold,'ACTIVE_DUPLICATE_ID'):run(p=[BASE,BASE])
    def test_all_identity_conflict_holds(self):
        with self.assertRaisesRegex(ScheduledArchiveHold,'IMMUTABLE_IDENTITY_CONFLICT'):run([{**BASE,'symbol':'WRONG'}],[])
    def test_all_price_preserved(self):
        p=run([{**BASE,'listing_price':{'vwap':100}}],[]);self.assertEqual(p['history'][0]['listing_price'],{'vwap':100})
    def test_all_price_and_first_active_clock_survive_schedule_archive(self):
        old={k:v for k,v in BASE.items() if k!='firstActiveAt'}
        old['listing_price']={'vwap':123}
        p=run([old],[])
        self.assertEqual(p['history'][0]['listing_price'],{'vwap':123})
        self.assertEqual(p['history'][0]['firstActiveAt'],BASE['firstActiveAt'])
    def test_mismatched_existing_clock_holds(self):
        old={**BASE,'firstActiveAt':'2026-10-28T00:00:00.000Z'}
        with self.assertRaisesRegex(ScheduledArchiveHold,'ACTIVE_CLOCK_CONFLICT'):
            run([old],[])
    def test_450_old_rows_intact(self):
        a=[{'id':None if i<435 else str(i),'symbol':str(i),'listing_price':{'vwap':i+1}} for i in range(450)]
        p=run(a,copy.deepcopy(a));self.assertEqual(p['all'][:450],a);self.assertEqual(p['history'][:450],a)
    def test_before_event_not_promoted(self):
        p=run(now=datetime(2026,10,31,tzinfo=timezone.utc));self.assertFalse(p['promoted'])
if __name__=='__main__':unittest.main()
