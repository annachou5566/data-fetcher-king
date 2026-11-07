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
    def test_legacy_history_relabels_without_loss_and_replays(self):
        old={**BASE,'status':'ended','lifecycle_state':'history',
             'contract_address':'confirmed-contract','chain_id':'CT_784',
             'firstActiveAt':'2026-10-07T00:00:00.000Z',
             'listing_price':{'vwap':42},'legacy_active_24h_verified':True}
        active={k:v for k,v in BASE.items() if k not in ('contract_address','chain_id')}
        before=copy.deepcopy(old)
        plan=run([old],[copy.deepcopy(old)],[active],[copy.deepcopy(active)])
        self.assertEqual(old,before)
        self.assertEqual(plan['all'],plan['history'])
        rec=plan['history'][0]
        self.assertEqual(rec['contract_address'],'confirmed-contract')
        self.assertEqual(rec['chain_id'],'CT_784')
        self.assertEqual(rec['firstActiveAt'],'2026-10-07T00:00:00.000Z')
        self.assertEqual(rec['listing_price'],{'vwap':42})
        self.assertTrue(rec['legacy_active_24h_verified'])
        self.assertEqual(rec['status'],'expired_unverified')
        self.assertFalse(rec['completion_verified'])
        replay=run(plan['all'],plan['history'],[active],[copy.deepcopy(active)])
        self.assertEqual(replay['all'],plan['all'])
        self.assertEqual(replay['history'],plan['history'])
        self.assertEqual(len(replay['promoted']),1)
    def test_present_contract_conflict_still_holds(self):
        prior={**BASE,'contract_address':'0x111','status':'ended'}
        active={**BASE,'contract_address':'0x222'}
        with self.assertRaisesRegex(ScheduledArchiveHold,'IMMUTABLE_IDENTITY_CONFLICT'):
            run([prior],[copy.deepcopy(prior)],[active],[copy.deepcopy(active)])
    def test_verified_completion_is_never_relabelled(self):
        prior={**BASE,'status':'ended','completion_verified':True}
        with self.assertRaisesRegex(ScheduledArchiveHold,'EXISTING_HISTORY_NEEDS_REVIEW'):
            run([prior],[copy.deepcopy(prior)])
    def test_nine_elapsed_four_new_five_existing(self):
        active=[{**BASE,'id':'ae:'+format(i,'024x'),'symbol':str(i)} for i in range(1,10)]
        past=[{**a,'status':'ended','listing_price':{'vwap':i+1}} for i,a in enumerate(active[4:])]
        all_rows=copy.deepcopy(past)
        hist_rows=copy.deepcopy(past)
        plan=run(all_rows,hist_rows,active,copy.deepcopy(active))
        self.assertEqual(len(plan['promoted']),9)
        self.assertEqual(len(plan['all']),9)
        self.assertEqual(len(plan['history']),9)
        self.assertEqual(plan['all'],plan['history'])
        self.assertEqual(all(r['status']=='expired_unverified' for r in plan['history']),True)
        self.assertEqual(len({r['id'] for r in plan['history']}),9)
        replay=run(plan['all'],plan['history'],active,copy.deepcopy(active))
        self.assertEqual(len(replay['history']),9)

    def test_recover_all_first_partial_pair_without_new_admission(self):
        from alpha_events_lite_schedule import reconcile_partial_schedule_pair
        old={**BASE,'status':'ended','lifecycle_state':'history',
             'listing_price':{'vwap':99},'legacy_active_24h_verified':True}
        archived={**old,'status':'expired_unverified',
                  'archive_reason':'scheduled_time_elapsed_unverified',
                  'completion_verified':False}
        recovered=reconcile_partial_schedule_pair([archived],[old])
        self.assertEqual(recovered,[archived])
        self.assertEqual(reconcile_partial_schedule_pair([archived],recovered),[archived])
        self.assertEqual(old['status'],'ended')
    def test_recover_partial_pair_conflicting_immutable_field_holds(self):
        from alpha_events_lite_schedule import reconcile_partial_schedule_pair
        old={**BASE,'status':'ended'}
        forged={**old,'symbol':'OTHER','status':'expired_unverified',
                'archive_reason':'scheduled_time_elapsed_unverified',
                'completion_verified':False}
        with self.assertRaises(Exception):
            reconcile_partial_schedule_pair([forged],[old])
    def test_recover_partial_pair_legacy_price_never_erased(self):
        from alpha_events_lite_schedule import reconcile_partial_schedule_pair
        old={**BASE,'status':'ended','listing_price':{'vwap':99}}
        forged={k:v for k,v in old.items() if k!='listing_price'}
        forged.update(status='expired_unverified',lifecycle_state='history',
                      archive_reason='scheduled_time_elapsed_unverified',
                      completion_verified=False)
        with self.assertRaisesRegex(ScheduledArchiveHold,'PARTIAL_PAIR_PRICE_ERASURE'):
            reconcile_partial_schedule_pair([forged],[old])

if __name__=='__main__':unittest.main()
