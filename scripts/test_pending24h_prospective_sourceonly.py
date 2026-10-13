import copy
import unittest
from datetime import datetime, timedelta, timezone
from history24h_cutover import plan, plan_qualified_pending24h, publish_pair, ALL_KEY, HISTORY_KEY, CutoverHold
from test_history24h_cutover import FakeR2

FIRST='2026-10-10T00:00:00.000Z'
CLOCK=datetime(2026,10,10,tzinfo=timezone.utc)
CID='ae:0123456789abcdef01234567'

def event(**changes):
    return dict(id=CID,symbol='SYN',event_time='2026-10-11T12:00:00.000Z',
                event_type='airdrop',status='PREDICTED',firstActiveAt=FIRST,**changes)

class Pending24hQualification(unittest.TestCase):
    def setUp(self):
        self.history=[dict(symbol=f'OLD{i}',event_time='2025-01-01T00:00:00.000Z',
                           status='ended',listing_price={'vwap':i+0.5,
                           'max_since':{'price':i+1,'date':'2025-01-02'}},
                           spot_listing_price={'vwap':i+0.1},legacy_blob=f'old-{i}') for i in range(445)]
        self.original=copy.deepcopy(self.history)
        self.base=plan(self.history,self.history,[],CLOCK+timedelta(days=1))
    def qualify(self,row=None,now=None,base=None):
        return plan_qualified_pending24h(base or self.base,[row or event()],
                                         now or CLOCK+timedelta(days=1))
    def test_01_exact_24h_add_one_and_preserve_445(self):
        p=self.qualify();self.assertEqual(len(p['history']),446);self.assertEqual(p['history'][:445],self.original)
        self.assertEqual(p['pending24h_promoted'],[CID]);self.assertEqual(p['history'][-1]['firstActiveAt'],FIRST)
        self.assertEqual(p['history'][-1]['status'],'ended');self.assertFalse(p['retire_pending'])
        self.assertFalse(p['retire_public_active']);self.assertFalse(p['retire_live'])
    def test_02_before_24h_remains_active(self):
        p=self.qualify(now=CLOCK+timedelta(days=1)-timedelta(milliseconds=1))
        self.assertEqual(len(p['history']),445);self.assertEqual(p['pending24h_promoted'],[])
    def test_03_replay_idempotent(self):
        p=self.qualify();r=self.qualify(base=p,now=CLOCK+timedelta(days=2))
        self.assertEqual(r['history'],p['history']);self.assertEqual(r['all'],p['all'])
    def test_04_no_first_active_is_not_silently_promoted(self):
        row=event();row.pop('firstActiveAt');p=self.qualify(row=row)
        self.assertEqual(len(p['history']),445)
    def test_05_never_guess_from_event_time(self):
        row=event();row.pop('firstActiveAt');row['event_time']='2025-01-01T00:00:00.000Z'
        self.assertEqual(self.qualify(row=row)['pending24h_promoted'],[])
    def test_06_future_clock_hold(self):
        with self.assertRaisesRegex(CutoverHold,'CLOCK_IN_FUTURE'):
            self.qualify(now=CLOCK-timedelta(milliseconds=1))
    def test_07_malformed_clock_hold(self):
        row=event();row['firstActiveAt']='2026-10-10';
        with self.assertRaises(CutoverHold):self.qualify(row=row)
    def test_08_identity_must_be_canonical(self):
        row=event();row['id']='not-canonical'
        with self.assertRaisesRegex(CutoverHold,'CANONICAL_ID_REQUIRED'):self.qualify(row=row)
    def test_09_duplicate_pending_id_hold(self):
        with self.assertRaisesRegex(CutoverHold,'DUPLICATE_ID'):
            plan_qualified_pending24h(self.base,[event(),event()],CLOCK+timedelta(days=1))
    def test_10_dual_clock_hold(self):
        row=event();row['firstLiveAt']=FIRST
        with self.assertRaisesRegex(CutoverHold,'DUAL_CLOCK'):self.qualify(row=row)
    def test_11_conflicting_existing_history_identity_hold(self):
        p=self.qualify();row=event();row['event_time']='2026-10-15T00:00:00.000Z'
        with self.assertRaisesRegex(CutoverHold,'IMMUTABLE_IDENTITY_CONFLICT'):
            self.qualify(row=row,base=p,now=CLOCK+timedelta(days=2))
    def test_12_preserve_existing_prices_when_updating_canonical(self):
        p=self.qualify();p['all'][-1]['listing_price']={'vwap':11,'max_since':{'price':12,'date':'2026-10-12'}}
        p['history'][-1]['listing_price']={'vwap':11,'max_since':{'price':12,'date':'2026-10-12'}}
        again=self.qualify(base=p,now=CLOCK+timedelta(days=2))
        self.assertEqual(again['all'][-1]['listing_price'],p['all'][-1]['listing_price'])
        self.assertEqual(again['history'][-1]['listing_price'],p['history'][-1]['listing_price'])
    def test_13_invalid_plan_owner_hold(self):
        bad={**self.base,'independent_writer':True}
        with self.assertRaisesRegex(CutoverHold,'UNQUALIFIED_PLAN'):self.qualify(base=bad)
    def test_14_early_archived_record_hold(self):
        p=self.qualify()
        with self.assertRaisesRegex(CutoverHold,'EARLY_OR_NONTERMINAL_HISTORY'):
            self.qualify(base=p,now=CLOCK+timedelta(days=1)-timedelta(milliseconds=1))
    def test_15_pair_cas_readback_required_before_publisher_retirement(self):
        candidate=self.qualify()
        client=FakeR2(self.history,self.history)
        response=publish_pair(client,'synthetic-free-bucket',candidate,client.etag[ALL_KEY],client.etag[HISTORY_KEY])
        self.assertTrue(response['pair_verified'])
        self.assertEqual(client.writes,[ALL_KEY,HISTORY_KEY])
        self.assertEqual(client.data[HISTORY_KEY],candidate['history'])
        self.assertFalse(candidate['retire_pending'])
    def test_16_all_ack_loss_does_not_commit_history(self):
        candidate=self.qualify()
        client=FakeR2(self.history,self.history)
        client.fail_after=ALL_KEY
        with self.assertRaises(RuntimeError):
            publish_pair(client,'synthetic-free-bucket',candidate,client.etag[ALL_KEY],client.etag[HISTORY_KEY])
        self.assertEqual(client.data[HISTORY_KEY],self.history)
        self.assertFalse(candidate['retire_pending'])
    def test_17_history_ack_loss_needs_reconcile_before_active_retire(self):
        candidate=self.qualify()
        client=FakeR2(self.history,self.history)
        client.fail_after=HISTORY_KEY
        with self.assertRaises(RuntimeError):
            publish_pair(client,'synthetic-free-bucket',candidate,client.etag[ALL_KEY],client.etag[HISTORY_KEY])
        self.assertEqual(client.writes,[ALL_KEY,HISTORY_KEY])
        self.assertEqual(client.data[HISTORY_KEY],candidate['history'])
        self.assertFalse(candidate['retire_pending'])
    def test_18_old_history_untouched_even_after_replay(self):
        p=self.qualify();self.qualify(base=p,now=CLOCK+timedelta(days=3))
        self.assertEqual(self.history,self.original)

if __name__=='__main__':unittest.main()
