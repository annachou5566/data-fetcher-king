import io
import json
import unittest
from datetime import datetime, timedelta, timezone
from history24h_cutover import (plan, publish_pair, snapshot, CutoverHold,
                                ALL_KEY, HISTORY_KEY, LIVE_KEY)

FIRST='2026-10-12T10:00:00.000Z'
NOW=datetime(2026,10,13,10,0,0,tzinfo=timezone.utc)
CID='ae:0123456789abcdef01234567'
LIVE={'id':CID,'firstLiveAt':FIRST,'symbol':'FUT','status':'live',
      'event_time':'2026-10-01T00:00:00Z','source_channel':'synthetic'}

def legacy(n=445):
    return [dict(id='legacy-'+str(i),symbol='LEG'+str(i),status='ended',
                 listing_price={'vwap':i+.5},spot_listing_price={'vwap':i+.25})
            for i in range(n)]

class FakeR2:
    def __init__(self,all_rows,hist_rows,live_rows=None):
        self.data={ALL_KEY:all_rows,HISTORY_KEY:hist_rows,LIVE_KEY:live_rows or []}
        self.etag={k:'etag-v1-'+k for k in self.data}
        self.writes=[];self.fail_before=None;self.fail_after=None;self.race=None
    def get_object(self, Bucket, Key):
        raw=json.dumps(self.data[Key]).encode()
        return {'ETag':self.etag[Key],'ContentLength':len(raw),'Body':io.BytesIO(raw)}
    def put_object(self, Bucket,Key,Body,IfMatch,**kw):
        if self.fail_before==Key:raise RuntimeError('lost before write')
        if self.race==Key:self.etag[Key]='concurrent-writer'
        if IfMatch!=self.etag[Key]:raise RuntimeError('PreconditionFailed')
        self.data[Key]=json.loads(Body.decode())
        self.etag[Key]='etag-v2-'+Key
        self.writes.append(Key)
        if self.fail_after==Key:raise RuntimeError('lost ack after write')

class History24h(unittest.TestCase):
    def setUp(self):
        self.legacy=legacy();self.live=[dict(LIVE)]
    def plan(self,now=NOW,all_rows=None,history=None,live=None):
        return plan(all_rows if all_rows is not None else self.legacy,
                    history if history is not None else self.legacy,
                    live if live is not None else self.live,now)
    def test_exact_half_open_minus_1ms(self):
        p=self.plan(NOW-timedelta(milliseconds=1));self.assertEqual(p['promoted'],[])
        self.assertEqual(len(p['history']),445)
    def test_exact_half_open_zero(self):
        p=self.plan();self.assertEqual(p['promoted'],[CID]);self.assertEqual(len(p['history']),446)
        self.assertEqual(p['history'][-1]['status'],'ended')
        self.assertEqual(p['history'][-1]['lifecycle_state'],'history')
    def test_exact_half_open_plus_1ms(self):
        self.assertEqual(self.plan(NOW+timedelta(milliseconds=1))['promoted'],[CID])
    def test_legacy_445_exact_price_preservation(self):
        p=self.plan();self.assertEqual(p['history'][:445],self.legacy)
        self.assertEqual(p['all'][:445],self.legacy)
    def test_idempotent_replay(self):
        a=self.plan();b=self.plan(all_rows=a['all'],history=a['history']);self.assertEqual(a['all'],b['all'])
        self.assertEqual(a['history'],b['history'])
    def test_stored_history_price_wins(self):
        previous=dict(LIVE,status='ended',listing_price={'vwap':99.0})
        p=self.plan(all_rows=self.legacy+[previous],history=self.legacy+[previous]);self.assertEqual(len(p['history']),446)
        self.assertEqual(p['history'][-1]['listing_price']['vwap'],99.)
    def test_missing_clock_does_not_infer_event_time(self):
        p=self.plan(live=[{'id':CID,'event_time':'2020-01-01T00:00:00Z'}])
        self.assertEqual(p['promoted'],[])
    def test_invalid_clock_rejected(self):
        with self.assertRaises(CutoverHold):self.plan(live=[dict(LIVE,firstLiveAt='2026-02-30T00:00:00.000Z')])
    def test_conflicting_identity_rejected(self):
        with self.assertRaises(CutoverHold):self.plan(all_rows=self.legacy+[dict(LIVE,symbol='EVIL')])
    def test_duplicate_canonical_rejected(self):
        with self.assertRaises(CutoverHold):self.plan(live=self.live*2)
    def test_early_history_reentry_rejected(self):
        with self.assertRaises(CutoverHold):self.plan(now=NOW-timedelta(milliseconds=1),
             all_rows=self.legacy+self.live,history=self.legacy+self.live)
    def test_history_not_contained_in_all_holds(self):
        with self.assertRaises(CutoverHold):self.plan(history=self.legacy+[{'id':'rogue'}])
    def test_single_writer_flags_false(self):
        p=self.plan();self.assertFalse(p['retire_live']);self.assertFalse(p['independent_writer'])
    def test_cas_pair_writes_all_then_history_and_never_live(self):
        f=FakeR2(self.legacy,self.legacy,self.live);p=self.plan()
        r=publish_pair(f,'free-bucket',p,f.etag[ALL_KEY],f.etag[HISTORY_KEY])
        self.assertEqual(f.writes,[ALL_KEY,HISTORY_KEY]);self.assertNotIn(LIVE_KEY,f.writes)
        self.assertTrue(r['pair_verified']);self.assertFalse(r['retire_live'])
    def test_cas_race_all_holds_without_history(self):
        f=FakeR2(self.legacy,self.legacy,self.live);f.race=ALL_KEY;p=self.plan()
        with self.assertRaises(RuntimeError):publish_pair(f,'b',p,f.etag[ALL_KEY],f.etag[HISTORY_KEY])
        self.assertEqual(f.writes,[])
    def test_cas_race_history_leaves_live_shadow(self):
        f=FakeR2(self.legacy,self.legacy,self.live);f.race=HISTORY_KEY;p=self.plan()
        with self.assertRaises(RuntimeError):publish_pair(f,'b',p,f.etag[ALL_KEY],f.etag[HISTORY_KEY])
        self.assertEqual(f.writes,[ALL_KEY]);self.assertEqual(f.data[LIVE_KEY],self.live)
    def test_lost_all_ack_leaves_history_and_live_untouched(self):
        f=FakeR2(self.legacy,self.legacy,self.live);f.fail_after=ALL_KEY;p=self.plan()
        with self.assertRaises(RuntimeError):publish_pair(f,'b',p,f.etag[ALL_KEY],f.etag[HISTORY_KEY])
        self.assertEqual(f.writes,[ALL_KEY]);self.assertEqual(f.data[HISTORY_KEY],self.legacy)
        self.assertEqual(f.data[LIVE_KEY],self.live)
    def test_lost_history_ack_recoverable_after_reread(self):
        f=FakeR2(self.legacy,self.legacy,self.live);f.fail_after=HISTORY_KEY;p=self.plan()
        with self.assertRaises(RuntimeError):publish_pair(f,'b',p,f.etag[ALL_KEY],f.etag[HISTORY_KEY])
        self.assertEqual(f.writes,[ALL_KEY,HISTORY_KEY]);self.assertEqual(f.data[LIVE_KEY],self.live)
        restarted=plan(f.data[ALL_KEY],f.data[HISTORY_KEY],self.live,NOW)
        self.assertEqual(restarted['history'],f.data[HISTORY_KEY])
    def test_preexisting_bad_clock_in_history_holds(self):
        old=dict(LIVE,firstLiveAt='2026-10-11T10:00:00.000Z',status='ended')
        with self.assertRaises(CutoverHold):self.plan(all_rows=self.legacy+[old],history=self.legacy+[old])
    def test_invalid_r2_body_holds_not_empty(self):
        f=FakeR2(self.legacy,self.legacy,self.live)
        f.data[HISTORY_KEY]={'wrong':'object'}
        with self.assertRaises(CutoverHold):snapshot(f,'b',HISTORY_KEY)

from history24h_cutover import reconcile_price_enrichment

class PriceEnrichment(unittest.TestCase):
    def test_445_legacy_prices_increment_without_loss(self):
        before=legacy()
        after=json.loads(json.dumps(before))
        after[0]['listing_price']={'vwap':123.5}
        enriched,history=reconcile_price_enrichment(before,after,before)
        self.assertEqual(len(history),445)
        self.assertEqual(history[0]['listing_price']['vwap'],123.5)
        self.assertEqual(history[1:],before[1:])
        p=plan(enriched,history,[dict(LIVE)],NOW)
        self.assertEqual(len(p['history']),446)
    def test_idless_duplicate_legacy_uses_exact_preimage_multiset(self):
        one={'symbol':'DUP','status':'ended','listing_price':{'vwap':1}}
        a,h=reconcile_price_enrichment([one,one],[one,one],[one,one]);self.assertEqual(len(h),2)
    def test_old_price_cannot_be_silently_deleted(self):
        old=legacy(1);new=json.loads(json.dumps(old));new[0]['listing_price']=None
        with self.assertRaises(CutoverHold):reconcile_price_enrichment(old,new,old)
    def test_identity_changes_hold(self):
        old=legacy(1);new=json.loads(json.dumps(old));new[0]['id']='wrong'
        with self.assertRaises(CutoverHold):reconcile_price_enrichment(old,new,old)
    def test_history_row_lost_in_all_holds(self):
        with self.assertRaises(CutoverHold):reconcile_price_enrichment([],[],legacy(1))
    def test_history_does_not_regress_to_live(self):
        old=legacy(1);new=json.loads(json.dumps(old));new[0]['status']='live'
        with self.assertRaises(CutoverHold):reconcile_price_enrichment(old,new,old)

if __name__ == "__main__":
    unittest.main(verbosity=2)