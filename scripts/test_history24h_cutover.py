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

class HistoryPartialPairRecovery(unittest.TestCase):
    def test_retry_after_all_only_commit_with_price_enrichment(self):
        old={'id':'legacy-A', 'symbol':'ABC', 'event_time':'2025-01-01T00:00:00Z',
             'event_type':'grab','status':'ended','listing_price':{'vwap':3}}
        enriched=dict(old,listing_price={'vwap':4})
        all_after=[enriched,dict(LIVE,status='ended',lifecycle_state='history')]
        fixed,history=reconcile_price_enrichment(all_after,all_after,[old])
        self.assertEqual(history[0]['listing_price']['vwap'],4)
        proposed=plan(fixed,history,[dict(LIVE)],NOW)
        self.assertEqual(len(proposed['history']),2)
        self.assertEqual(proposed['history'][0]['listing_price']['vwap'],4)
        self.assertFalse(proposed['retire_live'])
    def test_idless_legacy_price_ack_loss_recovers_if_unique(self):
        old={'symbol':'XYZ','event_time':'2025-08-01T08:00:00Z',
             'event_type':'grab','phase':'P2','chain_id':'56',
             'status':'ended','spot_listing_price':{'vwap':3}}
        updated=dict(old,spot_listing_price={'vwap':4})
        a,h=reconcile_price_enrichment([updated],[updated],[old])
        self.assertEqual(h[0]['spot_listing_price']['vwap'],4)
        self.assertEqual(a,h)
    def test_ambiguous_idless_multi_round_retry_fails_closed(self):
        old={'symbol':'XYZ','event_time':'2025-08-01T08:00:00Z',
             'event_type':'grab','phase':'P2','status':'ended',
             'listing_price':{'vwap':1}}
        current=[dict(old,listing_price={'vwap':2}),
                 dict(old,listing_price={'vwap':3})]
        with self.assertRaisesRegex(CutoverHold,'HISTORY_RETRY_IDENTITY_AMBIGUOUS'):
            reconcile_price_enrichment(current,current,[old])
    def test_idless_missing_event_time_cannot_fuzzy_match(self):
        old={'symbol':'XYZ','status':'ended','listing_price':{'vwap':1}}
        updated=dict(old,listing_price={'vwap':2})
        with self.assertRaisesRegex(CutoverHold,'HISTORY_PREIMAGE_NOT_IN_ALL'):
            reconcile_price_enrichment([updated],[updated],[old])
    def test_same_id_but_changed_event_time_not_reconciled(self):
        old={'id':'legacy-A','symbol':'XYZ','event_time':'2025-08-01T08:00:00Z',
             'status':'ended','listing_price':{'vwap':1}}
        updated=dict(old,event_time='2025-08-02T08:00:00Z',listing_price={'vwap':2})
        with self.assertRaisesRegex(CutoverHold,'HISTORY_IDENTITY_MOVED'):
            reconcile_price_enrichment([updated],[updated],[old])
    def test_all_cas_ack_loss_price_update_recovers_without_live_retire(self):
        old=legacy(445)
        changed=json.loads(json.dumps(old))
        changed[0]['listing_price']={'vwap':999}
        f=FakeR2(old,old,[dict(LIVE)])
        all_new,hist_new=reconcile_price_enrichment(old,changed,old)
        first=plan(all_new,hist_new,[dict(LIVE)],NOW)
        f.fail_after=ALL_KEY
        with self.assertRaises(RuntimeError):
            publish_pair(f,'b',first,f.etag[ALL_KEY],f.etag[HISTORY_KEY])
        self.assertEqual(f.data[LIVE_KEY],[dict(LIVE)])
        self.assertEqual(f.data[HISTORY_KEY],old)
        f.fail_after=None
        latest_all,_=snapshot(f,'b',ALL_KEY)
        latest_hist,_=snapshot(f,'b',HISTORY_KEY)
        recovered_all,recovered_hist=reconcile_price_enrichment(latest_all,latest_all,latest_hist)
        recovery=plan(recovered_all,recovered_hist,[dict(LIVE)],NOW)
        reply=publish_pair(f,'b',recovery,f.etag[ALL_KEY],f.etag[HISTORY_KEY])
        self.assertTrue(reply['pair_verified'])
        self.assertEqual(len(f.data[HISTORY_KEY]),446)
        self.assertEqual(f.data[HISTORY_KEY][0]['listing_price'],{'vwap':999})
        self.assertEqual(f.data[LIVE_KEY],[dict(LIVE)])
        self.assertEqual(sum(row.get('id')==CID for row in f.data[HISTORY_KEY]),1)
    def test_history_cas_failure_after_all_price_update_recovers(self):
        old=legacy(445)
        changed=json.loads(json.dumps(old))
        changed[1]['spot_listing_price']={'vwap':75}
        f=FakeR2(old,old,[dict(LIVE)]);f.fail_before=HISTORY_KEY
        a,h=reconcile_price_enrichment(old,changed,old)
        first=plan(a,h,[dict(LIVE)],NOW)
        with self.assertRaises(RuntimeError):
            publish_pair(f,'b',first,f.etag[ALL_KEY],f.etag[HISTORY_KEY])
        self.assertEqual(f.writes,[ALL_KEY]);f.fail_before=None
        prior_all,_=snapshot(f,'b',ALL_KEY)
        prior_history,_=snapshot(f,'b',HISTORY_KEY)
        a,h=reconcile_price_enrichment(prior_all,prior_all,prior_history)
        next_plan=plan(a,h,[dict(LIVE)],NOW)
        publish_pair(f,'b',next_plan,f.etag[ALL_KEY],f.etag[HISTORY_KEY])
        self.assertEqual(len(f.data[ALL_KEY]),446)
        self.assertEqual(f.data[HISTORY_KEY][1]['spot_listing_price'],{'vwap':75})
        self.assertEqual(f.data[LIVE_KEY],[dict(LIVE)])

    def test_same_id_but_changed_firstlive_not_reconciled(self):
        old={'id':'ae:0123456789abcdef01234567','symbol':'XYZ',
             'firstLiveAt':'2026-10-11T00:00:00.000Z','status':'ended',
             'listing_price':{'vwap':1}}
        updated=dict(old,firstLiveAt='2026-10-12T00:00:00.000Z',listing_price={'vwap':2})
        with self.assertRaisesRegex(CutoverHold,'HISTORY_IDENTITY_MOVED'):
            reconcile_price_enrichment([updated],[updated],[old])

if __name__ == "__main__":
    unittest.main(verbosity=2)


class LegacyPendingRecovery(unittest.TestCase):
    def setUp(self):
        from history24h_cutover import recover_stale_pending
        self.recover=recover_stale_pending
        self.now=datetime(2026,10,12,8,0,tzinfo=timezone.utc)
        self.modified=datetime(2026,10,9,9,0,tzinfo=timezone.utc)
        self.legacy=legacy(445)
        self.manifest=[{'id':f'ae:{i:024x}', 'symbol':s, 'event_time':ts} for i,(s,ts) in enumerate([
            ('BTR','2026-10-09T09:00:00+00:00'),
            ('CLO','2026-10-09T09:00:00+00:00'),
            ('US','2026-10-07T09:00:00+00:00')], 1)]
        self.pending=[{'symbol':x['symbol'], 'event_time':x['event_time'],
                       'amount_per_user':3, 'event_type':'grab','status':'upcoming'} for x in self.manifest]
        self.base=plan(self.legacy,self.legacy,[],self.now)
    def run_plan(self, **over):
        opts={'plan_value':self.base,'pending_rows':self.pending,'exact_manifest':self.manifest,
              'pending_last_modified':self.modified,'public_etag':'etag','pending_etag':'etag','now':self.now}
        opts.update(over)
        return self.recover(**opts)
    def test_stale_three_promoted_preserving_445_price_rows(self):
        p=self.run_plan();self.assertEqual(len(p['history']),448)
        self.assertEqual(p['history'][:445],self.legacy)
        self.assertEqual({x['symbol'] for x in p['history'][445:]},{'BTR','CLO','US'})
        self.assertTrue(all(x['status']=='ended' for x in p['history'][445:]))
        self.assertTrue(all('firstLiveAt' not in x for x in p['history'][445:]))
        self.assertFalse(p['retire_pending']);self.assertFalse(p['retire_public_active'])
    def test_replay_is_idempotent(self):
        p=self.run_plan();r=self.run_plan(plan_value=plan(p['all'],p['history'],[],self.now))
        self.assertEqual(p['history'],r['history']);self.assertEqual(p['all'],r['all'])
    def test_exact_one_millisecond_before_24h_is_hold(self):
        with self.assertRaisesRegex(CutoverHold,'SNAPSHOT_TOO_YOUNG'):
            self.run_plan(now=self.modified+timedelta(hours=24)-timedelta(milliseconds=1))
    def test_equal_24h_is_eligible(self):
        self.assertEqual(len(self.run_plan(now=self.modified+timedelta(hours=24))['legacy_promoted']),3)
    def test_requires_equal_r2_public_pending_etag(self):
        with self.assertRaisesRegex(CutoverHold,'PROOF_MISSING'):
            self.run_plan(public_etag='not-pending')
    def test_cannot_guess_when_pending_token_missing(self):
        with self.assertRaisesRegex(CutoverHold,'IDENTITY_NOT_UNIQUE'):
            self.run_plan(pending_rows=self.pending[:2])
    def test_cannot_match_symbol_alone_for_multi_round(self):
        with self.assertRaisesRegex(CutoverHold,'IDENTITY_NOT_UNIQUE'):
            self.run_plan(pending_rows=self.pending+[dict(self.pending[0])])
    def test_cannot_change_signed_canonical_identity(self):
        with self.assertRaisesRegex(CutoverHold,'CANONICAL_ID_CONFLICT'):
            self.run_plan(pending_rows=[dict(self.pending[0],id='ae:'+('f'*24))]+self.pending[1:])
    def test_preserve_old_price_on_existing_all(self):
        enriched=dict(self.legacy[0]);enriched['listing_price']={'vwap':5000}
        old=[enriched]+self.legacy[1:]
        p=self.run_plan(plan_value=plan(old,old,[],self.now))
        self.assertEqual(p['history'][0]['listing_price']['vwap'],5000)
    def test_reject_non_utc_event_time(self):
        bad=[dict(self.pending[0],event_time='2026-10-09T09:00:00')]+self.pending[1:]
        with self.assertRaises(CutoverHold): self.run_plan(pending_rows=bad)
    def test_no_r2_mutation_until_publish_pair(self):
        r=FakeR2(self.legacy,self.legacy,[])
        p=self.run_plan();self.assertEqual(r.writes,[])
        ack=publish_pair(r,'free-bucket',p,r.etag[ALL_KEY],r.etag[HISTORY_KEY])
        self.assertEqual(r.writes,[ALL_KEY,HISTORY_KEY]);self.assertTrue(ack['pair_verified'])
    def test_manual_manifest_must_be_exact(self):
        with self.assertRaisesRegex(CutoverHold,'MANIFEST_SHAPE'):
            self.run_plan(exact_manifest=[{'id':self.manifest[0]['id'],'symbol':'BTR'}])


class SnapshotMetadataProof(unittest.TestCase):
    def test_checked_metadata_returned_only_when_requested(self):
        f=FakeR2([],[]);last=datetime(2026,10,9,9,0,tzinfo=timezone.utc)
        old=f.get_object
        def get(Bucket,Key):return {**old(Bucket,Key),'LastModified':last}
        f.get_object=get
        self.assertEqual(len(snapshot(f,'b',ALL_KEY)),2)
        self.assertEqual(snapshot(f,'b',ALL_KEY,include_last_modified=True)[2],last)
    def test_missing_metadata_must_hold_before_writes(self):
        f=FakeR2([],[])
        with self.assertRaisesRegex(CutoverHold,'LASTMOD_PROOF_MISSING'):
            snapshot(f,'b',ALL_KEY,include_last_modified=True)
        self.assertEqual(f.writes,[])
