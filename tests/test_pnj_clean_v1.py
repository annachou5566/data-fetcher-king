"""PNJ Clean V1 + daily revision audit: no network or R2 egress."""
import importlib.util
import json
import os
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
sys.path.insert(0, str(SCRIPTS))
import pnj_history_backfill as pnj
import pnj_clean_v1 as clean
import pnj_daily_pipeline as daily


def raw(day='2026-11-12'):
    stamp=f'{date.fromisoformat(day):%d/%m/%Y} 08:35:12'
    return {'locations':[
       {'name':'TPHCM','gold_type':[{'name':'SJC','data':[
            {'gia_mua':'136.000','gia_ban':'139.000','updated_at':stamp}]}]},
       {'name':'Hà Nội','gold_type':[{'name':'PNJ','data':[
            {'gia_mua':'135.000','gia_ban':'139.000','updated_at':stamp}]}]},
       {'name':'Giá vàng nữ trang','gold_type':[
            {'name':'Nhẫn Trơn PNJ 999.9','data':[
                {'gia_mua':'133.700','gia_ban':'137.900','updated_at':stamp}]},
            {'name':'Vàng 750 (18K)','data':[
                {'gia_mua':'Đang cập nhật','gia_ban':'Đang cập nhật','updated_at':stamp}]}]}]}


def historical(day='2026-11-12', data=None):
    source=data or raw(day)
    return pnj.snapshot(date.fromisoformat(day),source,
                        pnj.inspect_payload(source,date.fromisoformat(day)),
                        '2026-11-13T02:00:00+00:00')


class FakeR2:
    def __init__(self): self.objects={}; self.writes=[]
    def read(self,key): return json.loads(json.dumps(self.objects[key])) if key in self.objects else None
    def write(self,key,doc):
        self.writes.append(key)
        self.objects[key]=json.loads(json.dumps(doc))


class CleanTests(unittest.TestCase):
    def test_sjc_and_ring_identity_and_units(self):
        day=date(2026,11,12)
        c=clean.normalize(day,historical(),pnj.key_for(day))
        self.assertEqual(c['summary']['target_status'],'BOTH_TARGETS_OBSERVED')
        self.assertEqual(c['summary']['sjc_rows'],1)
        self.assertEqual(c['summary']['pnj_ring_rows'],1)
        self.assertEqual(c['unit'],'VND_per_luong')
        self.assertEqual(c['quote_type'],'RETAIL_BID_ASK_NOT_OHLC')
        self.assertEqual(c['publication_status'],'INTERNAL_REVIEW_ONLY')
        self.assertEqual(c['quotes'][0]['buy_vnd_per_luong'],133700000)
        self.assertEqual(c['quotes'][0]['sell_vnd_per_luong'],137900000)
        self.assertEqual(c['quotes'][1]['buy_vnd_per_luong'],136000000)
        self.assertEqual(len(c['quotes']),2)
        for q in c['quotes']:
            self.assertFalse(set(q).intersection({'open','high','low','close','volume'}))

    def test_inverted_spread_quarantined_without_repair(self):
        d=raw();d['locations'][0]['gold_type'][0]['data'][0]['gia_mua']='140.000'
        c=clean.normalize(date(2026,11,12),historical(data=d),'raw')
        self.assertEqual(c['summary']['sjc_rows'],0)
        self.assertEqual(c['summary']['pnj_ring_rows'],1)
        self.assertEqual(c['summary']['target_status'],'PARTIAL_TARGET_COVERAGE')
        self.assertEqual(c['quarantine'][0]['reason'],'INVERTED_SPREAD')
        self.assertEqual(c['quarantine'][0]['source_buy'],'140.000')

    def test_out_of_day_rows_quarantined(self):
        d=raw();d['locations'][2]['gold_type'][0]['data'][0]['updated_at']='11/11/2026 08:35:12'
        c=clean.normalize(date(2026,11,12),historical(data=d),'raw')
        self.assertEqual(c['summary']['pnj_ring_rows'],0)
        self.assertEqual(c['summary']['target_status'],'PARTIAL_TARGET_COVERAGE')
        self.assertEqual(c['quarantine'][0]['reason'],'WRONG_SOURCE_DAY')

    def test_dont_alias_pnj_regional_to_ring(self):
        d=raw();d['locations'][2]['gold_type']=[]
        c=clean.normalize(date(2026,11,12),historical(data=d),'raw')
        self.assertEqual(c['summary']['pnj_ring_rows'],0)
        self.assertEqual(c['summary']['sjc_rows'],1)

    def test_quoted_price_absent_never_invented(self):
        d=raw()
        d['locations'][2]['gold_type'][0]['data'][0]['gia_ban']='Đang cập nhật'
        c=clean.normalize(date(2026,11,12),historical(data=d),'raw')
        self.assertEqual(c['summary']['pnj_ring_rows'],0)
        self.assertEqual(c['quarantine'][0]['reason'],'NON_NUMERIC_OR_ABSENT_PRICE')

    def test_same_timestamp_identical_dedup(self):
        d=raw();record=d['locations'][0]['gold_type'][0]['data'][0].copy()
        d['locations'][0]['gold_type'][0]['data'].append(record)
        c=clean.normalize(date(2026,11,12),historical(data=d),'raw')
        self.assertEqual(c['summary']['identical_duplicates_dropped'],1)
        self.assertEqual(c['summary']['sjc_rows'],1)

    def test_same_timestamp_conflict_quarantines_both_not_chooses_winner(self):
        d=raw();record=d['locations'][0]['gold_type'][0]['data'][0].copy()
        record['gia_mua']='135.500'
        d['locations'][0]['gold_type'][0]['data'].append(record)
        c=clean.normalize(date(2026,11,12),historical(data=d),'raw')
        self.assertEqual(c['summary']['sjc_rows'],0)
        self.assertEqual(c['quarantine'][0]['reason'],'CONFLICTING_SAME_TIMESTAMP')

    def test_raw_sha_integrity_required(self):
        doc=historical();doc['raw']['locations'][0]['gold_type'][0]['data'][0]['gia_mua']='999.999'
        with self.assertRaisesRegex(pnj.CollectionHold,'SOURCE_DAY_RAW_SHA256_MISMATCH'):
            clean.normalize(date(2026,11,12),doc,'raw')

    def test_empty_day_is_unverified_not_holiday_certified(self):
        day=date(2016,1,3)
        d=historical('2016-01-03',{'locations':[]})
        c=clean.normalize(day,d,pnj.key_for(day))
        self.assertEqual(c['summary']['target_status'],'NO_TARGET_QUOTES')
        self.assertEqual(c['summary']['source_day_status'],'EMPTY_UNVERIFIED')
        self.assertEqual(c['quotes'],[])

    def test_immutable_raw_and_clean_snapshot_idempotent(self):
        store=FakeR2();day=date(2026,11,12)
        original=historical();store.write(pnj.key_for(day),original)
        a,changed=clean.materialize_day(store,day)
        self.assertTrue(changed)
        self.assertEqual(a['sjc_rows'],1)
        count=len(store.writes)
        b,changed=clean.materialize_day(store,day)
        self.assertFalse(changed)
        self.assertEqual(count,len(store.writes))
        self.assertEqual(store.read(pnj.key_for(day)),original)
        self.assertIsNotNone(store.read(clean.snapshot_key(day,original['response_sha256'])))

    def test_raw_day_missing_holds_no_clean_writes(self):
        store=FakeR2()
        with self.assertRaisesRegex(pnj.CollectionHold,'SOURCE_DAY_MISSING'):
            clean.materialize_day(store,date(2026,11,12))
        self.assertEqual(store.writes,[])

    def test_history_resume_after_state_write_failure(self):
        class FailState(FakeR2):
            fail=True
            def write(self,key,doc):
                if key==clean.STATE_KEY and self.fail:
                    self.fail=False
                    raise pnj.CollectionHold('SIMULATED_STATE_WRITE_FAILURE')
                return super().write(key,doc)
        s=FailState()
        s.write(pnj.STATE_KEY,{'schema':'wa-pnj-history-backfill-state-v1',
                               'next_date':'2016-01-03'})
        for x in (date(2016,1,1),date(2016,1,2)):
            s.write(pnj.key_for(x),historical(x.isoformat()))
        with self.assertRaisesRegex(pnj.CollectionHold,'SIMULATED_STATE_WRITE_FAILURE'):
            clean.build_history(s,last=date(2016,1,2),max_days=2)
        self.assertIsNotNone(s.read(clean.day_key(date(2016,1,1))))
        out=clean.build_history(s,last=date(2016,1,2),max_days=2)
        self.assertTrue(out['caught_up'])
        self.assertEqual(out['next_date'],'2016-01-03')
        self.assertEqual(s.read(clean.STATE_KEY)['days_done'],2)

    def test_history_catches_up_with_seeded_two_days(self):
        s=FakeR2()
        s.write(pnj.STATE_KEY,{'schema':'wa-pnj-history-backfill-state-v1',
                               'next_date':'2016-01-03'})
        for x in (date(2016,1,1),date(2016,1,2)):
            s.write(pnj.key_for(x),historical(x.isoformat()))
        out=clean.build_history(s,last=date(2016,1,2),max_days=4000)
        self.assertEqual(out['processed'],2)
        self.assertEqual(s.read(clean.STATE_KEY)['days_done'],2)

    def test_revision_creates_new_source_preserves_baseline(self):
        s=FakeR2();day=date(2026,11,12)
        original=historical();s.write(pnj.key_for(day),original)
        d=raw();d['locations'][0]['gold_type'][0]['data'][0]['gia_mua']='136.500'
        summary=pnj.inspect_payload(d,day)
        o=daily.persist_recheck(s,day,d,summary,now=datetime(2026,11,13,tzinfo=timezone.utc))
        self.assertEqual(o['status'],'SOURCE_REVISION_SAVED')
        self.assertEqual(s.read(pnj.key_for(day)),original)
        current,source_key=clean.source_for_day(s,day)
        self.assertIn('/revisions/',source_key)
        self.assertEqual(current['raw'],d)
        self.assertEqual(s.read(clean.day_key(day))['source_sha256'],clean.canonical_hash(d))
        self.assertIsNotNone(s.read(clean.snapshot_key(day,original['response_sha256'])) if
                             False else s.read(clean.snapshot_key(day,clean.canonical_hash(d))))

    def test_recheck_same_raw_no_new_revision(self):
        s=FakeR2();day=date(2026,11,12)
        doc=historical();s.write(pnj.key_for(day),doc)
        o=daily.persist_recheck(s,day,raw(),doc['summary'])
        self.assertEqual(o['status'],'UNCHANGED')
        self.assertIsNone(s.read(clean.revision_pointer_key(day)))
        count=len(s.writes)
        o=daily.persist_recheck(s,day,raw(),doc['summary'])
        self.assertEqual(o['status'],'UNCHANGED')
        self.assertEqual(len(s.writes),count)

    def test_bad_latest_pointer_fails_closed(self):
        s=FakeR2();day=date(2026,11,12)
        s.write(pnj.key_for(day),historical())
        s.write(clean.revision_pointer_key(day),{'schema':'wa-pnj-source-latest-v1',
               'date':day.isoformat(),'object_key':'metal/other/strange.json','raw_sha256':'x'})
        with self.assertRaisesRegex(pnj.CollectionHold,'SOURCE_POINTER_INVALID'):
            clean.source_for_day(s,day)

    def test_daily_does_not_advance_day_or_refetch_history_if_caught_up(self):
        s=FakeR2()
        for day in [date(2026,11,10),date(2026,11,11),date(2026,11,12)]:
            s.write(pnj.key_for(day),historical(day.isoformat()))
        s.write(pnj.STATE_KEY,{'schema':'wa-pnj-history-backfill-state-v1',
                               'next_date':'2026-11-13','days_processed':3969,
                               'days_with_updates':3357,'empty_unverified_days':612,
                               'quarantined_rows':243})
        called=[]
        def fetcher(day):
            called.append(day)
            x=raw(day.isoformat())
            return x,pnj.inspect_payload(x,day)
        out=daily.run_daily(s,date_now=lambda:date(2026,11,13),
                            fetcher=fetcher,sleeper=lambda _:None)
        self.assertEqual(out['status'],'DAILY_OK')
        self.assertEqual(out['catchup']['processed'],0)
        self.assertEqual(called,[date(2026,11,10),date(2026,11,11),date(2026,11,12)])
        self.assertEqual(s.read(pnj.STATE_KEY)['next_date'],'2026-11-13')

    def test_no_automatic_access_without_run_approvals(self):
        with patch.object(clean,'R2',side_effect=AssertionError('NO_EGRESS')):
            self.assertEqual(clean.main([]),0)
        with patch.object(daily,'R2',side_effect=AssertionError('NO_EGRESS')):
            self.assertEqual(daily.main([]),0)
        with patch.dict(os.environ,{},clear=True):
            with patch.object(daily,'R2',side_effect=AssertionError('NO_EGRESS')):
                self.assertEqual(daily.main(['--run']),2)



    def test_revision_revert_uses_original_immutable_day(self):
        store=FakeR2();day=date(2026,11,12)
        original=historical();store.write(pnj.key_for(day),original)
        modified=raw();modified['locations'][0]['gold_type'][0]['data'][0]['gia_mua']='136.500'
        daily.persist_recheck(store,day,modified,pnj.inspect_payload(modified,day))
        result=daily.persist_recheck(store,day,raw(),original['summary'])
        self.assertEqual(result['status'],'SOURCE_REVISION_SAVED')
        source,key=clean.source_for_day(store,day)
        self.assertEqual(key,pnj.key_for(day))
        self.assertEqual(source,original)
        self.assertEqual(store.read(clean.day_key(day))['source_sha256'],
                         original['response_sha256'])

    def test_source_denial_during_recheck_persists_pause(self):
        store=FakeR2()
        for day in (date(2026,11,10),date(2026,11,11),date(2026,11,12)):
            store.write(pnj.key_for(day),historical(day.isoformat()))
        store.write(pnj.STATE_KEY,{'schema':'wa-pnj-history-backfill-state-v1',
                         'next_date':'2026-11-13','days_processed':3969,
                         'days_with_updates':3357,'empty_unverified_days':612,
                         'quarantined_rows':243})
        def denied(_):raise pnj.CollectionHold('UPSTREAM_HTTP_429')
        with self.assertRaisesRegex(pnj.CollectionHold,'UPSTREAM_HTTP_429'):
            daily.run_daily(store,fetcher=denied,date_now=lambda:date(2026,11,13),
                            sleeper=lambda _:None)
        final=store.read(pnj.STATE_KEY)
        self.assertEqual(final['paused_reason'],'UPSTREAM_HTTP_429')
        self.assertEqual(final['paused_date'],'2026-11-10')
        self.assertEqual(final['next_date'],'2026-11-13')
        with self.assertRaisesRegex(pnj.CollectionHold,'STATE_PAUSED_UPSTREAM_HTTP_429'):
            daily.run_daily(store,fetcher=denied,date_now=lambda:date(2026,11,13),
                            sleeper=lambda _:None)

    def test_network_drop_during_recheck_does_not_pause_or_modify_raw(self):
        store=FakeR2()
        for day in (date(2026,11,10),date(2026,11,11),date(2026,11,12)):
            store.write(pnj.key_for(day),historical(day.isoformat()))
        state={'schema':'wa-pnj-history-backfill-state-v1',
               'next_date':'2026-11-13','days_processed':3969,
               'days_with_updates':3357,'empty_unverified_days':612,
               'quarantined_rows':243}
        store.write(pnj.STATE_KEY,state)
        def failed(_):raise pnj.CollectionHold('UPSTREAM_NETWORK_ERROR')
        with self.assertRaisesRegex(pnj.CollectionHold,'UPSTREAM_NETWORK_ERROR'):
            daily.run_daily(store,fetcher=failed,date_now=lambda:date(2026,11,13),
                            sleeper=lambda _:None)
        self.assertEqual(store.read(pnj.STATE_KEY),state)
        self.assertIsNone(store.read(clean.revision_pointer_key(date(2026,11,10))))



    def test_source_empty_regression_is_archived_but_not_promoted(self):
        store=FakeR2();day=date(2026,11,12)
        original=historical()
        store.write(pnj.key_for(day),original)
        clean.materialize_day(store,day)
        before=store.read(clean.day_key(day))
        partial={'locations':[]}
        summary=pnj.inspect_payload(partial,day)
        result=daily.persist_recheck(store,day,partial,summary)
        self.assertEqual(result['status'],'SOURCE_REVISION_HELD_FOR_REVIEW')
        self.assertFalse(result['clean_updated'])
        self.assertEqual(store.read(clean.day_key(day)),before)
        self.assertEqual(store.read(pnj.key_for(day)),original)
        self.assertIsNone(store.read(clean.revision_pointer_key(day)))
        review_paths=[x for x in store.objects if '/review/' in x]
        self.assertEqual(len(review_paths),1)
        self.assertEqual(store.read(review_paths[0])['reason'],
                         'SOURCE_TARGET_COVERAGE_REGRESSION')

    def test_partial_ring_regression_is_not_auto_promoted(self):
        store=FakeR2();day=date(2026,11,12)
        original=historical();store.write(pnj.key_for(day),original)
        partial=raw()
        partial['locations'][2]['gold_type']=[]
        summary=pnj.inspect_payload(partial,day)
        self.assertEqual(summary['target_coverage'],'SJC_ONLY')
        result=daily.persist_recheck(store,day,partial,summary)
        self.assertEqual(result['status'],'SOURCE_REVISION_HELD_FOR_REVIEW')
        self.assertIsNone(store.read(clean.revision_pointer_key(day)))
        self.assertEqual(store.read(pnj.key_for(day)),original)



    def test_clean_stops_at_last_committed_source_day_not_uncollected_yesterday(self):
        s=FakeR2()
        # Exactly the live production state on 2026-11-14 at 09:10 ICT.
        s.write(pnj.STATE_KEY,{'schema':'wa-pnj-history-backfill-state-v1',
                               'next_date':'2026-11-13','days_processed':3969})
        old={'schema':'wa-pnj-clean-progress-v1',
             'next_date':'2026-11-13','days_done':3969,
             'days_with_both_targets':3303,'days_without_target':617}
        s.write(clean.STATE_KEY,old)
        count=len(s.writes)
        with patch.object(clean,'today_vn',return_value=date(2026,11,14)):
            out=clean.build_history(s,last=date(2026,11,13),max_days=4000)
        self.assertTrue(out['caught_up'])
        self.assertEqual(out['processed'],0)
        self.assertEqual(out['target_through'],'2026-11-12')
        self.assertEqual(out['next_date'],'2026-11-13')
        self.assertEqual(s.read(clean.STATE_KEY),old)
        self.assertEqual(len(s.writes),count)

    def test_clean_resumes_exactly_one_new_day_after_source_catches_up(self):
        s=FakeR2()
        s.write(pnj.STATE_KEY,{'schema':'wa-pnj-history-backfill-state-v1',
                               'next_date':'2026-11-14','days_processed':3970})
        s.write(pnj.key_for(date(2026,11,13)),historical('2026-11-13'))
        s.write(clean.STATE_KEY,{'schema':'wa-pnj-clean-progress-v1',
             'next_date':'2026-11-13','days_done':3969,
             'days_with_both_targets':3303,'days_without_target':617})
        with patch.object(clean,'today_vn',return_value=date(2026,11,14)):
            out=clean.build_history(s,last=date(2026,11,13),max_days=4000)
        self.assertTrue(out['caught_up'])
        self.assertEqual(out['processed'],1)
        self.assertEqual(out['target_through'],'2026-11-13')
        self.assertEqual(out['next_date'],'2026-11-14')
        self.assertEqual(s.read(clean.STATE_KEY)['days_done'],3970)
        self.assertIsNotNone(s.read(clean.day_key(date(2026,11,13))))

    def test_missing_or_invalid_source_cursor_holds_without_writes(self):
        for malformed in (None,{'schema':'bad','next_date':'2026-11-13'},
                          {'schema':'wa-pnj-history-backfill-state-v1',
                           'next_date':'not-a-date'}):
            s=FakeR2()
            if malformed is not None:s.write(pnj.STATE_KEY,malformed)
            before=len(s.writes)
            with patch.object(clean,'today_vn',return_value=date(2026,11,14)):
                with self.assertRaisesRegex(pnj.CollectionHold,'HISTORY_SOURCE_CURSOR'):
                    clean.build_history(s,last=date(2026,11,13))
            self.assertEqual(len(s.writes),before)


if __name__=='__main__':
    unittest.main()
