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
        self.assertNotIn('ohlc',str(c).lower())

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


if __name__=='__main__':
    unittest.main()
