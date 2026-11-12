import importlib.util
import json
import os
from pathlib import Path
import unittest
from datetime import date
from unittest.mock import patch

TARGET = Path(__file__).resolve().parents[1] / 'scripts' / 'pnj_history_backfill.py'
spec = importlib.util.spec_from_file_location('wa_pnj_history', TARGET)
pnj = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pnj)


def sample(day='2016-12-15'):
    d = date.fromisoformat(day)
    ts = f'{d:%d/%m/%Y} 08:11:18'
    return {'locations': [
        {'name': 'TPHCM', 'gold_type': [{'name': 'SJC', 'data': [
            {'gia_mua': '35.850', 'gia_ban': '36.350', 'updated_at': ts}]}]},
        {'name': 'Giá vàng nữ trang', 'gold_type': [
            {'name': 'Nhẫn Trơn PNJ 999.9', 'data': [
                {'gia_mua': '33.080', 'gia_ban': '33.580', 'updated_at': ts}]}]},
    ]}


class MemoryR2:
    def __init__(self, fail_state_once=False):
        self.d = {}
        self.fail_state_once = fail_state_once
    def read(self, key):
        return self.d.get(key)
    def write(self, key, doc):
        if self.fail_state_once and key == pnj.STATE_KEY:
            self.fail_state_once = False
            raise pnj.CollectionHold('SIMULATED_STATE_WRITE_FAILURE')
        self.d[key] = json.loads(json.dumps(doc))


class TestPNJ(unittest.TestCase):
    def test_source_is_pinned(self):
        self.assertEqual(pnj.ENDPOINT, 'https://edge-cf-api.pnj.io/ecom-frontend/v1/get-gold-price-history')
    def test_day_key(self):
        self.assertEqual(pnj.key_for(date(2016,12,15)), 'metal/pnj/history-v1/days/2016/12/2016-12-15.json')
    def test_day_bound(self):
        for n in [0,367,-1]:
            with self.assertRaises(ValueError): pnj.limit_days(n)
        self.assertEqual(pnj.limit_days(24), 24)
        self.assertEqual(pnj.limit_days(366), 366)
    def test_schema(self):
        self.assertEqual(pnj.inspect_payload(sample(), date(2016,12,15))['updates_with_valid_date'], 2)
    def test_cross_date_quarantine_not_fabricated(self):
        s = pnj.inspect_payload(sample(), date(2016,12,16))
        self.assertEqual(s['updates_with_valid_date'], 0)
        self.assertEqual(s['quarantined_row_count'], 2)
    def test_unknown_shape_hold(self):
        with self.assertRaises(pnj.CollectionHold): pnj.inspect_payload({'something':'else'}, date(2016,12,15))
    def test_empty_not_full_coverage(self):
        s = pnj.inspect_payload({'locations':[]},date(2016,12,15))
        self.assertEqual(s['coverage'],'EMPTY_UNVERIFIED')
    def test_source_fields(self):
        s = sample(); v = pnj.snapshot(date(2016,12,15),s,pnj.inspect_payload(s,date(2016,12,15)),'2026-11-12T00:00:00+00:00')
        self.assertEqual(v['raw'],s)
        self.assertEqual(v['publication_status'],'INTERNAL_REVIEW_ONLY')
        self.assertEqual(v['unit'],'thousand_VND_per_luong')
    def test_no_approval_no_collect(self):
        with patch.dict(os.environ,{},clear=True):
            with self.assertRaises(pnj.CollectionHold): pnj.check_approvals()
    def test_plan_no_network_storage(self):
        with patch.object(pnj,'R2',side_effect=AssertionError('R2 must not be called')):
            with patch.object(pnj,'fetch_day',side_effect=AssertionError('fetch must not be called')):
                self.assertEqual(pnj.main(['--max-days','18']),0)
    def test_end_to_end_three_days(self):
        storage=MemoryR2(); dates=[]
        def fake_fetch(d): dates.append(d);p=sample(d.isoformat());return p,pnj.inspect_payload(p,d)
        output=pnj.backfill(storage,first=date(2016,12,15),last=date(2016,12,17),max_days=3,delay=8,fetcher=fake_fetch,sleeper=lambda _:None)
        self.assertEqual(output['fetched'],3)
        self.assertEqual(len(dates),3)
        self.assertEqual(storage.d[pnj.STATE_KEY]['next_date'],'2016-12-18')
    def test_safe_recover_after_day_persisted_state_fails(self):
        storage=MemoryR2(fail_state_once=True); calls=[]
        def fake_fetch(d): calls.append(d);p=sample(d.isoformat());return p,pnj.inspect_payload(p,d)
        with self.assertRaises(pnj.CollectionHold):
            pnj.backfill(storage,first=date(2016,12,15),last=date(2016,12,16),max_days=2,delay=8,fetcher=fake_fetch,sleeper=lambda _:None)
        out=pnj.backfill(storage,first=date(2016,12,15),last=date(2016,12,16),max_days=2,delay=8,fetcher=fake_fetch,sleeper=lambda _:None)
        self.assertEqual(out['fetched'],1)
        self.assertEqual(out['reused'],1)
        self.assertEqual(len(calls),2)
    def test_rate_limit_pauses_future_schedules(self):
        storage=MemoryR2(); count=[]
        def denied(d): count.append(d);raise pnj.CollectionHold('UPSTREAM_HTTP_429')
        with self.assertRaises(pnj.CollectionHold):
            pnj.backfill(storage,first=date(2016,12,15),last=date(2016,12,16),max_days=2,delay=8,fetcher=denied)
        self.assertEqual(storage.d[pnj.STATE_KEY]['paused_reason'],'UPSTREAM_HTTP_429')
        with self.assertRaises(pnj.CollectionHold):
            pnj.backfill(storage,first=date(2016,12,15),last=date(2016,12,16),max_days=2,delay=8,fetcher=denied)
        self.assertEqual(len(count),1)
    def test_no_old_date(self):
        with self.assertRaises(ValueError):
            pnj.backfill(MemoryR2(),first=date(2015,12,31),last=date(2016,1,1),delay=8)
    def test_delay_floor(self):
        with self.assertRaises(ValueError):
            pnj.backfill(MemoryR2(),last=date(2016,1,1),delay=0)


    def test_year_checkpoint_written_for_every_processed_day(self):
        s = MemoryR2()
        def fetch(d):
            doc = sample(d.isoformat())
            return doc, pnj.inspect_payload(doc, d)
        out = pnj.backfill(s, first=date(2016, 12, 15), last=date(2016, 12, 16),
                           max_days=2, delay=8, fetcher=fetch, sleeper=lambda _: None)
        self.assertEqual(out['year'], 2016)
        self.assertEqual(out['year_scan_status'], 'IN_PROGRESS')
        y = s.read(pnj.year_key(2016))
        self.assertEqual(y['days_committed'], 2)
        self.assertEqual(y['days_observed'], 2)
        self.assertEqual(y['next_date'], '2016-12-17')
        self.assertFalse(y['started_from_january_1'])

    def test_hard_year_boundary_then_next_run_moves_to_next_year(self):
        s = MemoryR2()
        calls = []
        def fetch(d):
            calls.append(d)
            doc = sample(d.isoformat())
            return doc, pnj.inspect_payload(doc, d)
        first = pnj.backfill(s, first=date(2016, 12, 30), last=date(2017, 1, 2),
                            max_days=10, delay=8, fetcher=fetch, sleeper=lambda _: None)
        self.assertEqual(first['processed'], 2)
        self.assertTrue(first['year_boundary_reached'])
        self.assertEqual(first['next_date'], '2017-01-01')
        self.assertEqual(first['year_scan_status'], 'PARTIAL_YEAR_PRIOR_DAYS_UNVERIFIED')
        next_batch = pnj.backfill(s, first=date(2016, 12, 30), last=date(2017, 1, 2),
                                  max_days=10, delay=8, fetcher=fetch, sleeper=lambda _: None)
        self.assertEqual(next_batch['year'], 2017)
        self.assertEqual(next_batch['processed'], 2)
        self.assertTrue(next_batch['complete_through_target'])
        self.assertEqual(len(calls), 4)
        self.assertEqual(s.read(pnj.year_key(2017))['days_committed'], 2)

    def test_real_full_leap_year_receipt_without_gaps(self):
        s = MemoryR2()
        def fetch(d):
            doc = sample(d.isoformat())
            return doc, pnj.inspect_payload(doc, d)
        out = None
        for _ in range(16):  # 366 days / max 24 days per run => 16 bounded calls
            out = pnj.backfill(s, first=date(2016, 1, 1), last=date(2017, 1, 2),
                               max_days=24, delay=8, fetcher=fetch, sleeper=lambda _: None)
            if out['year_boundary_reached']:
                break
        yr = s.read(pnj.year_key(2016))
        self.assertEqual(yr['days_committed'], 366)
        self.assertEqual(yr['days_observed'], 366)
        self.assertEqual(yr['scan_status'], 'SCANNED_YEAR_TARGETS_OBSERVED_NO_FLAGGED_ROWS')
        self.assertEqual(yr['next_date'], '2017-01-01')
        self.assertTrue(yr['started_from_january_1'])
        self.assertEqual(out['year'], 2016)
        self.assertTrue(out['year_boundary_reached'])

    def test_global_cursor_failure_after_year_receipt_is_idempotent(self):
        s = MemoryR2(fail_state_once=True)
        calls = []
        def fetch(d):
            calls.append(d)
            doc = sample(d.isoformat())
            return doc, pnj.inspect_payload(doc, d)
        with self.assertRaises(pnj.CollectionHold):
            pnj.backfill(s, first=date(2016, 12, 15), last=date(2016, 12, 15),
                         max_days=1, delay=8, fetcher=fetch)
        self.assertEqual(s.read(pnj.year_key(2016))['days_committed'], 1)
        out = pnj.backfill(s, first=date(2016, 12, 15), last=date(2016, 12, 15),
                           max_days=1, delay=8, fetcher=fetch)
        self.assertEqual(out['reused'], 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(s.read(pnj.year_key(2016))['days_committed'], 1)

    def test_year_receipt_failure_after_day_object_retry_has_no_refetch(self):
        class FailYearOnce(MemoryR2):
            fail_year = True
            def write(self, key, doc):
                if self.fail_year and key == pnj.year_key(2016):
                    self.fail_year = False
                    raise pnj.CollectionHold('SIMULATED_YEAR_WRITE_ERROR')
                super().write(key, doc)
        s = FailYearOnce()
        calls = []
        def fetch(d):
            calls.append(d)
            doc = sample(d.isoformat())
            return doc, pnj.inspect_payload(doc, d)
        with self.assertRaises(pnj.CollectionHold):
            pnj.backfill(s, first=date(2016, 12, 15), last=date(2016, 12, 15),
                         max_days=1, delay=8, fetcher=fetch)
        self.assertIn(pnj.key_for(date(2016,12,15)), s.d)
        out = pnj.backfill(s, first=date(2016, 12, 15), last=date(2016, 12, 15),
                           max_days=1, delay=8, fetcher=fetch)
        self.assertEqual(out['reused'], 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(s.read(pnj.year_key(2016))['days_committed'], 1)

    def test_year_progress_cursor_conflict_fails_closed(self):
        s = MemoryR2()
        bad = pnj.new_year_progress(date(2016, 12, 15))
        bad['next_date'] = '2016-12-20'
        s.write(pnj.year_key(2016), bad)
        def fetch(d):
            doc = sample(d.isoformat())
            return doc, pnj.inspect_payload(doc, d)
        with self.assertRaisesRegex(pnj.CollectionHold, 'YEAR_PROGRESS_CURSOR_CONFLICT'):
            pnj.backfill(s, first=date(2016, 12, 15), last=date(2016, 12, 15),
                         max_days=1, delay=8, fetcher=fetch)
        self.assertIsNone(s.read(pnj.STATE_KEY))

    def test_empty_year_not_claimed_complete_coverage(self):
        s = MemoryR2()
        def fetch(d):
            p = {'locations': []}
            return p, pnj.inspect_payload(p, d)
        for _ in range(16):
            out = pnj.backfill(s, first=date(2016,1,1), last=date(2016,12,31),
                               max_days=24, delay=8, fetcher=fetch, sleeper=lambda _: None)
            if out['year_boundary_reached']:
                break
        yr = s.read(pnj.year_key(2016))
        self.assertEqual(yr['scan_status'], 'SCANNED_YEAR_HAS_EMPTY_UNVERIFIED_DAYS')
        self.assertEqual(yr['days_observed'], 0)
        self.assertEqual(yr['empty_unverified_days'], 366)
        self.assertTrue(yr['started_from_january_1'])



    def test_valid_source_product_completeness(self):
        s = pnj.inspect_payload(sample('2016-12-15'), date(2016,12,15))
        self.assertEqual(s['target_coverage'], 'BOTH')
        self.assertEqual(s['sjc_valid_rows'], 1)
        self.assertEqual(s['pnj_ring_valid_rows'], 1)
        self.assertEqual(s['invalid_price_rows'], 0)

    def test_invalid_price_quarantined_no_fake_coverage(self):
        d = sample()
        row = d['locations'][0]['gold_type'][0]['data'][0]
        row['gia_mua'] = '35.85abc'
        s = pnj.inspect_payload(d, date(2016,12,15))
        self.assertEqual(s['invalid_price_rows'], 1)
        self.assertEqual(s['target_coverage'], 'RING_ONLY')
        self.assertEqual(s['updates_with_valid_date'], 1)

    def test_inverted_buy_sell_quarantined(self):
        d = sample()
        row = d['locations'][1]['gold_type'][0]['data'][0]
        row['gia_mua'] = '34.580'
        row['gia_ban'] = '33.580'
        s = pnj.inspect_payload(d, date(2016,12,15))
        self.assertEqual(s['inverted_spread_rows'], 1)
        self.assertEqual(s['target_coverage'], 'SJC_ONLY')
        self.assertEqual(s['quarantined_row_count'], 1)

    def test_missing_prices_never_counted_as_valid(self):
        d = sample()
        for x in d['locations']:
            for y in x['gold_type']:
                for v in y['data']:
                    v['gia_mua'] = ''
                    v['gia_ban'] = ''
        s = pnj.inspect_payload(d, date(2016,12,15))
        self.assertEqual(s['coverage'], 'EMPTY_UNVERIFIED')
        self.assertEqual(s['quarantined_row_count'], 2)
        self.assertEqual(s['target_coverage'], 'NEITHER')

    def test_product_missing_marks_year_gap_without_claiming_full(self):
        storage = MemoryR2()
        def fetch(day):
            p = sample(day.isoformat())
            p['locations'][1]['gold_type'] = []
            return p, pnj.inspect_payload(p, day)
        for _ in range(16):
            out=pnj.backfill(storage,first=date(2016,1,1),last=date(2016,12,31),
                max_days=24,delay=8,fetcher=fetch,sleeper=lambda _:None)
            if out['year_boundary_reached']: break
        y=storage.read(pnj.year_key(2016))
        self.assertEqual(y['scan_status'],'SCANNED_YEAR_TARGET_PRODUCT_GAPS')
        self.assertEqual(y['days_missing_pnj_ring'],366)
        self.assertEqual(y['days_missing_sjc'],0)
        self.assertEqual(y['days_with_both_targets'],0)

    def test_source_response_missing_product_name_holds(self):
        d=sample()
        d['locations'][0]['gold_type'][0].pop('name')
        with self.assertRaisesRegex(pnj.CollectionHold, 'SCHEMA_PRODUCT_NAME_CHANGED'):
            pnj.inspect_payload(d,date(2016,12,15))



    def test_r2_write_requires_verified_readback(self):
        class Fake:
            def __init__(self): self.writes = []
            def put_object(self, **kw): self.writes.append(kw)
        store = pnj.R2.__new__(pnj.R2)
        store.bucket = 'demo'
        store.client = Fake()
        doc={'schema':'demo','status':'ok'}
        store.read = lambda key: doc
        store.write('safe/test.json',doc)
        self.assertEqual(len(store.client.writes),1)
        self.assertEqual(json.loads(store.client.writes[0]['Body']),doc)

    def test_r2_write_detects_corrupted_readback(self):
        class Fake:
            def put_object(self, **kw): pass
        store = pnj.R2.__new__(pnj.R2)
        store.bucket = 'demo'
        store.client = Fake()
        store.read = lambda key: {'status':'CORRUPT'}
        with self.assertRaisesRegex(pnj.CollectionHold,'R2_WRITE_READBACK_MISMATCH'):
            store.write('safe/test.json',{'status':'ok'})

    def test_raw_response_size_cap_free_tier(self):
        self.assertLessEqual(pnj.MAX_BYTES,128_000)


    def test_full_leap_year_in_single_bounded_run(self):
        storage = MemoryR2()
        called = []
        def synthetic(day):
            called.append(day)
            data = sample(day.isoformat())
            return data, pnj.inspect_payload(data, day)
        out = pnj.backfill(storage, first=date(2016,1,1), last=date(2017,1,3),
                           max_days=366, delay=10, fetcher=synthetic,
                           sleeper=lambda _:None)
        self.assertEqual(out['processed'], 366)
        self.assertTrue(out['year_boundary_reached'])
        self.assertEqual(out['next_date'], '2017-01-01')
        self.assertFalse(out['time_budget_reached'])
        self.assertEqual(len(called), 366)
        self.assertEqual(storage.read(pnj.year_key(2016))['days_committed'], 366)
        self.assertEqual(storage.read(pnj.year_key(2016))['scan_status'],
                         'SCANNED_YEAR_TARGETS_OBSERVED_NO_FLAGGED_ROWS')

    def test_graceful_runtime_cap_preserves_cursor(self):
        s = MemoryR2()
        def must_not_fetch(day):
            raise AssertionError('TIME_BUDGET_MUST_SKIP_FETCH')
        with patch.object(pnj.time, 'monotonic',
                          side_effect=[0, pnj.MAX_RUNTIME_SECONDS + 1]):
            out = pnj.backfill(s, first=date(2016,12,15), last=date(2016,12,16),
                               max_days=366, delay=10, fetcher=must_not_fetch)
        self.assertTrue(out['time_budget_reached'])
        self.assertEqual(out['processed'], 0)
        self.assertEqual(out['next_date'], '2016-12-15')
        self.assertIsNone(s.read(pnj.STATE_KEY))

    def test_default_plan_supports_year_without_egress(self):
        with patch.object(pnj,'R2',side_effect=AssertionError('R2 must not be called')):
            with patch.object(pnj,'fetch_day',side_effect=AssertionError('fetch must not be called')):
                self.assertEqual(pnj.main([]),0)

if __name__ == '__main__': unittest.main()
