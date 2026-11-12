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
        for n in [0,25,-1]:
            with self.assertRaises(ValueError): pnj.limit_days(n)
        self.assertEqual(pnj.limit_days(24), 24)
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

if __name__ == '__main__': unittest.main()
