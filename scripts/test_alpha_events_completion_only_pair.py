"""Offline R18 regression for the existing R2 pair writer: no real R2 access."""
import copy
import hashlib
import io
import json
import unittest
from alpha_events_completion_only_guard import (
    CompletionOnlyHold, ensure_all_contains_existing_history,
    enforce_no_unverified_history_admission, keep_existing_history_without_unverified_admission,
)
from history24h_cutover import snapshot, publish_pair

ALL = 'alpha-events/all.json'
HISTORY = 'alpha-events/history.json'
BUCKET = 'isolated-in-memory-only'

class FakeR2:
    def __init__(self, all_rows, history_rows):
        self.objects = {}
        self.writes = []
        self.fail_history_before_once = False
        self.fail_history_after_once = False
        self.store(ALL, all_rows)
        self.store(HISTORY, history_rows)

    def store(self, key, rows):
        self.objects[key] = json.dumps(rows, ensure_ascii=False, separators=(',', ':')).encode()

    def etag(self, key):
        return '"' + hashlib.md5(self.objects[key]).hexdigest() + '"'

    def get_object(self, *, Bucket, Key):
        assert Bucket == BUCKET
        if Key not in self.objects:
            raise KeyError(Key)
        payload = self.objects[Key]
        return {'ETag':self.etag(Key),'ContentLength':len(payload),'Body':io.BytesIO(payload)}

    def put_object(self, *, Bucket, Key, Body, IfMatch, **kwargs):
        assert Bucket == BUCKET
        if self.etag(Key) != IfMatch:
            raise RuntimeError('R2_PRECONDITION_FAILED')
        if Key == HISTORY and self.fail_history_before_once:
            self.fail_history_before_once = False
            raise RuntimeError('R2_PRECONDITION_FAILED')
        self.objects[Key] = bytes(Body)
        self.writes.append(Key)
        if Key == HISTORY and self.fail_history_after_once:
            self.fail_history_after_once = False
            raise RuntimeError('R2_LOST_ACK_AFTER_COMMIT')
        return {'ETag':self.etag(Key)}

    def rows(self, key):
        return json.loads(self.objects[key])

def propose(r2, update):
    all_rows, all_etag = snapshot(r2, BUCKET, ALL)
    history_before, hist_etag = snapshot(r2, BUCKET, HISTORY)
    proposed = [update(dict(x)) for x in all_rows]
    history_after, held = keep_existing_history_without_unverified_admission(history_before, proposed)
    ensure_all_contains_existing_history(proposed, history_after)
    enforce_no_unverified_history_admission(history_before, history_after)
    return {'all':proposed,'history':history_after,'promoted':[],
            'retire_live':False,'independent_writer':False}, all_etag, hist_etag, held

class ExistingPairTests(unittest.TestCase):
    def setUp(self):
        self.legacy=[{'id':None if i%3==0 else 42,'symbol':'EV'+str(i),
                      'status':'ended','listing_price':i+1} for i in range(450)]
        self.r2=FakeR2(copy.deepcopy(self.legacy),copy.deepcopy(self.legacy))
        self.enrich=lambda x:dict(x,listing_price=x['listing_price']+1)

    def test_all_and_history_cas_readback_preserve_450(self):
        plan,a,h,held=propose(self.r2,self.enrich)
        self.assertEqual(held,0)
        ack=publish_pair(self.r2,BUCKET,plan,a,h)
        self.assertTrue(ack['pair_verified'])
        self.assertFalse(ack['retire_live'])
        self.assertEqual(self.r2.rows(ALL),self.r2.rows(HISTORY))
        self.assertEqual(len(self.r2.rows(HISTORY)),450)

    def test_stale_cas_refuses_overwrite(self):
        plan,a,h,_=propose(self.r2,self.enrich)
        self.r2.store(ALL,self.legacy+[{'id':'different'}])
        with self.assertRaisesRegex(RuntimeError,'PRECONDITION'):
            publish_pair(self.r2,BUCKET,plan,a,h)
        self.assertEqual(self.r2.writes,[])

    def test_missing_history_fails_before_write(self):
        del self.r2.objects[HISTORY]
        with self.assertRaises(KeyError):
            propose(self.r2,self.enrich)
        self.assertEqual(self.r2.writes,[])

    def test_corrupt_history_holds_before_write(self):
        self.r2.objects[HISTORY]=b'not-json'
        with self.assertRaisesRegex(RuntimeError,'INVALID_JSON'):
            propose(self.r2,self.enrich)
        self.assertEqual(self.r2.writes,[])

    def test_unverified_new_row_never_admitted(self):
        cid='ae:'+'a'*24
        plan,a,h,held=propose(self.r2,lambda x:dict(x))
        self.assertEqual(held,0)
        plan['all'].append({'id':cid,'completed':True,'status':'ended'})
        self.assertEqual(len(plan['history']),450)
        publish_pair(self.r2,BUCKET,plan,a,h)
        self.assertEqual(len(self.r2.rows(HISTORY)),450)

    def test_partial_all_success_history_failure_recover_on_retry(self):
        plan,a,h,_=propose(self.r2,self.enrich)
        self.r2.fail_history_before_once=True
        with self.assertRaisesRegex(RuntimeError,'PRECONDITION'):
            publish_pair(self.r2,BUCKET,plan,a,h)
        self.assertEqual(len(self.r2.rows(HISTORY)),450)
        self.assertNotEqual(self.r2.rows(ALL),self.r2.rows(HISTORY))
        retry,ra,rh,_=propose(self.r2,lambda x:x)
        ack=publish_pair(self.r2,BUCKET,retry,ra,rh)
        self.assertTrue(ack['pair_verified'])
        self.assertEqual(self.r2.rows(ALL),self.r2.rows(HISTORY))

    def test_lost_history_ack_recover_idempotently(self):
        plan,a,h,_=propose(self.r2,self.enrich)
        self.r2.fail_history_after_once=True
        with self.assertRaisesRegex(RuntimeError,'LOST_ACK'):
            publish_pair(self.r2,BUCKET,plan,a,h)
        self.assertEqual(self.r2.rows(ALL),self.r2.rows(HISTORY))
        retry,ra,rh,_=propose(self.r2,lambda x:x)
        ack=publish_pair(self.r2,BUCKET,retry,ra,rh)
        self.assertTrue(ack['pair_verified'])
        self.assertEqual(self.r2.rows(ALL),self.r2.rows(HISTORY))

    def test_unverified_missing_legacy_row_holds(self):
        self.r2.store(ALL,self.legacy[:-1])
        with self.assertRaisesRegex(CompletionOnlyHold,'HISTORY_NOT_REPRESENTED_IN_ALL'):
            propose(self.r2,lambda x:x)
        self.assertEqual(self.r2.writes,[])

if __name__=='__main__':unittest.main()
