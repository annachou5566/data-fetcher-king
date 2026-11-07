import copy
import unittest
from alpha_events_completion_only_guard import (
    CompletionOnlyHold, enforce_no_unverified_history_admission as guard,
    keep_existing_history_without_unverified_admission as keep,
    ensure_all_contains_existing_history,
)
CID='ae:'+'a'*24

class GuardTests(unittest.TestCase):
    def test_preserve_450_duplicate_idless_price(self):
        old=[{'id':99 if i%3 else None,'event_time':f'2025-{i//30+1:02d}-{i%28+1:02d}',
              'project_name':str(i),'price_snapshot':{'last':i},'status':'ended'} for i in range(450)]
        safe,held=keep(old,old+[{'id':CID,'status':'ended','completed':True}])
        self.assertEqual(safe,old);self.assertEqual(held,1)
    def test_reject_unverified_noncanonical_new_id(self):
        self.assertEqual(keep([{'id':'old'}],[{'id':'old'},{'id':'new'}]),([{'id':'old'}],1))
    def test_reject_new_idless_event(self):
        before=[{'symbol':'X','status':'ended'}]
        self.assertEqual(keep(before,before+[{'symbol':'Y','status':'ended'}]),(before,1))
    def test_reject_new_canonical_even_completed_and_signed_fields(self):
        self.assertEqual(keep([],[{'id':CID,'completed':True,'terminal_proof_ref':'forge'}]),([],1))
    def test_price_update_existing_single_legacy(self):
        old=[{'id':13,'symbol':'BRD','listing_price':{'vwap':2},'status':'ended'}]
        new=[{'id':13,'symbol':'BRD','listing_price':{'vwap':3},'status':'ended'}]
        self.assertEqual(keep(old,new),(new,0))
    def test_price_erasure_rejected_and_previous_retained(self):
        old=[{'id':'old','price_snapshot':{'last':4},'status':'ended'}]
        new=[{'id':'old','price_snapshot':{},'status':'ended'}]
        self.assertEqual(keep(old,new),(old,1))
        with self.assertRaisesRegex(CompletionOnlyHold,'PRICE_ERASURE'):guard(old,new)
    def test_status_changed_rejected(self):
        before=[{'id':'old','status':'ended'}]
        self.assertEqual(keep(before,[{'id':'old','status':'active'}]),(before,1))
    def test_no_row_loss_even_with_incoming_short(self):
        before=[{'id':5,'symbol':'A'},{'id':5,'symbol':'B'}]
        self.assertEqual(keep(before,[before[0]]),(before,0))
        with self.assertRaisesRegex(CompletionOnlyHold,'CARDINALITY'):guard(before,[before[0]])
    def test_no_change_original_array(self):
        before=[{'id':'X','price_snapshot':{'last':4}}];candidate=copy.deepcopy(before)
        safe,_=keep(before,candidate);safe[0]['price_snapshot']['last']=9
        self.assertEqual(before[0]['price_snapshot']['last'],4)
    def test_duplicate_identity_but_different_prices_no_cross_erase(self):
        old=[{'id':3,'symbol':'DUP','price_snapshot':1},{'id':3,'symbol':'DUP','price_snapshot':2}]
        new=[{'id':3,'symbol':'DUP','price_snapshot':99},{'id':3,'symbol':'DUP','price_snapshot':2}]
        self.assertEqual(keep(old,new)[0],old)
        with self.assertRaisesRegex(CompletionOnlyHold,'AMBIGUOUS'):guard(old,new)
    def test_duplicate_exact_price_rows_pass(self):
        r={'id':3,'symbol':'DUP','price_snapshot':1}
        self.assertEqual(guard([r,r],[r,r]),2)
    def test_canonical_price_enrichment_passes(self):
        old=[{'id':CID,'symbol':'BRD','listing_price':1}]
        new=[{'id':CID,'symbol':'BRD','listing_price':2}]
        self.assertEqual(guard(old,new),1)
    def test_legacy_swap_same_size_rejected(self):
        with self.assertRaisesRegex(CompletionOnlyHold,'IDENTITY_CHANGE'):guard([{'id':2}],[{'id':3}])
    def test_missing_keys_invalid(self):
        with self.assertRaisesRegex(CompletionOnlyHold,'NOT_LIST'):guard(None,[])
        with self.assertRaisesRegex(CompletionOnlyHold,'ROW_INVALID'):keep([None],[])
    def test_trailing_new_history_even_if_old_erased_holds(self):
        with self.assertRaisesRegex(CompletionOnlyHold,'IDENTITY_CHANGE'):
            guard([{'id':4},{'id':5}],[{'id':5},{'id':6}])
    def test_mixed_old_new_preserves_old_in_order(self):
        old=[{'id':1,'price_snapshot':1},{'id':2,'price_snapshot':2}]
        new=[{'id':2,'price_snapshot':3},{'id':CID,'completed':True},{'id':1,'price_snapshot':7}]
        self.assertEqual(keep(old,new),([{'id':1,'price_snapshot':7},{'id':2,'price_snapshot':3}],1))


    def test_price_snapshot_cannot_silently_zero(self):
        old=[{'id':'old','price_snapshot':48.7,'status':'ended'}]
        safe,held=keep(old,[{'id':'old','price_snapshot':0,'status':'ended'}])
        self.assertEqual(safe,old);self.assertEqual(held,1)
    def test_nested_listing_price_cannot_zero(self):
        old=[{'id':'old','listing_price':{'vwap':1.4},'status':'ended'}]
        safe,held=keep(old,[{'id':'old','listing_price':{'vwap':0},'status':'ended'}])
        self.assertEqual(safe,old);self.assertEqual(held,1)
    def test_market_cap_zero_is_not_conflated_with_loss(self):
        old=[{'id':'old','market_cap':50000,'status':'ended'}]
        new=[{'id':'old','market_cap':0,'status':'ended'}]
        self.assertEqual(keep(old,new),(new,0))

    def test_all_history_preservation_rejects_missing_archive(self):
        with self.assertRaisesRegex(CompletionOnlyHold,'HISTORY_NOT_REPRESENTED_IN_ALL'):
            ensure_all_contains_existing_history([{'id':'other'}],[{'id':'legacy'}])

    def test_all_history_preservation_allows_price_enrichment(self):
        history=[{'id':3,'symbol':'XYZ','listing_price':{'vwap':2}}]
        all_rows=[{'id':3,'symbol':'XYZ','listing_price':{'vwap':4}}]
        self.assertEqual(ensure_all_contains_existing_history(all_rows, history),1)

    def test_all_history_preservation_rejects_price_erasure(self):
        with self.assertRaisesRegex(CompletionOnlyHold,'ALL_ARCHIVED_PRICE_ERASURE'):
            ensure_all_contains_existing_history([{'id':3,'listing_price':None}],
                [{'id':3,'listing_price':5}])

    def test_all_history_preservation_duplicate_identity_requires_exact_rows(self):
        hist=[{'id':3,'symbol':'X','price_snapshot':1},
              {'id':3,'symbol':'X','price_snapshot':2}]
        self.assertEqual(ensure_all_contains_existing_history(hist,hist),2)
        newer=[dict(hist[0],price_snapshot=9),hist[1]]
        with self.assertRaisesRegex(CompletionOnlyHold,'AMBIGUOUS_ALL_HISTORY_PRICE_PAIR'):
            ensure_all_contains_existing_history(newer,hist)

    def test_all_history_preservation_450_legacy_rows(self):
        old=[{'id':None if i%2 else 7,'event_time':str(i),
              'symbol':'T','price_snapshot':{'last':i}} for i in range(450)]
        self.assertEqual(ensure_all_contains_existing_history(old,old),450)

    def test_price_enrichment_internal_flags_do_not_change_identity(self):
        old=[{'id':42,'event_time':'2026-11-07','price_snapshot':{'last':2},
              'listing_price_unavailable':False,'_vwap_daybound_checked':False}]
        new=[dict(old[0],listing_price_unavailable=True,
                  _vwap_daybound_checked=True,_multiround_checked=True)]
        self.assertEqual(keep(old,new),(new,0))
        self.assertEqual(ensure_all_contains_existing_history(new,old),1)

    def test_event_identity_is_immutable_during_price_enrichment(self):
        old=[{'id':42,'event_time':'2026-11-07'}]
        bad=[{'id':42,'event_time':'2026-11-08'}]
        self.assertEqual(keep(old,bad),(old,1))
        with self.assertRaisesRegex(CompletionOnlyHold,'HISTORY_NOT_REPRESENTED_IN_ALL'):
            ensure_all_contains_existing_history(bad,old)

if __name__=='__main__':unittest.main()
