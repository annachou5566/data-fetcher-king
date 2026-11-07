import unittest
from alpha_events_completion_only_guard import (
    CompletionOnlyHold, enforce_no_unverified_history_admission as guard,
)
CID='ae:'+'a'*24

class CompletionOnlyWriterTests(unittest.TestCase):
    def test_idless_legacy(self): self.assertEqual(guard([{'status':'ended'}],[{'status':'ended'}]),0)
    def test_preserve_450_history_rows(self):
        rows=[{'status':'ended','legacy_index':i} for i in range(450)]
        self.assertEqual(guard(rows,rows),0)
    def test_expired_canonical_rejected(self):
        with self.assertRaisesRegex(CompletionOnlyHold,'NEW_CANONICAL_HISTORY_UNVERIFIED'):
            guard([{'id':'old'}],[{'id':'old'},{'id':CID,'event_time':'2020','status':'ended'}])
    def test_completed_true_is_not_proof(self):
        with self.assertRaisesRegex(CompletionOnlyHold,'NEW_CANONICAL_HISTORY_UNVERIFIED'):
            guard([],[{'id':CID,'status':'ended','completed':True,'terminal_proof_ref':'forged'}])
    def test_existing_canonical_can_be_price_enriched(self):
        self.assertEqual(guard([{'id':CID,'listing_price':1}],[{'id':CID,'listing_price':2}]),1)
    def test_duplicate_id_rejected(self):
        with self.assertRaisesRegex(CompletionOnlyHold,'DUPLICATE_ID'):
            guard([],[{'id':CID},{'id':CID}])
    def test_noncanonical_legacy_out_of_scope(self):
        self.assertEqual(guard([{'id':'older'}],[{'id':'older'},{'id':'new-legacy'}]),0)
    def test_bad_list_rejected(self):
        with self.assertRaisesRegex(CompletionOnlyHold,'NOT_LIST'): guard(None,[])
    def test_bad_row_rejected(self):
        with self.assertRaisesRegex(CompletionOnlyHold,'ROW_INVALID'): guard([None],[])
    def test_reject_without_logging_sensitive_identity(self):
        try: guard([],[{'id':CID}])
        except CompletionOnlyHold as e: self.assertNotIn(CID,str(e))

if __name__=='__main__':
    unittest.main()
