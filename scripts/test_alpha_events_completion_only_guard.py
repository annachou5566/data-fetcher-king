import unittest
from alpha_events_completion_only_guard import (
    CompletionOnlyHold,
    enforce_no_unverified_history_admission as guard,
    keep_existing_history_without_unverified_admission as filter_rows,
)
CID = 'ae:' + 'a' * 24
CID2 = 'ae:' + 'b' * 24


class CompletionOnlyWriterTests(unittest.TestCase):
    def test_idless_legacy(self):
        self.assertEqual(guard([{'status': 'ended'}], [{'status': 'ended'}]), 0)

    def test_preserve_450_history_rows(self):
        rows = [{'status': 'ended', 'legacy_index': i, 'price_snapshot': i}
                for i in range(450)]
        result, held = filter_rows(rows, rows + [{'id': CID, 'completed': True}])
        self.assertEqual(result, rows)
        self.assertEqual(held, 1)

    def test_expired_canonical_rejected_in_cutover(self):
        with self.assertRaisesRegex(CompletionOnlyHold, 'NEW_CANONICAL_HISTORY_UNVERIFIED'):
            guard([{'id': 'old'}], [{'id': 'old'}, {'id': CID, 'event_time': '2020', 'status': 'ended'}])

    def test_completed_true_is_not_proof(self):
        safe, held = filter_rows([], [{'id': CID, 'status': 'ended', 'completed': True,
                                      'terminal_proof_ref': 'forged'}])
        self.assertEqual(safe, [])
        self.assertEqual(held, 1)

    def test_existing_canonical_can_be_price_enriched(self):
        safe, held = filter_rows([{'id': CID, 'listing_price': 1}],
                                 [{'id': CID, 'listing_price': 2}])
        self.assertEqual(safe[0]['listing_price'], 2)
        self.assertEqual(held, 0)

    def test_duplicate_id_rejected(self):
        with self.assertRaisesRegex(CompletionOnlyHold, 'DUPLICATE_ID'):
            filter_rows([], [{'id': CID}, {'id': CID}])

    def test_noncanonical_legacy_out_of_scope(self):
        self.assertEqual(guard([{'id': 'older'}], [{'id': 'older'}, {'id': 'new-legacy'}]), 0)

    def test_bad_list_rejected(self):
        with self.assertRaisesRegex(CompletionOnlyHold, 'NOT_LIST'):
            filter_rows(None, [])

    def test_bad_row_rejected(self):
        with self.assertRaisesRegex(CompletionOnlyHold, 'ROW_INVALID'):
            filter_rows([None], [])

    def test_reject_without_logging_sensitive_identity(self):
        try:
            guard([], [{'id': CID}])
        except CompletionOnlyHold as e:
            self.assertNotIn(CID, str(e))

    def test_shrunken_history_holds(self):
        with self.assertRaisesRegex(CompletionOnlyHold, 'HISTORY_SHRANK'):
            filter_rows([{'legacy_index': 1}], [])

    def test_archived_canonical_removal_holds(self):
        with self.assertRaisesRegex(CompletionOnlyHold, 'ARCHIVED_CANONICAL_LOST'):
            filter_rows([{'id': CID}], [{'id': CID2}])

    def test_mixed_new_and_existing(self):
        safe, held = filter_rows([{'id': CID}], [{'id': CID, 'price_snapshot': 30}, {'id': CID2}])
        self.assertEqual(safe, [{'id': CID, 'price_snapshot': 30}])
        self.assertEqual(held, 1)

    def test_multiple_unverified_held(self):
        safe, held = filter_rows([{'id': 'legacy'}], [{'id': 'legacy'}, {'id': CID}, {'id': CID2}])
        self.assertEqual(held, 2)
        self.assertEqual(safe, [{'id': 'legacy'}])


if __name__ == '__main__':
    unittest.main()
