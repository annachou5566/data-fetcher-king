"""Offline deterministic regression for Alpha Events event-anchored price repair."""
import ast
import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest

from alpha_events_price_integrity import (
    RETRY_FIELD, anchor_is_before_event, has_qualified_peak,
    candidate_is_event_anchored, select_price_integrity_retries,
    apply_peak_only, snooze_unavailable_price,
)

NOW = datetime(2026, 11, 7, 12, 0, tzinfo=timezone.utc)
EVENT = "2026-10-19T13:00:00.000Z"
EVENT_MS = 1792414800000


def priced(**overrides):
    row = {
        "id": "ae:65f699d269475e69519b81da", "symbol": "CT",
        "contract_address": "0x0a092e544da31150b439a1aaa1a3a2214a867f46",
        "event_time": EVENT,
        "listing_price": {"vwap": 0.331516, "date": "2026-10-19"},
    }
    row.update(overrides)
    return row


def candidate(**overrides):
    result = {
        "vwap": 0.419, "date": "2026-10-19",
        "event_start_ms": EVENT_MS,
        "max_since": {"price": 0.500, "date": "2026-10-23"},
    }
    result.update(overrides)
    return result


class PriceIntegrity(unittest.TestCase):
    def test_missing_peak_selected_once(self):
        row = priced()
        self.assertEqual(select_price_integrity_retries([row], NOW),
                         {"anchor": 0, "peak": 1})
        self.assertEqual(row["_price_integrity_mode"], "peak")

    def test_peak_overlay_never_changes_claim_or_event_identity(self):
        row = priced()
        before = copy.deepcopy(row)
        self.assertTrue(apply_peak_only(row, candidate()))
        self.assertEqual(row["listing_price"]["vwap"], before["listing_price"]["vwap"])
        self.assertEqual(row["listing_price"]["date"], before["listing_price"]["date"])
        self.assertEqual(row["event_time"], before["event_time"])
        self.assertEqual(row["listing_price"]["max_since"]["price"], 0.5)

    def test_missing_or_pre_event_peak_rejected(self):
        row = priced()
        self.assertFalse(apply_peak_only(row, candidate(max_since=None)))
        self.assertFalse(apply_peak_only(row, candidate(max_since={"price": 9, "date": "2026-10-18"})))
        self.assertNotIn("max_since", row["listing_price"])

    def test_stale_listing_day_triggers_anchor_reprice(self):
        row = priced(listing_price={"vwap": 0.079583, "date": "2025-04-24"})
        self.assertTrue(anchor_is_before_event(row))
        self.assertEqual(select_price_integrity_retries([row], NOW),
                         {"anchor": 1, "peak": 0})
        self.assertTrue(row["_vwap_daybound_recompute"])

    def test_wrong_day_or_wrong_event_timestamp_is_never_accepted(self):
        row = priced()
        self.assertFalse(candidate_is_event_anchored(row, candidate(date="2026-10-18")))
        self.assertFalse(candidate_is_event_anchored(row, candidate(event_start_ms=EVENT_MS-60000)))
        self.assertFalse(candidate_is_event_anchored(row, candidate(max_since={"price": 1, "date": "2026-10-18"})))
        self.assertTrue(candidate_is_event_anchored(row, candidate()))

    def test_failed_provider_retry_is_bounded_to_24_hours(self):
        row = priced()
        snooze_unavailable_price(row, NOW)
        self.assertIn(RETRY_FIELD, row["listing_price"])
        self.assertEqual(select_price_integrity_retries([row], NOW+timedelta(hours=23)), {"anchor": 0, "peak": 0})
        self.assertEqual(select_price_integrity_retries([row], NOW+timedelta(hours=24)), {"anchor": 0, "peak": 1})

    def test_one_token_many_campaigns_never_share_event_peak(self):
        p1 = priced()
        p2 = priced(
            id="ae:111111111111111111111111",
            event_time="2026-10-22T11:00:00Z",
            listing_price={"vwap": 0.22, "date": "2026-10-22", "max_since": {"price": 0.3, "date": "2026-10-23"}},
        )
        self.assertEqual(select_price_integrity_retries([p1, p2], NOW),
                         {"anchor": 0, "peak": 1})
        self.assertNotIn("_price_integrity_mode", p2)
        self.assertFalse(apply_peak_only(p2, candidate()))

    def test_existing_qualified_peak_untouched(self):
        row = priced(listing_price={
            "vwap": 0.3, "date": "2026-10-19",
            "max_since": {"price": 0.35, "date": "2026-10-23"},
        })
        self.assertTrue(has_qualified_peak(row))
        self.assertEqual(select_price_integrity_retries([row], NOW), {"anchor": 0, "peak": 0})

    def test_missing_event_or_claim_cannot_be_fabricated(self):
        row = priced(listing_price=None)
        missing_clock = priced(event_time=None)
        self.assertEqual(select_price_integrity_retries([row, missing_clock], NOW), {"anchor": 0, "peak": 0})
        self.assertFalse(candidate_is_event_anchored(missing_clock, candidate()))

    def test_source_syntax_and_writer_integration(self):
        source = Path(__file__).with_name("sync_listing_prices.py").read_text(encoding="utf-8")
        ast.parse(source)
        self.assertIn("select_price_integrity_retries(all_events, limit=8)", source)
        self.assertIn("apply_peak_only(e, result)", source)
        self.assertIn("candidate_is_event_anchored(e, result)", source)
        self.assertIn('e.pop("_price_integrity_mode", None)', source)
        self.assertIn('snooze_unavailable_price(e)', source)

    def test_bounded_retry_batch_only_selects_eight(self):
        rows = [priced(id=f"ae:{i:024x}") for i in range(16)]
        selected = select_price_integrity_retries(rows, NOW, limit=8)
        self.assertEqual(selected, {"anchor": 0, "peak": 8})
        self.assertEqual(sum(x.get("_price_integrity_mode") == "peak" for x in rows), 8)

    def test_peak_from_different_claim_day_does_not_change_last_good(self):
        row = priced()
        before = copy.deepcopy(row)
        self.assertFalse(apply_peak_only(row, candidate(date="2026-10-20")))
        self.assertEqual(row, before)

    def test_manual_reset_is_not_destructive_and_slx_not_pinned(self):
        source = Path(__file__).with_name("sync_listing_prices.py").read_text(encoding="utf-8")
        module = ast.parse(source)
        f = next(node for node in module.body if
                 isinstance(node, ast.FunctionDef) and node.name == "invalidate_manual_reset_prices")
        body = ast.get_source_segment(source, f)
        self.assertNotIn('e["listing_price"] = None', body)
        self.assertNotIn('e["spot_listing_price"] = None', body)
        self.assertIn("MANUAL_RESET_PRICES = set()", source)

    def test_existing_writer_not_expanded(self):
        workflow = Path(__file__).resolve().parents[1] / ".github/workflows/sync_listing_prices.yml"
        body = workflow.read_text(encoding="utf-8")
        self.assertIn("group: sync-listing-prices", body)
        self.assertIn("workflow_dispatch:", body)
        self.assertNotIn("\n  push:", body)
        self.assertEqual(body.count("cron:"), 1)
        self.assertIn("cron: '*/30 * * * *'", body)


if __name__ == "__main__":
    unittest.main()
