"""Bounded source regression for the existing writer's prospective 24h schedule.

No network, R2, credentials or database writes. Run with Python stdlib unittest.
"""
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from history24h_cutover import plan, plan_qualified_active24h

ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = (ROOT / ".github/workflows/sync_listing_prices.yml").read_text()
PRICE_SOURCE = (ROOT / "scripts/sync_listing_prices.py").read_text()
CID = "ae:0123456789abcdef01234567"
START = "2026-10-16T07:41:00.000Z"
EV = {
    "id": CID,
    "symbol": "SYN",
    "event_time": "2026-10-14T10:00:00.000Z",
    "firstActiveAt": START,
    "status": "PREDICTED",
    "event_type": "airdrop",
}
LEGACY = {
    "id": "old-legacy",
    "symbol": "OLD",
    "event_time": "2025-01-01T00:00:00.000Z",
    "status": "ended",
    "listing_price": {"vwap": 10},
}
FIRST = datetime(2026, 10, 16, 7, 41, tzinfo=timezone.utc)


def scheduled_plan(now, active=None, old_all=None, old_history=None):
    all_rows = [LEGACY] if old_all is None else old_all
    hist_rows = [LEGACY] if old_history is None else old_history
    base = plan(all_rows, hist_rows, [], now)
    return plan_qualified_active24h(base, [EV] if active is None else active, now)


class ProspectiveScheduledHistoryGate(unittest.TestCase):
    def test_workflow_schedule_requires_explicit_opt_in(self):
        self.assertIn(
            "github.event_name == 'schedule' && vars.WA_ALPHA_EVENTS_HISTORY24H_PROSPECTIVE_SCHEDULE_APPROVED == '1'",
            WORKFLOW,
        )
        self.assertIn("WA_ALPHA_EVENTS_HISTORY24H_ACTIVE24H_ENABLED:", WORKFLOW)
        self.assertIn("WA_ALPHA_EVENTS_HISTORY24H_FIRSTACTIVE_PROOF_QUALIFIED:", WORKFLOW)
        self.assertIn("WA_ALPHA_EVENTS_HISTORY24H_ACTIVE_PUBLISHER_CAS_QUALIFIED:", WORKFLOW)
        self.assertIn("WA_ALPHA_EVENTS_HISTORY24H_ORACLE_REPLAY_QUALIFIED:", WORKFLOW)

    def test_first_active_mode_does_not_forge_live_proof(self):
        self.assertIn("if legacy_pending_enabled or pending24h_enabled or official1h_enabled:", PRICE_SOURCE)
        self.assertIn('if x != "WA_ALPHA_EVENTS_HISTORY24H_FIRSTLIVE_PROOF_QUALIFIED"', PRICE_SOURCE)
        self.assertIn("WA_ALPHA_EVENTS_HISTORY24H_FIRSTACTIVE_PROOF_QUALIFIED", PRICE_SOURCE)
        self.assertIn("WA_ALPHA_EVENTS_HISTORY24H_ACTIVE_PUBLISHER_CAS_QUALIFIED", PRICE_SOURCE)
        self.assertIn("WA_ALPHA_EVENTS_HISTORY24H_ORACLE_REPLAY_QUALIFIED", PRICE_SOURCE)

    def test_expired_event_time_does_not_override_recent_first_active(self):
        result = scheduled_plan(FIRST + timedelta(hours=23, minutes=59))
        self.assertEqual(result["pending24h_promoted"], [])
        self.assertEqual(result["history"], [LEGACY])

    def test_exact_first_active_deadline_commits_history_pair_plan(self):
        result = scheduled_plan(FIRST + timedelta(days=1))
        self.assertEqual(result["pending24h_promoted"], [CID])
        self.assertEqual(result["all"][-1], result["history"][-1])
        self.assertEqual(result["history"][-1]["status"], "ended")
        self.assertEqual(result["history"][-1]["firstActiveAt"], START)
        self.assertEqual(result["history"][0], LEGACY)
        self.assertEqual(result["retire_live"], False)
        self.assertEqual(result["retire_pending"], False)
        self.assertEqual(result["retire_public_active"], False)

    def test_replay_preserves_same_pair_and_prices(self):
        once = scheduled_plan(FIRST + timedelta(days=1))
        twice = scheduled_plan(
            FIRST + timedelta(days=2),
            old_all=once["all"],
            old_history=once["history"],
        )
        self.assertEqual(twice["all"], once["all"])
        self.assertEqual(twice["history"], once["history"])
        self.assertEqual(twice["history"][0]["listing_price"]["vwap"], 10)

    def test_missing_first_active_is_never_inferred(self):
        no_clock = {k: v for k, v in EV.items() if k != "firstActiveAt"}
        outcome = scheduled_plan(FIRST + timedelta(days=10), active=[no_clock])
        self.assertEqual(outcome["pending24h_promoted"], [])
        self.assertEqual(outcome["history"], [LEGACY])


if __name__ == "__main__":
    unittest.main()
