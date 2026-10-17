"""Offline-only source regression: no credentials, R2, DB, deployment or writes."""
import copy
import unittest
from datetime import datetime, timedelta, timezone

from history24h_cutover import plan
from alpha_events_official_onehour import (
    OfficialConfirmationHold, plan_qualified_official_onehour,
)

UTC = timezone.utc
ID = "ae:0123456789abcdef01234567"
T = datetime(2026, 10, 17, 1, 0, 5, tzinfo=UTC)
SOURCE = "telegram:binance_wallet_announcements:1866"
DIGEST = "a" * 64
BASE = {
    "id": ID, "symbol": "ABC", "event_time": "2026-10-17T03:00:00.000Z",
    "status": "PREDICTED", "firstActiveAt": "2026-10-15T00:00:00.000Z",
}
PROOF = {
    "receiptId": 600, "receiptKind": "BINANCE_OFFICIAL_OBSERVATION",
    "sourceClass": "APPROVED_BINANCE_OFFICIAL_RECEIPT",
    "binding": {
        "relevant": True, "relevanceReason": "MATCH",
        "eventType": "AIRDROP", "sourceKind": "BINANCE_WALLET",
        "sourceChannel": "binance_wallet_announcements",
        "sourceMessageId": 1866, "sourceIdentity": SOURCE,
        "sourceDigest": DIGEST, "sourcePublishedAt": "2026-10-17T01:00:00.000Z",
        "observedAt": "2026-10-17T01:00:04.000Z",
    },
    "verifiedSubject": {
        "productFamily": "ALPHA_EVENTS", "competitionExcluded": True,
        "canonicalId": ID, "symbol": "ABC",
        "sourceIdentity": SOURCE, "sourceDigest": DIGEST,
        "confirmedAction": "AIRDROP", "noticeStage": "CONFIRMED",
        "preAnnouncement": False,
    }
}
LEGACY = {"id": "old-legacy", "symbol": "OLD", "status": "ended",
          "listing_price": {"vwap": 1.2}}


def make_run(now=T, row=None, prior_all=None, prior_history=None):
    allrows = [LEGACY] if prior_all is None else prior_all
    histrows = [LEGACY] if prior_history is None else prior_history
    return plan_qualified_official_onehour(
        plan(allrows, histrows, [], now),
        [copy.deepcopy(row if row is not None else BASE)], now,
    )


class OfficialOneHour(unittest.TestCase):
    def test_five_day_old_prediction_never_archives(self):
        result = make_run(T + timedelta(days=5))
        self.assertEqual(result["official_promoted"], [])
        self.assertEqual(result["history"], [LEGACY])

    def test_correct_signed_confirmation_archives_immediately(self):
        row = {**BASE, "officialConfirmation": copy.deepcopy(PROOF)}
        result = make_run(row=row)
        self.assertEqual(result["official_promoted"], [ID])
        self.assertEqual(result["official_overdue"], [])
        self.assertEqual(result["all"][1], result["history"][1])
        self.assertEqual(result["history"][1]["status"], "ended")
        self.assertEqual(result["history"][1]["officialConfirmation"], PROOF)
        self.assertEqual(result["history"][0], LEGACY)
        self.assertIs(result["retire_live"], False)
        self.assertIs(result["retire_pending"], False)
        self.assertIs(result["retire_public_active"], False)

    def test_60_minutes_is_deadline_not_required_wait(self):
        row = {**BASE, "officialConfirmation": copy.deepcopy(PROOF)}
        a = make_run(now=T + timedelta(minutes=59), row=row)
        b = make_run(now=T + timedelta(hours=1), row=row)
        self.assertEqual(a["official_overdue"], [])
        self.assertEqual(b["official_overdue"], [ID])
        self.assertEqual(b["official_promoted"], [ID])

    def test_external_advisory_cannot_qualify(self):
        proof = copy.deepcopy(PROOF)
        proof["sourceClass"] = "APPROVED_EXTERNAL_ADVISORY_RECEIPT"
        with self.assertRaises(OfficialConfirmationHold):
            make_run(row={**BASE, "officialConfirmation": proof})

    def test_teaser_and_wrong_token_fail_closed(self):
        for field, val in [("preAnnouncement", True), ("noticeStage", "PREPARATION"),
                           ("symbol", "XYZ"), ("canonicalId", "ae:" + "f"*24),
                           ("sourceDigest", "b"*64)]:
            proof = copy.deepcopy(PROOF)
            proof["verifiedSubject"][field] = val
            with self.assertRaises(OfficialConfirmationHold, msg=field):
                make_run(row={**BASE, "officialConfirmation": proof})

    def test_competition_is_never_alpha_events_history(self):
        for key, value in [
            ("productFamily", "ALPHA_COMPETITION"),
            ("productFamily", None),
            ("competitionExcluded", False),
            ("competitionExcluded", None),
        ]:
            proof = copy.deepcopy(PROOF)
            proof["verifiedSubject"][key] = value
            with self.assertRaises(OfficialConfirmationHold, msg=key):
                make_run(row={**BASE, "officialConfirmation": proof})

    def test_bad_message_identity_and_clock_fail(self):
        for field, val in [("sourceMessageId", 1867),
                           ("sourcePublishedAt", "2026-10-17T01:02:00.000Z"),
                           ("observedAt", "2026-10-17T00:59:59.000Z")]:
            proof = copy.deepcopy(PROOF)
            proof["binding"][field] = val
            with self.assertRaises(OfficialConfirmationHold, msg=field):
                make_run(row={**BASE, "officialConfirmation": proof})

    def test_explicit_listing_tge_only_when_source_type_generic(self):
        for action in ("LISTING", "TGE"):
            proof = copy.deepcopy(PROOF)
            proof["binding"]["eventType"] = "ALPHA_EVENT"
            proof["verifiedSubject"]["confirmedAction"] = action
            self.assertEqual(
                make_run(row={**BASE, "officialConfirmation": proof})["official_promoted"],
                [ID],
            )

    def test_history_pair_replay_is_stable(self):
        row = {**BASE, "officialConfirmation": copy.deepcopy(PROOF)}
        first = make_run(row=row)
        second = make_run(now=T + timedelta(days=1), row=row,
                          prior_all=first["all"], prior_history=first["history"])
        self.assertEqual(second["all"], first["all"])
        self.assertEqual(second["history"], first["history"])
        self.assertEqual(second["history"][0]["listing_price"], LEGACY["listing_price"])

    def test_conflicting_existing_immutable_proof_fails(self):
        row = {**BASE, "officialConfirmation": copy.deepcopy(PROOF)}
        old = {**BASE, "officialConfirmation": copy.deepcopy(PROOF)}
        old["officialConfirmation"]["binding"]["sourceMessageId"] = 1999
        with self.assertRaises(OfficialConfirmationHold):
            make_run(row=row, prior_all=[LEGACY, old], prior_history=[LEGACY])



class SingleWriterWiring(unittest.TestCase):
    def test_official_mode_always_requires_separate_gate(self):
        from pathlib import Path
        p = Path(__file__).parent
        source = (p / "sync_listing_prices.py").read_text(encoding="utf-8")
        workflow = (p.parent / ".github/workflows/sync_listing_prices.yml").read_text(encoding="utf-8")
        self.assertIn('os.getenv("WA_ALPHA_EVENTS_OFFICIAL1H_ENABLED", "0") == "1"', source)
        self.assertIn('if official1h_enabled and not history24h_enabled:', source)
        self.assertIn("WA_AE_OFFICIAL1H_HOLD_MUTUALLY_EXCLUSIVE_HISTORY_MODES", source)
        self.assertIn("WA_AE_OFFICIAL1H_HOLD_PROOF_GATES_INCOMPLETE", source)
        self.assertIn("proposed = history_plan_official_onehour(", source)
        self.assertIn("WA_ALPHA_EVENTS_OFFICIAL1H_APPROVED == '1'", workflow)
        self.assertIn("WA_ALPHA_EVENTS_OFFICIAL1H_SIGNED_PUBLISHER_QUALIFIED", workflow)
        self.assertEqual(workflow.count("cron:"), 1)
        self.assertEqual(workflow.count("group: sync-listing-prices"), 1)

if __name__ == "__main__":
    unittest.main()
