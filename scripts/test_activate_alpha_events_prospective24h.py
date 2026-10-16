"""Synthetic-only guard tests; no GitHub requests, no R2 writes."""
import contextlib
import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import activate_alpha_events_prospective24h as a


def snapshot(path):
    legacy = [{"id": f"legacy-{i}", "symbol": "OLD", "status": "ended"}
              for i in range(448)]
    live = [{"id": rid, "symbol": sym, "event_type": "airdrop",
             "event_time": "2026-10-14T10:00:00.000Z",
             "firstActiveAt": stamp, "status": "PREDICTED"}
            for rid, (sym, stamp) in a.EXPECTED.items()]
    rows = {"pending": live, "upcoming": [], "live": [],
            "public/active": live, "all": legacy, "history": legacy}
    for key, payload in rows.items():
        (path / (key.replace("/", "_") + ".json")).write_text(
            json.dumps(payload), encoding="utf-8")


class SafeActivation(unittest.TestCase):
    def test_exact_snapshot_uses_existing_448_and_proposes_450(self):
        with tempfile.TemporaryDirectory() as t:
            snapshot(Path(t))
            with contextlib.redirect_stdout(io.StringIO()) as output:
                a.snapshot_gate(Path(t))
            self.assertIn("LIVE_SNAPSHOT_PLAN=PASS", output.getvalue())
            self.assertIn("PROPOSED_HISTORY=448_TO_450", output.getvalue())

    def test_no_false_trust_when_one_first_clock_different(self):
        with tempfile.TemporaryDirectory() as t:
            d = Path(t)
            snapshot(d)
            pending = json.loads((d / "pending.json").read_text())
            pending[0]["firstActiveAt"] = "2026-10-16T12:00:00.000Z"
            (d / "pending.json").write_text(json.dumps(pending))
            with self.assertRaisesRegex(RuntimeError, "ACTIVE_CLOCK_CHANGED"):
                a.snapshot_gate(d)

    def test_required_repository_guards_fail_closed(self):
        flags = {k: "1" for k in a.REQUIRED_ON}
        flags.update({k: "0" for k in a.REQUIRED_OFF})
        a.check_github_baseline(flags)
        flags[a.ACTIVATION] = "1"
        with self.assertRaisesRegex(RuntimeError, "ACTIVATION_ALREADY_ON"):
            a.check_github_baseline(flags)

    def test_activation_flag_written_last(self):
        with tempfile.TemporaryDirectory() as t:
            d = Path(t)
            snapshot(d)
            flags = {k: "1" for k in a.REQUIRED_ON}
            flags.update({k: "0" for k in a.REQUIRED_OFF})
            writes = []

            def mockgh(*args):
                if args == (f"repos/{a.REPO}/commits/main",):
                    return json.dumps({"sha": a.EXPECTED_MAIN})
                if args == (f"repos/{a.REPO}/actions/variables?per_page=100",):
                    return json.dumps({"total_count": len(flags),
                                       "variables": [{"name": k, "value": v}
                                                     for k, v in flags.items()]})
                if len(args) >= 3 and args[:2] == ("-X", "POST"):
                    name = next(x[5:] for x in args if x.startswith("name="))
                    flags[name] = "1"
                    writes.append(name)
                    return ""
                if len(args) >= 3 and args[:2] == ("-X", "PATCH"):
                    name = args[2].split("/")[-1]
                    flags[name] = "1"
                    writes.append(name)
                    return ""
                raise AssertionError("unexpected REST request")

            with patch.object(a, "gh", side_effect=mockgh), patch.object(
                a.sys, "argv", ["script", "activate", str(d)]
            ), contextlib.redirect_stdout(io.StringIO()) as output:
                a.main()
            self.assertEqual(writes, [*a.PROOFS, a.ACTIVATION])
            self.assertIn("GITHUB_ACTIVATION_GATE=PASS", output.getvalue())


if __name__ == "__main__":
    unittest.main()
