"""One-owner/one-schedule, bounded Alpha Events prospective 24h activation.

Runs ONLY on owner Debian with existing Wrangler R2 read-only snapshots and
an already-authenticated GitHub CLI. No R2/Cloudflare/Oracle mutation here.
The four proof variables are enabled before PROSPECTIVE_SCHEDULE_APPROVED,
which is the only switch changing the existing public writer's next schedule.
Never print credentials, arbitrary GitHub variables, or R2 event raw bodies.
"""
import json
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from history24h_cutover import plan, plan_qualified_active24h

REPO = "annachou5566/data-fetcher-king"
EXPECTED_MAIN = "38caf0554c9aac1050dfc5103ce94441d28f74af"
EXPECTED = {
    "ae:555ce0dafa08d30c1f68446f": ("DNA", "2026-10-15T11:32:09.201Z"),
    "ae:870c0c499b14f0a6d569f67a": ("DRV", "2026-10-14T08:02:38.333Z"),
}
REQUIRED_ON = [
    "WA_ALPHA_EVENTS_HISTORY24H_OWNER_APPROVED",
    "WA_ALPHA_EVENTS_HISTORY24H_SINGLE_WRITER_FENCED",
    "WA_ALPHA_EVENTS_HISTORY24H_MANUAL_SEED_FENCED",
    "WA_ALPHA_EVENTS_HISTORY24H_FREE_HEADROOM_QUALIFIED",
]
REQUIRED_OFF = ["WA_ALPHA_EVENTS_HISTORY24H_SCHEDULE_FENCED"]
PROOFS = [
    "WA_ALPHA_EVENTS_HISTORY24H_ROLLBACK_VERIFIED",
    "WA_ALPHA_EVENTS_HISTORY24H_FIRSTACTIVE_PROOF_QUALIFIED",
    "WA_ALPHA_EVENTS_HISTORY24H_ACTIVE_PUBLISHER_CAS_QUALIFIED",
    "WA_ALPHA_EVENTS_HISTORY24H_ORACLE_REPLAY_QUALIFIED",
]
ACTIVATION = "WA_ALPHA_EVENTS_HISTORY24H_PROSPECTIVE_SCHEDULE_APPROVED"
ALLOW = set(REQUIRED_ON + REQUIRED_OFF + PROOFS + [ACTIVATION])
UTC = timezone.utc
ISO = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{3}Z$")


def hold(label):
    raise RuntimeError("WA_AE24_HOLD_" + label)


def get_array(directory, key):
    file = directory / (key.replace("/", "_") + ".json")
    if not file.is_file() or file.stat().st_size > 2_000_000:
        hold("R2_READ_" + key.replace("/", "_"))
    rows = json.loads(file.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        hold("R2_ROWS_" + key.replace("/", "_"))
    return rows


def snapshot_gate(directory):
    data = {k: get_array(directory, k)
            for k in ("pending", "upcoming", "live", "public/active", "all", "history")}
    active = data["pending"] + data["upcoming"] + data["live"]
    if len(data["all"]) != 448 or len(data["history"]) != 448:
        hold("HISTORICAL_COUNT_MOVED")
    if len(active) != 2 or len(data["public/active"]) != 2:
        hold("ACTIVE_SET_MOVED")
    index = {}
    for row in active:
        rid = row.get("id")
        if rid not in EXPECTED or rid in index:
            hold("ACTIVE_IDENTITY_CHANGED")
        symbol, stamp = EXPECTED[rid]
        if row.get("symbol") != symbol or row.get("firstActiveAt") != stamp:
            hold("ACTIVE_CLOCK_CHANGED")
        if not ISO.fullmatch(stamp):
            hold("CLOCK_FORMAT")
        initial = datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
        if datetime.now(UTC) < initial + timedelta(hours=24):
            hold("NOT_YET_24H")
        index[rid] = row
    pub = {row.get("id"): row for row in data["public/active"]}
    if set(pub) != set(index):
        hold("PUBLIC_ACTIVE_MISMATCH")
    for rid, row in pub.items():
        if row.get("firstActiveAt") != index[rid]["firstActiveAt"]:
            hold("PUBLIC_CLOCK_MISMATCH")
    now = datetime.now(UTC)
    previous = plan(data["all"], data["history"], [], now)
    result = plan_qualified_active24h(previous, active, now)
    if (set(result.get("pending24h_promoted", [])) != set(EXPECTED)
            or len(result["history"]) != 450
            or len(result["all"]) != 450
            or result["history"][:448] != data["history"]
            or result["all"][:448] != data["all"]
            or result.get("retire_live") is not False
            or result.get("retire_pending") is not False
            or result.get("retire_public_active") is not False):
        hold("PURE_HISTORY_PLAN_FAILED")
    for rid in EXPECTED:
        a = next(row for row in result["all"] if row.get("id") == rid)
        h = next(row for row in result["history"] if row.get("id") == rid)
        if a != h or h.get("status") != "ended" or h.get("firstActiveAt") != EXPECTED[rid][1]:
            hold("HISTORY_PAIR_INVALID")
    print("LIVE_SNAPSHOT_PLAN=PASS")
    print("PROPOSED_HISTORY=448_TO_450")
    print("PROMOTED_IDS=DNA,DRV")
    print("NO_R2_WRITES=TRUE")


def gh(*args):
    proc = subprocess.run(["gh", "api", *args], text=True, capture_output=True, timeout=25)
    if proc.returncode:
        # Never echo GitHub's error output, request headers or auth data.
        hold("GITHUB_API_OR_AUTH_RC_" + str(proc.returncode))
    return proc.stdout


def read_vars():
    payload = json.loads(gh(f"repos/{REPO}/actions/variables?per_page=100"))
    entries = payload.get("variables")
    if not isinstance(entries, list) or len(entries) > 100 or payload.get("total_count", 0) > 100:
        hold("VARS_PAGINATION")
    found = {}
    for v in entries:
        name = v.get("name")
        if name in ALLOW:
            found[name] = str(v.get("value", ""))
    return found


def check_github_baseline(vars_map):
    for name in REQUIRED_ON:
        if vars_map.get(name) != "1":
            hold("PREAPPROVED_GATE_OFF_" + name)
    for name in REQUIRED_OFF:
        if vars_map.get(name, "0") not in ("", "0"):
            hold("SCHEDULE_NOT_FENCED_GATE")
    if vars_map.get(ACTIVATION, "0") not in ("", "0"):
        hold("ACTIVATION_ALREADY_ON")
    for name in PROOFS:
        if vars_map.get(name, "0") not in ("", "0", "1"):
            hold("INVALID_PROOF_VALUE")


def update_one(name, previous):
    if name not in ALLOW:
        hold("UNALLOWLISTED_VARIABLE")
    if name in previous:
        gh("-X", "PATCH", f"repos/{REPO}/actions/variables/{name}", "-f", "value=1")
    else:
        gh("-X", "POST", f"repos/{REPO}/actions/variables", "-f", f"name={name}", "-f", "value=1")
    readback = read_vars()
    if readback.get(name) != "1":
        hold("POSTCHECK_" + name)
    return readback


def main():
    print("WA_AE24_ACTIVATION_BEGIN", flush=True)
    if len(sys.argv) != 3 or sys.argv[1] not in ("preflight", "activate"):
        hold("ARGS_USE_PREFLIGHT_OR_ACTIVATE_AND_R2_DIR")
    mode, directory = sys.argv[1], Path(sys.argv[2])
    snapshot_gate(directory)
    exact = json.loads(gh(f"repos/{REPO}/commits/main"))
    if exact.get("sha") != EXPECTED_MAIN:
        hold("MAIN_SHA_MOVED")
    print("WRITER_MAIN=PINNED")
    flags = read_vars()
    check_github_baseline(flags)
    print("GITHUB_PREREQUISITES=PASS")
    if mode == "preflight":
        print("NO_GITHUB_VAR_WRITES=TRUE")
    else:
        for gate in PROOFS:
            if flags.get(gate) != "1":
                flags = update_one(gate, flags)
            print(gate + "=ON", flush=True)
        if any(flags.get(k) != "1" for k in PROOFS):
            hold("PROOF_GATE_POSTCHECK")
        # Only this last update changes the next scheduled writer invocation.
        flags = update_one(ACTIVATION, flags)
        print(ACTIVATION + "=ON", flush=True)
        print("GITHUB_ACTIVATION_GATE=PASS", flush=True)
        print("AWAIT_NATURAL_SCHEDULE_AND_ORACLE=TRUE")
    print("WA_AE24_ACTIVATION_END", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc)[:180], flush=True)
        print("WA_AE24_ACTIVATION_END", flush=True)
        sys.exit(2)
