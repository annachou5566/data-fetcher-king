"""Read-only exact Alpha Events Active/History snapshot qualification.

Provide directory containing the six Cloudflare R2 objects named
pending.json, upcoming.json, live.json, public_active.json, all.json,
history.json. No network credentials, no R2 writes, no event mutation.
"""
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

MAX_FILE = 2_000_000
CANON = re.compile(r"^ae:[0-9a-f]{24}$")
ISO = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{3}Z$")


def load(directory, name):
    file = directory / (name.replace("/", "_") + ".json")
    if not file.is_file() or file.stat().st_size > MAX_FILE:
        raise ValueError("UNAVAILABLE_OR_OVERSIZED_" + name)
    rows = json.loads(file.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or len(rows) > 20_000 or any(not isinstance(v, dict) for v in rows):
        raise ValueError("INVALID_ROWS_" + name)
    return rows


def utc_clock(value):
    if not isinstance(value, str) or not ISO.fullmatch(value):
        return None
    try:
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        return dt if dt.isoformat(timespec="milliseconds").replace("+00:00", "Z") == value else None
    except ValueError:
        return None


def indexed(rows):
    out = {}
    for row in rows:
        rid = row.get("id")
        if not isinstance(rid, str) or not CANON.fullmatch(rid):
            continue
        if rid in out:
            raise ValueError("DUPLICATE_CANONICAL_ID")
        out[rid] = row
    return out


def main(folder):
    d = Path(folder)
    keys = ("pending", "upcoming", "live", "public/active", "all", "history")
    data = {k: load(d, k) for k in keys}
    counts = {k: len(v) for k, v in data.items()}
    print("WA_AE_24H_READ_BEGIN")
    print("COUNTS=" + ",".join(f"{k}:{counts[k]}" for k in keys))
    physical = {}
    for bucket in ("pending", "upcoming", "live"):
        for row in data[bucket]:
            rid = row.get("id")
            if not isinstance(rid, str) or not CANON.fullmatch(rid):
                raise ValueError("PHYSICAL_ACTIVE_ID_UNVERIFIED")
            if rid in physical:
                raise ValueError("DUPLICATE_PHYSICAL_MEMBERSHIP")
            physical[rid] = (bucket, row)
    public = indexed(data["public/active"])
    if set(physical) != set(public):
        print("PUBLIC_PHYSICAL_IDENTITY=DIFFERS")
    else:
        print("PUBLIC_PHYSICAL_IDENTITY=MATCH")
    all_rows, hist_rows = indexed(data["all"]), indexed(data["history"])
    now = datetime.now(timezone.utc)
    for rid, row in sorted(public.items()):
        source = physical.get(rid)
        old = all_rows.get(rid)
        hist = hist_rows.get(rid)
        ph = source[1] if source else {}
        clock = utc_clock(ph.get("firstActiveAt"))
        same_clock = clock is not None and ph.get("firstActiveAt") == row.get("firstActiveAt")
        age = round((now - clock).total_seconds() / 3600, 2) if clock else None
        pair = bool(old and hist and old == hist and str(hist.get("status", "")).lower() == "ended")
        eligible = clock is not None and age >= 24
        state = ("CLOCK_UNVERIFIED" if not same_clock else
                 "BEFORE_24H" if not eligible else
                 "HISTORY_PAIR_PRESENT" if pair else "WAIT_HISTORY_PAIR")
        sym = str(row.get("symbol") or "?")[:20]
        when = str(row.get("event_time") or "?")[:35]
        print(f"EVENT={rid} SYMBOL={sym} BUCKET={source[0] if source else 'NONE'} "
              f"EVENT_TIME={when} FIRST_ACTIVE={ph.get('firstActiveAt')} "
              f"AGE_HOURS={age} PAIRED_ENDED={pair} STATE={state}")
    print("WA_AE_24H_READ_END")


if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            raise ValueError("EXPECTED_SNAPSHOT_DIRECTORY")
        main(sys.argv[1])
    except Exception as exc:
        print("WA_AE_24H_HOLD=" + str(exc)[:160])
        sys.exit(2)
