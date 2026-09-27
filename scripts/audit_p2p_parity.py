"""Bounded read-only parity audit: legacy p2p-data.json vs canonical daily partitions.

Default scope is the latest 30 UTC days present in legacy data. Max 90 days per run.
No R2 writes are performed.
"""
import argparse
import json
from collections import Counter
from datetime import datetime, timezone, timedelta

from fetch_p2p import get_r2, R2_KEY_LEGACY, R2_MANIFEST_KEY, _daily_key
from migrate_p2p_history import legacy_snapshot_to_records


def key(r):
    return (
        int(r["ts"]), r["exchange"], r["asset"], r["side"],
        None if r.get("price") is None else float(r["price"]),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    args = ap.parse_args()
    if args.days < 1 or args.days > 90:
        raise SystemExit("--days phải trong khoảng 1..90")

    r2, bucket = get_r2()
    legacy_obj = r2.get_object(Bucket=bucket, Key=R2_KEY_LEGACY)
    legacy = json.loads(legacy_obj["Body"].read().decode("utf-8")).get("snapshots", [])

    manifest_obj = r2.get_object(Bucket=bucket, Key=R2_MANIFEST_KEY)
    manifest = json.loads(manifest_obj["Body"].read().decode("utf-8"))
    dates = sorted(manifest.get("dates", []))
    if not dates:
        raise SystemExit("manifest không có dates")

    end = datetime.strptime(dates[-1], "%Y-%m-%d").date()
    start = end - timedelta(days=args.days - 1)
    selected = [d for d in dates if start.isoformat() <= d <= end.isoformat()]

    legacy_counter = Counter()
    for snap in legacy:
        try:
            day = datetime.fromtimestamp(snap[0], tz=timezone.utc).date()
        except Exception:
            continue
        if day < start or day > end:
            continue
        for r in legacy_snapshot_to_records(snap):
            legacy_counter[key(r)] += 1

    canonical_counter = Counter()
    missing = []
    for d in selected:
        obj = r2.get_object(Bucket=bucket, Key=_daily_key(d))
        if not obj:
            missing.append(d)
            continue
        data = json.loads(obj["Body"].read().decode("utf-8"))
        for r in data.get("records", []):
            if r.get("record_type") == "price":
                canonical_counter[key(r)] += 1

    legacy_only = legacy_counter - canonical_counter
    canonical_only = canonical_counter - legacy_counter

    print("P2P_PARITY_BEGIN")
    print(f"range={start.isoformat()}..{end.isoformat()}")
    print(f"days={len(selected)} missing_days={len(missing)}")
    print(f"legacy_price_records={sum(legacy_counter.values())}")
    print(f"canonical_price_records={sum(canonical_counter.values())}")
    print(f"legacy_only={sum(legacy_only.values())}")
    print(f"canonical_only={sum(canonical_only.values())}")
    print("P2P_PARITY_END")

    if missing or legacy_only:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
