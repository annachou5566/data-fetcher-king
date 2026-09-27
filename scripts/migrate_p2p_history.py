"""Bounded idempotent P2P legacy -> canonical daily partition migration.

Reads p2p-data.json and merges only missing price records into
p2p-snapshots/YYYY-MM-DD.json. Existing canonical records always win.

Safety:
- supports legacy v1 (5 fields) and v2 (9 fields);
- dedupe key includes record_type so liquidity cannot mask a missing price;
- only NoSuchKey may initialise a daily object/manifest;
- days with zero additions are never rewritten;
- manifest is rewritten only when its date set actually changes;
- optional --from-date/--to-date bounds remediation;
- --dry-run performs no R2 writes.
"""

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone

from fetch_p2p import (
    get_r2, build_long_records, _daily_key,
    R2_KEY_LEGACY, R2_MANIFEST_KEY, SCHEMA_VERSION,
)


def parse_date(value, flag):
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as e:
        raise SystemExit(f"{flag} phải dạng YYYY-MM-DD") from e


def load_legacy_snapshots(r2, bucket):
    obj = r2.get_object(Bucket=bucket, Key=R2_KEY_LEGACY)
    data = json.loads(obj["Body"].read().decode("utf-8"))
    snapshots = data.get("snapshots", [])
    print(f"legacy_snapshots={len(snapshots)}")
    return snapshots


def legacy_snapshot_to_records(snap):
    """Convert legacy v1 (5 fields) or v2 (9 fields) to long-form price records."""
    if not isinstance(snap, list) or len(snap) < 5:
        return []

    if len(snap) >= 9:
        return build_long_records(snap)

    ts = snap[0]
    values = [
        ("binance", "USDT", "BUY",  snap[1]),
        ("binance", "USDT", "SELL", snap[2]),
        ("binance", "USDC", "BUY",  snap[3]),
        ("binance", "USDC", "SELL", snap[4]),
    ]
    return [{
        "record_type": "price",
        "ts": ts,
        "exchange": exchange,
        "asset": asset,
        "fiat": "VND",
        "side": side,
        "price": price if price and price > 0 else None,
        "ads_count": None,
    } for exchange, asset, side, price in values]


def group_by_date(snapshots, *, skip_date=None, from_date=None, to_date=None):
    by_date = defaultdict(list)
    skipped_invalid = 0
    skipped_today = 0

    for snap in snapshots:
        records = legacy_snapshot_to_records(snap)
        if not records:
            skipped_invalid += 1
            continue
        try:
            date_str = datetime.fromtimestamp(
                snap[0], tz=timezone.utc
            ).strftime("%Y-%m-%d")
        except Exception:
            skipped_invalid += 1
            continue

        if skip_date and date_str == skip_date:
            skipped_today += 1
            continue
        if from_date and date_str < from_date:
            continue
        if to_date and date_str > to_date:
            continue
        by_date[date_str].extend(records)

    if skipped_invalid:
        print(f"skipped_invalid_snapshots={skipped_invalid}")
    if skipped_today:
        print(f"skipped_today_snapshots={skipped_today}")
    return by_date


def _record_type(r):
    value = r.get("record_type")
    if value:
        return value
    # Compatibility for historical price rows written before record_type existed.
    return "price" if "price" in r else ""


def _record_key(r):
    return (
        _record_type(r),
        r["ts"],
        r["exchange"],
        r["asset"],
        r["side"],
    )


def merge_day(r2, bucket, date_str, new_records, dry_run):
    key = _daily_key(date_str)
    existing = []
    try:
        obj = r2.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        existing = data.get("records", [])
        if not isinstance(existing, list):
            raise RuntimeError(f"{key} records không phải list")
    except r2.exceptions.NoSuchKey:
        pass

    seen = {_record_key(r) for r in existing}
    merged = list(existing)
    added = 0

    for r in new_records:
        k = _record_key(r)
        if k in seen:
            continue
        seen.add(k)
        merged.append(r)
        added += 1

    merged.sort(key=lambda r: (r.get("ts", 0), _record_type(r)))

    if dry_run or added == 0:
        return added, len(merged), False

    payload = {
        "schema_version": SCHEMA_VERSION,
        "date": date_str,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(merged),
        "records": merged,
    }
    r2.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=120",
    )
    return added, len(merged), True


def update_manifest_batch(r2, bucket, all_dates, dry_run):
    source_dates = set(all_dates)
    if not source_dates:
        return False

    manifest_exists = True
    current_dates = set()
    try:
        obj = r2.get_object(Bucket=bucket, Key=R2_MANIFEST_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        current_dates = set(data.get("dates", []))
    except r2.exceptions.NoSuchKey:
        manifest_exists = False

    merged_dates = sorted(current_dates | source_dates)
    changed = (not manifest_exists) or set(merged_dates) != current_dates

    if not changed:
        print("manifest_change=no")
        return False

    print(
        f"manifest_change=yes first_date={merged_dates[0]} "
        f"last_date={merged_dates[-1]} dates={len(merged_dates)}"
    )
    if dry_run:
        return True

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "first_date": merged_dates[0],
        "last_date": merged_dates[-1],
        "dates": merged_dates,
    }
    r2.put_object(
        Bucket=bucket,
        Key=R2_MANIFEST_KEY,
        Body=json.dumps(manifest, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=300",
    )
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Không ghi R2")
    ap.add_argument("--from-date", default=None, help="UTC YYYY-MM-DD, inclusive")
    ap.add_argument("--to-date", default=None, help="UTC YYYY-MM-DD, inclusive")
    args = ap.parse_args()

    from_day = parse_date(args.from_date, "--from-date")
    to_day = parse_date(args.to_date, "--to-date")
    if from_day and to_day and from_day > to_day:
        raise SystemExit("--from-date phải <= --to-date")

    today = datetime.now(timezone.utc).date()
    if to_day and to_day >= today and not args.dry_run:
        raise SystemExit("Migration write không được bao gồm ngày UTC hiện tại")

    print("P2P_MIGRATION_BEGIN")
    print(f"mode={'dry-run' if args.dry_run else 'write'}")
    print(
        f"range={(from_day.isoformat() if from_day else '*')}.."
        f"{(to_day.isoformat() if to_day else '*')}"
    )

    r2, bucket = get_r2()
    snapshots = load_legacy_snapshots(r2, bucket)
    if not snapshots:
        print("candidate_days=0")
        print("total_added=0")
        print("P2P_MIGRATION_END")
        return

    by_date = group_by_date(
        snapshots,
        skip_date=today.isoformat(),
        from_date=from_day.isoformat() if from_day else None,
        to_date=to_day.isoformat() if to_day else None,
    )
    if not by_date:
        print("candidate_days=0")
        print("total_added=0")
        print("P2P_MIGRATION_END")
        return

    print(f"candidate_days={len(by_date)}")
    print(f"candidate_range={min(by_date)}..{max(by_date)}")

    total_added = 0
    total_final = 0
    changed_days = []

    for date_str in sorted(by_date):
        added, final_count, wrote = merge_day(
            r2, bucket, date_str, by_date[date_str], args.dry_run
        )
        total_added += added
        total_final += final_count
        if added:
            changed_days.append((date_str, added))
        if wrote:
            print(f"wrote_day={date_str} added={added} final={final_count}")

    print(f"changed_days={len(changed_days)}")
    if changed_days:
        print(
            "changed_breakdown=" +
            ",".join(f"{date}:{added}" for date, added in changed_days)
        )
    else:
        print("changed_breakdown=-")

    update_manifest_batch(r2, bucket, by_date.keys(), args.dry_run)

    print(f"total_added={total_added}")
    print(f"total_final_records_across_candidates={total_final}")
    print("P2P_MIGRATION_END")


if __name__ == "__main__":
    main()
