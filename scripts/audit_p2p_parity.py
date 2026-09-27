"""Bounded read-only parity audit: legacy p2p-data.json vs canonical daily partitions.

Parity is defined on unique observation identity:
    (ts, exchange, asset, side)

Legacy may contain duplicate copies of the same observation. Duplicate multiplicity is
reported separately and is NOT treated as missing canonical history. A retirement gate
fails only when a canonical partition/object is missing, a legacy observation identity
is absent from canonical, or the same identity has a different price value.

Default scope is the latest 30 UTC days present in canonical manifest.
Max 90 days per run. No R2 writes are performed.
"""
import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta

from fetch_p2p import get_r2, R2_KEY_LEGACY, R2_MANIFEST_KEY, _daily_key
from migrate_p2p_history import legacy_snapshot_to_records


def identity_key(r):
    return (
        int(r["ts"]),
        r["exchange"],
        r["asset"],
        r["side"],
    )


def exact_key(r):
    return identity_key(r) + (
        None if r.get("price") is None else float(r["price"]),
    )


def price_value(r):
    value = r.get("price")
    return None if value is None else float(value)


def is_price_record(r):
    if r.get("record_type") == "price":
        return True
    # Backward compatibility: early canonical partitions predate record_type.
    # Only rows with the full price identity + price field qualify.
    return (
        r.get("record_type") in (None, "")
        and "price" in r
        and r.get("ts") is not None
        and r.get("exchange") is not None
        and r.get("asset") is not None
        and r.get("side") is not None
    )


def compact_identity_breakdown(counter):
    by_date = Counter()
    by_shape = Counter()
    for (ts, exchange, asset, side), count in counter.items():
        day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        by_date[day] += count
        by_shape[(exchange, asset, side)] += count
    dates = ",".join(f"{d}:{n}" for d, n in sorted(by_date.items())[:20]) or "-"
    shapes = ",".join(
        f"{e}/{a}/{s}:{n}" for (e, a, s), n in sorted(by_shape.items())
    ) or "-"
    return dates, shapes


def mismatch_samples(ids, legacy_prices, canonical_prices, limit=8):
    out = []
    for ts, exchange, asset, side in sorted(ids)[:limit]:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
        lv = sorted(legacy_prices[(ts, exchange, asset, side)], key=lambda x: (x is None, x))
        cv = sorted(canonical_prices[(ts, exchange, asset, side)], key=lambda x: (x is None, x))
        out.append(
            f"{dt}|{exchange}/{asset}/{side}|legacy={lv}|canonical={cv}"
        )
    return ";".join(out) or "-"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--end-date", default=None,
                    help="Kết thúc cửa sổ UTC YYYY-MM-DD; mặc định manifest.last_date")
    ap.add_argument("--report-only", action="store_true",
                    help="In discrepancy nhưng không exit 2; dùng để thu đủ nhiều cửa sổ read-only")
    ap.add_argument("--details", action="store_true",
                    help="In breakdown compact theo ngày và exchange/asset/side")
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

    manifest_end = datetime.strptime(dates[-1], "%Y-%m-%d").date()
    if args.end_date:
        try:
            end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("--end-date phải dạng YYYY-MM-DD")
        if end > manifest_end:
            end = manifest_end
    else:
        end = manifest_end
    start = end - timedelta(days=args.days - 1)
    selected = [d for d in dates if start.isoformat() <= d <= end.isoformat()]

    legacy_dates_all = []
    legacy_exact = Counter()
    legacy_ids = Counter()
    legacy_prices = defaultdict(set)

    for snap in legacy:
        try:
            day = datetime.fromtimestamp(snap[0], tz=timezone.utc).date()
            legacy_dates_all.append(day)
        except Exception:
            continue
        if day < start or day > end:
            continue
        for r in legacy_snapshot_to_records(snap):
            ik = identity_key(r)
            legacy_exact[exact_key(r)] += 1
            legacy_ids[ik] += 1
            legacy_prices[ik].add(price_value(r))

    canonical_exact = Counter()
    canonical_ids = Counter()
    canonical_prices = defaultdict(set)
    canonical_legacy_schema_price_rows = 0
    missing_objects = []

    for d in selected:
        obj = r2.get_object(Bucket=bucket, Key=_daily_key(d))
        if not obj:
            missing_objects.append(d)
            continue
        data = json.loads(obj["Body"].read().decode("utf-8"))
        for r in data.get("records", []):
            if not is_price_record(r):
                continue
            if r.get("record_type") in (None, ""):
                canonical_legacy_schema_price_rows += 1
            ik = identity_key(r)
            canonical_exact[exact_key(r)] += 1
            canonical_ids[ik] += 1
            canonical_prices[ik].add(price_value(r))

    legacy_key_set = set(legacy_ids)
    canonical_key_set = set(canonical_ids)
    common = legacy_key_set & canonical_key_set

    missing_canonical = Counter({
        ik: legacy_ids[ik]
        for ik in legacy_key_set - canonical_key_set
    })
    missing_legacy = Counter({
        ik: canonical_ids[ik]
        for ik in canonical_key_set - legacy_key_set
    })

    value_mismatch_ids = {
        ik for ik in common
        if legacy_prices[ik] != canonical_prices[ik]
    }
    value_mismatch = Counter({ik: 1 for ik in value_mismatch_ids})

    legacy_duplicate_excess = Counter({
        ik: legacy_ids[ik] - canonical_ids[ik]
        for ik in common
        if legacy_ids[ik] > canonical_ids[ik]
    })
    canonical_duplicate_excess = Counter({
        ik: canonical_ids[ik] - legacy_ids[ik]
        for ik in common
        if canonical_ids[ik] > legacy_ids[ik]
    })

    legacy_only_exact = legacy_exact - canonical_exact
    canonical_only_exact = canonical_exact - legacy_exact

    legacy_first = min(legacy_dates_all).isoformat() if legacy_dates_all else "-"
    legacy_last = max(legacy_dates_all).isoformat() if legacy_dates_all else "-"

    print("P2P_PARITY_BEGIN")
    print(f"range={start.isoformat()}..{end.isoformat()}")
    print(f"legacy_range={legacy_first}..{legacy_last}")
    print(f"manifest_days={len(selected)} missing_objects={len(missing_objects)}")
    print(f"legacy_records={sum(legacy_ids.values())} canonical_records={sum(canonical_ids.values())}")
    print(f"legacy_unique_keys={len(legacy_key_set)} canonical_unique_keys={len(canonical_key_set)}")
    print(f"canonical_legacy_schema_price_rows={canonical_legacy_schema_price_rows}")
    print(f"missing_canonical_keys={len(missing_canonical)}")
    print(f"missing_legacy_keys={len(missing_legacy)}")
    print(f"value_mismatch_keys={len(value_mismatch_ids)}")
    print(f"legacy_duplicate_excess={sum(legacy_duplicate_excess.values())}")
    print(f"canonical_duplicate_excess={sum(canonical_duplicate_excess.values())}")
    print(f"legacy_only_exact={sum(legacy_only_exact.values())}")
    print(f"canonical_only_exact={sum(canonical_only_exact.values())}")

    if args.details:
        miss_dates, miss_shapes = compact_identity_breakdown(
            Counter({ik: 1 for ik in missing_canonical})
        )
        dup_dates, dup_shapes = compact_identity_breakdown(legacy_duplicate_excess)
        mismatch_dates, mismatch_shapes = compact_identity_breakdown(value_mismatch)
        extra_dates, extra_shapes = compact_identity_breakdown(
            Counter({ik: 1 for ik in missing_legacy})
        )
        print(f"missing_canonical_dates={miss_dates}")
        print(f"missing_canonical_shapes={miss_shapes}")
        print(f"legacy_duplicate_dates={dup_dates}")
        print(f"legacy_duplicate_shapes={dup_shapes}")
        print(f"value_mismatch_dates={mismatch_dates}")
        print(f"value_mismatch_shapes={mismatch_shapes}")
        print(
            "value_mismatch_samples=" +
            mismatch_samples(value_mismatch_ids, legacy_prices, canonical_prices)
        )
        print(f"missing_legacy_dates={extra_dates}")
        print(f"missing_legacy_shapes={extra_shapes}")

    print("P2P_PARITY_END")

    blocking = bool(missing_objects or missing_canonical or value_mismatch_ids)
    if not args.report_only and blocking:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
