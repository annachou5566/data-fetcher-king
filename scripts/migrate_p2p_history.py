"""
scripts/migrate_p2p_history.py
────────────────────────────────────────────────────────────────────
Script chạy 1 LẦN DUY NHẤT (thủ công, không nằm trong workflow cron):
đọc toàn bộ p2p-data.json (legacy, ~6 tháng snapshot) → chuyển sang
kiến trúc partition theo ngày (p2p-snapshots/YYYY-MM-DD.json) mà
fetch_p2p.py (bản mới) đang dùng.

AN TOÀN khi chạy nhiều lần (idempotent):
  - Dedupe theo khoá (ts, exchange, asset, side) — chạy lại không tạo
    bản ghi trùng.
  - Nếu bot mới (fetch_p2p.py) đã ghi sẵn dữ liệu cho 1 ngày nào đó
    (vd chạy song song trong lúc chờ migrate), script sẽ MERGE chứ
    không ghi đè mất dữ liệu đó.
  - Không đụng gì tới p2p-data.json (chỉ đọc, không ghi).

Cách chạy:
    python scripts/migrate_p2p_history.py            # chạy thật
    python scripts/migrate_p2p_history.py --dry-run   # chỉ xem trước, không ghi R2
"""

import sys, json, argparse
from collections import defaultdict
from datetime import datetime, timezone

from fetch_p2p import (
    get_r2, build_long_records, _daily_key,
    R2_KEY_LEGACY, R2_MANIFEST_KEY, SCHEMA_VERSION,
)


def load_legacy_snapshots(r2, bucket):
    obj  = r2.get_object(Bucket=bucket, Key=R2_KEY_LEGACY)
    data = json.loads(obj["Body"].read().decode("utf-8"))
    snapshots = data.get("snapshots", [])
    print(f"📄 Đọc được {len(snapshots):,} snapshot từ {R2_KEY_LEGACY} (v{data.get('v', '?')})")
    return snapshots


def group_by_date(snapshots):
    """Gom snapshot theo ngày UTC, mỗi snapshot → 8 record long-format."""
    by_date = defaultdict(list)
    skipped = 0
    for snap in snapshots:
        if not isinstance(snap, list) or len(snap) < 9:
            skipped += 1
            continue
        ts = snap[0]
        try:
            date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            skipped += 1
            continue
        by_date[date_str].extend(build_long_records(snap))
    if skipped:
        print(f"⚠️  Bỏ qua {skipped} snapshot dạng lạ/lỗi (không đủ field)")
    return by_date


def _record_key(r):
    return (r["ts"], r["exchange"], r["asset"], r["side"])


def merge_day(r2, bucket, date_str, new_records, dry_run):
    key = _daily_key(date_str)
    existing = []
    try:
        obj  = r2.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        existing = data.get("records", [])
    except r2.exceptions.NoSuchKey:
        pass

    # Dedupe theo (ts, exchange, asset, side) — record mới (từ legacy) chỉ
    # được thêm nếu key đó CHƯA có sẵn (ưu tiên giữ data đang có, vì có thể
    # đã được bot mới ghi realtime với ads_count đầy đủ hơn về sau).
    seen  = {_record_key(r) for r in existing}
    added = 0
    merged = list(existing)
    for r in new_records:
        k = _record_key(r)
        if k in seen:
            continue
        seen.add(k)
        merged.append(r)
        added += 1

    merged.sort(key=lambda r: r["ts"])

    if dry_run:
        return added, len(merged)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "date":           date_str,
        "updated":        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":          len(merged),
        "records":        merged,
    }
    r2.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=120",
    )
    return added, len(merged)


def update_manifest_batch(r2, bucket, all_dates, dry_run):
    dates = set(all_dates)
    try:
        obj  = r2.get_object(Bucket=bucket, Key=R2_MANIFEST_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        dates |= set(data.get("dates", []))
    except r2.exceptions.NoSuchKey:
        pass

    dates = sorted(dates)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "first_date": dates[0],
        "last_date":  dates[-1],
        "dates":      dates,
    }
    if dry_run:
        print(f"  (dry-run) manifest sẽ có {len(dates)} ngày: {dates[0]} → {dates[-1]}")
        return
    r2.put_object(
        Bucket=bucket, Key=R2_MANIFEST_KEY,
        Body=json.dumps(manifest, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=300",
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Chỉ in ra kế hoạch, không ghi R2")
    args = ap.parse_args()

    print("🔄 Migrate p2p-data.json (legacy) → p2p-snapshots/YYYY-MM-DD.json (mới)")
    if args.dry_run:
        print("   ⚠️  DRY-RUN — sẽ không ghi gì vào R2")

    r2, bucket = get_r2()
    snapshots  = load_legacy_snapshots(r2, bucket)
    if not snapshots:
        print("Không có snapshot nào để migrate — dừng.")
        return

    by_date = group_by_date(snapshots)
    print(f"📅 Trải trên {len(by_date)} ngày: {min(by_date)} → {max(by_date)}")

    total_added, total_final = 0, 0
    for i, date_str in enumerate(sorted(by_date), 1):
        added, final_count = merge_day(r2, bucket, date_str, by_date[date_str], args.dry_run)
        total_added += added
        total_final += final_count
        print(f"  [{i}/{len(by_date)}] {date_str}: +{added} record mới (tổng ngày này: {final_count})")

    update_manifest_batch(r2, bucket, list(by_date.keys()), args.dry_run)

    print()
    print(f"✅ Xong — {total_added:,} record được thêm vào kiến trúc mới, "
          f"tổng {total_final:,} record trên {len(by_date)} ngày.")
    if args.dry_run:
        print("   Chạy lại KHÔNG có --dry-run để ghi thật vào R2.")


if __name__ == "__main__":
    main()
