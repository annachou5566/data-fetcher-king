"""
scripts/cleanup_old_liquidity.py — CHẠY 1 LẦN DUY NHẤT
────────────────────────────────────────────────────────────────────
Xóa các record_type="liquidity_snapshot" và "imbalance_index" được ghi
TRƯỚC thời điểm sửa cap SELL-side (2026-07-16 ~04:00 UTC) — vì các con số
đó tính sai (cap quá lỏng, không thể tính lại do không còn giữ ads thô).

Giữ nguyên 100% record_type="price" — KHÔNG đụng vào, dữ liệu giá vẫn đúng
từ trước tới giờ, không liên quan gì tới bug cap.

An toàn chạy lại nhiều lần (idempotent) — nếu chạy lại, các ngày đã dọn rồi
sẽ không còn record nào để xóa thêm.

Cách chạy:
  python scripts/cleanup_old_liquidity.py --dry-run   # xem trước, KHÔNG ghi
  python scripts/cleanup_old_liquidity.py              # xóa thật
"""

import os, json, sys, boto3
from datetime import datetime, timezone

# ⚠️ CHỈNH ĐÚNG mốc thời gian bạn deploy bản fetch_p2p.py đã sửa cap —
# lấy từ timestamp lần chạy ĐẦU TIÊN có log "verified=2,322,738" (bản mới).
CUTOFF_TS = int(datetime(2026, 7, 16, 3, 50, 0, tzinfo=timezone.utc).timestamp())

R2_DAILY_PREFIX = "p2p-snapshots/"


def get_r2():
    return boto3.client("s3",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
    ), os.environ["R2_BUCKET_NAME"]


def cleanup_file(r2, bucket, key, dry_run):
    try:
        obj = r2.get_object(Bucket=bucket, Key=key)
    except r2.exceptions.NoSuchKey:
        return None
    data = json.loads(obj["Body"].read().decode("utf-8"))
    records = data.get("records", [])

    before = len(records)
    kept = [
        r for r in records
        if not (
            r.get("record_type") in ("liquidity_snapshot", "imbalance_index")
            and r.get("ts", 0) < CUTOFF_TS
        )
    ]
    removed = before - len(kept)

    if removed == 0:
        return 0

    print(f"  {key}: {before} → {len(kept)} records (xóa {removed} bản ghi liquidity cũ)")

    if not dry_run:
        payload = {
            **data,
            "count": len(kept),
            "records": kept,
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        r2.put_object(
            Bucket=bucket, Key=key,
            Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json", CacheControl="max-age=120",
        )

    return removed


def main():
    dry_run = "--dry-run" in sys.argv
    print(f"🧹 Dọn dẹp Liquidity Index cũ (trước {datetime.fromtimestamp(CUTOFF_TS, tz=timezone.utc)})")
    print(f"   Chế độ: {'DRY-RUN (chỉ xem, không ghi)' if dry_run else 'THẬT (sẽ ghi đè R2)'}")

    r2, bucket = get_r2()

    # Đọc manifest để biết chính xác ngày nào có data — tránh dò mù
    try:
        obj = r2.get_object(Bucket=bucket, Key=f"{R2_DAILY_PREFIX}_manifest.json")
        manifest = json.loads(obj["Body"].read().decode("utf-8"))
        dates = manifest.get("dates", [])
    except Exception as e:
        print(f"❌ Không đọc được manifest: {e}")
        return

    total_removed = 0
    for date_str in dates:
        key = f"{R2_DAILY_PREFIX}{date_str}.json"
        removed = cleanup_file(r2, bucket, key, dry_run)
        if removed:
            total_removed += removed

    print(f"\n✅ Xong — tổng {total_removed} bản ghi liquidity cũ đã {'sẽ bị' if dry_run else 'được'} xóa")
    if dry_run:
        print("   Chạy lại KHÔNG có --dry-run để thực sự xóa.")


if __name__ == "__main__":
    main()
