"""
scripts/fetch_p2p_backfill.py
────────────────────────────────────────────────────────────────────
Script chạy 1 LẦN DUY NHẤT để backfill lịch sử P2P từ tháng 3/2023.

Nguồn: p2p.army (có data Binance P2P VND từ March 2023)
Chạy: GitHub Actions → GA IPs không bị p2p.army block (khác CF/browser IPs)

Sau khi chạy xong: p2p-data.json trong R2 sẽ có ~26,000+ data points
Bot fetch_p2p.py tiếp tục append bình thường từ đó trở đi.
────────────────────────────────────────────────────────────────────
"""

import os
import json
import time
import boto3
import requests
from datetime import datetime, timezone, timedelta

P2P_ARMY_URL = "https://p2p.army/v1/api/history/p2p_prices"
R2_KEY       = "p2p-data.json"
BATCH_LIMIT  = 1000   # số điểm mỗi request
MAX_KEEP     = 52_560  # ~1 năm × 6 lần/h × 24h (tăng gấp đôi cho backfill)

# ── R2 client ─────────────────────────────────────────────────────
def get_r2():
    return boto3.client(
        "s3",
        aws_access_key_id     = os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["R2_SECRET_ACCESS_KEY"],
        endpoint_url          = os.environ["R2_ENDPOINT_URL"],
    ), os.environ["R2_BUCKET_NAME"]

# ── Fetch 1 batch từ p2p.army ─────────────────────────────────────
def fetch_batch(session, asset, page=1):
    try:
        res = session.post(
            P2P_ARMY_URL,
            json    = {
                "market": "binance",
                "fiat":   "VND",
                "asset":  asset,
                "mode":   "ALL",
                "limit":  BATCH_LIMIT,
                "page":   page,
            },
            timeout = 20,
            headers = {
                "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Content-Type": "application/json",
            }
        )
        if res.status_code != 200:
            print(f"  ⚠️  p2p.army {asset} page={page} → HTTP {res.status_code}")
            return []
        data = res.json()
        pts  = data.get("history", [])
        print(f"  📦 {asset} page={page} → {len(pts)} points")
        return pts
    except Exception as e:
        print(f"  ⚠️  {asset} page={page}: {e}")
        return []

# ── Fetch tất cả history cho 1 asset ─────────────────────────────
def fetch_all_history(session, asset):
    print(f"\n🔄 Fetching {asset}/VND history từ p2p.army...")
    all_pts = []
    seen    = set()

    for page in range(1, 50):  # tối đa 50 pages × 1000 = 50,000 điểm
        pts = fetch_batch(session, asset, page)
        if not pts:
            print(f"  ✅ Hết data tại page {page}")
            break

        new_pts = []
        for pt in pts:
            key = pt.get("date", "")
            if key and key not in seen:
                seen.add(key)
                new_pts.append(pt)

        all_pts.extend(new_pts)
        time.sleep(0.5)  # rate limit nhẹ

        # Nếu page trả về < BATCH_LIMIT → đã hết data
        if len(pts) < BATCH_LIMIT:
            print(f"  ✅ Trang cuối tại page {page}")
            break

    print(f"  → Tổng: {len(all_pts)} điểm cho {asset}")
    return all_pts

# ── Convert p2p.army format → compact snapshot format ─────────────
def convert_to_snapshots(usdt_pts, usdc_pts):
    """
    p2p.army format: [{date, buy_avg, sell_avg, buy, sell}]
    Our format:      [[ts_sec, usdt_buy, usdt_sell, usdc_buy, usdc_sell]]
    """
    # Index USDT theo date string
    usdt_map = {}
    for pt in usdt_pts:
        d = pt.get("date", "")
        if not d:
            continue
        buy  = int(float(pt.get("buy_avg")  or pt.get("buy")  or 0))
        sell = int(float(pt.get("sell_avg") or pt.get("sell") or 0))
        if buy > 10000 or sell > 10000:
            usdt_map[d] = (buy, sell)

    # Index USDC theo date string
    usdc_map = {}
    for pt in usdc_pts:
        d = pt.get("date", "")
        if not d:
            continue
        buy  = int(float(pt.get("buy_avg")  or pt.get("buy")  or 0))
        sell = int(float(pt.get("sell_avg") or pt.get("sell") or 0))
        if buy > 10000 or sell > 10000:
            usdc_map[d] = (buy, sell)

    # Merge theo date
    all_dates = sorted(set(usdt_map.keys()) | set(usdc_map.keys()))
    snapshots = []
    for d in all_dates:
        try:
            ts = int(datetime.fromisoformat(d.replace("Z", "+00:00")).timestamp())
        except Exception:
            continue
        ub, us = usdt_map.get(d, (0, 0))
        cb, cs = usdc_map.get(d, (0, 0))
        snapshots.append([ts, ub, us, cb, cs])

    # Sort theo timestamp tăng dần
    snapshots.sort(key=lambda x: x[0])
    return snapshots

# ── Upload lên R2 (merge với data hiện có) ────────────────────────
def upload_to_r2(r2, bucket, new_snapshots):
    # Load existing snapshots từ R2 (nếu có)
    existing = []
    try:
        obj      = r2.get_object(Bucket=bucket, Key=R2_KEY)
        data     = json.loads(obj["Body"].read().decode("utf-8"))
        existing = data.get("snapshots", [])
        print(f"  📂 Existing snapshots in R2: {len(existing)}")
    except Exception:
        print("  📄 Chưa có file R2 → tạo mới")

    # Merge: backfill + existing, dedup theo timestamp
    seen = {}
    for s in new_snapshots + existing:
        seen[s[0]] = s  # existing sẽ override backfill nếu trùng timestamp

    merged = sorted(seen.values(), key=lambda x: x[0])
    if len(merged) > MAX_KEEP:
        merged = merged[-MAX_KEEP:]

    payload  = {
        "v":         1,
        "updated":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":     len(merged),
        "snapshots": merged,
    }
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    r2.put_object(
        Bucket       = bucket,
        Key          = R2_KEY,
        Body         = body,
        ContentType  = "application/json",
        CacheControl = "max-age=120",
    )
    return len(merged)

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("🕐 P2P BACKFILL — Lịch sử từ tháng 3/2023")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    session = requests.Session()

    # Fetch USDT và USDC history
    usdt_pts = fetch_all_history(session, "USDT")
    usdc_pts = fetch_all_history(session, "USDC")

    if not usdt_pts and not usdc_pts:
        print("\n❌ Không lấy được data từ p2p.army")
        print("   Có thể GA IPs cũng bị block hoặc API thay đổi")
        print("   → Thử download thủ công từ p2p.army và upload lên")
        return

    # Convert format
    print("\n🔄 Converting data format...")
    snapshots = convert_to_snapshots(usdt_pts, usdc_pts)
    print(f"   → {len(snapshots)} snapshots sau khi convert")

    if not snapshots:
        print("❌ Không có snapshot hợp lệ sau convert")
        return

    # Thống kê
    first_date = datetime.fromtimestamp(snapshots[0][0], tz=timezone.utc).strftime("%Y-%m-%d")
    last_date  = datetime.fromtimestamp(snapshots[-1][0], tz=timezone.utc).strftime("%Y-%m-%d")
    print(f"   Từ: {first_date} → {last_date}")
    print(f"   USDT BUY sample: {snapshots[-1][1]:,} ₫")

    # Upload R2
    print("\n💾 Uploading to R2...")
    r2, bucket = get_r2()
    total = upload_to_r2(r2, bucket, snapshots)

    print(f"\n✅ DONE — {total:,} snapshots in R2")
    print(f"   File size ≈ {total * 35 / 1024:.0f} KB")

if __name__ == "__main__":
    main()
