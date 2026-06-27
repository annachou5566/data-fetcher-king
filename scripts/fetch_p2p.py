"""
scripts/fetch_p2p.py
────────────────────────────────────────────────────────────────────
Bot độc lập: fetch Binance P2P USDT+USDC/VND → lưu vào R2

Chạy mỗi 10 phút qua GitHub Actions (step riêng trong update_data.yml)
Hoàn toàn độc lập với fetch_alpha.py — lỗi P2P không ảnh hưởng market data

Env vars cần (dùng chung secrets với fetch_alpha):
  R2_ACCESS_KEY_ID
  R2_SECRET_ACCESS_KEY
  R2_ENDPOINT_URL
  R2_BUCKET_NAME

R2 file: p2p-data.json
Format:  { "v":1, "updated":"...", "count":N,
           "snapshots": [[ts, usdt_buy, usdt_sell, usdc_buy, usdc_sell], ...] }
────────────────────────────────────────────────────────────────────
"""

import os
import json
import time
import boto3
import cloudscraper
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Config ────────────────────────────────────────────────────────
P2P_URL      = "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/ad-list"
R2_KEY       = "p2p-data.json"
MAX_KEEP     = 26_280   # ~6 tháng × 6 lần/h × 24h
FIAT         = "VND"
ASSETS       = ["USDT", "USDC"]
TRADE_TYPES  = ["BUY", "SELL"]

# ── R2 client ─────────────────────────────────────────────────────
def get_r2():
    key      = os.getenv("R2_ACCESS_KEY_ID")
    secret   = os.getenv("R2_SECRET_ACCESS_KEY")
    endpoint = os.getenv("R2_ENDPOINT_URL")
    bucket   = os.getenv("R2_BUCKET_NAME")
    if not all([key, secret, endpoint, bucket]):
        raise RuntimeError("Thiếu R2 env vars")
    client = boto3.client(
        "s3",
        aws_access_key_id     = key,
        aws_secret_access_key = secret,
        endpoint_url          = endpoint,
    )
    return client, bucket

# ── Fetch 1 combo từ Binance public API ──────────────────────────
def fetch_best_price(session, asset, trade_type):
    try:
        res = session.get(
            P2P_URL,
            params  = {"fiat": FIAT, "asset": asset, "tradeType": trade_type, "limit": "5"},
            timeout = 12,
        )
        if res.status_code != 200:
            print(f"  ⚠️  {asset}/{trade_type} HTTP {res.status_code}")
            return 0
        items = res.json().get("data", {}).get("items", [])
        if not items:
            print(f"  ⚠️  {asset}/{trade_type} — no items")
            return 0
        price = int(float(items[0].get("price", 0)))
        return price
    except Exception as e:
        print(f"  ⚠️  {asset}/{trade_type}: {e}")
        return 0

# ── Fetch tất cả 4 combos song song ──────────────────────────────
def fetch_snapshot():
    session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )
    combos  = [(a, t) for a in ASSETS for t in TRADE_TYPES]

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fetch_best_price, session, a, t): (a, t) for a, t in combos}
        prices  = {}
        for f in as_completed(futures):
            prices[futures[f]] = f.result()

    return [
        int(time.time()),
        prices.get(("USDT", "BUY"),  0),
        prices.get(("USDT", "SELL"), 0),
        prices.get(("USDC", "BUY"),  0),
        prices.get(("USDC", "SELL"), 0),
    ]

# ── Load → append → trim → upload R2 ─────────────────────────────
def save_snapshot(r2, bucket, snapshot):
    snapshots = []
    try:
        obj       = r2.get_object(Bucket=bucket, Key=R2_KEY)
        data      = json.loads(obj["Body"].read().decode("utf-8"))
        snapshots = data.get("snapshots", [])
    except r2.exceptions.NoSuchKey:
        print("  📄 p2p-data.json chưa có → tạo mới")
    except Exception as e:
        print(f"  ⚠️  Load R2: {e} → tạo mới")

    snapshots.append(snapshot)
    if len(snapshots) > MAX_KEEP:
        snapshots = snapshots[-MAX_KEEP:]

    payload  = {
        "v":         1,
        "updated":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":     len(snapshots),
        "snapshots": snapshots,
    }
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    r2.put_object(
        Bucket       = bucket,
        Key          = R2_KEY,
        Body         = body,
        ContentType  = "application/json",
        CacheControl = "max-age=120",
    )
    return len(snapshots)

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("💱 P2P Snapshot — Binance USDT+USDC/VND")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    print("📡 Fetching prices...", flush=True)
    snap = fetch_snapshot()
    ts, ub, us, cb, cs = snap
    print(f"   USDT  BUY={ub:,}  SELL={us:,}")
    print(f"   USDC  BUY={cb:,}  SELL={cs:,}")

    if not any([ub, us, cb, cs]):
        print("❌ Tất cả giá = 0, bỏ qua upload")
        return

    print("💾 Saving to R2...", flush=True)
    try:
        r2, bucket = get_r2()
        count = save_snapshot(r2, bucket, snap)
        print(f"✅ Done — {count:,} snapshots in R2")
    except Exception as e:
        print(f"❌ R2 error: {e}")
        raise

if __name__ == "__main__":
    main()
