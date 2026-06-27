"""
scripts/fetch_p2p.py
────────────────────────────────────────────────────────────────────
Bot: fetch P2P USDT/VND từ Binance + OKX + Bybit → lưu vào R2

Snapshot format v2 (9 phần tử):
  [ts, bnc_ub, bnc_us, bnc_cb, bnc_cs, okx_ub, okx_us, bbt_ub, bbt_us]
   0    1       2       3       4        5       6        7       8

Backward compat: snapshot cũ có 5 phần tử (Binance only) vẫn đọc được.

R2 file: p2p-data.json
Format:  { "v":2, "updated":"...", "count":N, "snapshots": [...] }
────────────────────────────────────────────────────────────────────
"""

import os, json, time, boto3, cloudscraper
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Config ────────────────────────────────────────────────────────
R2_KEY      = "p2p-data.json"
MAX_KEEP    = 26_280        # ~6 tháng
FIAT        = "VND"

# Binance
BNC_URL     = "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/ad-list"
BNC_ASSETS  = ["USDT", "USDC"]

# OKX — public, không cần auth
OKX_URL     = "https://www.okx.com/v3/c2c/tradingOrders/books"

# Bybit — public endpoint (unofficial, không cần auth)
BBT_URL     = "https://api2.bybit.com/fiat/otc/item/list"

# ── R2 ────────────────────────────────────────────────────────────
def get_r2():
    key, secret, endpoint, bucket = (
        os.getenv("R2_ACCESS_KEY_ID"),    os.getenv("R2_SECRET_ACCESS_KEY"),
        os.getenv("R2_ENDPOINT_URL"),     os.getenv("R2_BUCKET_NAME"),
    )
    if not all([key, secret, endpoint, bucket]):
        raise RuntimeError("Thiếu R2 env vars")
    return boto3.client("s3",
        aws_access_key_id=key, aws_secret_access_key=secret, endpoint_url=endpoint,
    ), bucket

# ── Fetchers ──────────────────────────────────────────────────────

def fetch_binance(session, asset, trade_type):
    """Giữ nguyên logic gốc."""
    try:
        res = session.get(BNC_URL,
            params={"fiat": FIAT, "asset": asset, "tradeType": trade_type, "limit": "5"},
            timeout=12)
        if res.status_code != 200:
            return 0
        items = res.json().get("data", {}).get("items", [])
        return int(float(items[0].get("price", 0))) if items else 0
    except Exception as e:
        print(f"  ⚠️  BNC {asset}/{trade_type}: {e}")
        return 0

def fetch_okx(session, trade_type):
    """
    trade_type: 'BUY'  → user mua  → advertiser đang SELL (side=sell)
                'SELL' → user bán  → advertiser đang BUY  (side=buy)
    """
    side = "sell" if trade_type == "BUY" else "buy"
    try:
        res = session.get(OKX_URL, params={
            "quoteCurrency": FIAT, "baseCurrency": "USDT",
            "side": side, "paymentMethod": "all",
            "userType": "all", "showTrade": "false",
            "showFollow": "false", "showAlreadyTraded": "false",
            "isAbleFilter": "false", "limit": "5",
        }, timeout=12)
        if res.status_code != 200:
            return 0
        data = res.json().get("data", [])
        if not data:
            return 0
        return int(float(data[0].get("price", 0)))
    except Exception as e:
        print(f"  ⚠️  OKX USDT/{trade_type}: {e}")
        return 0

def fetch_bybit(session, trade_type):
    """
    side: "1" → user mua (BUY)
          "0" → user bán (SELL)
    """
    side = "1" if trade_type == "BUY" else "0"
    try:
        res = session.post(BBT_URL, json={
            "tokenId": "USDT", "currencyId": FIAT,
            "payment": [], "side": side,
            "size": "5", "page": "1", "amount": "",
        }, timeout=12)
        if res.status_code != 200:
            return 0
        items = res.json().get("result", {}).get("items", [])
        if not items:
            return 0
        return int(float(items[0].get("price", 0)))
    except Exception as e:
        print(f"  ⚠️  BBT USDT/{trade_type}: {e}")
        return 0

# ── Fetch tất cả song song ────────────────────────────────────────
def fetch_snapshot():
    session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False}
    )

    tasks = {
        "bnc_ub":  (fetch_binance, session, "USDT", "BUY"),
        "bnc_us":  (fetch_binance, session, "USDT", "SELL"),
        "bnc_cb":  (fetch_binance, session, "USDC", "BUY"),
        "bnc_cs":  (fetch_binance, session, "USDC", "SELL"),
        "okx_ub":  (fetch_okx,    session, "BUY"),
        "okx_us":  (fetch_okx,    session, "SELL"),
        "bbt_ub":  (fetch_bybit,  session, "BUY"),
        "bbt_us":  (fetch_bybit,  session, "SELL"),
    }

    results = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fn, *args): key for key, (fn, *args) in tasks.items()}
        for f in as_completed(futures):
            results[futures[f]] = f.result()

    return [
        int(time.time()),
        results.get("bnc_ub", 0), results.get("bnc_us", 0),
        results.get("bnc_cb", 0), results.get("bnc_cs", 0),
        results.get("okx_ub", 0), results.get("okx_us", 0),
        results.get("bbt_ub", 0), results.get("bbt_us", 0),
    ]

# ── Save to R2 ────────────────────────────────────────────────────
def save_snapshot(r2, bucket, snapshot):
    snapshots = []
    try:
        obj  = r2.get_object(Bucket=bucket, Key=R2_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        snapshots = data.get("snapshots", [])
    except r2.exceptions.NoSuchKey:
        print("  📄 p2p-data.json chưa có → tạo mới")
    except Exception as e:
        print(f"  ⚠️  Load R2: {e} → tạo mới")

    snapshots.append(snapshot)
    if len(snapshots) > MAX_KEEP:
        snapshots = snapshots[-MAX_KEEP:]

    payload = {
        "v":         2,
        "updated":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":     len(snapshots),
        "snapshots": snapshots,
    }
    r2.put_object(
        Bucket=bucket, Key=R2_KEY,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=120",
    )
    return len(snapshots)

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("💱 P2P Snapshot — Binance + OKX + Bybit / USDT+USDC / VND")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    print("📡 Fetching...", flush=True)
    snap = fetch_snapshot()
    ts, bnc_ub, bnc_us, bnc_cb, bnc_cs, okx_ub, okx_us, bbt_ub, bbt_us = snap

    print(f"   Binance  USDT BUY={bnc_ub:,}  SELL={bnc_us:,}  |  USDC BUY={bnc_cb:,}  SELL={bnc_cs:,}")
    print(f"   OKX      USDT BUY={okx_ub:,}  SELL={okx_us:,}")
    print(f"   Bybit    USDT BUY={bbt_ub:,}  SELL={bbt_us:,}")

    if not any(snap[1:]):
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
