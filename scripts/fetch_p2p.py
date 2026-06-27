"""
scripts/fetch_p2p.py
────────────────────────────────────────────────────────────────────
Bot: fetch P2P USDT/VND từ Binance + OKX + Bybit → lưu vào R2
Snapshot format v2 (9 phần tử)
"""

import os, json, time, boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests  # Thay thế cloudscraper để bypass WAF triệt để

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
            print(f"  ⚠️  OKX {trade_type} HTTP {res.status_code}: {res.text[:100]}")
            return 0
            
        json_data = res.json()
        data = json_data.get("data", [])
        
        # Xử lý an toàn nếu OKX trả về dict do thay đổi format hoặc dính lỗi WAF
        if isinstance(data, dict):
            # Nếu WAF trả về lỗi JSON, hoặc dữ liệu bị lồng vào trong "items"
            items = data.get("items", [])
            if not items:
                print(f"  ⚠️  OKX {trade_type} Data bất thường (Dict): {str(json_data)[:150]}")
                return 0
            data = items
            
        if not data or not isinstance(data, list):
            return 0
            
        return int(float(data[0].get("price", 0)))
    except Exception as e:
        print(f"  ⚠️  OKX USDT/{trade_type} Exception: {repr(e)}")
        return 0

def fetch_bybit(session, trade_type):
    side = "1" if trade_type == "BUY" else "0"
    try:
        res = session.post(BBT_URL, json={
            "tokenId": "USDT", "currencyId": FIAT,
            "payment": [], "side": side,
            "size": "5", "page": "1", "amount": "",
        }, timeout=12)
        
        if res.status_code != 200:
            print(f"  ⚠️  BBT {trade_type} HTTP {res.status_code}: {res.text[:100]}")
            return 0
            
        json_data = res.json()
        items = json_data.get("result", {}).get("items", [])
        
        if not items:
            print(f"  ⚠️  BBT {trade_type} Không có items: {str(json_data)[:150]}")
            return 0
            
        return int(float(items[0].get("price", 0)))
    except Exception as e:
        print(f"  ⚠️  BBT USDT/{trade_type} Exception: {repr(e)}")
        return 0

# ── Fetch tất cả song song ────────────────────────────────────────
def fetch_snapshot():
    # curl_cffi giả lập Chrome 116 để đánh lừa WAF cực mạnh
    session = requests.Session(impersonate="chrome116")

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
