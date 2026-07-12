"""
scripts/fetch_p2p.py
────────────────────────────────────────────────────────────────────
Bot: fetch P2P USDT/VND từ Binance + OKX + Bybit → lưu vào R2

Ghi các lớp SONG SONG, độc lập nhau (1 lớp lỗi không làm chết lớp khác):

  1) LEGACY  — p2p-data.json (giữ nguyên 100%, không đổi gì)
  2) DAILY PRICE — p2p-snapshots/YYYY-MM-DD.json, record_type="price"
     (giữ nguyên 100%, không đổi gì)
  3) MỚI — LIQUIDITY INDEX (chỉ Binance, xem kiến trúc trong tài liệu
     p2p-liquidity-architecture.md) — record_type="liquidity_snapshot",
     ghi vào CÙNG file daily, khác record_type để phân biệt.

     ⚠️ OKX/Bybit CHƯA có Liquidity Index vì field name (surplusAmount,
     merchant trust...) chưa được xác minh chắc chắn cho 2 sàn này —
     script chỉ in log debug cấu trúc response để xác minh sau, KHÔNG
     đoán mò field để tránh dữ liệu sai.
"""

import os, json, time, boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests

# ── Config gốc (KHÔNG đổi) ──────────────────────────────────────────
R2_KEY_LEGACY   = "p2p-data.json"
MAX_KEEP        = 26_280
FIAT            = "VND"

R2_DAILY_PREFIX = "p2p-snapshots/"
R2_MANIFEST_KEY = "p2p-snapshots/_manifest.json"
SCHEMA_VERSION  = 1

BNC_URL     = "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/ad-list"
BNC_ASSETS  = ["USDT", "USDC"]
OKX_URL     = "https://www.okx.com/v3/c2c/tradingOrders/books"
BBT_URL     = "https://api2.bybit.com/fiat/otc/item/online"

# ── Config MỚI — Liquidity Index (theo kiến trúc đã chốt) ───────────
VERIFIED_MIN_ORDER_COUNT = 10
VERIFIED_MIN_FINISH_RATE = 0.85
SELL_CAP_MULTIPLIER      = 3
PAGE_SIZE                = 20
MAX_PAGE_SAFETY          = 50

# ── R2 (KHÔNG đổi) ───────────────────────────────────────────────────
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

# ── Fetchers giá (KHÔNG đổi gì so với bản gốc) ──────────────────────

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
        if isinstance(data, dict):
            items = data.get(side, [])
        elif isinstance(data, list):
            items = data
        else:
            items = []
        if not items:
            return 0
        return int(float(items[0].get("price", 0)))
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
            return 0
        return int(float(items[0].get("price", 0)))
    except Exception as e:
        print(f"  ⚠️  BBT USDT/{trade_type} Exception: {repr(e)}")
        return 0

def fetch_snapshot():
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

# ── LEGACY save (KHÔNG đổi) ─────────────────────────────────────────
def save_snapshot_legacy(r2, bucket, snapshot):
    snapshots = []
    try:
        obj  = r2.get_object(Bucket=bucket, Key=R2_KEY_LEGACY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        snapshots = data.get("snapshots", [])
    except r2.exceptions.NoSuchKey:
        print("  📄 p2p-data.json chưa có → tạo mới")
    except Exception as e:
        print(f"  ⚠️  Load R2 (legacy): {e} → tạo mới")
    snapshots.append(snapshot)
    if len(snapshots) > MAX_KEEP:
        snapshots = snapshots[-MAX_KEEP:]
    payload = {
        "v": 2, "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(snapshots), "snapshots": snapshots,
    }
    r2.put_object(
        Bucket=bucket, Key=R2_KEY_LEGACY,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=120",
    )
    return len(snapshots)

# ── DAILY PRICE records (KHÔNG đổi) ─────────────────────────────────
def build_long_records(snap):
    ts = snap[0]
    raw = {
        "bnc_ub": snap[1], "bnc_us": snap[2], "bnc_cb": snap[3], "bnc_cs": snap[4],
        "okx_ub": snap[5], "okx_us": snap[6], "bbt_ub": snap[7], "bbt_us": snap[8],
    }
    combos = [
        ("bnc_ub", "binance", "USDT", "BUY"),  ("bnc_us", "binance", "USDT", "SELL"),
        ("bnc_cb", "binance", "USDC", "BUY"),  ("bnc_cs", "binance", "USDC", "SELL"),
        ("okx_ub", "okx",     "USDT", "BUY"),  ("okx_us", "okx",     "USDT", "SELL"),
        ("bbt_ub", "bybit",   "USDT", "BUY"),  ("bbt_us", "bybit",   "USDT", "SELL"),
    ]
    records = []
    for field, exchange, asset, side in combos:
        price = raw[field]
        records.append({
            "record_type": "price",
            "ts": ts, "exchange": exchange, "asset": asset, "fiat": FIAT, "side": side,
            "price": price if price and price > 0 else None,
            "ads_count": None,
        })
    return records

def _daily_key(date_str):
    return f"{R2_DAILY_PREFIX}{date_str}.json"

def append_daily_records(r2, bucket, date_str, new_records):
    key = _daily_key(date_str)
    records = []
    try:
        obj  = r2.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        records = data.get("records", [])
    except r2.exceptions.NoSuchKey:
        pass
    except Exception as e:
        print(f"  ⚠️  Load R2 (daily {date_str}): {e} → tạo mới cho ngày này")
    records.extend(new_records)
    payload = {
        "schema_version": SCHEMA_VERSION, "date": date_str,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(records), "records": records,
    }
    r2.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=120",
    )
    return len(records)

def update_manifest(r2, bucket, date_str):
    manifest = {"schema_version": SCHEMA_VERSION, "first_date": date_str, "last_date": date_str, "dates": [date_str]}
    try:
        obj  = r2.get_object(Bucket=bucket, Key=R2_MANIFEST_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        dates = sorted(set(data.get("dates", [])) | {date_str})
        manifest = {"schema_version": SCHEMA_VERSION, "first_date": dates[0], "last_date": dates[-1], "dates": dates}
    except r2.exceptions.NoSuchKey:
        pass
    except Exception as e:
        print(f"  ⚠️  Load manifest: {e} → bỏ qua cập nhật manifest lần này")
        return
    r2.put_object(
        Bucket=bucket, Key=R2_MANIFEST_KEY,
        Body=json.dumps(manifest, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=300",
    )

def save_snapshot_daily(r2, bucket, snap):
    ts = snap[0]
    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    records = build_long_records(snap)
    total = append_daily_records(r2, bucket, date_str, records)
    update_manifest(r2, bucket, date_str)
    return date_str, total


# ══════════════════════════════════════════════════════════════════
# MỚI — LIQUIDITY INDEX (chỉ Binance, theo kiến trúc đã chốt)
# ══════════════════════════════════════════════════════════════════

def fetch_binance_ads_page(session, asset, trade_type, page):
    """Lấy 1 trang ads Binance. Trả về (items, total, ok)."""
    try:
        res = session.get(BNC_URL, params={
            "fiat": FIAT, "asset": asset, "tradeType": trade_type,
            "page": page, "rows": PAGE_SIZE,
        }, timeout=15)
        if res.status_code != 200:
            return [], 0, False
        body = res.json()
        data = body.get("data", {})
        items = data.get("items", []) if isinstance(data, dict) else []
        total = body.get("total", len(items))
        return items, total, True
    except Exception as e:
        print(f"  ⚠️  BNC liquidity page={page} lỗi: {e}")
        return [], 0, False


def fetch_binance_liquidity(session, asset, trade_type):
    """
    Phân trang lấy TOÀN BỘ ads, trả về:
    {
      liquidity_verified, liquidity_unverified, liquidity_total,
      merchant_count_verified, merchant_count_unverified, merchant_count_total,
      ad_count_raw, is_partial
    }
    Theo đúng công thức trong p2p-liquidity-architecture.md (mục 4).
    """
    merchants = {}   # userNo -> {"amount": float, "trust": "VERIFIED"/"UNVERIFIED"}
    page = 1
    total_seen = 0
    total_reported = None
    is_partial = False
    ad_count_raw = 0
    debug_printed = False

    while True:
        items, total, ok = fetch_binance_ads_page(session, asset, trade_type, page)
        if not ok:
            is_partial = True
            break
        if total_reported is None:
            total_reported = total

        if not items:
            break

        for item in items:
            ad_count_raw += 1

            # In log debug 1 lần duy nhất để xác nhận đúng field name thật
            if not debug_printed:
                print(f"  🔍 DEBUG [BNC {asset}/{trade_type}] mẫu keys 1 ad: {list(item.keys())}")
                adv = item.get("advertiser", {})
                print(f"  🔍 DEBUG advertiser keys: {list(adv.keys()) if isinstance(adv, dict) else 'KHÔNG có advertiser dict'}")
                debug_printed = True

            try:
                surplus = float(item.get("surplusAmount", 0) or 0)
                max_single = float(item.get("maxSingleTransAmount", 0) or 0)
                adv = item.get("advertiser", {}) or {}
                user_no = adv.get("userNo")
                month_order_count = int(adv.get("monthOrderCount", 0) or 0)
                # monthFinishRate Binance trả dạng 0-1 (VD 0.98) — nếu là dạng % (98) thì tự chia 100
                raw_finish_rate = adv.get("monthFinishRate", 0) or 0
                finish_rate = raw_finish_rate / 100 if raw_finish_rate > 1 else raw_finish_rate
            except Exception as e:
                print(f"  ⚠️  Parse ad lỗi, bỏ qua ad này: {e}")
                continue

            if not user_no:
                continue  # không xác định được merchant, bỏ qua để không tính sai dedupe

            # Áp cap cho SELL-side (merchant mua, tiền không đảm bảo thật)
            if trade_type == "SELL" and max_single > 0:
                amount = min(surplus, max_single * SELL_CAP_MULTIPLIER)
            else:
                amount = surplus

            trust = (
                "VERIFIED"
                if month_order_count >= VERIFIED_MIN_ORDER_COUNT and finish_rate >= VERIFIED_MIN_FINISH_RATE
                else "UNVERIFIED"
            )

            # Dedupe theo merchant: lấy MAX
            existing = merchants.get(user_no)
            if existing is None or amount > existing["amount"]:
                merchants[user_no] = {"amount": amount, "trust": trust}

        total_seen += len(items)
        if total_seen >= total_reported or page >= MAX_PAGE_SAFETY:
            if page >= MAX_PAGE_SAFETY and total_seen < total_reported:
                is_partial = True
            break
        page += 1
        time.sleep(0.2)  # nhẹ nhàng, tránh rate-limit

    liquidity_verified = sum(m["amount"] for m in merchants.values() if m["trust"] == "VERIFIED")
    liquidity_unverified = sum(m["amount"] for m in merchants.values() if m["trust"] == "UNVERIFIED")
    merchant_count_verified = sum(1 for m in merchants.values() if m["trust"] == "VERIFIED")
    merchant_count_unverified = sum(1 for m in merchants.values() if m["trust"] == "UNVERIFIED")

    return {
        "liquidity_verified": round(liquidity_verified, 2),
        "liquidity_unverified": round(liquidity_unverified, 2),
        "liquidity_total": round(liquidity_verified + liquidity_unverified, 2),
        "merchant_count_verified": merchant_count_verified,
        "merchant_count_unverified": merchant_count_unverified,
        "merchant_count_total": merchant_count_verified + merchant_count_unverified,
        "ad_count_raw": ad_count_raw,
        "is_partial": is_partial,
    }


def build_liquidity_records(session, ts):
    """Chạy Liquidity Index cho Binance, cả BUY/SELL, cả USDT/USDC."""
    records = []
    for asset in BNC_ASSETS:
        for side in ("BUY", "SELL"):
            print(f"  📊 Liquidity BNC {asset}/{side}...", flush=True)
            stats = fetch_binance_liquidity(session, asset, side)
            records.append({
                "record_type": "liquidity_snapshot",
                "ts": ts, "exchange": "binance", "asset": asset, "fiat": FIAT, "side": side,
                **stats,
            })
            print(f"     verified={stats['liquidity_verified']:,.0f}  "
                  f"unverified={stats['liquidity_unverified']:,.0f}  "
                  f"merchants={stats['merchant_count_total']}  "
                  f"partial={stats['is_partial']}")

    # Imbalance Index — tính riêng cho USDT (asset chính, thanh khoản nhất)
    usdt_records = {r["side"]: r for r in records if r["asset"] == "USDT"}
    if "BUY" in usdt_records and "SELL" in usdt_records:
        for kind in ("verified", "total"):
            l_buy = usdt_records["BUY"][f"liquidity_{kind}"]
            l_sell = usdt_records["SELL"][f"liquidity_{kind}"]
            denom = l_buy + l_sell
            imbalance = (l_sell - l_buy) / denom if denom > 0 else None
            records.append({
                "record_type": "imbalance_index",
                "ts": ts, "exchange": "binance", "asset": "USDT", "fiat": FIAT,
                "kind": kind,   # "verified" hoặc "total"
                "liquidity_buy": l_buy,
                "liquidity_sell": l_sell,
                "imbalance_index": round(imbalance, 4) if imbalance is not None else None,
            })

    return records


# ── Main ──────────────────────────────────────────────────────────
def main():
    print("💱 P2P Snapshot — Binance + OKX + Bybit / USDT+USDC / VND")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    print("📡 Fetching giá...", flush=True)
    snap = fetch_snapshot()
    ts, bnc_ub, bnc_us, bnc_cb, bnc_cs, okx_ub, okx_us, bbt_ub, bbt_us = snap

    print(f"   Binance  USDT BUY={bnc_ub:,}  SELL={bnc_us:,}  |  USDC BUY={bnc_cb:,}  SELL={bnc_cs:,}")
    print(f"   OKX      USDT BUY={okx_ub:,}  SELL={okx_us:,}")
    print(f"   Bybit    USDT BUY={bbt_ub:,}  SELL={bbt_us:,}")

    if not any(snap[1:]):
        print("❌ Tất cả giá = 0, bỏ qua upload")
        return

    r2, bucket = get_r2()

    print("💾 Saving legacy (p2p-data.json)...", flush=True)
    try:
        count = save_snapshot_legacy(r2, bucket, snap)
        print(f"✅ Legacy OK — {count:,} snapshots")
    except Exception as e:
        print(f"❌ Legacy save error: {e}")

    print("💾 Saving daily price partition...", flush=True)
    try:
        date_str, total = save_snapshot_daily(r2, bucket, snap)
        print(f"✅ Daily price OK — {date_str}: {total:,} records hôm nay")
    except Exception as e:
        print(f"❌ Daily price save error: {e}")
        raise

    # ── Lớp MỚI: Liquidity Index (độc lập, lỗi không ảnh hưởng 2 lớp trên) ──
    print("📊 Fetching Liquidity Index (Binance)...", flush=True)
    try:
        session = requests.Session(impersonate="chrome116")
        liquidity_records = build_liquidity_records(session, ts)
        total_liq = append_daily_records(r2, bucket, date_str, liquidity_records)
        print(f"✅ Liquidity Index OK — {date_str}: {total_liq:,} records hôm nay (tổng cả price + liquidity)")
    except Exception as e:
        print(f"❌ Liquidity Index error (không ảnh hưởng phần giá đã lưu ở trên): {e}")

if __name__ == "__main__":
    main()
