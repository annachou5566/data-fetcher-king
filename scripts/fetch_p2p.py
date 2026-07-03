"""
scripts/fetch_p2p.py
────────────────────────────────────────────────────────────────────
Bot: fetch P2P USDT/VND từ Binance + OKX + Bybit → lưu vào R2

Ghi SONG SONG 2 nơi mỗi lần chạy:

  1) LEGACY  — p2p-data.json (snapshot format v2, mảng vị trí, giữ tối đa
     MAX_KEEP bản ghi ~6 tháng). Giữ nguyên 100% logic cũ, KHÔNG đổi gì,
     để API/frontend hiện tại tiếp tục chạy không cần sửa gì thêm.

  2) MỚI — p2p-snapshots/YYYY-MM-DD.json (long-format, mỗi bản ghi có tên
     field rõ ràng, KHÔNG giới hạn thời gian lưu — vì partition theo ngày
     nên mỗi lần ghi chỉ đụng vào đúng 1 ngày, không phải đọc/ghi lại toàn
     bộ lịch sử). Đây là nền cho API/kho dữ liệu dài hạn sau này (kể cả
     bán API cho bên thứ 3) — schema ổn định, mở rộng không cần đổi version
     đọc dữ liệu cũ.

  Nếu 1 trong 2 lớp ghi lỗi, lớp còn lại vẫn chạy bình thường (độc lập,
  không phụ thuộc nhau) — bot không "chết" toàn bộ vì 1 lỗi cục bộ.
"""

import os, json, time, boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests

# ── Config ────────────────────────────────────────────────────────
R2_KEY_LEGACY   = "p2p-data.json"
MAX_KEEP        = 26_280        # ~6 tháng — CHỈ áp dụng cho file legacy
FIAT            = "VND"

R2_DAILY_PREFIX = "p2p-snapshots/"     # p2p-snapshots/2026-07-03.json
R2_MANIFEST_KEY = "p2p-snapshots/_manifest.json"
SCHEMA_VERSION  = 1

# Binance
BNC_URL     = "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/ad-list"
BNC_ASSETS  = ["USDT", "USDC"]

# OKX — public, không cần auth
OKX_URL     = "https://www.okx.com/v3/c2c/tradingOrders/books"

# Bybit — public endpoint mới (đổi từ list -> online)
BBT_URL     = "https://api2.bybit.com/fiat/otc/item/online"

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

# ── Fetchers (KHÔNG đổi gì so với bản gốc) ──────────────────────────

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

        # Parse cấu trúc mới của OKX: data là dict chứa key "buy" và "sell"
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

# ── Fetch tất cả song song (KHÔNG đổi gì so với bản gốc) ────────────
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

# ── LEGACY: save p2p-data.json (KHÔNG đổi gì so với bản gốc) ───────
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
        "v":         2,
        "updated":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":     len(snapshots),
        "snapshots": snapshots,
    }
    r2.put_object(
        Bucket=bucket, Key=R2_KEY_LEGACY,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=120",
    )
    return len(snapshots)

# ── MỚI: long-format records ────────────────────────────────────────
# Mỗi lần chạy sinh ra 8 record riêng biệt (1 record = 1 sàn+asset+side),
# thay vì gộp cứng vào 1 mảng vị trí. price=None (không phải 0) khi fetch
# lỗi/không có giá — đúng ngữ nghĩa "không có dữ liệu" thay vì "giá = 0".
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
            "ts":        ts,
            "exchange":  exchange,
            "asset":     asset,
            "fiat":      FIAT,
            "side":      side,
            "price":     price if price and price > 0 else None,
            "ads_count": None,   # để dành cho sau này (tracking theo phương thức thanh toán)
        })
    return records

def _daily_key(date_str):
    return f"{R2_DAILY_PREFIX}{date_str}.json"

# Ghi/append vào file của ĐÚNG 1 ngày — không đụng tới các ngày khác, nên
# tốc độ ghi không phụ thuộc vào tổng lịch sử đã tích luỹ (khác hẳn file
# legacy càng ngày càng nặng).
def append_daily_records(r2, bucket, date_str, new_records):
    key = _daily_key(date_str)
    records = []
    try:
        obj  = r2.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        records = data.get("records", [])
    except r2.exceptions.NoSuchKey:
        pass  # ngày mới, chưa có file — tạo mới bên dưới
    except Exception as e:
        print(f"  ⚠️  Load R2 (daily {date_str}): {e} → tạo mới cho ngày này")

    records.extend(new_records)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "date":           date_str,
        "updated":        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":          len(records),
        "records":        records,
    }
    r2.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=120",
    )
    return len(records)

# Manifest — danh sách ngày đã có dữ liệu, để sau này 1 API/Worker biết
# range dữ liệu tồn tại mà KHÔNG cần gọi R2 "list objects" (chậm + tốn phí
# hơn đọc 1 file JSON nhỏ).
def update_manifest(r2, bucket, date_str):
    manifest = {"schema_version": SCHEMA_VERSION, "first_date": date_str, "last_date": date_str, "dates": [date_str]}
    try:
        obj  = r2.get_object(Bucket=bucket, Key=R2_MANIFEST_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        dates = set(data.get("dates", []))
        dates.add(date_str)
        dates = sorted(dates)
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "first_date": dates[0],
            "last_date":  dates[-1],
            "dates":      dates,
        }
    except r2.exceptions.NoSuchKey:
        pass  # manifest chưa có — tạo mới với đúng 1 ngày hôm nay
    except Exception as e:
        print(f"  ⚠️  Load manifest: {e} → bỏ qua cập nhật manifest lần này")
        return

    r2.put_object(
        Bucket=bucket, Key=R2_MANIFEST_KEY,
        Body=json.dumps(manifest, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=300",
    )

def save_snapshot_daily(r2, bucket, snap):
    ts       = snap[0]
    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    records  = build_long_records(snap)
    total    = append_daily_records(r2, bucket, date_str, records)
    update_manifest(r2, bucket, date_str)
    return date_str, total

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

    r2, bucket = get_r2()

    print("💾 Saving legacy (p2p-data.json)...", flush=True)
    try:
        count = save_snapshot_legacy(r2, bucket, snap)
        print(f"✅ Legacy OK — {count:,} snapshots")
    except Exception as e:
        # Lỗi ở legacy KHÔNG được làm dừng cả job — layer mới vẫn phải chạy
        # (và ngược lại), vì frontend hiện tại phụ thuộc vào legacy còn
        # tương lai phụ thuộc vào layer mới — 2 bên độc lập nhau.
        print(f"❌ Legacy save error: {e}")

    print("💾 Saving daily partition (p2p-snapshots/...)...", flush=True)
    try:
        date_str, total = save_snapshot_daily(r2, bucket, snap)
        print(f"✅ Daily OK — {date_str}: {total:,} records hôm nay")
    except Exception as e:
        print(f"❌ Daily save error: {e}")
        raise   # layer mới là mục tiêu chính của thay đổi này — nếu lỗi thì fail job để được chú ý sớm

if __name__ == "__main__":
    main()
