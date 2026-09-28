"""
scripts/fetch_p2p.py
────────────────────────────────────────────────────────────────────
Bot: fetch P2P USDT/VND từ Binance + OKX + Bybit → lưu vào R2

Ghi các lớp SONG SONG, độc lập nhau (1 lớp lỗi không làm chết lớp khác):

  1) LEGACY  — p2p-data.json (giữ nguyên 100%, không đổi gì)
  2) DAILY PRICE — p2p-snapshots/YYYY-MM-DD.json, record_type="price"
     (giữ nguyên 100%, không đổi gì)
  3) LIQUIDITY v1 — Binance historical compatibility record
     record_type="liquidity_snapshot" (không rewrite history cũ).

  4) LIQUIDITY v2 R1 — prospective per-exchange records for Binance/OKX/Bybit
     record_type="liquidity_v2_snapshot", same daily owner, versioned explicitly.

     v2 uses one symmetric bounded advertised-capacity rule on BUY and SELL.
     It is NOT executed volume and NOT a causal market-pressure signal.
     Cross-provider ALL remains excluded until cross-provider identity/capital
     double-count can be bounded honestly.
"""

import os, json, time, boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from curl_cffi import requests

# ── Config gốc (KHÔNG đổi) ──────────────────────────────────────────
R2_KEY_LEGACY   = "p2p-data.json"
WRITE_LEGACY   = os.getenv("P2P_WRITE_LEGACY", "1").strip().lower() not in {"0", "false", "no", "off"}
MAX_KEEP        = 26_280
FIAT            = "VND"

R2_DAILY_PREFIX = "p2p-snapshots/"
R2_MANIFEST_KEY = "p2p-snapshots/_manifest.json"
R2_MARKET_KEY   = "p2p-snapshots/_market-latest.json"
SCHEMA_VERSION  = 1
MARKET_SCHEMA_VERSION = 2

BNC_URL     = "https://www.binance.com/bapi/c2c/v1/public/c2c/agent/ad-list"
BNC_LIQUIDITY_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
BNC_ASSETS  = ["USDT", "USDC"]
OKX_URL     = "https://www.okx.com/v3/c2c/tradingOrders/books"
OKX_LIQUIDITY_URL = "https://www.okx.com/v3/c2c/tradingOrders/getMarketplaceAdsPrelogin"
BBT_URL     = "https://api2.bybit.com/fiat/otc/item/online"

LIQUIDITY_V2_METHODOLOGY = "p2p-liquidity-v2-r1"
LIQUIDITY_V2_CAP_MULTIPLIER = 3
LIQUIDITY_V2_FLAT_CAP_STABLE = 10_000
LIQUIDITY_V2_BYBIT_PAGE_SIZE = 50
LIQUIDITY_V2_BYBIT_MAX_PAGES = 20
LIQUIDITY_V2_AGGREGATE_STATUS = "EXCLUDED"
LIQUIDITY_V2_AGGREGATE_REASON = "cross_provider_identity_and_capital_overlap_not_proven"

# ── Config MỚI — Liquidity Index (theo kiến trúc đã chốt) ───────────
VERIFIED_MIN_ORDER_COUNT = 10
VERIFIED_MIN_FINISH_RATE = 0.85
SELL_CAP_MULTIPLIER      = 3
# Trần tuyệt đối, ĐỘC LẬP với maxSingleTransAmount merchant tự khai — theo
# đúng phương pháp P2P.army đang dùng (họ dùng flat 10,000 USDT/ad). Lý do
# cần thêm lớp này: cap ×3 một mình không đủ chặt, vì maxSingleTransAmount
# bản thân nó CŨNG là số tự khai, không có gì đảm bảo trung thực — merchant
# khai giới hạn lệnh "khủng" (vd 1.5 tỷ VND) thì cap ×3 vẫn cho phép 1 ad
# đóng góp hàng trăm nghìn đô vào tổng, gần như vô hiệu hoá mục đích chống
# ảo ban đầu. Lấy MIN của cả 2 cách → chặt hơn cách nào thì áp dụng cách đó.
SELL_CAP_FLAT_USDT       = 10_000
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
        # Fail closed: transient read/parse/auth errors are NOT equivalent to
        # NoSuchKey. Re-initialising here could overwrite the whole archive.
        raise RuntimeError(f"Không đọc được R2 legacy; từ chối ghi đè archive: {e}") from e
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
        # Only an actual NoSuchKey may initialise a new daily partition.
        # Any other read/JSON/auth/network error must abort this write.
        raise RuntimeError(
            f"Không đọc được R2 daily {date_str}; từ chối ghi đè partition: {e}"
        ) from e
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
        raise RuntimeError(
            f"Không đọc được P2P manifest; từ chối ghi manifest mới: {e}"
        ) from e
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
# LIQUIDITY v1 compatibility + prospective per-exchange v2
# ══════════════════════════════════════════════════════════════════

def fetch_binance_ads_page(session, asset, trade_type, page):
    """Lấy 1 trang ads Binance từ endpoint ĐẦY ĐỦ (adv/search).
    Trả về (items_chuẩn_hóa, total, ok) — mỗi item đã gộp sẵn field
    từ 'adv' + 'advertiser' cho đồng nhất với phần code xử lý phía sau.
    """
    try:
        res = session.post(BNC_LIQUIDITY_URL, json={
            "page": page, "rows": PAGE_SIZE, "payTypes": [],
            "asset": asset, "tradeType": trade_type, "fiat": FIAT,
        }, timeout=15)
        if res.status_code != 200:
            print(f"  ⚠️  BNC liquidity HTTP {res.status_code}: {res.text[:150]}")
            return [], 0, False
        body = res.json()
        raw_items = body.get("data", []) or []
        total = body.get("total", len(raw_items))

        items = []
        for entry in raw_items:
            adv = entry.get("adv", {}) or {}
            advertiser = entry.get("advertiser", {}) or {}
            # Gộp phẳng lại để phần xử lý phía dưới dùng chung logic
            merged = dict(adv)
            merged["advertiser"] = advertiser
            items.append(merged)

        return items, total, True
    except Exception as e:
        print(f"  ⚠️  BNC liquidity page={page} lỗi: {e}")
        return [], 0, False


def _first_present(d, keys):
    for k in keys:
        if d.get(k) not in (None, ""):
            return d.get(k)
    return None


def _normalize_pay_methods(item):
    raw = item.get("tradeMethods") or item.get("payTypes") or []
    out = []
    for method in raw:
        if isinstance(method, str):
            name = method.strip()
        elif isinstance(method, dict):
            name = str(
                method.get("tradeMethodName")
                or method.get("tradeMethodShortName")
                or method.get("identifier")
                or method.get("payType")
                or method.get("payId")
                or ""
            ).strip()
        else:
            name = ""
        if name and name not in out:
            out.append(name)
    return out


def _normalize_market_ad(item):
    try:
        price = float(item.get("price") or 0)
        if price <= 10000:
            return None
        min_fiat = float(_first_present(
            item, ["minSingleTransAmount", "minTransAmount", "minAmount"]
        ) or 0)
        max_fiat = float(_first_present(
            item, ["maxSingleTransAmount", "maxTransAmount", "maxAmount"]
        ) or 0)
        available = float(_first_present(
            item, ["surplusAmount", "tradableAmount", "tradableQuantity", "availableAmount"]
        ) or 0)
        advertiser = item.get("advertiser", {}) or {}
        month_orders = int(_first_present(advertiser, ["monthOrderCount"]) or 0)
        raw_finish_rate = float(_first_present(advertiser, ["monthFinishRate"]) or 0)
        finish_rate = raw_finish_rate / 100 if raw_finish_rate > 1 else raw_finish_rate
        merchant = str(_first_present(
            advertiser, ["nickName", "userNo", "advNo"]
        ) or "").strip()
        merchant_id = str(_first_present(
            advertiser, ["userNo", "advNo", "nickName"]
        ) or "").strip()
        ad_id = str(_first_present(item, ["advNo", "id"]) or "").strip()
    except Exception:
        return None

    return {
        "price": price,
        "minFiat": min_fiat,
        "maxFiat": max_fiat,
        "availableCrypto": available,
        "payTypes": _normalize_pay_methods(item),
        "merchant": merchant,
        "merchantId": merchant_id,
        "adId": ad_id,
        "providerOrderCount": month_orders,
        "providerCompletionRate": round(finish_rate, 6),
        "monthOrders": month_orders,
        "monthRate": round(finish_rate, 6),
    }


def fetch_binance_side(session, asset, trade_type):
    """Fetch one complete Binance side once and derive both liquidity + market ads.

    If pagination cannot reach Binance's reported total within MAX_PAGE_SAFETY,
    is_partial=True. Callers MUST NOT publish a new canonical market snapshot
    from partial data; keeping the previous last-good snapshot is safer.
    """
    merchants = {}
    market_ads = []
    page = 1
    total_seen = 0
    total_reported = None
    is_partial = False
    ad_count_raw = 0

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

            normalized = _normalize_market_ad(item)
            if normalized is not None:
                market_ads.append(normalized)

            try:
                surplus = float(_first_present(
                    item, ["surplusAmount", "tradableAmount", "tradableQuantity"]
                ) or 0)
                max_single_fiat = float(_first_present(
                    item, ["maxSingleTransAmount", "maxTransAmount"]
                ) or 0)
                price = float(item.get("price") or 0)
                max_single = (max_single_fiat / price) if price > 0 else 0
                adv = item.get("advertiser", {}) or {}
                user_no = _first_present(adv, ["userNo", "advNo", "nickName"])
                month_order_count = int(_first_present(adv, ["monthOrderCount"]) or 0)
                raw_finish_rate = float(_first_present(adv, ["monthFinishRate"]) or 0)
                finish_rate = raw_finish_rate / 100 if raw_finish_rate > 1 else raw_finish_rate
            except Exception as e:
                print(f"  ⚠️  Parse ad lỗi, bỏ qua ad này: {e}")
                continue

            if not user_no:
                continue

            if trade_type == "SELL":
                candidates = [surplus, SELL_CAP_FLAT_USDT]
                if max_single > 0:
                    candidates.append(max_single * SELL_CAP_MULTIPLIER)
                amount = min(candidates)
            else:
                amount = surplus

            trust = (
                "VERIFIED"
                if month_order_count >= VERIFIED_MIN_ORDER_COUNT and finish_rate >= VERIFIED_MIN_FINISH_RATE
                else "UNVERIFIED"
            )

            existing = merchants.get(user_no)
            if existing is None or amount > existing["amount"]:
                merchants[user_no] = {"amount": amount, "trust": trust}

        total_seen += len(items)
        if total_seen >= (total_reported or 0):
            break
        if page >= MAX_PAGE_SAFETY:
            is_partial = True
            break
        page += 1
        time.sleep(0.2)

    # Binance's reported total is a moving count. Ads may appear/disappear while
    # pages are being fetched, so a small final count drift is not proof of a
    # partial fetch. Only an actual page failure or the hard safety cap marks
    # the snapshot partial.
    liquidity_verified = sum(m["amount"] for m in merchants.values() if m["trust"] == "VERIFIED")
    liquidity_unverified = sum(m["amount"] for m in merchants.values() if m["trust"] == "UNVERIFIED")
    merchant_count_verified = sum(1 for m in merchants.values() if m["trust"] == "VERIFIED")
    merchant_count_unverified = sum(1 for m in merchants.values() if m["trust"] == "UNVERIFIED")

    stats = {
        "liquidity_verified": round(liquidity_verified, 2),
        "liquidity_unverified": round(liquidity_unverified, 2),
        "liquidity_total": round(liquidity_verified + liquidity_unverified, 2),
        "merchant_count_verified": merchant_count_verified,
        "merchant_count_unverified": merchant_count_unverified,
        "merchant_count_total": merchant_count_verified + merchant_count_unverified,
        "ad_count_raw": ad_count_raw,
        "market_ad_count": len(market_ads),
        "reported_ad_count": total_reported,
        "is_partial": is_partial,
    }
    return stats, market_ads


def _normalise_rate(value):
    try:
        rate = float(value or 0)
    except Exception:
        return None
    if rate > 1:
        rate /= 100
    return round(rate, 6) if rate >= 0 else None


def _identity_value(item, keys):
    for key in keys:
        value = item.get(key) if isinstance(item, dict) else None
        if value in (None, ""):
            continue
        text = str(value).strip()
        if not text or text.lower() in {"0", "-1", "none", "null"}:
            continue
        return text
    return ""


def _normalize_okx_v2_ad(item, asset):
    try:
        if str(item.get("quoteCurrency") or "").upper() != FIAT:
            return None
        if str(item.get("baseCurrency") or "").upper() != asset:
            return None
        price = float(item.get("price") or 0)
        min_fiat = float(item.get("quoteMinAmountPerOrder") or 0)
        max_fiat = float(item.get("quoteMaxAmountPerOrder") or 0)
        available = float(item.get("availableAmount") or 0)
        if price <= 10_000 or max_fiat <= 0 or available <= 0:
            return None
        merchant_id = _identity_value(item, ["merchantId", "publicUserId", "userId", "nickName"])
        ad_id = _identity_value(item, ["id", "advertisementId"])
        if not merchant_id or not ad_id:
            return None
    except Exception:
        return None

    methods = item.get("paymentMethods") or []
    methods = [str(x).strip() for x in methods if str(x).strip()] if isinstance(methods, list) else []
    return {
        "price": price,
        "minFiat": min_fiat,
        "maxFiat": max_fiat,
        "availableCrypto": available,
        "payTypes": methods,
        "merchant": str(item.get("nickName") or merchant_id).strip(),
        "merchantId": merchant_id,
        "adId": ad_id,
        "providerOrderCount": int(float(item.get("completedOrderQuantity") or 0)),
        "providerCompletionRate": _normalise_rate(item.get("completedRate")),
    }


def _okx_v2_items(body, user_side):
    data = body.get("data") if isinstance(body, dict) else None
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    rows = data.get(user_side.lower())
    if isinstance(rows, list):
        return rows
    return []


def fetch_okx_v2_side(session, asset, user_side):
    """Fetch one full unauthenticated OKX P2P book with explicit terminal paging."""
    ads = []
    seen_ids = set()
    page = 1
    partial = False
    raw_count = 0

    while page <= MAX_PAGE_SAFETY:
        try:
            res = session.get(OKX_LIQUIDITY_URL, params={
                "paymentMethod": "all",
                "userType": "all",
                "hideOverseasVerificationAds": "false",
                "sortType": "price_asc",
                "limit": "1000",
                "currentPage": str(page),
                "numberPerPage": "1000",
                "side": user_side,
                "fiatCurrency": FIAT,
                "cryptoCurrency": asset,
            }, timeout=15)
            if res.status_code != 200:
                partial = True
                break
            items = _okx_v2_items(res.json(), user_side)
        except Exception:
            partial = True
            break

        if not items:
            break

        new_ids = 0
        for item in items:
            raw_count += 1
            normalized = _normalize_okx_v2_ad(item, asset)
            if normalized is None:
                continue
            ad_id = normalized["adId"]
            if ad_id in seen_ids:
                continue
            seen_ids.add(ad_id)
            ads.append(normalized)
            new_ids += 1

        if new_ids == 0:
            # Endpoint ignored the page cursor or returned a repeating page.
            partial = True
            break

        page += 1
        time.sleep(0.12)
    else:
        partial = True

    return {
        "reported_ad_count": None,
        "ad_count_raw": raw_count,
        "market_ad_count": len(ads),
        "pages_fetched": max(1, page - 1),
        "is_partial": partial,
    }, ads


def _normalize_bybit_v2_ad(item, asset):
    try:
        if str(item.get("currencyId") or "").upper() != FIAT:
            return None
        if str(item.get("tokenId") or "").upper() != asset:
            return None
        price = float(item.get("price") or 0)
        min_fiat = float(item.get("minAmount") or 0)
        max_fiat = float(item.get("maxAmount") or 0)
        available = float(_first_present(item, ["lastQuantity", "quantity"]) or 0)
        if price <= 10_000 or max_fiat <= 0 or available <= 0:
            return None
        merchant_id = _identity_value(
            item, ["accountId", "userMaskId", "merchantId", "nickName", "userId"]
        )
        ad_id = _identity_value(item, ["id", "itemId"])
        if not merchant_id or not ad_id:
            return None
    except Exception:
        return None

    payment_ids = item.get("payments") or []
    payment_ids = [str(x).strip() for x in payment_ids if str(x).strip()] if isinstance(payment_ids, list) else []
    return {
        "price": price,
        "minFiat": min_fiat,
        "maxFiat": max_fiat,
        "availableCrypto": available,
        # Bybit's keyless endpoint exposes payment IDs here, not qualified
        # human-readable method names. Keep them separate; do not mislabel IDs.
        "payTypes": [],
        "paymentIds": payment_ids,
        "merchant": str(item.get("nickName") or merchant_id).strip(),
        "merchantId": merchant_id,
        "adId": ad_id,
        "providerOrderCount": int(float(item.get("recentOrderNum") or 0)),
        "providerCompletionRate": _normalise_rate(item.get("recentExecuteRate")),
        "sourceCreatedAt": item.get("createDate"),
    }


def fetch_bybit_v2_side(session, asset, user_side):
    """Fetch one complete Bybit keyless web book, bounded by reported total + hard cap.

    Bybit side is maker perspective: maker SELL (1) serves taker/user BUY,
    maker BUY (0) serves taker/user SELL.
    """
    provider_side = "1" if user_side == "BUY" else "0"
    ads = []
    seen_ids = set()
    first_total = None
    page = 1
    partial = False
    raw_count = 0

    while page <= LIQUIDITY_V2_BYBIT_MAX_PAGES:
        try:
            res = session.post(BBT_URL, json={
                "userId": "",
                "tokenId": asset,
                "currencyId": FIAT,
                "payment": [],
                "side": provider_side,
                "size": str(LIQUIDITY_V2_BYBIT_PAGE_SIZE),
                "page": str(page),
                "amount": "",
                "authMaker": False,
                "canTrade": False,
            }, timeout=15)
            if res.status_code != 200:
                partial = True
                break
            result = res.json().get("result", {}) or {}
            items = result.get("items", []) or []
            if first_total is None:
                try:
                    first_total = int(result.get("count"))
                except Exception:
                    first_total = None
        except Exception:
            partial = True
            break

        if not items:
            if first_total is not None and len(seen_ids) < first_total:
                partial = True
            break

        for item in items:
            raw_count += 1
            normalized = _normalize_bybit_v2_ad(item, asset)
            if normalized is None:
                continue
            ad_id = normalized["adId"]
            if ad_id in seen_ids:
                continue
            seen_ids.add(ad_id)
            ads.append(normalized)

        if first_total is not None and len(seen_ids) >= first_total:
            break
        if len(items) < LIQUIDITY_V2_BYBIT_PAGE_SIZE:
            if first_total is not None and len(seen_ids) < first_total:
                partial = True
            break

        page += 1
        time.sleep(0.12)
    else:
        partial = True

    return {
        "reported_ad_count": first_total,
        "ad_count_raw": raw_count,
        "market_ad_count": len(ads),
        "pages_fetched": page,
        "is_partial": partial,
    }, ads


def build_liquidity_v2_record(provider, asset, side, ads, ts, source_stats=None):
    """Canonical symmetric bounded advertised capacity for one venue/asset/side.

    Structural qualification is intentionally provider-neutral. Provider-native
    order/completion metrics are retained in ads for evidence but are NOT used
    as a cross-provider trust score because their measurement windows are not
    proven equivalent.
    """
    merchants = {}
    qualified_ads = 0
    payment_values = set()

    for ad in ads:
        try:
            price = float(ad.get("price") or 0)
            max_fiat = float(ad.get("maxFiat") or 0)
            available = float(ad.get("availableCrypto") or 0)
            merchant_id = str(ad.get("merchantId") or "").strip()
        except Exception:
            continue
        if price <= 10_000 or max_fiat <= 0 or available <= 0 or not merchant_id:
            continue

        max_order_crypto = max_fiat / price
        effective = min(
            available,
            LIQUIDITY_V2_FLAT_CAP_STABLE,
            max_order_crypto * LIQUIDITY_V2_CAP_MULTIPLIER,
        )
        if effective <= 0:
            continue

        qualified_ads += 1
        for method in (ad.get("payTypes") or []):
            if isinstance(method, str) and method.strip():
                payment_values.add(method.strip())
        for payment_id in (ad.get("paymentIds") or []):
            if isinstance(payment_id, str) and payment_id.strip():
                payment_values.add("id:" + payment_id.strip())

        contribution = {
            "crypto": effective,
            "vnd": effective * price,
        }
        current = merchants.get(merchant_id)
        if current is None or contribution["crypto"] > current["crypto"]:
            merchants[merchant_id] = contribution

    capacity_crypto = sum(x["crypto"] for x in merchants.values())
    capacity_vnd = sum(x["vnd"] for x in merchants.values())
    source_stats = source_stats or {}

    return {
        "record_type": "liquidity_v2_snapshot",
        "methodology_version": LIQUIDITY_V2_METHODOLOGY,
        "ts": ts,
        "exchange": provider,
        "asset": asset,
        "fiat": FIAT,
        "side": side,
        "capacity_crypto": round(capacity_crypto, 2),
        "capacity_vnd": round(capacity_vnd),
        "qualified_ad_count": qualified_ads,
        "qualified_merchant_count": len(merchants),
        "source_ad_count": len(ads),
        "reported_ad_count": source_stats.get("reported_ad_count"),
        "pages_fetched": source_stats.get("pages_fetched"),
        "is_partial": bool(source_stats.get("is_partial")),
        "qualification_policy": "structural_public_ad_v1",
        "capacity_policy": "min_available_flat10000_maxorderx3_v1",
        "merchant_dedupe": "max_contribution_per_provider_merchant",
        "payment_method_value_count": len(payment_values),
        "aggregate_eligible": False,
        "aggregate_status": LIQUIDITY_V2_AGGREGATE_STATUS,
        "aggregate_exclusion_reason": LIQUIDITY_V2_AGGREGATE_REASON,
    }


def fetch_binance_liquidity(session, asset, trade_type):
    """Compatibility wrapper for callers that only need liquidity stats."""
    stats, _ = fetch_binance_side(session, asset, trade_type)
    return stats


def build_liquidity_and_market(session, ts):
    """Build legacy v1 plus prospective per-exchange Liquidity v2 records.

    Binance remains the compatibility owner for the top-level canonical market
    shape. v2 provider books are attached under providers without changing the
    existing R2 key. Per-provider failure is fail-closed. Cross-provider ALL is
    intentionally not emitted while cross-venue identity/capital overlap is
    unproven.
    """
    records = []
    market_assets = {}
    provider_market = {}
    market_complete = True

    # Binance v1 compatibility + v2 source ads.
    binance_v2 = {}
    for asset in BNC_ASSETS:
        market_assets[asset] = {}
        binance_v2[asset] = {}
        for side in ("BUY", "SELL"):
            print(f"  📊 Liquidity/market BNC {asset}/{side}...", flush=True)
            stats, ads = fetch_binance_side(session, asset, side)
            records.append({
                "record_type": "liquidity_snapshot",
                "ts": ts, "exchange": "binance", "asset": asset, "fiat": FIAT, "side": side,
                **stats,
            })

            market_assets[asset][side] = {
                "ads": ads,
                "ad_count": len(ads),
                "reported_ad_count": stats.get("reported_ad_count"),
            }
            binance_v2[asset][side] = {
                "ads": ads,
                "ad_count": len(ads),
                "reported_ad_count": stats.get("reported_ad_count"),
                "complete": not stats.get("is_partial") and bool(ads),
            }

            if stats["is_partial"] or not ads:
                market_complete = False
            else:
                records.append(
                    build_liquidity_v2_record("binance", asset, side, ads, ts, stats)
                )

            print(
                f"     v1_verified={stats['liquidity_verified']:,.0f} "
                f"v1_unverified={stats['liquidity_unverified']:,.0f} "
                f"merchants={stats['merchant_count_total']} "
                f"market_ads={len(ads)} partial={stats['is_partial']}"
            )

    provider_market["binance"] = {
        "complete": market_complete,
        "assets": binance_v2,
    }

    # Historical v1 imbalance remains Binance-only and keeps its original
    # asymmetric semantics. It is not reused as a v2 pressure signal.
    usdt_records = {
        r["side"]: r
        for r in records
        if r.get("record_type") == "liquidity_snapshot"
        and r.get("exchange") == "binance"
        and r.get("asset") == "USDT"
    }
    if "BUY" in usdt_records and "SELL" in usdt_records:
        for kind in ("verified", "total"):
            l_buy = usdt_records["BUY"][f"liquidity_{kind}"]
            l_sell = usdt_records["SELL"][f"liquidity_{kind}"]
            denom = l_buy + l_sell
            imbalance = (l_sell - l_buy) / denom if denom > 0 else None
            records.append({
                "record_type": "imbalance_index",
                "ts": ts, "exchange": "binance", "asset": "USDT", "fiat": FIAT,
                "kind": kind,
                "liquidity_buy": l_buy,
                "liquidity_sell": l_sell,
                "imbalance_index": round(imbalance, 4) if imbalance is not None else None,
            })

    # Qualified v2 provider books. These do not affect Binance last-good market
    # publication: an OKX/Bybit issue cannot erase a healthy Binance market.
    for provider, fetcher in (
        ("okx", fetch_okx_v2_side),
        ("bybit", fetch_bybit_v2_side),
    ):
        provider_assets = {}
        provider_complete = True
        for asset in BNC_ASSETS:
            provider_assets[asset] = {}
            for side in ("BUY", "SELL"):
                print(f"  📊 Liquidity v2 {provider.upper()} {asset}/{side}...", flush=True)
                stats, ads = fetcher(session, asset, side)
                complete = not stats.get("is_partial") and bool(ads)
                provider_assets[asset][side] = {
                    "ads": ads if complete else [],
                    "ad_count": len(ads) if complete else 0,
                    "reported_ad_count": stats.get("reported_ad_count"),
                    "complete": complete,
                }
                if not complete:
                    provider_complete = False
                    print(
                        f"     EXCLUDED side: ads={len(ads)} partial={stats.get('is_partial')}"
                    )
                    continue

                record = build_liquidity_v2_record(provider, asset, side, ads, ts, stats)
                records.append(record)
                print(
                    f"     capacity={record['capacity_crypto']:,.2f} {asset} "
                    f"merchants={record['qualified_merchant_count']} ads={record['qualified_ad_count']}"
                )

        provider_market[provider] = {
            "complete": provider_complete,
            "assets": provider_assets,
        }

    market = None
    if market_complete:
        market = {
            "schema_version": MARKET_SCHEMA_VERSION,
            "record_type": "market_snapshot",
            "ts": ts,
            "exchange": "binance",
            "fiat": FIAT,
            "complete": True,
            "assets": market_assets,
            "providers": provider_market,
            "liquidity_v2": {
                "methodology_version": LIQUIDITY_V2_METHODOLOGY,
                "aggregate_status": LIQUIDITY_V2_AGGREGATE_STATUS,
                "aggregate_exclusion_reason": LIQUIDITY_V2_AGGREGATE_REASON,
            },
        }

    return records, market


def build_liquidity_records(session, ts):
    """Compatibility wrapper; current main uses build_liquidity_and_market()."""
    records, _ = build_liquidity_and_market(session, ts)
    return records


def save_market_snapshot(r2, bucket, market):
    if not market or not market.get("complete"):
        raise ValueError("refusing to publish incomplete market snapshot")
    r2.put_object(
        Bucket=bucket,
        Key=R2_MARKET_KEY,
        Body=json.dumps(market, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=30",
    )
    return sum(
        len(side.get("ads", []))
        for asset in market.get("assets", {}).values()
        for side in asset.values()
    )


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

    if WRITE_LEGACY:
        print("💾 Saving legacy (p2p-data.json)...", flush=True)
        try:
            count = save_snapshot_legacy(r2, bucket, snap)
            print(f"✅ Legacy OK — {count:,} snapshots")
        except Exception as e:
            print(f"❌ Legacy save error: {e}")
    else:
        print("ℹ️  Legacy p2p-data.json write disabled; canonical daily partitions remain active")

    print("💾 Saving daily price partition...", flush=True)
    try:
        date_str, total = save_snapshot_daily(r2, bucket, snap)
        print(f"✅ Daily price OK — {date_str}: {total:,} records hôm nay")
    except Exception as e:
        print(f"❌ Daily price save error: {e}")
        raise

    # ── Liquidity v1 compatibility + prospective per-exchange v2 ──
    print("📊 Fetching Liquidity v1 + per-exchange v2...", flush=True)
    try:
        session = requests.Session(impersonate="chrome116")
        liquidity_records, market_snapshot = build_liquidity_and_market(session, ts)
        total_liq = append_daily_records(r2, bucket, date_str, liquidity_records)
        print(f"✅ Liquidity write OK — {date_str}: {total_liq:,} records hôm nay (price + v1 + v2)")

        if market_snapshot:
            market_ads = save_market_snapshot(r2, bucket, market_snapshot)
            print(f"✅ Canonical market snapshot OK — {market_ads:,} sanitized ads")
        else:
            print("⚠️  Canonical market snapshot SKIPPED — partial/empty Binance ads; giữ last-good")
    except Exception as e:
        print(f"❌ Liquidity Index error (không ảnh hưởng phần giá đã lưu ở trên): {e}")

if __name__ == "__main__":
    main()
