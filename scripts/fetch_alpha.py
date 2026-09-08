import json
import os
import time
import threading
import random
import re
import hashlib
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import requests
import cloudscraper
import boto3
from botocore.config import Config

# ─────────────────────────────────────────────
# 1. CẤU HÌNH
# ─────────────────────────────────────────────
load_dotenv()

# RUN_MODE: "market_data" | "tails_update" | "full"
RUN_MODE = os.getenv("RUN_MODE", "full")

# MAX_WORKERS  : số thread xử lý token song song (nên > MAX_CONCURRENT để thread luôn sẵn sàng)
# MAX_CONCURRENT: số HTTP request đồng thời tới Binance (đây là van an toàn thực sự)
MAX_WORKERS    = int(os.getenv("MAX_WORKERS", "6"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "4"))  # ← valve an toàn chống ban

R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL      = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME       = os.getenv("R2_BUCKET_NAME")

API_AGG_TICKER   = os.getenv("BINANCE_INTERNAL_AGG_API")
API_AGG_KLINES   = os.getenv("BINANCE_INTERNAL_KLINES_API")
API_PUBLIC_SPOT  = "https://api.binance.com/api/v3/exchangeInfo"

ACTIVE_SPOT_SYMBOLS = set()
OLD_DATA_MAP        = {}

# ─────────────────────────────────────────────
# 2. THREAD-SAFE INFRASTRUCTURE
# ─────────────────────────────────────────────

# Semaphore: giới hạn số HTTP request đồng thời tới Binance
# Dù MAX_WORKERS=6, chỉ tối đa MAX_CONCURRENT=4 request được gọi Binance cùng lúc
_request_semaphore = None  # khởi tạo trong fetch_data() sau khi đọc env

# Thread-local session: mỗi thread có cloudscraper riêng, tránh race condition
_thread_local = threading.local()

def get_session():
    """Lấy cloudscraper session của thread hiện tại. Tạo mới nếu chưa có."""
    if not hasattr(_thread_local, 'session'):
        s = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
        )
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/121.0.0.0 Safari/537.36",
            "Referer": "https://www.binance.com/en/alpha",
            "Origin":  "https://www.binance.com",
            "Accept":  "application/json",
        })
        _thread_local.session = s
    return _thread_local.session

# ─────────────────────────────────────────────
# 3. R2 CLIENT
# ─────────────────────────────────────────────
def get_r2_client():
    if not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY or not R2_ENDPOINT_URL or not R2_BUCKET_NAME:
        raise RuntimeError("Thiếu cấu hình R2 bắt buộc; fail closed.")
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )

# ─────────────────────────────────────────────
# 4. KEY MAP + MINIFY (giữ nguyên từ bản gốc)
# ─────────────────────────────────────────────
KEY_MAP = {
    "id": "i", "symbol": "s", "name": "n", "icon": "ic",
    "chain": "cn", "chain_icon": "ci", "contract": "ct",
    "status": "st", "price": "p", "change_24h": "c",
    "market_cap": "mc", "fdv": "f", "liquidity": "l", "volume": "v",
    "holders": "h",
    "rolling_24h": "r24", "daily_total": "dt",
    "daily_limit": "dl", "daily_onchain": "do",
    "chart": "ch", "listing_time": "lt", "tx_count": "tx",
    "offline": "off", "listingCex": "cex",
    "onlineTge": "tge",
    "onlineAirdrop": "air",
    "mul_point": "mp",
    # [SỬA] Cờ + volume thật cho nhóm token cổ phiếu tokenized (XOMon, VRTon...)
    # ist = is_stock (1/0), rv = real_vol (volume thị trường chứng khoán gốc),
    # tk = ticker mã chứng khoán gốc (XOM, VRT...) để hiển thị trong tooltip.
    "is_stock": "ist", "real_vol": "rv", "stock_ticker": "tk"
}

def minify_token_data(token):
    minified = {}
    minified[KEY_MAP["id"]]       = token.get("id")
    minified[KEY_MAP["symbol"]]   = token.get("symbol")
    minified[KEY_MAP["name"]]     = token.get("name")
    minified[KEY_MAP["icon"]]     = token.get("icon")
    minified[KEY_MAP["chain"]]    = token.get("chain")
    minified[KEY_MAP["chain_icon"]] = token.get("chain_icon")
    minified[KEY_MAP["contract"]] = token.get("contract")

    minified[KEY_MAP["status"]]     = token.get("status")
    minified[KEY_MAP["price"]]      = token.get("price")
    minified[KEY_MAP["change_24h"]] = token.get("change_24h")
    minified[KEY_MAP["mul_point"]]  = token.get("mul_point")

    minified[KEY_MAP["market_cap"]] = int(token.get("market_cap", 0))
    minified[KEY_MAP["fdv"]]        = int(token.get("fdv", 0))
    minified[KEY_MAP["holders"]]    = int(token.get("holders", 0))
    minified[KEY_MAP["liquidity"]]  = int(token.get("liquidity", 0))
    minified[KEY_MAP["tx_count"]]   = int(token.get("tx_count", 0))

    minified[KEY_MAP["listing_time"]]   = token.get("listing_time")
    minified[KEY_MAP["offline"]]        = 1 if token.get("offline")       else 0
    minified[KEY_MAP["listingCex"]]     = 1 if token.get("listingCex")    else 0
    minified[KEY_MAP["onlineTge"]]      = 1 if token.get("onlineTge")     else 0
    minified[KEY_MAP["onlineAirdrop"]]  = 1 if token.get("onlineAirdrop") else 0

    # [SỬA] chỉ xuất field is_stock/real_vol/ticker khi token thật sự là tokenized-stock,
    # tránh phình payload cho ~640 token Alpha bình thường không cần field này.
    if token.get("is_stock"):
        minified[KEY_MAP["is_stock"]]     = 1
        minified[KEY_MAP["real_vol"]]     = int(token.get("real_vol", 0))
        minified[KEY_MAP["stock_ticker"]] = token.get("stock_ticker", "")

    vol = token.get("volume", {})
    minified[KEY_MAP["volume"]] = {
        KEY_MAP["rolling_24h"]:  int(vol.get("rolling_24h", 0)),
        KEY_MAP["daily_total"]:  int(vol.get("daily_total", 0)),
        KEY_MAP["daily_limit"]:  int(vol.get("daily_limit", 0)),
        KEY_MAP["daily_onchain"]:int(vol.get("daily_onchain", 0)),
    }
    minified[KEY_MAP["chart"]] = token.get("chart", [])
    return minified

# ─────────────────────────────────────────────
# 5. FETCH SMART — DIRECT BINANCE ONLY
# ─────────────────────────────────────────────

def fetch_smart(target_url, retries=3):
    """
    Gọi trực tiếp Binance, không qua proxy trung gian.
    - Semaphore giới hạn đồng thời tối đa MAX_CONCURRENT requests.
    - Jitter nhỏ để tránh burst.
    - 418/429/503 retry có giới hạn; hết retry trả None để caller fail closed.
    - Không có paid/proxy fallback.
    """
    if not target_url or "None" in target_url:
        return None

    session = get_session()

    for attempt in range(retries):
        retry_wait = 0
        with _request_semaphore:
            time.sleep(random.uniform(0.3, 0.8))
            try:
                res = session.get(target_url, timeout=15)
            except Exception as exc:
                print(f"\n⚠️ Direct request error: {exc}", flush=True)
                res = None

            if res is not None:
                if res.status_code == 200:
                    try:
                        data = res.json()
                    except Exception:
                        data = None
                    if isinstance(data, dict):
                        if "symbols" in data:
                            return data
                        if data.get("code") == "000000":
                            return data
                        # Một số public Binance endpoints trả JSON object không có code wrapper.
                        return data

                elif res.status_code in (418, 429, 503):
                    retry_wait = 30
                    print(
                        f"\n⚠️ Direct HTTP {res.status_code} — bounded retry "
                        f"{attempt + 1}/{retries}",
                        flush=True,
                    )
                elif res.status_code == 502:
                    retry_wait = 2
                else:
                    print(
                        f"\n⚠️ Direct HTTP {res.status_code} — source unavailable",
                        flush=True,
                    )

        if attempt < retries - 1:
            time.sleep(retry_wait or 1)

    return None

# ─────────────────────────────────────────────
# 6. HELPERS (giữ nguyên từ bản gốc)
# ─────────────────────────────────────────────
def safe_float(v):
    try:   return float(v) if v else 0.0
    except: return 0.0

def load_old_data_from_r2(r2_client):
    if not r2_client: return {}
    try:
        obj  = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key='market-data.json')
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return {(t.get('i') or t.get('id')): t for t in data.get('data', []) if t.get('i') or t.get('id')}
    except Exception as e:
        print(f"⚠️ Không tải được cache từ R2 (Lần đầu chạy?): {e}")
        return {}

def get_active_spot_symbols():
    try:
        print("⏳ Check Spot Market...", end=" ", flush=True)
        data = fetch_smart(API_PUBLIC_SPOT)
        if data and "symbols" in data:
            res = {s["baseAsset"] for s in data["symbols"] if s["status"] == "TRADING"}
            print(f"OK ({len(res)})")
            return res
    except Exception:
        pass
    return set()

def fetch_details_optimized(chain_id, contract_addr):
    """Giữ nguyên logic gốc — chỉ dùng thread-local session thông qua fetch_smart."""
    if not API_AGG_KLINES: return 0, 0, 0, [], False

    no_lower_chains = ["CT_501", "CT_784"]
    clean_addr = str(contract_addr)
    if chain_id not in no_lower_chains:
        clean_addr = clean_addr.lower()

    base_url      = f"{API_AGG_KLINES}?chainId={chain_id}&interval=1d&limit=30&tokenAddress={clean_addr}"
    d_total       = 0.0
    d_limit       = 0.0
    chart_data    = []
    has_limit_vol = False

    try:
        res_limit = fetch_smart(f"{base_url}&dataType=limit")
        if res_limit and res_limit.get("data") and res_limit["data"].get("klineInfos"):
            k_infos = res_limit["data"]["klineInfos"]
            if k_infos:
                d_limit = safe_float(k_infos[-1][5])
                if d_limit > 0:
                    has_limit_vol = True
                elif len(k_infos) > 1 and safe_float(k_infos[-2][5]) > 0:
                    has_limit_vol = True
    except Exception:
        pass

    try:
        res_agg = fetch_smart(f"{base_url}&dataType=aggregate")
        if res_agg and res_agg.get("data") and res_agg["data"].get("klineInfos"):
            k_infos = res_agg["data"]["klineInfos"]
            if k_infos:
                d_total    = safe_float(k_infos[-1][5])
                chart_data = [{"p": safe_float(k[4]), "v": safe_float(k[5])} for k in k_infos]
    except Exception:
        pass

    d_market = max(d_total - d_limit, 0)
    return d_total, d_limit, d_market, chart_data, has_limit_vol


def fetch_stock_chart_only(chain_id, contract_addr):
    """
    [MỚI] Dành riêng cho token cổ phiếu tokenized (stockState=true).
    CHỈ gọi dataType=aggregate để lấy chart giá (giá trị thật, hợp lệ dù
    volume=0) — KHÔNG gọi dataType=limit vì luôn trả lỗi -5101 "current
    token not support limit data source" (đã kiểm chứng thực tế với TQQQon),
    tránh lãng phí 1 request/token/lần chạy luôn-luôn-thất-bại.
    """
    if not API_AGG_KLINES: return []
    no_lower_chains = ["CT_501", "CT_784"]
    clean_addr = str(contract_addr)
    if chain_id not in no_lower_chains:
        clean_addr = clean_addr.lower()
    url = f"{API_AGG_KLINES}?chainId={chain_id}&interval=1d&limit=30&tokenAddress={clean_addr}&dataType=aggregate"
    try:
        res = fetch_smart(url)
        if res and res.get("data") and res["data"].get("klineInfos"):
            k_infos = res["data"]["klineInfos"]
            return [{"p": safe_float(k[4]), "v": safe_float(k[5])} for k in k_infos]
    except Exception:
        pass
    return []

# ─────────────────────────────────────────────
# 7. PROCESS SINGLE TOKEN (logic giữ nguyên gốc, bỏ sleep)
# ─────────────────────────────────────────────
def process_single_token(item):
    """Xử lý 1 token song song. Throttle nằm trong fetch_smart, không cần sleep ở đây."""
    aid = item.get("alphaId")
    if not aid: return None

    vol_rolling    = safe_float(item.get("volume24h"))
    symbol         = item.get("symbol")
    contract       = item.get("contractAddress")
    chain_id       = item.get("chainId")
    is_offline     = item.get("offline", False)
    is_listing_cex = item.get("listingCex", False)

    # [SỬA] Token cổ phiếu/ETF tokenized (XOMon, VRTon, TQQQon...) — nhận diện qua
    # field `stockState` thật từ chính token-list API (đáng tin hơn dò hậu tố "on"
    # trong symbol). Nhóm này KHÔNG giao dịch qua Alpha DEX nên gọi agg-klines luôn
    # trả volume=0 hoặc lỗi -5101 "not support limit data source" — bỏ qua hẳn,
    # dùng thẳng rwaInfo.dynamicInfo.volume24h (volume thị trường chứng khoán thật)
    # làm số phụ hiển thị tooltip, còn daily_total vẫn giữ volume on-chain Alpha
    # (đúng bản chất "hoạt động trade trên Binance") theo yêu cầu hiển thị cả 2 số.
    is_stock     = bool(item.get("stockState"))
    real_vol     = 0.0
    stock_ticker = ""
    if is_stock:
        rwa_info = item.get("rwaInfo") or {}
        dyn_info = rwa_info.get("dynamicInfo") or {}
        meta_info = rwa_info.get("metaInfo") or {}
        real_vol     = safe_float(dyn_info.get("volume24h"))
        stock_ticker = meta_info.get("ticker", "") or ""

    status           = "ALPHA"
    need_limit_check = False
    force_skip_fetch = False  # True → bỏ qua API call dù vol_rolling > 0

    if is_stock:
        # Token cổ phiếu tokenized: không có breakdown limit/onchain, không cần
        # xác minh sống/chết qua klines (rwaInfo.openState đã cho biết đủ rồi).
        force_skip_fetch = True

    if is_offline:
        if is_listing_cex or symbol in ACTIVE_SPOT_SYMBOLS:
            status = "SPOT"
        else:
            status           = "PRE_DELISTED"
            need_limit_check = True

    if OLD_DATA_MAP and aid in OLD_DATA_MAP:
        old_item      = OLD_DATA_MAP[aid]
        cached_status = old_item.get(KEY_MAP["status"])

        if cached_status == "DELISTED":
            status = "DELISTED"
            if is_offline and not is_listing_cex and symbol not in ACTIVE_SPOT_SYMBOLS:
                # Re-verify 1 lần/ngày lúc 00:xx UTC (midnight run).
                # Các lần còn lại: skip hoàn toàn, dùng lại cache.
                if datetime.utcnow().hour == 0:
                    need_limit_check = True   # midnight → check lại
                else:
                    need_limit_check = False  # bình thường → skip
                    force_skip_fetch = True   # kể cả khi vol_rolling > 0

        elif cached_status == "SPOT":
            # SPOT đã xác nhận + vẫn offline/cex → không cần klines
            if is_offline and (is_listing_cex or symbol in ACTIVE_SPOT_SYMBOLS):
                force_skip_fetch = True
                status = "SPOT"

    should_fetch = (not force_skip_fetch) and (vol_rolling > 0 or need_limit_check)

    daily_total = daily_limit = daily_onchain = 0.0
    chart_data  = []

    if is_stock:
        # [SỬA] Không đi qua nhánh should_fetch bình thường (vốn gọi cả limit+aggregate).
        # Lấy chart giá thật qua fetch_stock_chart_only (chỉ 1 request, không lỗi),
        # daily_total = volume on-chain Alpha (đúng bản chất, đã có sẵn trong vol_rolling).
        print(f"📈 {symbol} (stock)...", end=" ", flush=True)
        chart_data  = fetch_stock_chart_only(chain_id, contract)
        daily_total = vol_rolling
        print("OK")
    elif should_fetch:
        print(f"📡 {symbol}...", end=" ", flush=True)
        try:
            d_t, d_l, d_m, chart, has_limit = fetch_details_optimized(chain_id, contract)
            daily_total, daily_limit, daily_onchain = d_t, d_l, d_m
            chart_data = chart

            if need_limit_check:
                if has_limit:
                    status = "ALPHA"
                    print("✅ ALIVE (Revived)")
                else:
                    status = "DELISTED"
                    print("❌ DEAD")
            else:
                if status == "DELISTED": status = "ALPHA"
                print("OK")

            if daily_total <= 0: daily_total = vol_rolling

        except Exception as e:
            print(f"⚠️ Err: {e}")
            daily_total = vol_rolling
            if need_limit_check: status = "DELISTED"
    else:
        daily_total = vol_rolling
        if status == "PRE_DELISTED": status = "DELISTED"
        # Reuse chart từ cache cho cả DEAD lẫn SPOT (không fetch lại)
        if status in ("DELISTED", "SPOT") and OLD_DATA_MAP and aid in OLD_DATA_MAP:
            old_item = OLD_DATA_MAP[aid]
            if old_item.get(KEY_MAP["chart"]):
                chart_data = old_item.get(KEY_MAP["chart"])

    return {
        "id": aid, "symbol": symbol, "name": item.get("name"),
        "icon": item.get("iconUrl"), "chain": item.get("chainName", ""),
        "chain_icon": item.get("chainIconUrl"), "contract": contract,
        "offline": is_offline, "listingCex": is_listing_cex, "status": status,
        "is_stock": is_stock, "real_vol": real_vol, "stock_ticker": stock_ticker,
        "onlineTge":    item.get("onlineTge", False),
        "onlineAirdrop": item.get("onlineAirdrop", False),
        "mul_point":    safe_float(item.get("mulPoint")),
        "listing_time": item.get("listingTime", 0),
        "tx_count":     safe_float(item.get("count24h")),
        "price":        safe_float(item.get("price")),
        "change_24h":   safe_float(item.get("percentChange24h")),
        "liquidity":    safe_float(item.get("liquidity")),
        "market_cap":   safe_float(item.get("marketCap")),
        "fdv":          safe_float(item.get("fdv")),
        "holders":      safe_float(item.get("holders")),
        "volume": {
            "rolling_24h":   vol_rolling,
            "daily_total":   daily_total,
            "daily_limit":   daily_limit,
            "daily_onchain": daily_onchain,
        },
        "chart": chart_data,
    }

# ─────────────────────────────────────────────
# 8. TAILS (giữ nguyên logic gốc, thêm parallel)
# ─────────────────────────────────────────────
def build_suffix_sum(klines, yesterday_str):
    """
    [SỬA] Nến 1m -> mỗi nến đúng 1 phút, KHÔNG còn chia đều 5 phút như bản cũ
    (bản cũ dùng nến 5m rồi chia vol/5 cho từng phút -> sai vì volume không
    phân bố đều trong 5 phút, gây lệch dailyTot = rolling24h - tail).
    """
    arr        = [0.0] * 1440
    minute_map = [0.0] * 1440
    if not klines: return arr

    for k in klines:
        try:
            dt = datetime.utcfromtimestamp(int(k[0]) / 1000.0)
            if dt.strftime('%Y-%m-%d') == yesterday_str:
                minute = dt.hour * 60 + dt.minute
                if 0 <= minute < 1440:
                    minute_map[minute] += float(k[5] or 0)
        except Exception:
            pass

    running_sum = 0.0
    for i in range(1439, -1, -1):
        running_sum += minute_map[i]
        arr[i]       = round(running_sum, 2)
    return arr

def _parse_tail_klines_response(res, aid, data_type):
    if not isinstance(res, dict):
        raise RuntimeError(f"Tail source unavailable: {aid}:{data_type}")

    code = str(res.get("code") or "")
    if code == "-5101":
        return "unsupported", []
    if code == "-5095":
        return "invalid_address", []
    if code and code != "000000":
        raise RuntimeError(f"Tail business code {code}: {aid}:{data_type}")

    data = res.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("klineInfos"), list):
        raise RuntimeError(f"Tail kline contract invalid: {aid}:{data_type}")

    return "supported", data.get("klineInfos") or []


def _fetch_tail_response_with_contract_retry(url, aid, data_type, attempts=2):
    """
    Retry only transient malformed-success/source envelopes. Explicit Binance
    business codes (-5101/-5095/other non-000000) are not retried or coerced.
    Persistent malformed responses still fail closed.
    """
    last_exc = None
    for attempt in range(max(1, attempts)):
        res = fetch_smart(url, retries=1)
        try:
            return _parse_tail_klines_response(res, aid, data_type)
        except RuntimeError as exc:
            message = str(exc)
            retryable = (
                message.startswith("Tail source unavailable:")
                or message.startswith("Tail kline contract invalid:")
            )
            if not retryable:
                raise
            last_exc = exc
            if attempt < max(1, attempts) - 1:
                time.sleep(0.3)
                continue
            raise

    raise last_exc or RuntimeError(
        f"Tail contract retry exhausted: {aid}:{data_type}"
    )


def _fetch_klines_page(base_url, data_type, end_ts, aid):
    """
    Chỉ dùng endTime để phân trang. Phân biệt explicit -5101 unsupported với
    source unavailable/malformed; missing không bao giờ bị ép thành zero.
    """
    url = f"{base_url}&dataType={data_type}&endTime={end_ts}"
    return _fetch_tail_response_with_contract_retry(
        url, aid, data_type, attempts=2
    )


def _fetch_full_day_klines(base_url, data_type, y_start_ts, y_end_ts, aid):
    all_rows = {}
    cursor_end = y_end_ts
    guard = 0
    capability = None

    while cursor_end > y_start_ts and guard < 10:
        guard += 1
        page_capability, rows = _fetch_klines_page(
            base_url, data_type, cursor_end, aid
        )
        if capability and page_capability != capability:
            raise RuntimeError(
                f"Tail capability drift: {aid}:{data_type}"
            )
        capability = page_capability

        if page_capability == "unsupported":
            return "unsupported", []
        if page_capability == "invalid_address":
            raise RuntimeError(
                f"Tail invalid address for active fetch: {aid}:{data_type}"
            )

        # Explicit successful empty result means no rows for this window.
        if not rows:
            break

        oldest_ts = None
        for k in rows:
            k_ts = int(k[0])
            all_rows[k_ts] = k
            if oldest_ts is None or k_ts < oldest_ts:
                oldest_ts = k_ts

        if oldest_ts is None or oldest_ts <= y_start_ts:
            break

        # Successful short page means Binance exhausted available historical
        # candles before the requested endTime. Stop here instead of paging
        # into pre-listing time for newly-listed tokens.
        if len(rows) < 1000:
            break

        if oldest_ts - 1 >= cursor_end:
            break

        cursor_end = oldest_ts - 1
        time.sleep(0.2)

    return capability or "supported", list(all_rows.values())


def _tail_clean_addr(t):
    contract = str(t.get("contractAddress"))
    # Lowercase only canonical EVM 0x-hex addresses. Preserve non-EVM
    # case-sensitive addresses such as TRON/Base58 (CT_195).
    if re.fullmatch(r"0x[0-9a-fA-F]+", contract):
        return contract.lower()
    return contract


def _offline_tail_alive(t):
    """
    Mirror process_single_token PRE_DELISTED liveness rule with current Binance
    first-party limit data. -5101 or no positive recent limit volume => exclude.
    Transport/business ambiguity raises and fails the whole artifact.
    """
    aid = str(t.get("alphaId") or "")
    chain_id = t.get("chainId")
    contract = t.get("contractAddress")
    if not aid or not contract or chain_id in (None, ""):
        raise RuntimeError(f"Offline tail identity invalid: {aid or 'UNKNOWN'}")

    url = (
        f"{API_AGG_KLINES}?chainId={chain_id}"
        f"&interval=1d&limit=30&tokenAddress={_tail_clean_addr(t)}"
        f"&dataType=limit"
    )
    capability, rows = _fetch_tail_response_with_contract_retry(
        url, aid, "limit-liveness", attempts=2
    )
    if capability == "unsupported":
        return False, "limit-unsupported"
    if capability == "invalid_address":
        return False, "bad-token-address"

    latest = safe_float(rows[-1][5]) if rows else 0.0
    previous = safe_float(rows[-2][5]) if len(rows) > 1 else 0.0
    alive = latest > 0 or previous > 0
    return alive, "positive-limit-volume" if alive else "no-recent-limit-volume"


def _build_live_tail_cohort(raw_tokens):
    online = []
    pending = []
    excluded_spot = 0
    seen = set()

    for t in raw_tokens:
        aid = str(t.get("alphaId") or "")
        if not aid:
            continue
        if aid in seen:
            raise RuntimeError(f"Tail cohort duplicate alphaId: {aid}")
        seen.add(aid)

        if not t.get("contractAddress") or t.get("chainId") in (None, ""):
            raise RuntimeError(f"Tail cohort missing identity: {aid}")

        if not bool(t.get("offline", False)):
            online.append(t)
            continue

        if bool(t.get("listingCex", False)):
            excluded_spot += 1
            continue

        pending.append(t)

    revived = []
    excluded_offline = 0
    unsupported_offline = 0
    invalid_address_offline = 0
    worker_errors = []

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, max(1, len(pending)))) as executor:
        futures = {
            executor.submit(_offline_tail_alive, t): t
            for t in pending
        }
        for future in as_completed(futures):
            t = futures[future]
            try:
                alive, reason = future.result()
                if alive:
                    revived.append(t)
                else:
                    excluded_offline += 1
                    if reason == "limit-unsupported":
                        unsupported_offline += 1
                    elif reason == "bad-token-address":
                        invalid_address_offline += 1
            except Exception as exc:
                worker_errors.append(
                    f"{t.get('alphaId')}: {exc}"
                )

    if worker_errors:
        raise RuntimeError(
            f"Offline tail liveness errors: {len(worker_errors)}; "
            "không publish artifact."
        )

    cohort = online + revived
    if not cohort:
        raise RuntimeError("Tail cohort rỗng; không publish artifact.")

    print(
        f"TAILS_COHORT live={len(cohort)} online={len(online)} "
        f"offline_probed={len(pending)} revived={len(revived)} "
        f"offline_excluded={excluded_offline} "
        f"offline_unsupported={unsupported_offline} "
        f"offline_bad_address={invalid_address_offline} "
        f"spot_excluded={excluded_spot}"
    )
    return cohort


def _fetch_tail_single(t, yesterday_str, y_start_ts, y_end_ts):
    aid      = str(t.get("alphaId") or "")
    chain_id = t.get("chainId")
    contract = t.get("contractAddress")

    if not aid or not contract or chain_id in (None, ""):
        raise RuntimeError(f"Tail identity invalid: {aid or 'UNKNOWN'}")

    base_url = (
        f"{API_AGG_KLINES}?chainId={chain_id}"
        f"&interval=1m&limit=1000&tokenAddress={_tail_clean_addr(t)}"
    )

    total_capability, rows_tot = _fetch_full_day_klines(
        base_url, "aggregate", y_start_ts, y_end_ts, aid
    )
    if total_capability != "supported":
        raise RuntimeError(f"Aggregate tail unsupported: {aid}")
    # build_suffix_sum([]) is an explicit all-zero 1440 series only because
    # the source response itself was successful and structurally valid.
    t_total = build_suffix_sum(rows_tot, yesterday_str)

    limit_applicable = str(chain_id) == "56"
    if not limit_applicable:
        return aid, t.get("symbol"), t_total, None, "not_applicable"

    limit_capability, rows_lim = _fetch_full_day_klines(
        base_url, "limit", y_start_ts, y_end_ts, aid
    )
    if limit_capability == "unsupported":
        return aid, t.get("symbol"), t_total, None, "unsupported"

    t_limit = build_suffix_sum(rows_lim, yesterday_str)
    return aid, t.get("symbol"), t_total, t_limit, "supported"


def generate_and_upload_tails(r2_client, raw_tokens, results):
    today_str     = datetime.utcnow().strftime('%Y-%m-%d')
    yesterday_dt  = datetime.utcnow() - timedelta(days=1)
    yesterday_str = yesterday_dt.strftime('%Y-%m-%d')
    force_tails   = os.getenv("FORCE_TAILS", "false").lower() == "true"

    # Ranh giới chính xác 00:00:00.000 -> 23:59:59.999 UTC của ngày trước.
    y_day_start = yesterday_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    y_start_ts  = int(y_day_start.timestamp() * 1000)
    y_end_ts    = y_start_ts + (24 * 60 * 60 * 1000) - 1

    # Chỉ skip khi HEAD chứng minh đúng schema + đúng boundary + complete.
    try:
        head = r2_client.head_object(Bucket=R2_BUCKET_NAME, Key='tails_cache.json')
        meta = head.get('Metadata') or {}
        already_complete = (
            meta.get('wa-schema') == '2'
            and meta.get('boundary-date') == yesterday_str
            and meta.get('complete') == 'true'
        )
        if not force_tails and already_complete:
            print("\n⏭️ tails_cache.json v2 đã complete cho đúng UTC boundary; bỏ qua.")
            return {"uploaded": False, "skipped": True, "boundary_date": yesterday_str}
    except Exception:
        pass

    # Live Binance state owns the tail cohort. Do not let stale market-data
    # status keep an offline token alive or hide a newly-online token.
    valid_tokens = _build_live_tail_cohort(raw_tokens)
    expected_ids = {
        str(t.get("alphaId")) for t in valid_tokens
    }
    limit_applicable_ids = {
        str(t.get("alphaId")) for t in valid_tokens
        if str(t.get("chainId")) == "56"
    }
    total_count = len(valid_tokens)

    print(
        f"\n🦊 Bắt đầu tạo Tails v2 — {total_count} tokens alive "
        f"(parallel, concurrent={MAX_CONCURRENT})"
    )

    tails_total = {}
    tails_limit = {}
    unsupported_limit_ids = set()
    completed   = [0]
    worker_errors = []
    _lock       = threading.Lock()

    def worker_wrapper(t):
        aid, symbol, t_total, t_limit, limit_capability = _fetch_tail_single(
            t, yesterday_str, y_start_ts, y_end_ts
        )
        with _lock:
            completed[0] += 1
            print(
                f"   [{completed[0]}/{total_count}] Tail {symbol}... "
                f"OK limit={limit_capability}",
                flush=True,
            )
        return aid, t_total, t_limit, limit_capability

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker_wrapper, t) for t in valid_tokens]
        for future in as_completed(futures):
            try:
                aid, t_total, t_limit, limit_capability = future.result()
                if aid:
                    aid = str(aid)
                    if t_total is not None:
                        tails_total[aid] = t_total
                    if limit_capability == "supported":
                        if t_limit is None:
                            raise RuntimeError(
                                f"Supported limit tail missing series: {aid}"
                            )
                        tails_limit[aid] = t_limit
                    elif limit_capability == "unsupported":
                        unsupported_limit_ids.add(aid)
            except Exception as exc:
                worker_errors.append(str(exc))

    if worker_errors:
        raise RuntimeError(
            f"Tail worker errors: {len(worker_errors)}; không publish artifact."
        )

    missing_total = sorted(expected_ids - set(tails_total))
    if missing_total:
        raise RuntimeError(
            "Tail coverage incomplete; không publish artifact. "
            f"missing_total={len(missing_total)}"
        )

    covered_total_ids = set(tails_total)
    supported_limit_ids = set(tails_limit)
    classified_limit_ids = supported_limit_ids | unsupported_limit_ids

    if supported_limit_ids & unsupported_limit_ids:
        raise RuntimeError(
            "Tail limit capability overlap; không publish artifact."
        )
    if classified_limit_ids != limit_applicable_ids:
        raise RuntimeError(
            "Tail limit capability incomplete; không publish artifact. "
            f"applicable={len(limit_applicable_ids)} "
            f"classified={len(classified_limit_ids)}"
        )
    if not classified_limit_ids.issubset(expected_ids):
        raise RuntimeError(
            "Tail limit capability outside total cohort; không publish artifact."
        )

    bad_total_shape = sorted(
        aid for aid in expected_ids
        if not isinstance(tails_total.get(aid), list) or len(tails_total[aid]) != 1440
    )
    bad_limit_shape = sorted(
        aid for aid in supported_limit_ids
        if not isinstance(tails_limit.get(aid), list) or len(tails_limit[aid]) != 1440
    )
    if bad_total_shape or bad_limit_shape:
        raise RuntimeError(
            "Tail series shape invalid; không publish artifact. "
            f"bad_total={len(bad_total_shape)} "
            f"bad_limit={len(bad_limit_shape)}"
        )

    tails_limit = {
        aid: tails_limit[aid]
        for aid in sorted(supported_limit_ids)
    }

    def stable_hash(ids):
        return hashlib.sha256(
            "\n".join(sorted(ids)).encode("utf-8")
        ).hexdigest()

    generated_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    payload = {
        "schema_version": 2,
        "boundary_date": yesterday_str,
        "window_start": datetime.utcfromtimestamp(y_start_ts / 1000).isoformat() + "Z",
        "window_end": datetime.utcfromtimestamp(y_end_ts / 1000).isoformat() + "Z",
        "generated_at": generated_at,
        "complete": True,

        "expected_token_count": len(expected_ids),
        "covered_total_count": len(covered_total_ids),
        "expected_ids_hash": stable_hash(expected_ids),
        "covered_total_ids_hash": stable_hash(covered_total_ids),

        "limit_applicable_token_count": len(limit_applicable_ids),
        "classified_limit_token_count": len(classified_limit_ids),
        "limit_applicable_ids_hash": stable_hash(limit_applicable_ids),
        "classified_limit_ids_hash": stable_hash(classified_limit_ids),

        "expected_limit_token_count": len(supported_limit_ids),
        "covered_limit_count": len(supported_limit_ids),
        "expected_limit_ids_hash": stable_hash(supported_limit_ids),
        "covered_limit_ids_hash": stable_hash(supported_limit_ids),

        "unsupported_limit_token_count": len(unsupported_limit_ids),
        "unsupported_limit_ids": sorted(unsupported_limit_ids),
        "unsupported_limit_ids_hash": stable_hash(unsupported_limit_ids),

        "total": tails_total,
        "limit": tails_limit,
    }

    body = json.dumps(payload, separators=(',', ':')).encode('utf-8')
    payload_sha256 = hashlib.sha256(body).hexdigest()

    print("☁️ Uploading validated Tails v2 to R2...")
    r2_client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key='tails_cache.json',
        Body=body,
        ContentType='application/json',
        Metadata={
            'wa-schema': '2',
            'boundary-date': yesterday_str,
            'complete': 'true',
            'payload-sha256': payload_sha256,
        },
    )

    # Bounded read-after-write verification without downloading the large body.
    head = r2_client.head_object(Bucket=R2_BUCKET_NAME, Key='tails_cache.json')
    meta = head.get('Metadata') or {}
    if (
        meta.get('wa-schema') != '2'
        or meta.get('boundary-date') != yesterday_str
        or meta.get('complete') != 'true'
        or meta.get('payload-sha256') != payload_sha256
        or int(head.get('ContentLength') or -1) != len(body)
    ):
        raise RuntimeError("Tail R2 postcondition mismatch after PUT.")

    print(
        f"✅ tails_cache.json v2 complete: boundary={yesterday_str}, "
        f"tokens={len(expected_ids)}"
    )
    return {
        "uploaded": True,
        "skipped": False,
        "boundary_date": yesterday_str,
        "payload_sha256": payload_sha256,
    }

# ─────────────────────────────────────────────
# 9. HÀM CHÍNH
# ─────────────────────────────────────────────
def fetch_data():
    global ACTIVE_SPOT_SYMBOLS, OLD_DATA_MAP, _request_semaphore
    start = time.time()

    # Khởi tạo semaphore ở đây (sau khi đọc env)
    _request_semaphore = threading.Semaphore(MAX_CONCURRENT)

    print(f"⚙️  RUN_MODE={RUN_MODE}  workers={MAX_WORKERS}  concurrent={MAX_CONCURRENT}")
    print(f"   Rate: ~{MAX_CONCURRENT} req / 1.2s avg ≈ {MAX_CONCURRENT * 50:.0f} req/phút (an toàn)")
    print("🌐 Upstream mode: direct Binance only; no proxy/paid fallback.")

    r2 = get_r2_client()

    results       = []
    target_tokens = []

    # ═══════════════════════════════════════════════
    # PHASE 1: MARKET DATA
    # ═══════════════════════════════════════════════
    if RUN_MODE in ("full", "market_data"):
        OLD_DATA_MAP        = load_old_data_from_r2(r2)
        ACTIVE_SPOT_SYMBOLS = get_active_spot_symbols()

        print("⏳ Lấy danh sách token...", end=" ", flush=True)
        try:
            raw_res = fetch_smart(API_AGG_TICKER)
        except Exception:
            raw_res = None
        if not raw_res:
            raise RuntimeError("Không lấy được Alpha ticker source; fail closed.")

        raw_data      = raw_res.get("data", [])
        target_tokens = sorted(raw_data, key=lambda x: safe_float(x.get("volume24h")), reverse=True)
        print(f"Done ({len(target_tokens)})")

        print(f"🚀 Processing {len(target_tokens)} tokens (parallel, {MAX_WORKERS} workers, {MAX_CONCURRENT} concurrent HTTP)...")

        worker_errors = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_single_token, t) for t in target_tokens]
            for future in as_completed(futures):
                try:
                    r = future.result()
                    if r:
                        results.append(r)
                except Exception as e:
                    worker_errors.append(str(e))
                    print(f"⚠️ Token worker error: {e}")

        if worker_errors:
            raise RuntimeError(
                f"Market token workers failed: {len(worker_errors)}; không publish partial artifact."
            )
        if not results:
            raise RuntimeError("Market result rỗng; không publish artifact.")

        results.sort(key=lambda x: x["volume"]["daily_total"], reverse=True)

        print(f"🔒 Minifying {len(results)} tokens...")
        minified_results = [minify_token_data(t) for t in results]

        final_output = {
            "meta": {
                "u": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "t": len(minified_results),
                "c": "WaveAlpha Data"
            },
            "data": minified_results
        }
        json_str = json.dumps(final_output, ensure_ascii=False, separators=(',', ':'))

        print("☁️ Uploading to Cloudflare R2...")
        try:
            r2.put_object(
                Bucket=R2_BUCKET_NAME,
                Key='market-data.json',
                Body=json_str.encode('utf-8'),
                ContentType='application/json',
                CacheControl='max-age=60'
            )
            print("✅ Uploaded market-data.json")

            today_str = datetime.now().strftime("%Y-%m-%d")
            r2.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=f'history/{today_str}.json',
                Body=json_str.encode('utf-8'),
                ContentType='application/json'
            )
            print(f"✅ Uploaded history/{today_str}.json")
        except Exception as e:
            print(f"❌ R2 Upload Failed: {e}")
            raise

    # ═══════════════════════════════════════════════
    # PHASE 2: TAILS (tails_update hoặc full)
    # ═══════════════════════════════════════════════
    if RUN_MODE in ("full", "tails_update"):

        # Nếu chạy riêng tails_update: load danh sách token + status từ R2 cache
        if RUN_MODE == "tails_update":
            OLD_DATA_MAP = load_old_data_from_r2(r2)

            print("⏳ Lấy danh sách token cho tails...", end=" ", flush=True)
            try:
                raw_res = fetch_smart(API_AGG_TICKER)
                if raw_res:
                    target_tokens = raw_res.get("data", [])
                    print(f"Done ({len(target_tokens)})")
                else:
                    raise RuntimeError("Không lấy được Alpha ticker source cho tails.")
            except Exception as e:
                raise RuntimeError(f"Tail source unavailable: {e}") from e

            # Dùng status từ cache (không re-fetch market data)
            for t in target_tokens:
                aid = t.get("alphaId")
                if not aid: continue
                cached_status = OLD_DATA_MAP.get(aid, {}).get(KEY_MAP["status"], "ALPHA")
                results.append({"id": aid, "status": cached_status})

        generate_and_upload_tails(r2, target_tokens, results)

    mode_label = {"full": "FULL", "market_data": "MARKET DATA", "tails_update": "TAILS"}.get(RUN_MODE, RUN_MODE)
    print(f"🏁 DONE [{mode_label}]! Total: {time.time() - start:.1f}s")

if __name__ == "__main__":
    fetch_data()
