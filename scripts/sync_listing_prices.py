"""
scripts/sync_listing_prices.py
───────────────────────────────
Backfill "listing_price" (giá ngày đầu niêm yết) cho từng event trong
alpha-events/*.json trên R2, dùng lại API_AGG_KLINES nội bộ — nguồn này
hoạt động cho MỌI token Alpha (kể cả token chưa từng lên CEX), khác với
endpoint /api/v3/klines công khai chỉ có token đã spot-listed.

Idempotent: chỉ fetch những event CHƯA có listing_price. Chạy lại bao
nhiêu lần cũng an toàn, không tốn thêm request cho token đã xử lý.

Env cần (đã có sẵn trong GitHub Secrets, dùng chung với fetch_alpha.py):
  BINANCE_INTERNAL_KLINES_API, PROXY_WORKER_URL,
  R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME
"""

import json
import os
import time
import threading
import random
import urllib.parse
import requests
import zipfile
import io
import csv as _csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from dotenv import load_dotenv
import boto3
from botocore.config import Config

load_dotenv()

# Tái sử dụng fetch_smart() + get_session() đã có sẵn (proxy, retry, jitter, 429-backoff)
import fetch_alpha as fa

MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "4"))
fa._request_semaphore = threading.Semaphore(MAX_CONCURRENT)

API_AGG_KLINES = os.getenv("BINANCE_INTERNAL_KLINES_API")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# [SỬA] fetch_listing_price chạy đa luồng (ThreadPoolExecutor) — lý do fail
# phải lưu theo TỪNG THREAD riêng (threading.local), KHÔNG được gắn lên
# function object dùng chung, nếu không các luồng sẽ ghi đè lẫn nhau và
# log sẽ hiện SAI lý do (của token khác) cho từng token.
_fail_reason_local = threading.local()

# Một số chain non-EVM dùng prefix CT_ trong API nội bộ (giống logic trong fetch_alpha.py)
NO_LOWER_CHAINS = {"CT_501", "CT_784", "501", "784"}


def get_r2():
    return boto3.client(
        's3',
        endpoint_url=os.getenv("R2_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version='s3v4'),
    )


def load_json(r2, key):
    try:
        obj = r2.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def upload_json(r2, key, data):
    body = json.dumps(data, default=str, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    r2.put_object(
        Bucket=R2_BUCKET_NAME, Key=key, Body=body,
        ContentType='application/json', CacheControl='public, max-age=60',
    )
    print(f"  [R2] {key} updated ({len(data)} events, {len(body)//1024}KB)")


def _find_day_candles_1d(k_infos, target_date_str):
    """
    Tìm nến 1d đúng ngày CỦA EVENT ĐANG XỬ LÝ (target_date_str) — có thể là
    ngày niêm yết (TGE) hoặc ngày của một đợt airdrop/claim sau đó, vì mỗi
    event trong all.json có event_time riêng và được gọi hàm này riêng.

    QUAN TRỌNG: KHÔNG được fallback về k_infos[0] khi không tìm thấy đúng
    ngày — trước đây code làm vậy và nó âm thầm trả về nến ĐẦU TIÊN trong
    mảng (thường gần ngày niêm yết gốc), khiến các event airdrop lần 2/3
    của cùng token bị gán nhầm giá của ngày niêm yết đầu tiên. Bug này rất
    khó phát hiện vì field "date" trong kết quả trông vẫn hợp lệ.

    Thay vào đó: cho phép dung sai ±1 ngày (lệch timezone/UTC-cutoff nhẹ)
    nhưng phải log rõ khi dùng dung sai, và trả None (không đoán bừa) nếu
    hoàn toàn không có nến nào gần target_date_str.
    """
    try:
        target_dt = datetime.strptime(target_date_str, '%Y-%m-%d')
    except Exception:
        return None

    exact = None
    nearest = None
    nearest_diff = None

    for k in k_infos:
        try:
            k_dt = datetime.utcfromtimestamp(int(k[0]) / 1000)
        except Exception:
            continue
        day = k_dt.strftime('%Y-%m-%d')
        if day == target_date_str:
            exact = k
            break
        diff = abs((k_dt - target_dt).total_seconds())
        if nearest_diff is None or diff < nearest_diff:
            nearest_diff = diff
            nearest = k

    if exact:
        return exact

    # Dung sai tối đa 1 ngày (86400s) — dùng cho lệch UTC-cutoff nhẹ, KHÔNG
    # phải để che lấp việc thiếu dữ liệu ở ngày event thực sự xa hơn nhiều.
    if nearest is not None and nearest_diff is not None and nearest_diff <= 86400:
        if DEBUG:
            near_day = datetime.utcfromtimestamp(int(nearest[0]) / 1000).strftime('%Y-%m-%d')
            print(f"[debug] không có nến đúng ngày {target_date_str}, dùng nến gần nhất {near_day} (lệch {nearest_diff/3600:.1f}h)", end=" ")
        return nearest

    return None


def _compute_vwap(hourly_candles, target_date_str):
    """
    VWAP = Σ(typical_price × volume) / Σ(volume), chỉ tính trong đúng 24h
    của ngày niêm yết. typical_price = (high+low+close)/3 — phản ánh đúng
    vùng giá mà phần lớn volume diễn ra, không bị lệch về giá mở cửa thấp
    như cách dùng open đơn thuần.
    """
    num, den = 0.0, 0.0
    for k in hourly_candles:
        try:
            day = datetime.utcfromtimestamp(int(k[0]) / 1000).strftime('%Y-%m-%d')
        except Exception:
            continue
        if day != target_date_str:
            continue
        high, low, close, vol = fa.safe_float(k[2]), fa.safe_float(k[3]), fa.safe_float(k[4]), fa.safe_float(k[5])
        if vol <= 0:
            continue
        typical = (high + low + close) / 3
        num += typical * vol
        den += vol
    return (num / den) if den > 0 else None


def _max_price_since(k_infos, since_date_str, ref_price=None):
    """
    Giá CAO NHẤT (theo nến high) kể từ since_date_str (ngày của event —
    listing hoặc airdrop) đến hiện tại. Dùng để trả lời câu hỏi "nếu bán
    đúng đỉnh thì lời bao nhiêu", đối chiếu với việc bán ngay lúc claim
    (vwap ngày event) hoặc hold tới hiện tại (giá now).

    Tái sử dụng LUÔN mảng nến 1d đã fetch sẵn cho fetch_listing_price
    (limit=1000 ngày, thường đã phủ từ lúc token mới list tới hiện tại)
    — không tốn thêm request nào.

    SANITY CHECK: API klines nội bộ đôi khi trả về 1 nến bị lỗi decimal/
    trùng địa chỉ (vd TAIKO ngày 2026-01-06 trả high = 4.9e19, trong khi
    giá thật ~$0.6). Nếu KHÔNG lọc, 1 nến rác này khiến "peak return" của
    riêng token đó là số vô nghĩa (hàng tỷ %), và vì "Avg peak" ở frontend
    là trung bình cộng thuần trên toàn bộ token, 1 outlier đủ để kéo sập
    cả con số trung bình của toàn chart.

    Quy tắc: 1 nến high chỉ được chấp nhận là đỉnh mới nếu nó không vượt
    quá MAX_JUMP_MULT (mặc định 50x) so với close của nến liền trước (hoặc
    ref_price = giá lúc event, nếu chưa có nến nào trước đó). Nến bất
    thường sẽ bị bỏ qua và log ra, không âm thầm nuốt.
    """
    try:
        since_dt = datetime.strptime(since_date_str, '%Y-%m-%d')
    except Exception:
        return None

    MAX_JUMP_MULT = 50  # không token nào x50 trong 1 ngày rồi giữ nguyên mãi — đó là lỗi data

    relevant = []
    for k in k_infos:
        try:
            k_dt = datetime.utcfromtimestamp(int(k[0]) / 1000)
        except Exception:
            continue
        if k_dt < since_dt:
            continue
        relevant.append((k_dt, k))
    relevant.sort(key=lambda x: x[0])

    best_price, best_date = None, None
    prev_close = ref_price  # điểm neo đầu tiên: giá lúc event (vwap/close)

    for k_dt, k in relevant:
        high = fa.safe_float(k[2])
        close = fa.safe_float(k[4])
        if high <= 0:
            continue

        if prev_close and prev_close > 0 and high > prev_close * MAX_JUMP_MULT:
            if DEBUG:
                print(f"[debug] bỏ qua nến bất thường {k_dt.date()}: high={high} vs prev_close={prev_close} (>{MAX_JUMP_MULT}x)", end=" ")
            if close > 0:
                prev_close = close
            continue

        if best_price is None or high > best_price:
            best_price = high
            best_date = k_dt.strftime('%Y-%m-%d')

        if close > 0:
            prev_close = close

    return {"price": best_price, "date": best_date} if best_price is not None else None


ALPHA_TRADE_KLINES_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/klines"


def fetch_alpha_trade_klines_official(alpha_id, interval, start_ms=None, end_ms=None, limit=1500):
    """
    Gọi API Alpha Klines CHÍNH THỨC, có docs công khai của Binance:
    https://developers.binance.com/docs/alpha/market-data/rest-api/klines

    [SỬA] KHÁC BIỆT SỐNG CÒN so với API nội bộ cũ (chainId+tokenAddress+
    dataType=aggregate): API này CÓ HỖ TRỢ THẬT startTime/endTime (docs
    xác nhận rõ). API nội bộ kia âm thầm LỜ HẲN startTime dù không báo
    lỗi gì — đã kiểm chứng bằng thực nghiệm: 2 lần chạy cách nhau 1 ngày,
    số nến trả về của MỌI token đều tăng đúng +1, bất kể startTime truyền
    vào là gì → chứng minh nó luôn trả "N nến gần nhất tính từ lúc gọi",
    khiến mọi event cũ hơn ~300 ngày luôn "rỗng" dù token vẫn còn sống.

    symbol format: "{alphaId}USDT" (vd "ALPHA_1011USDT") — alphaId lấy
    từ chính token list API (fetch_alpha_token_status_map trả về).

    Trả về list [[open_time, open, high, low, close, volume, close_time,
    ...], ...] — CÙNG FORMAT với klines Binance chuẩn (open_time ở mili-
    giây), tương thích thẳng với _compute_vwap/_max_price_since hiện có,
    không cần đổi gì thêm. Trả None nếu lỗi/không có dữ liệu.
    """
    if not alpha_id:
        return None
    params = {"symbol": f"{alpha_id}USDT", "interval": interval, "limit": min(limit, 1500)}
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    try:
        res = requests.get(
            ALPHA_TRADE_KLINES_URL, params=params, timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        if res.status_code != 200:
            return None
        payload = res.json()
        if payload.get("code") != "000000":
            return None
        data = payload.get("data")
        return data if isinstance(data, list) and data else None
    except Exception:
        return None


def fetch_listing_price(chain_id, contract, target_date_str, alpha_id=None):
    """
    Trả về {vwap, open, close, date, max_since} của ngày event (listing
    hoặc airdrop), hoặc None nếu không tìm được dữ liệu nào / token chưa
    có klines nội bộ.

    vwap: giá đại diện chính — tính từ nến 1h trong đúng 24h ngày event,
          phản ánh đúng vùng giá có nhiều volume nhất (không lệch thấp
          như open đơn thuần, không cần volume-profile phức tạp).
    open / close: giữ lại để tham khảo / fallback khi vwap không tính được
                  (ví dụ thiếu dữ liệu 1h, hoặc volume = 0 cả ngày).
    max_since: {price, date} — giá cao nhất từ ngày event đến hiện tại
               (để so sánh "bán lúc claim" vs "hold" vs "bán đúng đỉnh").

    alpha_id: nếu có (từ fetch_alpha_token_status_map), ƯU TIÊN dùng API
    Alpha klines CHÍNH THỨC trước — hỗ trợ startTime thật, không giới hạn
    "N nến gần nhất" như API nội bộ. Chỉ fallback về API nội bộ (chainId/
    tokenAddress) khi không có alpha_id hoặc API chính thức không có data
    (vd token đã fullyDelisted khỏi Alpha, API official không còn trả).
    """
    official_attempted = False
    official_note = "không có alphaId (symbol không có trong 637 token đối chiếu, hoặc chưa graduate/chưa từng lên Alpha)"

    if alpha_id:
        official_attempted = True
        try:
            target_ms = int(datetime.strptime(target_date_str, "%Y-%m-%d").timestamp() * 1000)
            day_start, day_end = target_ms, target_ms + 86400000 - 1

            k_hourly = fetch_alpha_trade_klines_official(alpha_id, "1h", start_ms=day_start, end_ms=day_end, limit=24)
            if k_hourly:
                open_price  = fa.safe_float(k_hourly[0][1])
                close_price = fa.safe_float(k_hourly[-1][4])
                vwap = _compute_vwap(k_hourly, target_date_str)
                ref_price = vwap if vwap is not None else close_price

                # Nến ngày để tính max_since — kể từ event tới hiện tại
                k_daily = fetch_alpha_trade_klines_official(alpha_id, "1d", start_ms=day_start, limit=1500)
                max_since = _max_price_since(k_daily, target_date_str, ref_price=ref_price) if k_daily else None

                _fail_reason_local.value = None
                return {
                    "vwap": ref_price,
                    "open": open_price,
                    "close": close_price,
                    "date": target_date_str,
                    "max_since": max_since,
                }
            official_note = f"API chính thức KHÔNG có nến 1h nào cho alphaId={alpha_id} trong ngày {target_date_str} (đã thử, trả rỗng)"
        except Exception as ex:
            official_note = f"API chính thức lỗi: {ex}"
            if DEBUG: print(f"[debug] {official_note}", end=" ")

    if not API_AGG_KLINES:
        _fail_reason_local.value = f"[official: {official_note}] API_AGG_KLINES secret rỗng/chưa truyền vào script"
        if DEBUG: print(f"[debug] {_fail_reason_local.value}", end=" ")
        return None
    if not chain_id or not contract:
        _fail_reason_local.value = f"[official: {official_note}] thiếu chain_id={chain_id!r} hoặc contract={contract!r} trong event data"
        if DEBUG: print(f"[debug] {_fail_reason_local.value}", end=" ")
        return None

    addr = str(contract)
    chain_variants = [str(chain_id)]
    if str(chain_id) in ("501", "784"):
        chain_variants.append(f"CT_{chain_id}")  # thử biến thể Solana/Sui

    fail_reason = "không có chain nào để thử (chain_id/contract rỗng?)"

    # [SỬA] BUG THẬT: không truyền startTime thì API (giống mọi API kiểu
    # Binance klines) mặc định trả về N nến GẦN NHẤT TÍNH TỪ BÂY GIỜ, chứ
    # không phải N nến kể từ ngày event. Với event càng cũ, cửa sổ trả về
    # càng không chạm tới được ngày cần tìm — đúng như log thực tế cho
    # thấy: token trả về đều đặn ~270-320 nến (không phải 1000 như đã xin)
    # và luôn KHÔNG khớp ngày với các event quá 300 ngày trước. Neo
    # startTime vào target_date_str (trừ đệm vài ngày) để cửa sổ trả về
    # LUÔN bao trùm đúng ngày cần tìm, bất kể event cũ bao lâu.
    try:
        target_ms = int(datetime.strptime(target_date_str, "%Y-%m-%d").timestamp() * 1000)
    except (ValueError, TypeError):
        target_ms = None
    day_start_ms = target_ms - 5 * 86400000 if target_ms is not None else None  # đệm 5 ngày trước

    for cid in chain_variants:
        clean_addr = addr if cid in NO_LOWER_CHAINS else addr.lower()
        base = f"{API_AGG_KLINES}?chainId={cid}&tokenAddress={clean_addr}&dataType=aggregate"

        # 1) Nến ngày — luôn cần để có open/close + xác định đúng ngày
        day_url = f"{base}&interval=1d&limit=1000"
        if day_start_ms is not None:
            day_url += f"&startTime={day_start_ms}"
        try:
            res_day = fa.fetch_smart(day_url, retries=2)
        except Exception as ex:
            fail_reason = f"fetch_smart exception (1d, chain={cid}): {ex}"
            if DEBUG: print(f"[debug] {fail_reason}", end=" ")
            res_day = None

        if res_day is None:
            fail_reason = f"API aggregator không trả dữ liệu (1d, chain={cid}) — proxy lỗi, hoặc token/contract không được API nhận diện"
            if DEBUG: print(f"[debug] fetch_smart trả None (1d, chain={cid}) — proxy/secret/network lỗi", end=" ")
            continue

        k_day = (res_day.get("data") or {}).get("klineInfos")
        if not k_day:
            fail_reason = f"API trả về NHƯNG không có klineInfos (chain={cid}) — token/contract này API aggregator không có data (dù token có thể vẫn đang sống trên Alpha)"
            if DEBUG: print(f"[debug] res_day có trả về nhưng không có klineInfos (chain={cid}). keys={list(res_day.keys())}", end=" ")
            continue

        day_match = _find_day_candles_1d(k_day, target_date_str)
        if not day_match:
            fail_reason = f"API có {len(k_day)} nến ngày (chain={cid}) nhưng KHÔNG khớp ngày {target_date_str} — có thể ngoài phạm vi lịch sử API giữ, hoặc target_date_str sai"
            if DEBUG: print(f"[debug] có {len(k_day)} nến nhưng không khớp ngày {target_date_str}", end=" ")
            continue

        actual_date = datetime.utcfromtimestamp(int(day_match[0]) / 1000).strftime('%Y-%m-%d')
        open_price  = fa.safe_float(day_match[1])
        close_price = fa.safe_float(day_match[4])

        # 2) Nến 1h trong đúng ngày đó — để tính VWAP chính xác hơn
        vwap = None
        try:
            hour_start_ms = int(day_match[0]) - 86400000  # đệm 1 ngày trước cho chắc
            hourly_url = f"{base}&interval=1h&limit=1000&startTime={hour_start_ms}"
            res_hourly = fa.fetch_smart(hourly_url, retries=1)
            k_hourly = (res_hourly or {}).get("data", {}).get("klineInfos") if res_hourly else None
            if k_hourly:
                vwap = _compute_vwap(k_hourly, actual_date)
        except Exception:
            pass

        ref_price = vwap if vwap is not None else close_price
        return {
            "vwap":  vwap if vwap is not None else close_price,  # fallback: dùng close nếu không có 1h data
            "open":  open_price,
            "close": close_price,
            "date":  actual_date,
            "max_since": _max_price_since(k_day, actual_date, ref_price=ref_price),
        }

    _fail_reason_local.value = f"[official: {official_note}] {fail_reason}"
    return None


BINANCE_VISION_BASE = "https://data.binance.vision"


def _download_vision_zip(url, retries=2):
    """
    Tải trực tiếp 1 file zip klines từ Binance Vision.
    Đây là file TĨNH trên CDN (S3), KHÔNG phải REST API — nên:
      - KHÔNG bị Binance chặn geo (451) như /api/v3/klines
      - KHÔNG cần proxy qua Render → KHÔNG tốn bandwidth Render
      - KHÔNG cần x-api-key
    Trả về bytes nếu có, None nếu file chưa được publish (404, bình thường
    với dữ liệu quá mới — ví dụ hôm nay/tháng này) hoặc lỗi mạng.
    """
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=20)
            if res.status_code == 200:
                return res.content
            if res.status_code == 404:
                return None  # chưa publish — không phải lỗi, cứ fallback proxy
        except Exception:
            pass
        time.sleep(0.5)
    return None


def _normalize_kline_row_ts(r):
    """
    Chuẩn hoá open_time (r[0]) và close_time (r[6], nếu có) về MILI-giây.

    [SỬA] Từ 1/1/2025, Binance Vision xuất timestamp ở MICRO-giây (16 chữ
    số) thay vì MILI-giây (13 chữ số) như REST API cũ:
    https://github.com/binance/binance-public-data
    ("The timestamp for SPOT Data from January 1st 2025 onwards will be
    in microseconds.")

    Không chuẩn hoá sẽ khiến int(ts)/1000 ra sai đơn vị 1000 lần →
    datetime.utcfromtimestamp() nhận giá trị khổng lồ → "year XXXXX is
    out of range". Ngưỡng 1e14 phân biệt an toàn: epoch mili-giây cho
    mọi ngày thực tế (1970–2100) luôn < 5e12, còn epoch micro-giây từ
    2025 trở đi luôn > 1.7e15 — không thể nhầm lẫn.
    """
    try:
        ot = float(r[0])
        if ot > 1e14:
            r[0] = str(ot / 1000.0)
        if len(r) > 6:
            ct = float(r[6])
            if ct > 1e14:
                r[6] = str(ct / 1000.0)
    except (ValueError, IndexError):
        pass
    return r


def _parse_vision_klines_csv(zip_bytes):
    """
    Giải nén + parse CSV klines từ Binance Vision thành list dạng
    [[open_time, open, high, low, close, volume, close_time, ...], ...]
    — CÙNG THỨ TỰ CỘT và CÙNG ĐƠN VỊ (mili-giây) như klines trả về từ
    REST API /api/v3/klines, nên toàn bộ code xử lý phía sau (VWAP,
    max-price-since,...) dùng index k[2]/k[3]/k[4]/k[5] không cần đổi gì.
    """
    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding='utf-8')
                reader = _csv.reader(text)
                for r in reader:
                    if not r or not r[0].strip():
                        continue
                    try:
                        float(r[0])  # bỏ dòng header (nếu có) không phải số
                    except ValueError:
                        continue
                    rows.append(_normalize_kline_row_ts(r))
    except Exception:
        return []
    return rows


def _binance_vision_daily_klines(symbol, interval, date_str):
    """1 file = 1 ngày. Dùng cho khung nhỏ (1h,...) — ví dụ VWAP 24h tại ngày listing."""
    pair = f"{symbol}USDT"
    url = f"{BINANCE_VISION_BASE}/data/spot/daily/klines/{pair}/{interval}/{pair}-{interval}-{date_str}.zip"
    content = _download_vision_zip(url)
    return _parse_vision_klines_csv(content) if content else None


def _binance_vision_monthly_klines(symbol, interval, year_month):
    """1 file = 1 tháng. Dùng cho khung lớn (1d,...) — tránh phải tải hàng trăm file ngày."""
    pair = f"{symbol}USDT"
    url = f"{BINANCE_VISION_BASE}/data/spot/monthly/klines/{pair}/{interval}/{pair}-{interval}-{year_month}.zip"
    content = _download_vision_zip(url)
    return _parse_vision_klines_csv(content) if content else None


def _try_vision_klines(symbol, interval, start_ms, limit):
    """
    Cố lấy klines từ Binance Vision trước. Trả None nếu không đủ dữ liệu
    (ví dụ khoảng thời gian quá mới, file chưa publish) để caller fallback
    sang proxy Render.
    """
    if start_ms is None:
        return None  # Vision cần biết đúng ngày/tháng — không hỗ trợ kiểu "N nến gần nhất tính từ giờ"
    if start_ms <= 0:
        # Trick "startTime=0 → nến sớm nhất" chỉ REST API Binance hỗ trợ,
        # Vision không có cách tương đương (không muốn dò từ 1970) → bỏ
        # qua Vision, để caller fallback thẳng qua proxy Render.
        return None

    start_dt = datetime.utcfromtimestamp(start_ms / 1000)
    rows = []

    if interval == "1d":
        # Gom các file THÁNG từ tháng listing tới tháng hiện tại
        cur = start_dt.replace(day=1)
        today = datetime.utcnow()
        months_tried = 0
        while cur <= today and months_tried < 36:  # chặn tối đa 3 năm, tránh vòng lặp vô hạn
            ym = cur.strftime("%Y-%m")
            part = _binance_vision_monthly_klines(symbol, interval, ym)
            if part:
                rows.extend(part)
            months_tried += 1
            cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)  # sang tháng kế tiếp
    else:
        # Gom các file NGÀY, đủ số ngày để phủ hết "limit" nến của khung nhỏ
        cur = start_dt.date()
        days_needed = max(1, (limit // 24) + 2)
        for _ in range(days_needed):
            part = _binance_vision_daily_klines(symbol, interval, cur.strftime("%Y-%m-%d"))
            if part:
                rows.extend(part)
            cur += timedelta(days=1)

    if not rows:
        return None

    rows = [r for r in rows if float(r[0]) >= start_ms]
    if not rows:
        return None
    rows.sort(key=lambda r: float(r[0]))
    return rows[:limit]


def _binance_public_klines(symbol, interval, start_ms=None, limit=1000, retries=2):
    """
    Lấy klines public Binance cho {symbol}USDT.

    [SỬA] KHÔNG còn dùng Render nữa — chỉ dùng Binance Vision
    (data.binance.vision, file tĩnh trên CDN). Lý do bỏ hẳn Render:
      - Binance ban theo IP. Render dùng IP chia sẻ (shared pool) — chỉ
        cần 1-2 request "xui" trúng lúc Binance đang nhạy cảm là dính 418,
        và ban đó ảnh hưởng LUÔN service Render khác (kể cả fetch_alpha.py
        cùng chạy trên đó).
      - Vision đã đủ dùng cho gần như mọi trường hợp thực tế (dữ liệu quá
        khứ tại ngày listing luôn có sẵn từ lâu, trừ token vừa list trong
        vài giờ/ngày gần nhất — trường hợp đó đơn giản là chưa có dữ liệu
        để tính, trả None và tự động thử lại ở lần chạy sau khi Vision đã
        publish, KHÔNG cần proxy chữa cháy).
    retries giữ lại trong signature để không phải sửa call site, nhưng
    không còn dùng (Vision không cần retry theo kiểu rate-limit).
    """
    return _try_vision_klines(symbol, interval, start_ms, limit)


def fetch_listing_price_public_spot(symbol, target_date_str):
    """
    Fallback cho token đã graduate khỏi Alpha (spot_listed=true): API klines
    DEX nội bộ thường không còn lịch sử cho nhóm này vì volume đã chuyển hẳn
    sang CEX order book. Dùng /api/v3/klines công khai của Binance qua proxy
    nội bộ (_binance_public_klines) — bắt buộc phải qua proxy vì Binance
    chặn IP GitHub Actions khi gọi thẳng.
    """
    try:
        start_ms = int(datetime.strptime(target_date_str, "%Y-%m-%d").timestamp() * 1000)
    except Exception:
        return None

    try:
        rows = _binance_public_klines(symbol, "1h", start_ms=start_ms, limit=24)
        if not isinstance(rows, list) or not rows:
            print(f"  [warn] public spot klines rỗng/lỗi cho {symbol} (kiểm tra proxy hoặc symbol có thật cặp USDT không)")
            return None

        num, den = 0.0, 0.0
        for k in rows:
            high, low, close, vol = float(k[2]), float(k[3]), float(k[4]), float(k[5])
            if vol <= 0:
                continue
            typical = (high + low + close) / 3
            num += typical * vol
            den += vol

        if den <= 0:
            return None

        max_since = None
        MAX_JUMP_MULT = 50  # cùng ngưỡng sanity-check như _max_price_since ở trên
        try:
            daily_rows = _binance_public_klines(symbol, "1d", start_ms=start_ms, limit=1000)
            if isinstance(daily_rows, list) and daily_rows:
                    best_price, best_date = None, None
                    prev_close = num / den if den > 0 else None  # neo bằng vwap ngày listing
                    for k in daily_rows:
                        high = float(k[2])
                        close = float(k[4])
                        if high <= 0:
                            continue
                        if prev_close and prev_close > 0 and high > prev_close * MAX_JUMP_MULT:
                            if DEBUG: print(f"[debug] bỏ qua nến bất thường public spot cho {symbol}: high={high} vs prev_close={prev_close}", end=" ")
                            if close > 0:
                                prev_close = close
                            continue
                        if best_price is None or high > best_price:
                            best_price = high
                            best_date = datetime.utcfromtimestamp(int(k[0]) / 1000).strftime('%Y-%m-%d')
                        if close > 0:
                            prev_close = close
                    if best_price is not None:
                        max_since = {"price": best_price, "date": best_date}
        except Exception as ex:
            if DEBUG: print(f"[debug] public spot max_since exception cho {symbol}: {ex}", end=" ")

        return {
            "vwap":  num / den,
            "open":  float(rows[0][1]),
            "close": float(rows[-1][4]),
            "date":  target_date_str,
            "max_since": max_since,
            "source": "public_spot",   # đánh dấu nguồn khác để dễ debug sau này
        }
    except Exception as ex:
        if DEBUG: print(f"[debug] public spot fallback exception cho {symbol}: {ex}", end=" ")
        return None



def fetch_spot_listing_date(symbol, since_date_str=None):
    """
    Lấy NGÀY THẬT token bắt đầu có lệnh khớp trên Binance spot — không đoán,
    không phụ thuộc cron job, không cần tra CMC/CoinGecko.

    [SỬA] KHÔNG còn dùng mẹo "startTime=0 → nến cũ nhất" nữa — đó là hành
    vi riêng của REST API Binance, phải đi qua Render nên có rủi ro bị
    Binance ban IP (đã dính thật ở lần chạy trước). Thay bằng: dò các file
    THÁNG (1d) trên Binance Vision, bắt đầu từ tháng token được list Alpha
    (since_date_str, nếu có) tiến dần về sau — vì ngày spot-listing luôn
    SAU ngày Alpha-listing nên không cần dò từ gốc lịch sử Binance (2017).
    File tháng đầu tiên có dữ liệu → nến đầu tiên trong đó chính là ngày
    spot-listing thật (Binance Vision chỉ có data từ ngày cặp bắt đầu giao
    dịch, không có nến rỗng phía trước).
    """
    try:
        if since_date_str:
            start_dt = datetime.strptime(since_date_str[:10], "%Y-%m-%d")
        else:
            start_dt = datetime.utcnow() - timedelta(days=730)  # không có hint: lùi tối đa 2 năm

        cur = start_dt.replace(day=1)
        today = datetime.utcnow()
        months_tried = 0
        while cur <= today and months_tried < 36:  # chặn tối đa 3 năm tìm kiếm
            ym = cur.strftime("%Y-%m")
            part = _binance_vision_monthly_klines(symbol, "1d", ym)
            if part:
                part.sort(key=lambda r: float(r[0]))
                open_ms = int(float(part[0][0]))
                return datetime.utcfromtimestamp(open_ms / 1000).strftime('%Y-%m-%d')
            months_tried += 1
            cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)

        print(f"  [warn] fetch_spot_listing_date: không tìm thấy dữ liệu Vision cho {symbol} (chưa có cặp {symbol}USDT hoặc chưa publish)")
        return None
    except Exception as ex:
        print(f"  [warn] fetch_spot_listing_date lỗi cho {symbol}: {ex}")
        return None




_ALPHA_STATUS_MAP = {}  # {SYMBOL: {"listingCex", "fullyDelisted", "alphaId"}} — set trong main()


def _get_alpha_id(symbol):
    st = _ALPHA_STATUS_MAP.get((symbol or "").upper())
    return st.get("alphaId") if st else None


def _process_one(e, idx, total):
    """Worker cho 1 token — chạy trong thread pool. Trả về (event, result|None)."""
    contract = e.get("contract_address")
    chain_id = e.get("chain_id")
    date_str = (e.get("event_time") or e.get("date") or "")[:10]
    symbol = e.get("symbol") or e.get("token") or "?"

    result = fetch_listing_price(chain_id, contract, date_str, alpha_id=_get_alpha_id(symbol))
    dex_fail_reason = getattr(_fail_reason_local, "value", None) if not result else None

    if not result and e.get("spot_listed") and not e.get("listing_price_unavailable"):
        # [SỬA] Token đã graduate khỏi Alpha — trước đây dùng NHẦM date_str
        # (ngày list Alpha) để tra giá spot, trong khi cặp SYMBOLUSDT
        # thường chỉ bắt đầu có nến THẬT SỰ nhiều tuần/tháng SAU đó → luôn
        # "rỗng" dù symbol đúng và có cặp thật (đã xác minh HEMI, SAPIEN,
        # HOLO đều đang giao dịch spot thật trên Binance). Giờ tìm NGÀY
        # SPOT-LISTING THẬT trước (từ chính klines Binance), rồi mới lấy
        # giá đúng ngày đó.
        # listing_price_unavailable=True nghĩa là đã đối chiếu API Alpha
        # và xác nhận token CHẾT HẲN (fullyDelisted, chưa từng lên CEX) —
        # bỏ qua nhánh này vì chắc chắn Binance spot sẽ không có gì, tránh
        # tốn request/thời gian dò ngày spot-listing vô ích.
        spot_date = fetch_spot_listing_date(symbol, since_date_str=date_str)
        if spot_date:
            result = fetch_listing_price_public_spot(symbol, spot_date)
            if result:
                e["spot_listed_at"] = spot_date

    tag = f"OK  vwap=${result['vwap']:.6f}  (open=${result['open']:.6f})" if result else "no data"
    print(f"  [{idx}/{total}] {symbol} ({date_str})... {tag}", flush=True)
    if not result and dex_fail_reason:
        print(f"      ↳ lý do DEX/aggregator fail: {dex_fail_reason}", flush=True)

    return e, result


def enrich_events(events):
    """
    Mutates events in-place, returns count of newly-filled entries.
    Chạy song song có giới hạn (MAX_CONCURRENT) thay vì tuần tự —
    416 token tuần tự dễ vượt timeout 15 phút của GitHub Actions,
    chạy song song giảm thời gian xuống còn ~1/N.
    """
    todo = [
        e for e in events
        if not e.get("listing_price")
        and e.get("contract_address")
        and (e.get("event_time") or e.get("date"))
    ]
    total = len(todo)
    filled = 0

    if not todo:
        return 0

    print(f"  Xử lý {total} token, chạy song song tối đa {MAX_CONCURRENT} luồng...")

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = {
            pool.submit(_process_one, e, i + 1, total): e
            for i, e in enumerate(todo)
        }
        for fut in as_completed(futures):
            e, result = fut.result()
            if result:
                e["listing_price"] = result
                filled += 1
            else:
                e["listing_price"] = None  # đánh dấu đã thử, lần sau vẫn tự retry vì None là falsy

    return filled


def _process_spot_one(e, idx, total):
    """Worker: xác định ngày spot-listing THẬT (từ klines Binance, không đoán),
    rồi fetch giá đúng ngày đó (tái dùng fetch_listing_price / public spot)."""
    contract = e.get("contract_address")
    chain_id = e.get("chain_id")
    symbol   = e.get("symbol") or e.get("token") or "?"
    alpha_date = (e.get("event_time") or e.get("date") or "")[:10] or None

    spot_date = fetch_spot_listing_date(symbol, since_date_str=alpha_date)
    if not spot_date:
        print(f"  [spot {idx}/{total}] {symbol}... không tìm được ngày spot-listing (symbol lệch/chưa có cặp USDT?)", flush=True)
        return e, None, None

    result = fetch_listing_price(chain_id, contract, spot_date, alpha_id=_get_alpha_id(symbol))
    if not result:
        result = fetch_listing_price_public_spot(symbol, spot_date)

    tag = f"OK  price=${result['vwap']:.6f}" if result else "no data"
    print(f"  [spot {idx}/{total}] {symbol} @ {spot_date}... {tag}", flush=True)
    return e, spot_date, result


def enrich_spot_listing_prices(events):
    """
    Với các event có spot_listed=true: tự tra ngày spot-listing THẬT của
    token trực tiếp từ Binance (fetch_spot_listing_date — nến sớm nhất của
    cặp SYMBOLUSDT), rồi fetch giá đúng ngày đó — để so sánh 3 chiến lược:
    bán trước spot (giá claim) / hold tới lúc spot (spot_listing_price) /
    hold dài hơn (giá hiện tại, tính ở frontend).

    Không phụ thuộc market-data.json hay cron-job diff nữa — chính xác hơn
    VÀ backfill được ngay cho token đã list từ lâu, không cần chờ transition.

    Idempotent: chỉ fetch event nào CHƯA có spot_listing_price.
    """
    todo = [
        e for e in events
        if e.get("spot_listed")
        and not e.get("spot_listing_price")
        and not e.get("listing_price_unavailable")
        and e.get("contract_address")
        and (e.get("symbol") or e.get("token"))
    ]
    total = len(todo)
    if not total:
        return 0

    print(f"  Xử lý {total} token cần giá lúc spot-listing, chạy song song tối đa {MAX_CONCURRENT} luồng...")
    filled = 0
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = {
            pool.submit(_process_spot_one, e, i + 1, total): e
            for i, e in enumerate(todo)
        }
        for fut in as_completed(futures):
            e, spot_date, result = fut.result()
            if spot_date:
                e["spot_listed_at"] = spot_date
            if result:
                e["spot_listing_price"] = result
                filled += 1
            else:
                e["spot_listing_price"] = None  # đánh dấu đã thử, None là falsy -> tự retry lần sau

    return filled


ALPHA_TOKEN_LIST_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"


def fetch_alpha_token_status_map():
    """
    Đối chiếu với danh sách token Alpha THẬT từ chính Binance — để phân
    biệt rạch ròi 2 trường hợp "no data" hoàn toàn khác nhau, mà log text
    (tên tag "DEX pair not found") không phân biệt được:

    1. Token đã fullyDelisted khỏi Alpha NHƯNG listingCex=true → đã
       GRADUATE lên spot thật (rời Alpha vì THÀNH CÔNG, không phải chết).
       Ví dụ thực tế: AIGENSYN, GENIUS, CHIP đều đúng case này.
    2. Token đã fullyDelisted khỏi Alpha VÀ listingCex=false → CHẾT THẬT
       (rút khỏi Alpha, chưa từng lên spot) — sẽ KHÔNG BAO GIỜ có klines
       Binance nào cả, retry mỗi 30 phút chỉ tốn request vô ích.

    Trả về dict {SYMBOL: {"listingCex": bool, "fullyDelisted": bool, "alphaId": str|None}}.
    Trả về {} nếu lỗi mạng/API — code gọi PHẢI coi thiếu dữ liệu là
    "chưa biết", tuyệt đối KHÔNG suy diễn thành "token chết", vì endpoint
    này chưa chắc đầy đủ 100% (không rõ có giới hạn/xoay vòng dữ liệu).
    """
    try:
        res = requests.get(
            ALPHA_TOKEN_LIST_URL, timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        if res.status_code != 200:
            print(f"  [warn] fetch_alpha_token_status_map: HTTP {res.status_code}")
            return {}
        payload = res.json()
        data = payload.get("data") or []
        out = {}
        for t in data:
            sym = (t.get("symbol") or "").upper()
            if not sym:
                continue
            out[sym] = {
                "listingCex": bool(t.get("listingCex")),
                "fullyDelisted": bool(t.get("fullyDelisted")),
                "alphaId": t.get("alphaId") or None,
            }
        return out
    except Exception as ex:
        print(f"  [warn] fetch_alpha_token_status_map lỗi (bỏ qua, coi như chưa biết): {ex}")
        return {}


def apply_alpha_status(events, status_map):
    """
    Mutates events in-place dựa trên status_map (từ fetch_alpha_token_status_map).
    Trả về (số token tự phát hiện graduate lên spot, số token đánh dấu chết hẳn).
    Chỉ xử lý event CHƯA có listing_price — không đụng vào event đã xong.
    """
    marked_spot, marked_dead = 0, 0
    if not status_map:
        return 0, 0

    for e in events:
        if e.get("listing_price"):
            continue
        sym = (e.get("symbol") or e.get("token") or "").upper()
        st = status_map.get(sym)
        if not st:
            continue  # không có trong danh sách API -> chưa biết, không suy diễn

        if st["fullyDelisted"] and st["listingCex"] and not e.get("spot_listed"):
            e["spot_listed"] = True
            marked_spot += 1
        elif st["fullyDelisted"] and not st["listingCex"]:
            if not e.get("listing_price_unavailable"):
                e["listing_price_unavailable"] = True
                marked_dead += 1

    return marked_spot, marked_dead


def main():
    print(f"🔑 API_AGG_KLINES configured: {bool(API_AGG_KLINES)}")
    print(f"🔑 PROXY_WORKER_URL configured: {bool(fa.PROXY_WORKER_URL)}  (chỉ dùng cho Alpha aggregator API — klines public đã chuyển hẳn sang Binance Vision, KHÔNG còn qua Render)")
    if DEBUG:
        print(f"   API_AGG_KLINES = {API_AGG_KLINES}")
        print(f"   PROXY_WORKER_URL = {fa.PROXY_WORKER_URL}")

    r2 = get_r2()
    print("⏳ Loading alpha-events/all.json from R2...")
    all_events = load_json(r2, "alpha-events/all.json")
    print(f"   {len(all_events)} events loaded")

    if not all_events:
        print("⚠️  No events found — nothing to backfill.")
        return

    print("⏳ Đối chiếu với danh sách token Alpha thật từ Binance...")
    status_map = fetch_alpha_token_status_map()
    global _ALPHA_STATUS_MAP
    _ALPHA_STATUS_MAP = status_map
    marked_spot, marked_dead = apply_alpha_status(all_events, status_map)
    print(f"   Đối chiếu {len(status_map)} token — tự phát hiện {marked_spot} token đã graduate lên spot, "
          f"đánh dấu {marked_dead} token đã chết hẳn (fullyDelisted, chưa từng lên CEX — sẽ không retry nữa)")

    filled = enrich_events(all_events)
    print(f"\n✅ Backfilled {filled} new listing prices")

    filled_spot = enrich_spot_listing_prices(all_events)
    print(f"✅ Backfilled {filled_spot} giá lúc spot-listing (ngày lấy trực tiếp từ klines Binance)")

    # Re-split theo status rồi ghi lại cả 4 file
    upcoming = [e for e in all_events if e.get("status") == "upcoming"]
    live     = [e for e in all_events if e.get("status") == "live"]
    history  = [e for e in all_events if e.get("status") in ("ended", None) or e.get("status") not in ("upcoming", "live")]

    upload_json(r2, "alpha-events/all.json",      all_events)
    upload_json(r2, "alpha-events/upcoming.json", upcoming)
    upload_json(r2, "alpha-events/live.json",     live)
    upload_json(r2, "alpha-events/history.json",  history)

    print("🏁 DONE")


if __name__ == "__main__":
    main()
