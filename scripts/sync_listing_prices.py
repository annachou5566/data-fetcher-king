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
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
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


def fetch_listing_price(chain_id, contract, target_date_str):
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
    """
    if not API_AGG_KLINES:
        if DEBUG: print("[debug] API_AGG_KLINES secret rỗng/chưa truyền vào script", end=" ")
        return None
    if not chain_id or not contract:
        if DEBUG: print(f"[debug] thiếu chain_id={chain_id!r} hoặc contract={contract!r}", end=" ")
        return None

    addr = str(contract)
    chain_variants = [str(chain_id)]
    if str(chain_id) in ("501", "784"):
        chain_variants.append(f"CT_{chain_id}")  # thử biến thể Solana/Sui

    for cid in chain_variants:
        clean_addr = addr if cid in NO_LOWER_CHAINS else addr.lower()
        base = f"{API_AGG_KLINES}?chainId={cid}&tokenAddress={clean_addr}&dataType=aggregate"

        # 1) Nến ngày — luôn cần để có open/close + xác định đúng ngày
        try:
            res_day = fa.fetch_smart(f"{base}&interval=1d&limit=1000", retries=2)
        except Exception as ex:
            if DEBUG: print(f"[debug] fetch_smart exception (1d, chain={cid}): {ex}", end=" ")
            res_day = None

        if res_day is None:
            if DEBUG: print(f"[debug] fetch_smart trả None (1d, chain={cid}) — proxy/secret/network lỗi", end=" ")
            continue

        k_day = (res_day.get("data") or {}).get("klineInfos")
        if not k_day:
            if DEBUG: print(f"[debug] res_day có trả về nhưng không có klineInfos (chain={cid}). keys={list(res_day.keys())}", end=" ")
            continue

        day_match = _find_day_candles_1d(k_day, target_date_str)
        if not day_match:
            if DEBUG: print(f"[debug] có {len(k_day)} nến nhưng không khớp ngày {target_date_str}", end=" ")
            continue

        actual_date = datetime.utcfromtimestamp(int(day_match[0]) / 1000).strftime('%Y-%m-%d')
        open_price  = fa.safe_float(day_match[1])
        close_price = fa.safe_float(day_match[4])

        # 2) Nến 1h trong đúng ngày đó — để tính VWAP chính xác hơn
        vwap = None
        try:
            res_hourly = fa.fetch_smart(f"{base}&interval=1h&limit=1000", retries=1)
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

    return None


def fetch_listing_price_public_spot(symbol, target_date_str):
    """
    Fallback cho token đã graduate khỏi Alpha (spot_listed=true): API klines
    DEX nội bộ thường không còn lịch sử cho nhóm này vì volume đã chuyển hẳn
    sang CEX order book. Dùng thẳng /api/v3/klines công khai của Binance,
    không cần proxy nội bộ, vì token đã có cặp XXXUSDT chính thức.
    """
    try:
        start_ms = int(datetime.strptime(target_date_str, "%Y-%m-%d").timestamp() * 1000)
    except Exception:
        return None

    try:
        res = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={
                "symbol": f"{symbol}USDT",
                "interval": "1h",
                "startTime": start_ms,
                "limit": 24,   # đúng 24h ngày listing
            },
            timeout=10,
        )
        if res.status_code != 200:
            if DEBUG: print(f"[debug] public spot klines http {res.status_code} cho {symbol}", end=" ")
            return None
        rows = res.json()
        if not isinstance(rows, list) or not rows:
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
            res_daily = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={
                    "symbol": f"{symbol}USDT",
                    "interval": "1d",
                    "startTime": start_ms,
                    "limit": 1000,
                },
                timeout=10,
            )
            if res_daily.status_code == 200:
                daily_rows = res_daily.json()
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



def _process_one(e, idx, total):
    """Worker cho 1 token — chạy trong thread pool. Trả về (event, result|None)."""
    contract = e.get("contract_address")
    chain_id = e.get("chain_id")
    date_str = (e.get("event_time") or e.get("date") or "")[:10]
    symbol = e.get("symbol") or e.get("token") or "?"

    result = fetch_listing_price(chain_id, contract, date_str)

    if not result and e.get("spot_listed"):
        # Token đã graduate khỏi Alpha — thử nguồn CEX công khai
        result = fetch_listing_price_public_spot(symbol, date_str)

    tag = f"OK  vwap=${result['vwap']:.6f}  (open=${result['open']:.6f})" if result else "no data (DEX pair not found)"
    print(f"  [{idx}/{total}] {symbol} ({date_str})... {tag}", flush=True)

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


def main():
    print(f"🔑 API_AGG_KLINES configured: {bool(API_AGG_KLINES)}")
    print(f"🔑 PROXY_WORKER_URL configured: {bool(fa.PROXY_WORKER_URL)}")
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

    filled = enrich_events(all_events)
    print(f"\n✅ Backfilled {filled} new listing prices")

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
