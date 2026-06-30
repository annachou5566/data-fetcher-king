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
    """Tìm nến 1d đúng ngày niêm yết (dùng làm fallback nếu không lấy được nến 1h)."""
    match = None
    for k in k_infos:
        try:
            day = datetime.utcfromtimestamp(int(k[0]) / 1000).strftime('%Y-%m-%d')
        except Exception:
            continue
        if day == target_date_str:
            match = k
            break
    return match or (k_infos[0] if k_infos else None)


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


def fetch_listing_price(chain_id, contract, target_date_str):
    """
    Trả về {vwap, open, close, date} của ngày niêm yết, hoặc None nếu
    không tìm được dữ liệu nào / token chưa có klines nội bộ.

    vwap: giá đại diện chính — tính từ nến 1h trong đúng 24h ngày listing,
          phản ánh đúng vùng giá có nhiều volume nhất (không lệch thấp
          như open đơn thuần, không cần volume-profile phức tạp).
    open / close: giữ lại để tham khảo / fallback khi vwap không tính được
                  (ví dụ thiếu dữ liệu 1h, hoặc volume = 0 cả ngày).
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

        return {
            "vwap":  vwap if vwap is not None else close_price,  # fallback: dùng close nếu không có 1h data
            "open":  open_price,
            "close": close_price,
            "date":  actual_date,
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

        return {
            "vwap":  num / den,
            "open":  float(rows[0][1]),
            "close": float(rows[-1][4]),
            "date":  target_date_str,
            "source": "public_spot",   # đánh dấu nguồn khác để dễ debug sau này
        }
    except Exception as ex:
        if DEBUG: print(f"[debug] public spot fallback exception cho {symbol}: {ex}", end=" ")
        return None



def enrich_events(events):
    """Mutates events in-place, returns count of newly-filled entries."""
    filled = 0
    total_todo = sum(
        1 for e in events
        if not e.get("listing_price") and e.get("contract_address") and (e.get("event_time") or e.get("date"))
    )
    done = 0

    for e in events:
        if e.get("listing_price"):
            continue  # đã có rồi — idempotent, không fetch lại

        contract = e.get("contract_address")
        chain_id = e.get("chain_id")
        date_str = (e.get("event_time") or e.get("date") or "")[:10]
        symbol = e.get("symbol") or e.get("token") or "?"

        if not contract or not date_str:
            continue

        done += 1
        print(f"  [{done}/{total_todo}] {symbol} ({date_str})...", end=" ", flush=True)

        result = fetch_listing_price(chain_id, contract, date_str)

        if not result and e.get("spot_listed"):
            # Token đã graduate khỏi Alpha — thử nguồn CEX công khai
            result = fetch_listing_price_public_spot(symbol, date_str)
            if result and DEBUG:
                print("[debug] dùng fallback public_spot ", end="")

        if result:
            e["listing_price"] = result
            filled += 1
            print(f"OK  vwap=${result['vwap']:.6f}  (open=${result['open']:.6f})")
        else:
            e["listing_price"] = None  # đánh dấu đã thử, lần sau vẫn tự retry vì None là falsy
            print("no data (DEX pair not found)")

        time.sleep(0.15)  # nhẹ tay với proxy, tránh burst

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
