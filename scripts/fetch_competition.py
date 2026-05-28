import json
import os
import time
import urllib.parse
from datetime import datetime, timedelta
from dotenv import load_dotenv
import cloudscraper
import boto3
from botocore.config import Config
import requests

# --- 1. CẤU HÌNH ---
load_dotenv()

R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL      = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME       = os.getenv("R2_BUCKET_NAME")
SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_KEY         = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
PROXY_WORKER_URL     = os.getenv("PROXY_WORKER_URL")
API_AGG_KLINES       = os.getenv("BINANCE_INTERNAL_KLINES_API")

# --- R2 CLIENT ---
def get_r2_client():
    if not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY:
        print("⚠️ Thiếu R2 Credentials!")
        return None
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )

session = cloudscraper.create_scraper()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Referer": "https://www.binance.com/en/alpha"
})

def fetch_smart(target_url, retries=3):
    is_render = "onrender.com" in (PROXY_WORKER_URL or "")
    if not target_url: return None
    for i in range(retries):
        if PROXY_WORKER_URL:
            try:
                encoded = urllib.parse.quote(target_url, safe='')
                proxy_url = f"{PROXY_WORKER_URL}?url={encoded}"
                timeout = 60 if (is_render and i == 0) else 30
                res = session.get(proxy_url, timeout=timeout)
                if res.status_code == 200:
                    data = res.json()
                    if isinstance(data, dict): return data
            except: pass
        try:
            res = session.get(target_url, timeout=15)
            if res.status_code == 200: return res.json()
        except: pass
        time.sleep(1)
    return None

def safe_float(v):
    try: return float(v) if v else 0.0
    except: return 0.0

def upload_r2(r2, key, obj):
    """Upload JSON object lên R2. Trả về True nếu thành công."""
    try:
        r2.put_object(
            Bucket=R2_BUCKET_NAME, Key=key,
            Body=json.dumps(obj, separators=(',', ':')).encode('utf-8'),
            ContentType='application/json',
            CacheControl='no-cache, no-store, must-revalidate'
        )
        return True
    except Exception as e:
        print(f"   ❌ R2 upload '{key}' error: {e}")
        return False

# ═══════════════════════════════════════════════════════════════
# PHASE 1 — SLOW JOB (giữ nguyên logic cũ, chỉ refactor gọn hơn)
# ═══════════════════════════════════════════════════════════════

def get_active_tournaments():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("⚠️ Thiếu cấu hình Supabase!")
        return []
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    try:
        url = f"{SUPABASE_URL}/rest/v1/tournaments?select=id,name,contract,data"
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            print(f"❌ Supabase Error: {res.status_code} - {res.text}")
            return []
        data = res.json()
        active_list = []
        lookback_date = datetime.now().strftime("%Y-%m-%d")
        for item in data:
            name = item.get("name", "Unknown")
            if name == "ARB" or item.get("id") == -1: continue
            meta = item.get("data") or {}
            contract = item.get("contract") or meta.get("contractAddress")
            end_date = meta.get("end")
            end_time = meta.get("endTime", "23:59")
            end_at_iso = f"{end_date}T{end_time}:00Z" if end_date else None
            if contract:
                if not end_date or end_date >= lookback_date:
                    chain_id = meta.get("chainId")
                    if chain_id:
                        active_list.append({
                            "symbol":     name,
                            "contract":   contract.lower().strip(),
                            "chainId":    chain_id,
                            "alphaId":    meta.get("alphaId"),
                            "quoteAsset": meta.get("quoteAsset", "USDT"),
                            "logo":       meta.get("iconUrl", ""),
                            "chainLogo":  meta.get("chainIconUrl", ""),
                            "end_at":     end_at_iso
                        })
                    else:
                        print(f"⚠️ {name}: Thiếu chainId")
        return active_list
    except Exception as e:
        print(f"❌ get_active_tournaments: {e}")
        return []


def fetch_limit_history(token_info):
    if not API_AGG_KLINES: return []
    alpha_id    = token_info.get("alphaId")
    contract    = token_info.get("contract")
    chain_id    = token_info.get("chainId")
    quote_asset = token_info.get("quoteAsset", "USDT")
    c_id_str    = str(chain_id).lower()
    if c_id_str == "8453" or "base" in c_id_str or "sol" in c_id_str:
        quote_asset = "USDC"
    if alpha_id:
        url = f"https://www.binance.com/bapi/defi/v1/public/alpha-trade/klines?symbol={alpha_id}{quote_asset}&interval=1h&limit=168"
    else:
        url = f"{API_AGG_KLINES}?chainId={chain_id}&interval=1h&limit=168&tokenAddress={contract}&dataType=limit"
    data = fetch_smart(url)
    chart_points = []
    k_infos = []
    if data and data.get("data"):
        if isinstance(data["data"], list):           k_infos = data["data"]
        elif data["data"].get("klineInfos"):         k_infos = data["data"]["klineInfos"]
    for k in k_infos:
        try:
            ts = int(k[0]); high = safe_float(k[2]); low = safe_float(k[3])
            limit_vol_usd = safe_float(k[7]); tx_count = int(k[8]) if len(k) > 8 else 0
            risk = 0
            if low > 0:
                sp = ((high - low) / low) * 100
                if sp > 5: risk = 2
                elif sp > 2: risk = 1
            if limit_vol_usd > 0 or tx_count > 0:
                chart_points.append([ts, int(limit_vol_usd), tx_count, risk])
        except: continue
    return chart_points


def run_slow_job(target_tokens, r2):
    """Phase 1: Fetch 7-day hourly history → upload competition-history.json"""
    print("=" * 52)
    print("📦 PHASE 1  —  7-Day History")
    print("=" * 52)
    history_data = {}
    for t in target_tokens:
        print(f"   📊 {t['symbol']}...", end=" ", flush=True)
        points = fetch_limit_history(t)
        if points:
            history_data[t["contract"]] = {
                "s":  t["symbol"],  "q": t["quoteAsset"],
                "l":  t["logo"],    "cl": t["chainLogo"],
                "e":  t.get("end_at"), "h": points
            }
            print(f"OK ({len(points)}h)")
        else:
            print("No Data")
        time.sleep(0.5)

    payload = {"updated_at": int(time.time() * 1000), "note": "7 Days Limit", "data": history_data}
    ok = upload_r2(r2, "competition-history.json", payload)
    print(f"   {'✅' if ok else '❌'} competition-history.json ({len(history_data)} tokens)")


# ═══════════════════════════════════════════════════════════════
# PHASE 2 — FAST LOOP (mới)
# ═══════════════════════════════════════════════════════════════

# Cache smart money: tránh gọi API mỗi 10s
_sm_cache = {}   # { "contract_chainId": { "data": {...}, "ts": float } }
SM_TTL    = 60   # Giây

def fetch_smart_money(contract, chain_id):
    key = f"{contract}_{chain_id}"
    cached = _sm_cache.get(key)
    if cached and time.time() - cached["ts"] < SM_TTL:
        return cached["data"]
    url = (
        "https://web3.binance.com/bapi/defi/v4/public/wallet-direct/"
        f"buw/wallet/market/token/dynamic/info?chainId={chain_id}&contractAddress={contract}"
    )
    data   = fetch_smart(url)
    result = (data or {}).get("data") or {}
    _sm_cache[key] = {"data": result, "ts": time.time()}
    return result


def fetch_1m_klines(token_info):
    """Lấy 4 nến 1 phút gần nhất (1 nến đang chạy + 3 đã đóng)."""
    alpha_id    = token_info.get("alphaId")
    contract    = token_info.get("contract")
    chain_id    = token_info.get("chainId")
    quote_asset = token_info.get("quoteAsset", "USDT")
    c_id_str    = str(chain_id).lower()
    if c_id_str == "8453" or "base" in c_id_str or "sol" in c_id_str:
        quote_asset = "USDC"
    if alpha_id:
        url = (
            "https://www.binance.com/bapi/defi/v1/public/alpha-trade/klines"
            f"?symbol={alpha_id}{quote_asset}&interval=1m&limit=4"
        )
    else:
        url = (
            f"{API_AGG_KLINES}"
            f"?chainId={chain_id}&interval=1m&limit=4&tokenAddress={contract}&dataType=limit"
        )
    data = fetch_smart(url)
    if not data or not data.get("data"): return []
    raw = data["data"]
    if isinstance(raw, list):                return raw
    if isinstance(raw, dict):
        return raw.get("klineInfos") or []
    return []


def compute_analysis(klines, sm):
    """
    Tính 7 chỉ số từ 1m klines + smart money data.

    klines layout:
      k[0] Open Time   k[1] Open    k[2] High    k[3] Low
      k[4] Close       k[5] Vol(USD agg?) k[6] CloseTime
      k[7] Limit Vol USD (trường mở rộng Binance Alpha)
      k[8] Tx Count    (trường mở rộng Binance Alpha)

    Return: dict với key minified hoặc None nếu thiếu data
    """
    if not klines or len(klines) < 2:
        return None

    def g(k, i, default=0.0):
        try: return float(k[i]) if len(k) > i and k[i] else default
        except: return default

    k_done = klines[-2]   # Nến đã đóng hoàn chỉnh gần nhất
    k_live = klines[-1]   # Nến đang chạy

    price      = g(k_live, 4) or g(k_done, 4)
    open_done  = g(k_done, 1)
    high_done  = g(k_done, 2)
    low_done   = g(k_done, 3)

    # Ưu tiên k[7] (limit vol); fallback k[5] (agg vol)
    limit_vol  = g(k_done, 7) or g(k_done, 5)
    tx_count   = int(g(k_done, 8)) if len(k_done) > 8 else 0

    if price <= 0:
        return None

    # SPEED — $/s dựa vào nến 60s vừa đóng
    speed = round(limit_vol / 60.0, 2) if limit_vol > 0 else 0.0

    # TICKET — avg order size
    ticket = round(limit_vol / tx_count, 2) if tx_count > 0 else 0.0

    # SPREAD proxy — High-Low range của nến đóng
    spread = round(((high_done - low_done) / low_done) * 100, 4) if low_done > 0 else 0.0

    # TREND — % thay đổi giá so với close nến trước đó
    close_prev = g(klines[-3], 4) if len(klines) >= 3 else open_done
    trend = round(((price - close_prev) / close_prev) * 100, 4) if close_prev > 0 else 0.0

    # DROP — % rớt khỏi đỉnh trong 4 nến
    highs = [g(k, 2) for k in klines if g(k, 2) > 0]
    peak  = max(highs) if highs else price
    drop  = round(((price - peak) / peak) * 100, 4) if peak > 0 else 0.0

    # NET FLOW — buy_1h - sell_1h từ smart money (cache 60s)
    buy_vol  = safe_float(sm.get("volume1hBuy",  0))
    sell_vol = safe_float(sm.get("volume1hSell", 0))
    net_flow = round(buy_vol - sell_vol, 2)

    # Trả về key minified để tiết kiệm bandwidth R2
    return {
        "sp":  speed,
        "tkt": ticket,
        "spd": spread,
        "tr":  trend,
        "dr":  drop,
        "fl":  net_flow,
        "p":   round(price, 10)
    }


def run_fast_update(target_tokens, r2):
    """
    1 vòng fast loop:
      - Fetch 1m klines + smart money cho mỗi token
      - Tính analysis
      - Upload competition-live.json → R2
    Trả về (n_ok, elapsed_s)
    """
    t0       = time.time()
    live_map = {}

    for token in target_tokens:
        contract = token.get("contract")
        chain_id = token.get("chainId")
        if not contract or not chain_id:
            continue
        try:
            klines = fetch_1m_klines(token)
            sm     = fetch_smart_money(contract, chain_id)
            result = compute_analysis(klines, sm)
            if result:
                live_map[contract] = result
        except Exception as e:
            print(f"   ⚠️ {token.get('symbol','?')}: {e}")
        time.sleep(0.25)   # 250ms throttle giữa các token

    if not live_map:
        return (0, time.time() - t0)

    payload = {"ts": int(time.time() * 1000), "d": live_map}
    upload_r2(r2, "competition-live.json", payload)

    return (len(live_map), time.time() - t0)


def run_fast_loop(target_tokens, r2, budget_sec=300, interval_sec=10):
    """
    Chạy fast loop trong budget_sec giây, nghỉ interval_sec giữa mỗi vòng.
    Mặc định: 5 phút, 10s/vòng → ~30 lần upload.
    """
    print()
    print("=" * 52)
    print("⚡ PHASE 2  —  Fast Loop  (competition-live.json)")
    print(f"   Budget: {budget_sec}s | Interval: {interval_sec}s")
    print("=" * 52)

    deadline   = time.time() + budget_sec
    loop_count = 0

    while time.time() < deadline:
        loop_count += 1
        n_ok, elapsed = run_fast_update(target_tokens, r2)

        remaining = deadline - time.time()
        print(
            f"   ⚡ #{loop_count:02d} | {n_ok}/{len(target_tokens)} tokens"
            f" | {elapsed:.1f}s | remain {remaining:.0f}s"
        )

        sleep_time = max(0, interval_sec - elapsed)
        if time.time() + sleep_time > deadline:
            break
        if sleep_time > 0:
            time.sleep(sleep_time)

    print(f"   ✅ Fast loop done — {loop_count} updates in {budget_sec}s")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    job_start = time.time()

    r2 = get_r2_client()
    if not r2: return

    # Lấy danh sách token MỘT LẦN — dùng chung cho cả 2 phase
    print("⏳ Lấy danh sách giải từ Supabase...", end=" ")
    target_tokens = get_active_tournaments()
    print(f"OK ({len(target_tokens)} giải)")

    if not target_tokens:
        print("❌ Không tìm thấy giải nào. Kiểm tra lại DB Supabase.")
        return

    # Phase 1: slow job (giữ nguyên như cũ)
    run_slow_job(target_tokens, r2)
    phase1_elapsed = time.time() - job_start
    print(f"   Phase 1 xong: {phase1_elapsed:.1f}s\n")

    # Phase 2: fast loop với phần thời gian còn lại
    # Tổng cửa sổ an toàn = 5.5 phút (cron 6 phút → còn 30s đệm)
    TOTAL_WINDOW = 5.5 * 60
    remaining_budget = max(30, TOTAL_WINDOW - phase1_elapsed)
    run_fast_loop(target_tokens, r2, budget_sec=int(remaining_budget), interval_sec=10)

    print(f"\n🏁 Tổng: {time.time() - job_start:.1f}s")


if __name__ == "__main__":
    main()
