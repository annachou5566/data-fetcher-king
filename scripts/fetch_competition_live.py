"""
fetch_competition_live.py
Chỉ chạy Phase 2 (fast loop) — độc lập với history fetcher.
GitHub Actions gọi script này mỗi 5 phút, loop 4.5 phút bên trong.
"""
import json, os, time, urllib.parse
from dotenv import load_dotenv
import cloudscraper
import boto3
from botocore.config import Config
import requests

load_dotenv()

R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL      = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME       = os.getenv("R2_BUCKET_NAME")
SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_KEY         = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
PROXY_WORKER_URL     = os.getenv("PROXY_WORKER_URL")
API_AGG_KLINES       = os.getenv("BINANCE_INTERNAL_KLINES_API")

def get_r2_client():
    if not R2_ACCESS_KEY_ID: return None
    return boto3.client('s3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4'))

session = cloudscraper.create_scraper()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Referer": "https://www.binance.com/en/alpha"
})

def fetch_smart(url, retries=2):
    if not url: return None
    for _ in range(retries):
        if PROXY_WORKER_URL:
            try:
                enc = urllib.parse.quote(url, safe='')
                r = session.get(f"{PROXY_WORKER_URL}?url={enc}", timeout=20)
                if r.status_code == 200 and isinstance(r.json(), dict): return r.json()
            except: pass
        try:
            r = session.get(url, timeout=12)
            if r.status_code == 200: return r.json()
        except: pass
    return None

def safe_float(v):
    try: return float(v) if v else 0.0
    except: return 0.0

# ── Supabase: lấy danh sách token 1 lần, cache suốt vòng loop ──

def get_active_tournaments():
    if not SUPABASE_URL or not SUPABASE_KEY: return []
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    try:
        res = requests.get(f"{SUPABASE_URL}/rest/v1/tournaments?select=id,name,contract,data",
                           headers=headers, timeout=10)
        if res.status_code != 200: return []
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        result = []
        for item in res.json():
            name = item.get("name", "")
            if name == "ARB" or item.get("id") == -1: continue
            meta = item.get("data") or {}
            contract = item.get("contract") or meta.get("contractAddress")
            end_date = meta.get("end")
            if contract and (not end_date or end_date >= today):
                chain_id = meta.get("chainId")
                if chain_id:
                    result.append({
                        "symbol":     name,
                        "contract":   contract.lower().strip(),
                        "chainId":    chain_id,
                        "alphaId":    meta.get("alphaId"),
                        "quoteAsset": meta.get("quoteAsset", "USDT"),
                        "end_at":     f"{end_date}T{meta.get('endTime','23:59')}:00Z" if end_date else None
                    })
        return result
    except Exception as e:
        print(f"❌ Supabase: {e}")
        return []

# ── Smart money cache ──

_sm_cache = {}

def fetch_smart_money(contract, chain_id):
    key = f"{contract}_{chain_id}"
    cached = _sm_cache.get(key)
    if cached and time.time() - cached["ts"] < 60:
        return cached["data"]
    url = (f"https://web3.binance.com/bapi/defi/v4/public/wallet-direct/"
           f"buw/wallet/market/token/dynamic/info?chainId={chain_id}&contractAddress={contract}")
    data = fetch_smart(url)
    result = (data or {}).get("data") or {}
    _sm_cache[key] = {"data": result, "ts": time.time()}
    return result

# ── 1-minute klines ──

def fetch_1m_klines(token):
    alpha_id    = token.get("alphaId")
    contract    = token.get("contract")
    chain_id    = token.get("chainId")
    quote_asset = token.get("quoteAsset", "USDT")
    if str(chain_id).lower() in ("8453", "base", "sol"):
        quote_asset = "USDC"
    if alpha_id:
        url = (f"https://www.binance.com/bapi/defi/v1/public/alpha-trade/klines"
               f"?symbol={alpha_id}{quote_asset}&interval=1m&limit=4")
    else:
        url = (f"{API_AGG_KLINES}?chainId={chain_id}&interval=1m&limit=4"
               f"&tokenAddress={contract}&dataType=limit")
    data = fetch_smart(url)
    if not data or not data.get("data"): return []
    raw = data["data"]
    if isinstance(raw, list): return raw
    if isinstance(raw, dict): return raw.get("klineInfos") or []
    return []

# ── Compute ──

def compute(klines, sm):
    if not klines or len(klines) < 2: return None
    def g(k, i):
        try: return float(k[i]) if len(k) > i and k[i] else 0.0
        except: return 0.0

    k_done = klines[-2]; k_live = klines[-1]
    price     = g(k_live, 4) or g(k_done, 4)
    high      = g(k_done, 2); low = g(k_done, 3)
    limit_vol = g(k_done, 7) or g(k_done, 5)
    tx_count  = int(g(k_done, 8))
    if price <= 0: return None

    close_prev = g(klines[-3], 4) if len(klines) >= 3 else g(k_done, 1)
    highs      = [g(k, 2) for k in klines if g(k, 2) > 0]
    peak       = max(highs) if highs else price

    return {
        "sp":  round(limit_vol / 60.0, 2)           if limit_vol > 0 else 0.0,
        "tkt": round(limit_vol / tx_count, 2)       if tx_count  > 0 else 0.0,
        "spd": round((high - low) / low * 100, 4)   if low > 0       else 0.0,
        "tr":  round((price - close_prev) / close_prev * 100, 4) if close_prev > 0 else 0.0,
        "dr":  round((price - peak) / peak * 100, 4) if peak > 0      else 0.0,
        "fl":  round(safe_float(sm.get("volume1hBuy",0)) - safe_float(sm.get("volume1hSell",0)), 2),
        "p":   round(price, 10)
    }

# ── Single update ──

def run_update(tokens, r2):
    live = {}
    for t in tokens:
        try:
            k  = fetch_1m_klines(t)
            sm = fetch_smart_money(t["contract"], t["chainId"])
            r  = compute(k, sm)
            if r: live[t["contract"]] = r
        except Exception as e:
            print(f"  ⚠ {t['symbol']}: {e}", flush=True)
        time.sleep(0.25)

    if not live: return False
    try:
        r2.put_object(
            Bucket=R2_BUCKET_NAME, Key="competition-live.json",
            Body=json.dumps({"ts": int(time.time()*1000), "d": live},
                            separators=(',',':')).encode(),
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
        return True
    except Exception as e:
        print(f"  ❌ R2: {e}", flush=True)
        return False

# ── Main ──

def main():
    print("🚀 Competition Live Fetcher", flush=True)
    r2 = get_r2_client()
    if not r2: return

    print("⏳ Lấy token list...", flush=True)
    tokens = get_active_tournaments()
    print(f"✅ {len(tokens)} tokens: {[t['symbol'] for t in tokens]}", flush=True)
    if not tokens: return

    # Loop trong 4.5 phút (workflow timeout = 5 phút)
    BUDGET   = 4.5 * 60
    INTERVAL = 10
    deadline = time.time() + BUDGET
    loop     = 0

    while time.time() < deadline:
        loop += 1
        t0 = time.time()
        ok = run_update(tokens, r2)
        elapsed = time.time() - t0
        remain  = deadline - time.time()
        print(f"⚡ #{loop:02d} {'✅' if ok else '❌'} {elapsed:.1f}s | remain {remain:.0f}s", flush=True)
        sleep = max(0, INTERVAL - elapsed)
        if time.time() + sleep > deadline: break
        if sleep > 0: time.sleep(sleep)

    print(f"🏁 Done: {loop} loops", flush=True)

if __name__ == "__main__":
    main()
