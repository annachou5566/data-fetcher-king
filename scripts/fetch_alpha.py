import json
import os
import time
import urllib.parse
from datetime import datetime, timedelta
from dotenv import load_dotenv
import requests 
import cloudscraper
import boto3 
from botocore.config import Config

# --- 1. CẤU HÌNH ---
load_dotenv()

# Cấu hình R2 / S3
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

PROXY_WORKER_URL = os.getenv("PROXY_WORKER_URL")
API_AGG_TICKER = os.getenv("BINANCE_INTERNAL_AGG_API")
API_AGG_KLINES = os.getenv("BINANCE_INTERNAL_KLINES_API")
API_PUBLIC_SPOT = "https://api.binance.com/api/v3/exchangeInfo"

ACTIVE_SPOT_SYMBOLS = set()
OLD_DATA_MAP = {}

# --- KHỞI TẠO KẾT NỐI R2 (OBJECT STORAGE) ---
def get_r2_client():
    if not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY:
        print("⚠️ Thiếu R2 Credentials! Kiểm tra GitHub Secrets.")
        return None
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4')
    )

# --- KHỞI TẠO SESSION REQUESTS ---
session = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
)
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Referer": "https://www.binance.com/en/alpha",
    "Origin": "https://www.binance.com",
    "Accept": "application/json"
})

# --- MAPPING LÀM RỐI DỮ LIỆU (ĐẦY ĐỦ 100%) ---
KEY_MAP = {
    "id": "i", "symbol": "s", "name": "n", "icon": "ic",
    "chain": "cn", "chain_icon": "ci", "contract": "ct",
    "status": "st", "price": "p", "change_24h": "c",
    "market_cap": "mc", "liquidity": "l", "volume": "v",
    "holders": "h",
    "rolling_24h": "r24", "daily_total": "dt",
    "daily_limit": "dl", "daily_onchain": "do",
    "chart": "ch", "listing_time": "lt", "tx_count": "tx",
    "offline": "off", "listingCex": "cex",
    "onlineTge": "tge",
    "onlineAirdrop": "air",
    # [MỚI] Thêm Mul Point
    "mul_point": "mp"
}

def minify_token_data(token):
    minified = {}
    # 1. Các trường cơ bản
    minified[KEY_MAP["id"]] = token.get("id")
    minified[KEY_MAP["symbol"]] = token.get("symbol")
    minified[KEY_MAP["name"]] = token.get("name")
    minified[KEY_MAP["icon"]] = token.get("icon")
    
    # 2. Các trường Chain (Mạng lưới) - ĐÃ BỔ SUNG ĐẦY ĐỦ
    minified[KEY_MAP["chain"]] = token.get("chain")           # Tên mạng
    minified[KEY_MAP["chain_icon"]] = token.get("chain_icon") # Logo mạng (Cái bạn đang tìm)
    minified[KEY_MAP["contract"]] = token.get("contract")

    # 3. Trạng thái & Giá
    minified[KEY_MAP["status"]] = token.get("status")
    minified[KEY_MAP["price"]] = token.get("price")
    minified[KEY_MAP["change_24h"]] = token.get("change_24h")
    minified[KEY_MAP["mul_point"]] = token.get("mul_point")   # [MỚI] Điểm nhân

    # 4. Số liệu tài chính (Ép kiểu int cho gọn nếu số lớn)
    minified[KEY_MAP["market_cap"]] = int(token.get("market_cap", 0))
    minified[KEY_MAP["holders"]] = int(token.get("holders", 0))
    minified[KEY_MAP["liquidity"]] = int(token.get("liquidity", 0))
    minified[KEY_MAP["tx_count"]] = int(token.get("tx_count", 0))
    
    # 5. Thông tin Listing / Offline
    minified[KEY_MAP["listing_time"]] = token.get("listing_time")
    minified[KEY_MAP["offline"]] = 1 if token.get("offline") else 0
    minified[KEY_MAP["listingCex"]] = 1 if token.get("listingCex") else 0
    minified[KEY_MAP["onlineTge"]] = 1 if token.get("onlineTge") else 0
    minified[KEY_MAP["onlineAirdrop"]] = 1 if token.get("onlineAirdrop") else 0

    # 6. Volume (Giữ nguyên cấu trúc object con)
    vol = token.get("volume", {})
    minified[KEY_MAP["volume"]] = {
        KEY_MAP["rolling_24h"]: int(vol.get("rolling_24h", 0)),
        KEY_MAP["daily_total"]: int(vol.get("daily_total", 0)),
        KEY_MAP["daily_limit"]: int(vol.get("daily_limit", 0)),
        KEY_MAP["daily_onchain"]: int(vol.get("daily_onchain", 0))
    }
    
    # 7. Biểu đồ
    minified[KEY_MAP["chart"]] = token.get("chart", [])
    
    return minified

# --- HÀM GỌI API ---
def fetch_smart(target_url, retries=3):
    is_render = "onrender.com" in (PROXY_WORKER_URL or "")
    if not target_url or "None" in target_url: return None

    for i in range(retries):
        if PROXY_WORKER_URL:
            try:
                encoded_target = urllib.parse.quote(target_url, safe='')
                proxy_final_url = f"{PROXY_WORKER_URL}?url={encoded_target}"
                current_timeout = 60 if (is_render and i == 0) else 30
                res = session.get(proxy_final_url, timeout=current_timeout)
                if res.status_code == 200:
                    data = res.json()
                    if isinstance(data, dict):
                        if "symbols" in data: return data 
                        if data.get("code") == "000000": return data
                elif res.status_code == 502: time.sleep(3)
            except: pass
        
        try:
            res = session.get(target_url, timeout=15)
            if res.status_code == 200:
                data = res.json()
                if "symbols" in data: return data
                if data.get("code") == "000000": return data
        except: pass
        time.sleep(1)
    return None

def safe_float(v):
    try: return float(v) if v else 0.0
    except: return 0.0

# --- TẢI DATA CŨ TỪ R2 (THAY VÌ LOAD LOCAL) ---
def load_old_data_from_r2(r2_client):
    if not r2_client: return {}
    try:
        # Tải file market-data.json từ R2 về bộ nhớ
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key='market-data.json')
        data = json.loads(obj['Body'].read().decode('utf-8'))
        
        # Vì dữ liệu cũ trên R2 đã bị Minify (làm rối), ta cần map ngược lại ID
        # để code logic hiểu được. (Tuy nhiên, logic check limit chủ yếu cần ID,
        # nếu minify ID vẫn giữ nguyên thì OK).
        # Ở đây đơn giản hóa: Nếu đã minify thì ID là key "i"
        
        tokens = data.get('data', [])
        mapped_data = {}
        for t in tokens:
            # Map key 'i' (minified) hoặc 'id' (legacy)
            tid = t.get('i') or t.get('id')
            if tid: mapped_data[tid] = t
            
        return mapped_data
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
    except: pass
    return set()

def fetch_details_optimized(chain_id, contract_addr):
    if not API_AGG_KLINES: return 0, 0, 0, []
    no_lower_chains = ["CT_501", "CT_784"]
    clean_addr = str(contract_addr)
    if chain_id not in no_lower_chains: clean_addr = clean_addr.lower()
    
    base_url = f"{API_AGG_KLINES}?chainId={chain_id}&interval=1d&limit=30&tokenAddress={clean_addr}"
    d_total, d_limit = 0.0, 0.0
    chart_data = []

    try:
        res_limit = fetch_smart(f"{base_url}&dataType=limit")
        if res_limit and res_limit.get("data") and res_limit["data"].get("klineInfos"):
            k_infos = res_limit["data"]["klineInfos"]
            if k_infos: d_limit = safe_float(k_infos[-1][5])
    except: pass

    try:
        res_agg = fetch_smart(f"{base_url}&dataType=aggregate")
        if res_agg and res_agg.get("data") and res_agg["data"].get("klineInfos"):
            k_infos = res_agg["data"]["klineInfos"]
            if k_infos:
                d_total = safe_float(k_infos[-1][5])
                chart_data = [{"p": safe_float(k[4]), "v": safe_float(k[5])} for k in k_infos]
    except: pass

    d_market = d_total - d_limit
    if d_market < 0: d_market = 0 
    return d_total, d_limit, d_market, chart_data

def process_single_token(item):
    aid = item.get("alphaId")
    if not aid: return None

    vol_rolling = safe_float(item.get("volume24h"))
    symbol = item.get("symbol")
    contract = item.get("contractAddress")
    chain_id = item.get("chainId")
    is_offline = item.get("offline", False)
    is_listing_cex = item.get("listingCex", False)
    
    status = "ALPHA"
    need_limit_check = False 
    if is_offline:
        if is_listing_cex or symbol in ACTIVE_SPOT_SYMBOLS: status = "SPOT"
        else:
            status = "PRE_DELISTED"
            need_limit_check = True

    # --- [MỚI] LOGIC CACHE: CHẶN TOKEN ĐÃ CHẾT TỪ LẦN TRƯỚC ---
    # Mục đích: Nếu lịch sử (OLD_DATA_MAP) ghi nhận là DELISTED thì bỏ qua luôn.
    # Lưu ý: OLD_DATA_MAP dùng key đã minify (ví dụ: KEY_MAP["status"] = "st")
    if OLD_DATA_MAP and aid in OLD_DATA_MAP:
        old_item = OLD_DATA_MAP[aid]
        # Kiểm tra trạng thái cũ
        if old_item.get(KEY_MAP["status"]) == "DELISTED":
            status = "DELISTED"
            need_limit_check = False  # Tắt cờ check limit để không chui vào should_fetch
    # -----------------------------------------------------------

    should_fetch = False
    if vol_rolling > 0 and (status == "ALPHA" or status == "PRE_DELISTED"):
        should_fetch = True
    
    daily_total, daily_limit, daily_onchain = 0.0, 0.0, 0.0
    chart_data = []
    
    # Logic Cache: Cần xử lý khéo hơn vì key cache đã bị minify
    # Nhưng để an toàn cho phiên bản này, ta tạm ưu tiên fetch mới.
    
    if should_fetch:
        print(f"📡 {symbol}...", end=" ", flush=True)
        try:
            d_t, d_l, d_m, chart = fetch_details_optimized(chain_id, contract)
            daily_total, daily_limit, daily_onchain = d_t, d_l, d_m
            chart_data = chart
            
            if need_limit_check:
                if daily_limit > 0:
                    status = "ALPHA"
                    print("✅ ALIVE")
                else:
                    status = "DELISTED"
                    print("❌ DEAD")
            else: print("OK")
            if daily_total <= 0: daily_total = vol_rolling
        except Exception as e:
            print(f"⚠️ Err: {e}")
            daily_total = vol_rolling
            if need_limit_check: status = "DELISTED"
    else:
        daily_total = vol_rolling
        if status == "PRE_DELISTED": status = "DELISTED"
        
        # [MỚI] Tái sử dụng Chart cũ nếu có (để không bị mất biểu đồ khi skip fetch)
        if status == "DELISTED" and OLD_DATA_MAP and aid in OLD_DATA_MAP:
            old_item = OLD_DATA_MAP[aid]
            if old_item.get(KEY_MAP["chart"]):
                chart_data = old_item.get(KEY_MAP["chart"])

    return {
        "id": aid, "symbol": symbol, "name": item.get("name"),
        "icon": item.get("iconUrl"), "chain": item.get("chainName", ""),
        "chain_icon": item.get("chainIconUrl"), "contract": contract,
        "offline": is_offline, "listingCex": is_listing_cex, "status": status,
        "onlineTge": item.get("onlineTge", False),
        "onlineAirdrop": item.get("onlineAirdrop", False),
        "mul_point": safe_float(item.get("mulPoint")),
        "listing_time": item.get("listingTime", 0),
        "tx_count": safe_float(item.get("count24h")),
        "price": safe_float(item.get("price")),
        "change_24h": safe_float(item.get("percentChange24h")),
        "liquidity": safe_float(item.get("liquidity")),
        "market_cap": safe_float(item.get("marketCap")),
        "holders": safe_float(item.get("holders")),
        "volume": {
            "rolling_24h": vol_rolling, "daily_total": daily_total,
            "daily_limit": daily_limit, "daily_onchain": daily_onchain
        },
        "chart": chart_data
    }

# --- [MỚI] THUẬT TOÁN TÍNH CÁI ĐUÔI 1440 PHÚT ---
def build_suffix_sum(klines, yesterday_str):
    arr = [0.0] * 1440
    if not klines: return arr
    minute_map = [0.0] * 1440
    
    for k in klines:
        try:
            candle_ts = int(k[0])
            dt = datetime.utcfromtimestamp(candle_ts / 1000.0)
            if dt.strftime('%Y-%m-%d') == yesterday_str:
                start_min = dt.hour * 60 + dt.minute
                vol_per_min = float(k[5] or 0) / 5.0
                for i in range(5):
                    if start_min + i < 1440:
                        minute_map[start_min + i] += vol_per_min
        except: pass
        
    running_sum = 0.0
    for i in range(1439, -1, -1):
        running_sum += minute_map[i]
        arr[i] = round(running_sum, 2) # Làm tròn 2 số thập phân để siêu nén JSON
    return arr

def generate_and_upload_tails(r2_client, raw_tokens):
    print("\n🦊 Bắt đầu quét Cái Đuôi 5m cho toàn thị trường...")
    yesterday_str = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    tails_total, tails_limit = {}, {}
    valid_tokens = [t for t in raw_tokens if safe_float(t.get("volume24h")) > 0]
    
    for idx, t in enumerate(valid_tokens):
        aid = t.get("alphaId")
        chain_id = t.get("chainId")
        contract = t.get("contractAddress")
        if not aid or not contract: continue
        
        if idx % 50 == 0: print(f"   Đang xử lý {idx}/{len(valid_tokens)} token...")
        
        clean_addr = str(contract)
        if chain_id not in ["CT_501", "CT_784"]: clean_addr = clean_addr.lower()
        
        base_url = f"{API_AGG_KLINES}?chainId={chain_id}&interval=5m&limit=1000&tokenAddress={clean_addr}"
        
        try:
            res_tot = fetch_smart(f"{base_url}&dataType=aggregate", retries=1)
            if res_tot and "data" in res_tot and "klineInfos" in res_tot["data"]:
                tails_total[aid] = build_suffix_sum(res_tot["data"]["klineInfos"], yesterday_str)
                
            res_lim = fetch_smart(f"{base_url}&dataType=limit", retries=1)
            if res_lim and "data" in res_lim and "klineInfos" in res_lim["data"]:
                tails_limit[aid] = build_suffix_sum(res_lim["data"]["klineInfos"], yesterday_str)
        except: pass
        time.sleep(0.1) # Lách Ban IP mượt mà
        
    print("☁️ Đang Upload Tails lên R2...")
    json_str = json.dumps({"total": tails_total, "limit": tails_limit}, separators=(',', ':'))
    try:
        r2_client.put_object(Bucket=R2_BUCKET_NAME, Key='tails_cache.json', Body=json_str.encode('utf-8'), ContentType='application/json')
        print("✅ Đã lưu tails_cache.json thành công!")
    except Exception as e: print(f"❌ Upload Tails Failed: {e}")


def fetch_data():
    global ACTIVE_SPOT_SYMBOLS, OLD_DATA_MAP
    start = time.time()
    
    r2 = get_r2_client()
    if not r2: return

    OLD_DATA_MAP = load_old_data_from_r2(r2)
    ACTIVE_SPOT_SYMBOLS = get_active_spot_symbols()
    
    print("⏳ List...", end=" ", flush=True)
    try: raw_res = fetch_smart(API_AGG_TICKER)
    except: return
    if not raw_res: return
    
    raw_data = raw_res.get("data", [])
    print(f"Done ({len(raw_data)})")

    target_tokens = raw_data
    target_tokens.sort(key=lambda x: safe_float(x.get("volume24h")), reverse=True)
    
    results = []
    print(f"🚀 Processing {len(target_tokens)} Tokens (R2 Storage Mode)...")
    
    for t in target_tokens:
        r = process_single_token(t)
        if r: results.append(r)
        time.sleep(0.5)
    results.sort(key=lambda x: x["volume"]["daily_total"], reverse=True)

    # --- MINIFY DATA ---
    print(f"🔒 Minifying...")
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

    # --- UPLOAD TO CLOUDFLARE R2 ---
    print("☁️ Uploading to Cloudflare R2...")
    try:
        # 1. Upload File Mới Nhất
        r2.put_object(
            Bucket=R2_BUCKET_NAME,
            Key='market-data.json',
            Body=json_str.encode('utf-8'),
            ContentType='application/json',
            CacheControl='max-age=60' # Cache 1 phút
        )
        print("✅ Uploaded market-data.json")

        # 2. Upload File Lịch Sử
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
        
        generate_and_upload_tails(r2, target_tokens)

    print(f"🏁 DONE! Total: {time.time()-start:.1f}s")

if __name__ == "__main__":
    fetch_data()
