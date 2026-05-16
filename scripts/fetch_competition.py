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

R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
PROXY_WORKER_URL = os.getenv("PROXY_WORKER_URL")
API_AGG_KLINES = os.getenv("BINANCE_INTERNAL_KLINES_API")

# --- KẾT NỐI R2 ---
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
                encoded_target = urllib.parse.quote(target_url, safe='')
                proxy_final_url = f"{PROXY_WORKER_URL}?url={encoded_target}"
                current_timeout = 60 if (is_render and i == 0) else 30
                res = session.get(proxy_final_url, timeout=current_timeout)
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

# --- HÀM LẤY GIẢI ĐẤU (ĐÃ FIX DEBUG) ---
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
        # Lấy TOÀN BỘ giải đấu, không lọc ID
        url = f"{SUPABASE_URL}/rest/v1/tournaments?select=id,name,contract,data"
        res = requests.get(url, headers=headers, timeout=10)
        
        if res.status_code != 200:
            print(f"❌ Supabase Error: {res.status_code} - {res.text}")
            return []
        
        data = res.json()
        active_list = []
        
        # Tính ngày Lookback (Lùi lại 3 ngày để lấy cả giải vừa xong)
        now = datetime.now()
        lookback_date = now.strftime("%Y-%m-%d")
        print(f"📅 Debug Date: Today={now.strftime('%Y-%m-%d')}, Lookback={lookback_date}")

        for item in data:
            name = item.get("name", "Unknown")
            # Bỏ qua dòng mẫu ARB hoặc dòng lỗi
            if name == "ARB" or item.get("id") == -1: continue

            meta = item.get("data")
            if not meta: meta = {} # Handle null data

            # Logic tìm Contract Address
            contract = item.get("contract") # Cột bên ngoài
            if not contract: contract = meta.get("contractAddress") # Cột bên trong JSON
            
            # Logic tìm ngày và giờ kết thúc
            end_date = meta.get("end")
            end_time = meta.get("endTime", "23:59") # Nếu thiếu giờ, mặc định là cuối ngày
            
            # Ghép thành chuỗi ISO 8601 chuẩn UTC (có chữ Z ở cuối)
            # Ví dụ kết quả: "2026-02-11T11:00:00Z" -> Giúp JS so sánh chính xác từng giây
            end_at_iso = None
            if end_date:
                end_at_iso = f"{end_date}T{end_time}:00Z"

            # Debug in ra để soi
            # print(f"   - Check {name}: End={end_at_iso} | Contract={contract}")

            # Điều kiện lọc:
            # 1. Có Contract
            # 2. Chưa kết thúc HOẶC (Đã kết thúc nhưng vẫn trong khoảng Lookback 3 ngày để lưu History)
            if contract:
                if not end_date or end_date >= lookback_date:
                    chain_id = meta.get("chainId")
                    logo_url = meta.get("iconUrl", "")
                    chain_icon = meta.get("chainIconUrl", "")
                    
                    if chain_id:
                        active_list.append({
                            "symbol": name,
                            "contract": contract.lower().strip(),
                            "chainId": chain_id,
                            "alphaId": meta.get("alphaId"),
                            "quoteAsset": meta.get("quoteAsset", "USDT"),
                            "logo": logo_url,
                            "chainLogo": chain_icon,
                            "end_at": end_at_iso  # <--- QUAN TRỌNG: Truyền mốc thời gian UTC xuống Client test
                        })
                    else:
                        print(f"⚠️ {name}: Thiếu chainId")
                else:
                    # print(f"⏭️ {name}: Đã kết thúc từ {end_date} (Skip)")
                    pass
            else:
                print(f"⚠️ {name}: Thiếu Contract Address")

        return active_list

    except Exception as e:
        print(f"❌ Exception in get_active_tournaments: {e}")
        return []

def fetch_limit_history(token_info):
    if not API_AGG_KLINES: return []
    alpha_id = token_info.get("alphaId")
    contract = token_info.get("contract")
    chain_id = token_info.get("chainId")
    quote_asset = token_info.get("quoteAsset")
    
    # Base/Solana -> USDC
    c_id_str = str(chain_id).lower()
    if c_id_str == "8453" or "base" in c_id_str or "sol" in c_id_str: quote_asset = "USDC"
    
    limit_hours = 168 # 7 ngày
    url = ""
    if alpha_id:
        url = f"https://www.binance.com/bapi/defi/v1/public/alpha-trade/klines?symbol={alpha_id}{quote_asset}&interval=1h&limit={limit_hours}"
    else:
        url = f"{API_AGG_KLINES}?chainId={chain_id}&interval=1h&limit={limit_hours}&tokenAddress={contract}&dataType=limit"

    data = fetch_smart(url)
    chart_points = []
    k_infos = []
    
    if data and data.get("data"):
        if isinstance(data["data"], list): k_infos = data["data"]
        elif data["data"].get("klineInfos"): k_infos = data["data"]["klineInfos"]

    for k in k_infos:
        try:
            ts = int(k[0])
            high, low = safe_float(k[2]), safe_float(k[3])
            limit_vol_usd = safe_float(k[7]) 
            tx_count = int(k[8]) if len(k) > 8 else 0
            
            risk = 0
            if low > 0:
                spread_pct = ((high - low) / low) * 100
                if spread_pct > 5: risk = 2
                elif spread_pct > 2: risk = 1
            
            if limit_vol_usd > 0 or tx_count > 0:
                chart_points.append([ts, int(limit_vol_usd), tx_count, risk])
        except: continue
    return chart_points

def main():
    start = time.time()
    r2 = get_r2_client()
    if not r2: return

    print("⏳ Đang lấy danh sách giải đấu từ Supabase...", end=" ")
    target_tokens = get_active_tournaments()
    print(f"OK ({len(target_tokens)} giải active)")
    
    if not target_tokens:
        print("❌ Vẫn không thấy giải nào. Hãy kiểm tra lại DB Supabase.")
        return

    history_data = {}
    print(f"🚀 Scanning {len(target_tokens)} active tournaments...")

    for t in target_tokens:
        print(f"📊 {t['symbol']}...", end=" ", flush=True)
        points = fetch_limit_history(t)
        if points:
            history_data[t["contract"]] = { 
                "s": t["symbol"], 
                "q": t["quoteAsset"], 
                "l": t["logo"],
                "cl": t["chainLogo"],
                "e": t.get("end_at"),
                "h": points 
            }
            print(f"OK ({len(points)}h)")
        else:
            print("No Data")
        time.sleep(0.5)

    final_json = { "updated_at": int(time.time() * 1000), "note": "7 Days Limit", "data": history_data }
    
    try:
        r2.put_object(
            Bucket=R2_BUCKET_NAME, Key='competition-history.json',
            Body=json.dumps(final_json, separators=(',', ':')).encode('utf-8'),
            ContentType='application/json', CacheControl='no-cache, no-store, must-revalidate'
        )
        print("✅ competition-history.json uploaded!")
    except Exception as e: print(f"❌ Upload Error: {e}")
    print(f"🏁 Done: {time.time()-start:.1f}s")

if __name__ == "__main__":
    main()
