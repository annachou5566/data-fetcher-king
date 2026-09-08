import os
import json
import time
from datetime import datetime
import cloudscraper
import boto3
from botocore.config import Config
from supabase import create_client

# --- CẤU HÌNH ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") 

R2_ENDPOINT = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET_NAME")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ LỖI: Thiếu biến môi trường Supabase.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
s3 = boto3.client('s3', endpoint_url=R2_ENDPOINT,
                  aws_access_key_id=R2_ACCESS_KEY_ID, aws_secret_access_key=R2_SECRET_ACCESS_KEY,
                  config=Config(signature_version='s3v4'))

session = cloudscraper.create_scraper()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36",
    "Referer": "https://www.binance.com/en/alpha"
})

def fetch_smart(target_url, retries=3):
    """Direct Binance fetch only. Không proxy/paid fallback."""
    if not target_url:
        return None

    for attempt in range(retries):
        retry_wait = 0
        try:
            res = session.get(target_url, timeout=15)
        except Exception as exc:
            print(f"⚠️ Direct request error: {exc}")
            res = None

        if res is not None:
            if res.status_code == 200:
                try:
                    return res.json()
                except Exception:
                    return None
            if res.status_code in (418, 429, 503):
                retry_wait = 30
                print(
                    f"⚠️ Direct HTTP {res.status_code}; bounded retry "
                    f"{attempt + 1}/{retries}"
                )
            else:
                print(f"⚠️ Direct HTTP {res.status_code}; source unavailable")

        if attempt < retries - 1:
            time.sleep(retry_wait or 1)

    return None

# [ĐÃ SỬA]: Tra bằng chain_id và contract thay vì alpha_id
def fetch_binance_history(chain_id, contract, start_ts):
    """ Lấy volume klines 1 ngày từ Start Date đến Hết ngày hôm qua """
    try:
        # 1. Gọi API Total (CEX + On-chain)
        url_tot = f"https://www.binance.com/bapi/defi/v1/public/alpha-trade/agg-klines?chainId={chain_id}&interval=1d&limit=100&tokenAddress={contract}&dataType=aggregate"
        res_tot = fetch_smart(url_tot)
        
        # 2. Gọi API Limit (Bao trọn USDT + USDC + BNB...)
        url_lim = f"https://www.binance.com/bapi/defi/v1/public/alpha-trade/agg-klines?chainId={chain_id}&interval=1d&limit=100&tokenAddress={contract}&dataType=limit"
        res_lim = fetch_smart(url_lim)

        if res_tot is None or res_lim is None:
            raise RuntimeError("Binance base-history source unavailable")
        
        history_total = []
        history_limit = []
        
        today_start_ts = int(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

        # Xử lý Total Volume (Lấy k[5] làm USD)
        if res_tot and res_tot.get("code") == "000000" and res_tot.get("data"):
            k_infos_total = res_tot["data"].get("klineInfos", [])
            for k in k_infos_total:
                k_ts = int(k[0])
                if k_ts >= start_ts and k_ts < today_start_ts:
                    date_str = datetime.utcfromtimestamp(k_ts/1000).strftime('%Y-%m-%d')
                    history_total.append({"date": date_str, "vol": float(k[5])})

        # Xử lý Limit Volume (Lấy k[5] làm USD)
        if res_lim and res_lim.get("code") == "000000" and res_lim.get("data"):
            k_infos_limit = res_lim["data"].get("klineInfos", [])
            for k in k_infos_limit:
                k_ts = int(k[0])
                if k_ts >= start_ts and k_ts < today_start_ts:
                    date_str = datetime.utcfromtimestamp(k_ts/1000).strftime('%Y-%m-%d')
                    history_limit.append({"date": date_str, "vol": float(k[5])})
                    
        return history_total, history_limit
    except Exception as e:
        print(f"Error fetching {contract}: {e}")
        return None, None

def main():
    print(">>> BẮT ĐẦU TẠO BASE DATA CHO NODE.JS (ACTIVE ONLY) <<<")
    
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    response = supabase.table("tournaments").select("*").neq('id', -1).execute()
    all_recs = response.data
    
    export_data = {}
    count_active = 0
    failed_active = []

    for t in all_recs:
        try:
            meta = t.get("data", {})
            alpha_id = meta.get("alphaId")
            if not alpha_id: continue 

            is_active = True
            if meta.get("ai_prediction", {}).get("status_label") == "FINALIZED":
                is_active = False
            if meta.get("end") and meta.get("end") < today_str:
                is_active = False

            if not is_active: continue

            print(f"-> Xử lý Base Volume: {meta.get('name')} ({alpha_id})...")
            
            # [ĐÃ SỬA]: Mapping lại ChainId và Contract cẩn thận
            contract = meta.get("contract", "").strip().lower()
            chain_id = meta.get("chainId")
            if not chain_id and meta.get("chain"):
                c_str = str(meta.get("chain")).lower().strip()
                chain_map = {'bsc': 56, 'bnb': 56, 'eth': 1, 'ethereum': 1, 'arb': 42161, 'arbitrum': 42161, 'base': 8453, 'op': 10, 'optimism': 10, 'polygon': 137, 'matic': 137}
                chain_id = chain_map.get(c_str)

            if not chain_id or not contract:
                print(f"Bỏ qua {alpha_id} do thiếu chainId hoặc contract")
                continue

            start_str = meta.get("start")
            start_time_str = meta.get("startTime", "00:00")
            if len(start_time_str) == 5: start_time_str += ":00"
            start_dt = datetime.strptime(f"{start_str}T{start_time_str}Z", "%Y-%m-%dT%H:%M:%SZ")
            start_ts = int(start_dt.timestamp() * 1000)

            # [ĐÃ SỬA]: Gọi hàm với chain_id và contract
            hist_total, hist_limit = fetch_binance_history(chain_id, contract, start_ts)
            if hist_total is None or hist_limit is None:
                failed_active.append(alpha_id)
                continue
            
            export_data[alpha_id] = {
                "base_total_vol": sum(item['vol'] for item in hist_total),
                "base_limit_vol": sum(item['vol'] for item in hist_limit),
                "history_total": hist_total,
                "history_limit": hist_limit,
                "start_ts": start_ts
            }
            count_active += 1
            
        except Exception as e:
            print(f"Lỗi tại {t.get('name')}: {e}")
            failed_active.append(str(t.get("id") or t.get("name") or "unknown"))

    if failed_active:
        raise RuntimeError(
            f"Base data incomplete for {len(set(failed_active))} active tournaments; "
            "không publish partial artifact."
        )

    if not export_data:
        raise RuntimeError("Base data rỗng; không publish artifact.")

    s3.put_object(
        Bucket=R2_BUCKET,
        Key='tournaments-base.json',
        Body=json.dumps(export_data),
        ContentType='application/json',
        CacheControl='max-age=60'
    )
    print(f"🎉 HOÀN THÀNH! Đã tạo tournaments-base.json cho {count_active} giải đấu.")

if __name__ == "__main__":
    main()
