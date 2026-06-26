"""
scripts/fetch_vcb.py (Nâng cấp thành Multi-Bank Fetcher)
────────────────────────────────────────────────────────────────────
Bot: Fetch tỷ giá USD/VND từ SBV và Big 4 (VCB, BID, CTG, TCB)
Chiến lược: 
  - Tự động lấy JWT Token mới từ vnappmob để vượt giới hạn 15 ngày.
  - Tách data thành macro-rates.json và bank-details.json lưu lên R2.
────────────────────────────────────────────────────────────────────
"""

import os, json, time, boto3, requests
from datetime import datetime, timedelta, timezone, date
from concurrent.futures import ThreadPoolExecutor, as_completed

START_DATE = "2015-01-01"
API_TOKEN_URL = "https://api.vnappmob.com/api/request_api_key?scope=exchange_rate"
API_RATE_URL = "https://api.vnappmob.com/api/v2/exchange_rate/"

MACRO_KEY = "macro-rates.json"
BANK_KEY = "bank-details.json"

def get_r2():
    key, secret, endpoint, bucket = (
        os.getenv("R2_ACCESS_KEY_ID"), os.getenv("R2_SECRET_ACCESS_KEY"),
        os.getenv("R2_ENDPOINT_URL"),  os.getenv("R2_BUCKET_NAME"),
    )
    if not all([key, secret, endpoint, bucket]):
        raise RuntimeError("Thiếu R2 env vars")
    return boto3.client("s3",
        aws_access_key_id=key, aws_secret_access_key=secret, endpoint_url=endpoint,
    ), bucket

def get_fresh_token():
    try:
        r = requests.get(API_TOKEN_URL, timeout=10)
        r.raise_for_status()
        return r.json().get("results")
    except Exception as e:
        raise RuntimeError(f"Không thể lấy API Token: {e}")

def load_existing(r2, bucket, key):
    try:
        obj = r2.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data.get("rows", [])
    except r2.exceptions.NoSuchKey:
        return []
    except Exception as e:
        print(f"  ⚠️  Load {key} lỗi: {e} → tạo mới")
        return []

def fetch_day_all(date_str, token):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    banks = ["sbv", "vcb", "bid", "ctg", "tcb"]
    
    macro_row = {"date": date_str}
    bank_row = {"date": date_str}
    valid_bank_sells = []
    
    for b in banks:
        try:
            r = requests.get(f"{API_RATE_URL}{b}?date={date_str}", headers=headers, timeout=10)
            if r.status_code == 200:
                results = r.json().get("results", [])
                usd = next((x for x in results if x.get("currency") == "USD"), None)
                if usd and usd.get("sell"):
                    sell_val = int(float(usd.get("sell")))
                    if b == "sbv":
                        macro_row["sbv_sell"] = sell_val
                    else:
                        bank_row[f"{b}_sell"] = sell_val
                        valid_bank_sells.append(sell_val)
        except Exception:
            pass
        time.sleep(0.1) # Tránh rate limit của vnappmob
        
    if not valid_bank_sells and "sbv_sell" not in macro_row:
        return None, None
        
    if valid_bank_sells:
        macro_row["vn_bank_index_sell"] = round(sum(valid_bank_sells) / len(valid_bank_sells))
        
    return macro_row, bank_row

def save_to_r2(r2, bucket, key, all_rows):
    payload = {
        "v": 1,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(all_rows),
        "rows": all_rows,
    }
    r2.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=3600",
    )
    print(f"  💾 Đã lưu {len(all_rows):,} rows vào {key}")

def main():
    print("🏦 Macro & Bank Rates Bot — Đa nguồn")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    r2, bucket = get_r2()
    print("🔑 Đang lấy Token mới...")
    token = get_fresh_token()

    macro_existing = load_existing(r2, bucket, MACRO_KEY)
    bank_existing = load_existing(r2, bucket, BANK_KEY)
    
    known_dates = {row["date"] for row in macro_existing}

    start = START_DATE if not known_dates else max(known_dates)
    cur = datetime.strptime(start, "%Y-%m-%d").date()
    today = date.today()
    
    dates = [
        (cur + timedelta(days=i)).isoformat()
        for i in range((today - cur).days + 1)
        if (cur + timedelta(days=i)).isoformat() not in known_dates
    ]

    if not dates:
        print("✅ Dữ liệu Up-to-date.")
        return

    mode = "BACKFILL" if len(dates) > 10 else "INCREMENTAL"
    print(f"📡 [{mode}] Fetch {len(dates)} ngày: {dates[0]} → {dates[-1]}")

    new_macros, new_banks = [], []
    total = len(dates)
    
    # Hạn chế số worker để không spam API sập nguồn
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(fetch_day_all, d, token): d for d in dates}
        done = 0
        for f in as_completed(futures):
            m_row, b_row = f.result()
            if m_row and b_row:
                new_macros.append(m_row)
                new_banks.append(b_row)
            done += 1
            if done % 50 == 0:
                print(f"   ... {done}/{total}")

    if not new_macros:
        print("❌ Không thu thập được data mới.")
        return

    # Hợp nhất và sort
    seen_m = {r["date"]: r for r in macro_existing}
    seen_m.update({r["date"]: r for r in new_macros})
    all_macros = sorted(seen_m.values(), key=lambda r: r["date"])

    seen_b = {r["date"]: r for r in bank_existing}
    seen_b.update({r["date"]: r for r in new_banks})
    all_banks = sorted(seen_b.values(), key=lambda r: r["date"])

    save_to_r2(r2, bucket, MACRO_KEY, all_macros)
    save_to_r2(r2, bucket, BANK_KEY, all_banks)
    
    last = all_macros[-1]
    print("✅ Đã hoàn tất!")
    print(f"   Tham chiếu mới nhất ({last['date']}): SBV={last.get('sbv_sell')} | INDEX={last.get('vn_bank_index_sell')}")

if __name__ == "__main__":
    main()
