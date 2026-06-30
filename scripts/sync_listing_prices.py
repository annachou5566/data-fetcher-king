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


def fetch_listing_price(chain_id, contract, target_date_str):
    """
    Trả về {open, close, date} của ngày niêm yết, hoặc None nếu không
    tìm được nến nào khớp / token chưa có dữ liệu klines nội bộ.
    """
    if not API_AGG_KLINES or not chain_id or not contract:
        return None

    addr = str(contract)
    chain_variants = [str(chain_id)]
    if str(chain_id) in ("501", "784"):
        chain_variants.append(f"CT_{chain_id}")  # thử biến thể Solana/Sui

    for cid in chain_variants:
        clean_addr = addr if cid in NO_LOWER_CHAINS else addr.lower()
        url = (
            f"{API_AGG_KLINES}?chainId={cid}&interval=1d&limit=1000"
            f"&tokenAddress={clean_addr}&dataType=aggregate"
        )
        try:
            res = fa.fetch_smart(url, retries=2)
        except Exception:
            res = None

        if not res:
            continue
        k_infos = (res.get("data") or {}).get("klineInfos")
        if not k_infos:
            continue

        # Tìm nến đúng ngày niêm yết; nếu không có thì lấy nến sớm nhất
        # (thường là chính ngày listing vì token mới chưa có lịch sử trước đó)
        match = None
        for k in k_infos:
            try:
                day = datetime.utcfromtimestamp(int(k[0]) / 1000).strftime('%Y-%m-%d')
            except Exception:
                continue
            if day == target_date_str:
                match = k
                break
        if not match:
            match = k_infos[0]

        try:
            return {
                "open":  fa.safe_float(match[1]),
                "close": fa.safe_float(match[4]),
                "date":  datetime.utcfromtimestamp(int(match[0]) / 1000).strftime('%Y-%m-%d'),
            }
        except Exception:
            continue

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
        if result:
            e["listing_price"] = result
            filled += 1
            print(f"OK  open=${result['open']}")
        else:
            e["listing_price"] = None  # đánh dấu đã thử, tránh fetch lại vô hạn lần sau
            print("no data (DEX pair not found)")

        time.sleep(0.15)  # nhẹ tay với proxy, tránh burst

    return filled


def main():
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
