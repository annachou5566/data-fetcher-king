"""
scripts/fetch_interbank.py
────────────────────────────────────────────────────────────────────
Bot: Fetch USD/VND interbank rate hàng ngày → append vào investing-data.json
Nguồn: open.er-api.com (free, no key, updates 24h/lần)

Chạy 1 lần/ngày, nối tiếp lịch sử investing.com đã có sẵn trong R2.
KHÔNG ghi đè — chỉ append ngày mới nếu chưa có.
────────────────────────────────────────────────────────────────────
"""

import os, json, boto3
from datetime import datetime, timezone
from curl_cffi import requests

R2_KEY  = "investing-data.json"
API_URL = "https://open.er-api.com/v6/latest/USD"

def get_r2():
    return boto3.client("s3",
        aws_access_key_id     = os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["R2_SECRET_ACCESS_KEY"],
        endpoint_url          = os.environ["R2_ENDPOINT_URL"],
    ), os.environ["R2_BUCKET_NAME"]

def main():
    print("💱 Interbank Rate Bot — open.er-api.com")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    session = requests.Session(impersonate="chrome116")
    res = session.get(API_URL, timeout=15)
    if res.status_code != 200:
        print(f"❌ API HTTP {res.status_code}")
        return

    data = res.json()
    rate = data.get("rates", {}).get("VND")
    if not rate or rate < 10000:
        print(f"❌ VND rate không hợp lệ: {rate}")
        return

    close = round(rate)
    print(f"  ✅ USD/VND hôm nay: {close:,} ₫")

    r2, bucket = get_r2()
    try:
        obj      = r2.get_object(Bucket=bucket, Key=R2_KEY)
        payload  = json.loads(obj["Body"].read().decode("utf-8"))
        rows     = payload.get("rows", [])
    except Exception:
        print("❌ Không đọc được investing-data.json — cần upload trước")
        return

    # Append nếu chưa có hôm nay
    existing_dates = {r["date"] for r in rows}
    if today in existing_dates:
        print(f"  ℹ️  Đã có data ngày {today}, cập nhật lại")
        rows = [r for r in rows if r["date"] != today]

    rows.append({"date": today, "close": close, "high": close, "low": close, "open": close})
    rows.sort(key=lambda r: r["date"])

    payload["rows"]    = rows
    payload["count"]   = len(rows)
    payload["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload["source"]  = "investing.com (lịch sử) + open.er-api.com (daily)"

    body = json.dumps(payload, separators=(",",":")).encode()
    r2.put_object(
        Bucket=bucket, Key=R2_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=86400",
    )
    print(f"✅ Done — {len(rows)} rows trong R2 (mới nhất: {today} = {close:,} ₫)")

if __name__ == "__main__":
    main()
