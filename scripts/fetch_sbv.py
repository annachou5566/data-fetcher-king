"""
scripts/fetch_sbv.py
────────────────────────────────────────────────────────────────────
Fetch tỷ giá USD/VND lịch sử từ stooq.com (primary) + Yahoo Finance (fallback)
→ lưu vào R2: sbv-data.json

Dữ liệu: tỷ giá interbank USD/VND, xấp xỉ tỷ giá trung tâm SBV
Lịch sử: từ 2016-01-04 đến nay
Tần suất: hàng ngày (ngày làm việc)

Env: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME
────────────────────────────────────────────────────────────────────
"""

import os, json, time, boto3, requests
from datetime import datetime, timedelta, timezone, date
from io import StringIO

START_DATE = "2016-01-04"
R2_KEY     = "sbv-data.json"
HEADERS    = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# ─────────────────────────────────────────────────────────────────
# R2
# ─────────────────────────────────────────────────────────────────
def get_r2():
    key, secret, endpoint, bucket = (
        os.getenv("R2_ACCESS_KEY_ID"), os.getenv("R2_SECRET_ACCESS_KEY"),
        os.getenv("R2_ENDPOINT_URL"),  os.getenv("R2_BUCKET_NAME"),
    )
    if not all([key, secret, endpoint, bucket]):
        raise RuntimeError("Thiếu R2 env vars")
    return boto3.client("s3",
        aws_access_key_id=key, aws_secret_access_key=secret,
        endpoint_url=endpoint,
    ), bucket

def load_existing(r2, bucket):
    try:
        obj  = r2.get_object(Bucket=bucket, Key=R2_KEY)
        data = json.loads(obj["Body"].read())
        rows = data.get("rows", [])
        print(f"  📦 R2 hiện có: {len(rows):,} rows")
        return rows
    except r2.exceptions.NoSuchKey:
        print("  📄 sbv-data.json chưa có → backfill từ đầu")
        return []
    except Exception as e:
        print(f"  ⚠️  Load R2: {e}")
        return []

def save_to_r2(r2, bucket, all_rows):
    payload = {
        "v":       1,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":   len(all_rows),
        "rows":    all_rows,
    }
    r2.put_object(
        Bucket=bucket, Key=R2_KEY,
        Body=json.dumps(payload, separators=(",", ":")).encode(),
        ContentType="application/json",
        CacheControl="max-age=3600",
    )

# ─────────────────────────────────────────────────────────────────
# NGUỒN 1: stooq.com — CSV download, 1 request lấy toàn bộ
# ─────────────────────────────────────────────────────────────────
def fetch_stooq(from_date: str, to_date: str) -> list:
    """
    stooq.com trả CSV với cột: Date,Open,High,Low,Close,Volume
    Symbol USDVND = tỷ giá USD/VND interbank
    """
    d1 = from_date.replace("-", "")
    d2 = to_date.replace("-", "")
    url = f"https://stooq.com/q/d/l/?s=usdvnd&d1={d1}&d2={d2}&i=d"

    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    if "No data" in r.text or len(r.text) < 50:
        raise ValueError("stooq trả về rỗng")

    rows = []
    lines = r.text.strip().split("\n")
    # Header: Date,Open,High,Low,Close,Volume
    for line in lines[1:]:
        parts = line.strip().split(",")
        if len(parts) < 5:
            continue
        try:
            date_str = parts[0].strip()   # YYYY-MM-DD
            close    = float(parts[4])    # Close price
            # Validate: USD/VND phải trong khoảng 20000–35000
            if 20_000 <= close <= 35_000:
                rows.append({"date": date_str, "central": int(close)})
        except (ValueError, IndexError):
            continue

    return sorted(rows, key=lambda x: x["date"])

# ─────────────────────────────────────────────────────────────────
# NGUỒN 2: Yahoo Finance — fallback nếu stooq lỗi
# ─────────────────────────────────────────────────────────────────
def fetch_yahoo(from_date: str, to_date: str) -> list:
    """
    Yahoo Finance API v8 — symbol USDVND=X
    Trả JSON với timestamps Unix + close prices
    """
    ts_from = int(datetime.strptime(from_date, "%Y-%m-%d").timestamp())
    ts_to   = int(datetime.strptime(to_date,   "%Y-%m-%d").timestamp()) + 86400

    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/USDVND=X"
        f"?interval=1d&period1={ts_from}&period2={ts_to}&events=history"
    )
    headers = {**HEADERS, "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()

    data = r.json()
    result = data["chart"]["result"][0]
    timestamps = result["timestamp"]
    closes     = result["indicators"]["quote"][0]["close"]

    rows = []
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        if 20_000 <= close <= 35_000:
            rows.append({"date": date_str, "central": int(close)})

    return sorted(rows, key=lambda x: x["date"])

# ─────────────────────────────────────────────────────────────────
# FETCH với retry + fallback
# ─────────────────────────────────────────────────────────────────
def fetch_all(from_date: str, to_date: str) -> list:
    # Nguồn 1: stooq
    for attempt in range(3):
        try:
            rows = fetch_stooq(from_date, to_date)
            print(f"  ✅ stooq: {len(rows)} rows")
            return rows
        except Exception as e:
            print(f"  ⚠️  stooq attempt {attempt+1}: {e}")
            time.sleep(2 ** attempt)

    # Nguồn 2: Yahoo Finance
    print("  🔄 Fallback → Yahoo Finance...")
    for attempt in range(3):
        try:
            rows = fetch_yahoo(from_date, to_date)
            print(f"  ✅ Yahoo: {len(rows)} rows")
            return rows
        except Exception as e:
            print(f"  ⚠️  Yahoo attempt {attempt+1}: {e}")
            time.sleep(2 ** attempt)

    raise RuntimeError("Cả stooq và Yahoo Finance đều thất bại")

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    print("📈 USD/VND Rate Bot — stooq.com + Yahoo Finance")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"   Lịch sử từ: {START_DATE}")

    r2, bucket = get_r2()
    existing   = load_existing(r2, bucket)
    known      = {row["date"] for row in existing}
    today      = date.today().isoformat()

    # Xác định range cần fetch
    from_date = START_DATE if not known else (
        # Lùi lại 7 ngày để fill gap cuối tuần/lễ có thể bị bỏ sót
        (datetime.strptime(max(known), "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
    )

    if from_date >= today:
        last = existing[-1]
        print(f"✅ Up-to-date. Rate mới nhất ({last['date']}): {last['central']:,} VND/USD")
        return

    is_backfill = not known
    print(f"\n📡 [{'BACKFILL' if is_backfill else 'INCREMENTAL'}] "
          f"{from_date} → {today}")

    new_rows = fetch_all(from_date, today)

    if not new_rows:
        print("❌ Không có data")
        return

    # Merge + dedup + sort
    seen = {row["date"]: row for row in existing}
    seen.update({row["date"]: row for row in new_rows})
    all_rows = sorted(seen.values(), key=lambda r: r["date"])

    added = len(all_rows) - len(existing)
    print(f"   +{added} rows mới | Tổng: {len(all_rows):,} rows")

    print(f"💾 Lưu → R2 ({R2_KEY})...", flush=True)
    save_to_r2(r2, bucket, all_rows)

    last = all_rows[-1]
    print(f"✅ Done — {len(all_rows):,} rows trong R2")
    print(f"   Rate mới nhất ({last['date']}): {last['central']:,} VND/USD")

if __name__ == "__main__":
    main()
