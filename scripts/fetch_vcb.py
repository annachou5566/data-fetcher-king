"""
scripts/fetch_vcb.py
────────────────────────────────────────────────────────────────────
Bot: Fetch Vietcombank USD/VND exchange rates → lưu vĩnh viễn vào R2

Chiến lược:
  - Lần đầu (workflow_dispatch): backfill toàn bộ từ 2010-01-01
  - Hàng ngày: chỉ fetch ngày còn thiếu (incremental, < 5 giây)
  - R2 là nguồn sự thật — data tích lũy mãi mãi, không phụ thuộc VCB API

Env vars (dùng chung secrets với fetch_p2p):
  R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME

R2 file: vcb-data.json
Format:  { "v":1, "updated":"...", "count":N,
           "rows": [{"date":"YYYY-MM-DD","cash":N,"transfer":N,"sell":N}, ...] }
────────────────────────────────────────────────────────────────────
"""

import os, json, time, boto3, requests
from datetime import datetime, timedelta, timezone, date

START_DATE = "2010-01-01"
VCB_API    = "https://www.vietcombank.com.vn/api/exchangerates"
R2_KEY     = "vcb-data.json"
HEADERS    = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json",
}

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

def load_existing(r2, bucket):
    try:
        obj  = r2.get_object(Bucket=bucket, Key=R2_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        rows = data.get("rows", [])
        print(f"  📦 R2 hiện có: {len(rows):,} rows")
        return rows
    except r2.exceptions.NoSuchKey:
        print("  📄 vcb-data.json chưa có → backfill từ đầu")
        return []
    except Exception as e:
        print(f"  ⚠️  Load R2 lỗi: {e} → tạo mới")
        return []

def fetch_day(date_str, session):
    try:
        r = session.get(VCB_API, params={"date": date_str}, timeout=15)
        if r.status_code == 404:
            return None  # Cuối tuần / ngày lễ
        r.raise_for_status()
        js  = r.json()
        usd = next((x for x in js.get("Data", []) if x.get("currencyCode") == "USD"), None)
        if not usd:
            return None
        return {
            "date":     (js.get("Date") or date_str)[:10],
            "cash":     int(usd.get("cash")     or 0) or None,
            "transfer": int(usd.get("transfer") or 0) or None,
            "sell":     int(usd.get("sell")     or 0) or None,
        }
    except Exception as e:
        print(f"  ⚠️  {date_str}: {e}")
        return None

def fetch_range(dates, session):
    rows, total = [], len(dates)
    for i, d in enumerate(dates):
        rec = fetch_day(d, session)
        if rec:
            rows.append(rec)
        if total > 100 and (i + 1) % 100 == 0:
            print(f"   ... {i+1}/{total} ({d})")
        time.sleep(0.05 if total > 100 else 0.3)
    return rows

def save_to_r2(r2, bucket, all_rows):
    payload = {
        "v":       1,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":   len(all_rows),
        "rows":    all_rows,
    }
    r2.put_object(
        Bucket=bucket, Key=R2_KEY,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json", CacheControl="max-age=3600",
    )

def main():
    print("🏦 VCB Rate Bot — Vietcombank USD/VND")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    r2, bucket  = get_r2()
    existing    = load_existing(r2, bucket)
    known_dates = {row["date"] for row in existing}

    # Tính ngày cần fetch
    start = START_DATE if not known_dates else max(known_dates)
    cur   = datetime.strptime(start, "%Y-%m-%d").date()
    today = date.today()
    dates = [
        (cur + timedelta(days=i)).isoformat()
        for i in range((today - cur).days + 1)
        if (cur + timedelta(days=i)).isoformat() not in known_dates
    ]

    if not dates:
        last = existing[-1]
        print(f"✅ Up-to-date. Rate mới nhất ({last['date']}): "
              f"transfer={last['transfer']:,}  sell={last['sell']:,}")
        return

    mode = "BACKFILL" if len(dates) > 10 else "INCREMENTAL"
    print(f"📡 [{mode}] Fetch {len(dates)} ngày: {dates[0]} → {dates[-1]}", flush=True)

    session  = requests.Session()
    session.headers.update(HEADERS)
    new_rows = fetch_range(dates, session)
    print(f"   ✅ {len(new_rows)} rows hợp lệ "
          f"(bỏ qua {len(dates) - len(new_rows)} ngày không có data — cuối tuần/lễ)")

    if not new_rows and not existing:
        print("❌ Không có data, bỏ qua upload")
        return

    # Merge + dedup + sort
    seen = {row["date"]: row for row in existing}
    seen.update({row["date"]: row for row in new_rows})
    all_rows = sorted(seen.values(), key=lambda r: r["date"])

    print(f"💾 Lưu {len(all_rows):,} rows → R2...", flush=True)
    save_to_r2(r2, bucket, all_rows)

    last = all_rows[-1]
    print(f"✅ Done — {len(all_rows):,} rows trong R2")
    print(f"   Rate mới nhất ({last['date']}): "
          f"cash={last['cash']:,}  transfer={last['transfer']:,}  sell={last['sell']:,}")

if __name__ == "__main__":
    main()
