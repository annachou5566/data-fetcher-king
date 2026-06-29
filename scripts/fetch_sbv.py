"""
scripts/fetch_sbv.py — v3
────────────────────────────────────────────────────────────────────
Lấy tỷ giá SBV — chỉ lấy data thật, không tính toán gì hết.

Nguồn: sbv.gov.vn Liferay API (content structure 3450514)
  → Tỷ giá tham khảo Cục QLNH: USD Mua/Bán
  → Kèm tỷ giá trung tâm nếu API trả về (content structure khác)

Nếu không lấy được → exit, không lưu gì.
Lịch sử tích lũy từng ngày giống fetch_p2p.py.

R2 file: sbv-data.json
Format:
{
  "v": 1,
  "updated": "...",
  "count": N,
  "rows": [
    {"date":"2026-06-27", "ref_buy":23986, "ref_sell":26404},
    ...
  ]
}
────────────────────────────────────────────────────────────────────
"""

import os, json, time, boto3
from datetime import datetime, timezone, timedelta
from curl_cffi import requests

R2_KEY  = "sbv-data.json"
SBV_API = "https://sbv.gov.vn/o/headless-delivery/v1.0/content-structures/3450514/structured-contents"

# ── R2 ─────────────────────────────────────────────────────────────
def get_r2():
    return boto3.client("s3",
        aws_access_key_id     = os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["R2_SECRET_ACCESS_KEY"],
        endpoint_url          = os.environ["R2_ENDPOINT_URL"],
    ), os.environ["R2_BUCKET_NAME"]

def load_existing(r2, bucket):
    try:
        obj  = r2.get_object(Bucket=bucket, Key=R2_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data.get("rows", [])
    except Exception:
        return []

# ── Parse 1 Liferay item ───────────────────────────────────────────
def parse_item(item):
    date_str = None
    ref_buy  = None
    ref_sell = None

    for f in item.get("contentFields", []):
        name = f.get("name", "")
        val  = f.get("contentFieldValue", {})

        if name == "ngayApDung":
            raw = val.get("data", "")
            if raw:
                dt       = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                date_str = (dt + timedelta(hours=7)).strftime("%Y-%m-%d")

        elif name == "tyGiaThamKhaos":
            nested   = f.get("nestedContentFields", [])
            currency = None
            mua = ban = None
            for nf in nested:
                n = nf.get("name","")
                v = nf.get("contentFieldValue",{}).get("data","")
                if n == "ngoaiTe":  currency = str(v)
                elif n == "mua":
                    try: mua = int(float(v))
                    except: pass
                elif n == "ban":
                    try: ban = int(float(v))
                    except: pass
            if currency and "USD" in currency:
                ref_buy  = mua
                ref_sell = ban

    if not date_str:
        return None
    if not ref_buy and not ref_sell:
        return None

    return {"date": date_str, "ref_buy": ref_buy, "ref_sell": ref_sell}

# ── Fetch từ SBV API ───────────────────────────────────────────────
def fetch_sbv(session, page=1):
    """Trả về (rows, last_page) hoặc (None, 1) nếu lỗi."""
    try:
        res = session.get(SBV_API, params={
            "pageSize": 100, "page": page, "sort": "datePublished:desc",
        }, timeout=20, headers={
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept":          "application/json",
            "Accept-Language": "vi-VN,vi;q=0.9",
            "Referer":         "https://sbv.gov.vn/",
        })

        if res.status_code != 200:
            print(f"  ⚠️  HTTP {res.status_code}")
            return None, 1

        data      = res.json()
        items     = data.get("items", [])
        last_page = data.get("lastPage", 1)
        rows      = [r for r in (parse_item(i) for i in items) if r]
        return rows, last_page

    except Exception as e:
        print(f"  ⚠️  {e}")
        return None, 1

# ── Save ────────────────────────────────────────────────────────────
def save(r2, bucket, rows_by_date):
    all_rows = sorted(rows_by_date.values(), key=lambda r: r["date"])
    payload  = {
        "v":       1,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":   len(all_rows),
        "rows":    all_rows,
    }
    r2.put_object(
        Bucket      = bucket, Key = R2_KEY,
        Body        = json.dumps(payload, separators=(",",":"), ensure_ascii=False).encode(),
        ContentType = "application/json", CacheControl = "max-age=3600",
    )
    return len(all_rows)

# ── Main ────────────────────────────────────────────────────────────
def main():
    print("🏦 SBV Rate Bot")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    session = requests.Session(impersonate="chrome116")

    # Thử page 1 trước
    print("\n📡 Thử SBV API...")
    rows, last_page = fetch_sbv(session, page=1)

    if rows is None:
        print("❌ SBV API không trả lời (IP bị block hoặc timeout)")
        print("   → Không lưu gì. Data sẽ tích lũy khi API accessible.")
        print("   💡 Giải pháp: route qua Render/proxy có IP Việt Nam")
        return

    if not rows:
        print("⚠️  API trả lời nhưng không có data USD/VND")
        return

    print(f"  ✅ Trang 1/{last_page} — {len(rows)} rows")

    # Load existing
    r2, bucket   = get_r2()
    existing     = load_existing(r2, bucket)
    rows_by_date = {r["date"]: r for r in existing}
    prev_count   = len(rows_by_date)
    print(f"  📦 R2 hiện có: {prev_count} rows")

    # Merge trang 1
    new_count = 0
    for row in rows:
        d = row["date"]
        if d not in rows_by_date:
            new_count += 1
        rows_by_date[d] = {**rows_by_date.get(d, {}), **row}

    # Backfill: nếu có nhiều trang VÀ chưa đủ data lịch sử
    oldest = min(rows_by_date.keys()) if rows_by_date else "9999"
    needs_backfill = oldest > "2020-01-01" and last_page > 1

    if needs_backfill:
        print(f"  🔄 Backfill {last_page - 1} trang còn lại...")
        for page in range(2, last_page + 1):
            page_rows, _ = fetch_sbv(session, page)
            if page_rows is None:
                print(f"  ⚠️  Dừng tại page {page} (lỗi)")
                break
            added = 0
            for row in page_rows:
                d = row["date"]
                if d not in rows_by_date:
                    rows_by_date[d] = row; added += 1
                else:
                    rows_by_date[d] = {**rows_by_date[d], **row}
            new_count += added
            print(f"  📄 Page {page}/{last_page} → +{added} rows (total: {len(rows_by_date)})")
            time.sleep(0.4)

    # Save
    total = save(r2, bucket, rows_by_date)
    latest = sorted(rows_by_date.values(), key=lambda r: r["date"])[-1]
    print(f"\n✅ Done — {total} rows (+{new_count} mới)")
    print(f"   Mới nhất ({latest['date']}): Mua {latest.get('ref_buy','N/A'):,}  Bán {latest.get('ref_sell','N/A'):,}")

if __name__ == "__main__":
    main()
