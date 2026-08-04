"""
scripts/fetch_sbv.py — final
────────────────────────────────────────────────────────────────────
Fetch 2 API SBV → merge → lưu R2

① Content 137473: Tỷ giá TRUNG TÂM  (field: TyGiaSo)
② Content 3450514: Tỷ giá THAM KHẢO Cục QLNH (field: mua, ban)

R2 file: sbv-data.json
Format:
{
  "v": 2,
  "rows": [
    {
      "date": "2026-06-29",
      "central":  25201,   ← Tỷ giá trung tâm
      "ref_buy":  23991,   ← Tham khảo Mua USD
      "ref_sell": 26411    ← Tham khảo Bán USD
    }, ...
  ]
}
────────────────────────────────────────────────────────────────────
"""

import os, json, time, boto3
from datetime import datetime, timezone, timedelta
from curl_cffi import requests

R2_KEY        = "sbv-data.json"
URL_CENTRAL   = "https://sbv.gov.vn/o/headless-delivery/v1.0/content-structures/137473/structured-contents"
URL_REF       = "https://sbv.gov.vn/o/headless-delivery/v1.0/content-structures/3450514/structured-contents"
HEADERS       = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Accept":          "application/json",
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Referer":         "https://sbv.gov.vn/",
}

MAX_RETRIES   = 5      # số lần thử lại khi bị "Connection closed abruptly"
RETRY_SLEEP   = 3      # giây, tăng dần theo backoff
PAGE_SLEEP    = 1.0    # giây nghỉ giữa các page (tránh bị chặn do gọi quá nhanh)

def new_session():
    """Tạo session curl_cffi mới (dùng khi session cũ bị server đóng kết nối)."""
    return requests.Session(impersonate="chrome116")

# ── R2 ─────────────────────────────────────────────────────────────
def get_r2():
    return boto3.client("s3",
        aws_access_key_id     = os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["R2_SECRET_ACCESS_KEY"],
        endpoint_url          = os.environ["R2_ENDPOINT_URL"],
    ), os.environ["R2_BUCKET_NAME"]

def load_existing(r2, bucket):
    try:
        obj = r2.get_object(Bucket=bucket, Key=R2_KEY)
        return json.loads(obj["Body"].read().decode("utf-8")).get("rows", [])
    except Exception:
        return []

# ── Date helper ─────────────────────────────────────────────────────
def to_date(raw_utc):
    """'2026-06-28T17:00:00Z' → '2026-06-29' (UTC+7)"""
    try:
        dt = datetime.fromisoformat(raw_utc.replace("Z", "+00:00"))
        return (dt + timedelta(hours=7)).strftime("%Y-%m-%d")
    except Exception:
        return None

def friendly_to_date(friendly):
    """'29/06/2026' → '2026-06-29'"""
    try:
        d, m, y = friendly.strip().split("/")
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    except Exception:
        return None

# ── Fetch tất cả pages của 1 API ────────────────────────────────────
def fetch_all_pages(session, url, parse_fn, label):
    all_rows  = {}
    page      = 1
    last_page = 1

    while page <= last_page:
        attempt = 0
        while True:
            attempt += 1
            try:
                res = session.get(url, params={
                    "pageSize": 100, "page": page, "sort": "datePublished:desc",
                }, timeout=30, headers=HEADERS)

                if res.status_code != 200:
                    print(f"  ⚠️  {label} page={page}: HTTP {res.status_code}")
                    if attempt <= MAX_RETRIES:
                        time.sleep(RETRY_SLEEP * attempt)
                        session = new_session()
                        continue
                    return all_rows  # bỏ cuộc với API này, giữ data đã có

                data      = res.json()
                items     = data.get("items", [])
                last_page = data.get("lastPage", 1)

                new = 0
                for item in items:
                    row = parse_fn(item)
                    if row and row.get("date"):
                        all_rows[row["date"]] = row
                        new += 1

                print(f"  📄 {label} page {page}/{last_page} → {new}/{len(items)} rows")
                break  # thành công → thoát vòng retry, sang page tiếp theo

            except Exception as e:
                print(f"  ⚠️  {label} page={page} (thử {attempt}/{MAX_RETRIES}): {e}")
                if attempt <= MAX_RETRIES:
                    time.sleep(RETRY_SLEEP * attempt)
                    session = new_session()  # kết nối cũ có thể đã bị server đóng, tạo session mới
                    continue
                print(f"  ❌ {label}: bỏ cuộc sau {MAX_RETRIES} lần thử, giữ {len(all_rows)} rows đã lấy được")
                return all_rows

        page += 1
        time.sleep(PAGE_SLEEP)

    return all_rows

# ── Parse Tỷ giá Trung tâm (structure 137473) ──────────────────────
def parse_central(item):
    fields = {f["name"]: f.get("contentFieldValue", {}).get("data", "")
              for f in item.get("contentFields", [])}

    # Ngày: dùng friendlyUrlPath hoặc NgayBatDau
    date = friendly_to_date(item.get("friendlyUrlPath", "")) or \
           to_date(fields.get("NgayBatDau", ""))
    if not date:
        return None
    # Validate YYYY-MM-DD format
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except Exception:
        return None

    val = fields.get("TyGiaSo", "")
    try:
        central = int(float(val))
        if not (20000 < central < 40000):
            return None
    except Exception:
        return None

    return {"date": date, "central": central}

# ── Parse Tỷ giá Tham khảo Cục QLNH (structure 3450514) ────────────
def parse_ref(item):
    fields   = item.get("contentFields", [])
    date_str = None
    ref_buy  = None
    ref_sell = None

    for f in fields:
        name = f.get("name", "")
        val  = f.get("contentFieldValue", {}).get("data", "")

        if name == "ngayApDung":
            date_str = to_date(val)

        elif name == "tyGiaThamKhaos":
            nested   = f.get("nestedContentFields", [])
            currency = None
            mua = ban = None
            for nf in nested:
                n = nf.get("name", "")
                v = nf.get("contentFieldValue", {}).get("data", "")
                if n == "ngoaiTe":  currency = str(v)
                elif n == "mua":
                    try: mua = int(float(v))
                    except: pass
                elif n == "ban":
                    try: ban = int(float(v))
                    except: pass
            if currency and "USD" in currency:
                ref_buy, ref_sell = mua, ban

    if not date_str:
        return None
    if not ref_buy and not ref_sell:
        return None
    return {"date": date_str, "ref_buy": ref_buy, "ref_sell": ref_sell}

# ── Save ────────────────────────────────────────────────────────────
def save(r2, bucket, rows_by_date):
    # Chặn các row có ngày ở TƯƠNG LAI (dữ liệu rác từ API) — mốc so sánh là hôm nay theo giờ VN (UTC+7)
    today_ict = (datetime.now(timezone.utc) + timedelta(hours=7)).strftime("%Y-%m-%d")
    bad = [d for d in list(rows_by_date.keys()) if d > today_ict]
    for d in bad:
        print(f"  🗑️  Bỏ row ngày tương lai (dữ liệu rác): {d}")
        del rows_by_date[d]

    all_rows = sorted(rows_by_date.values(), key=lambda r: r["date"])
    payload  = {
        "v":       2,
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
    print("🏦 SBV Rate Bot — Trung tâm + Tham khảo")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    r2, bucket = get_r2()

    # Load existing
    existing     = load_existing(r2, bucket)
    rows_by_date = {r["date"]: r for r in existing}
    print(f"  📦 R2 hiện có: {len(rows_by_date)} rows")

    # ── Fetch Tỷ giá Trung tâm ──────────────────────────────────────
    print("\n📡 Tỷ giá TRUNG TÂM (structure 137473)...")
    central_rows = fetch_all_pages(new_session(), URL_CENTRAL, parse_central, "Central")
    if not central_rows:
        print("  ❌ Không lấy được Trung tâm")
    else:
        for d, row in central_rows.items():
            rows_by_date.setdefault(d, {})["date"]    = d
            rows_by_date[d]["central"] = row["central"]
        print(f"  ✅ {len(central_rows)} rows trung tâm")

    # ── Fetch Tỷ giá Tham khảo ──────────────────────────────────────
    print("\n📡 Tỷ giá THAM KHẢO Cục QLNH (structure 3450514)...")
    ref_rows = fetch_all_pages(new_session(), URL_REF, parse_ref, "Ref")
    if not ref_rows:
        print("  ❌ Không lấy được Tham khảo")
    else:
        for d, row in ref_rows.items():
            rows_by_date.setdefault(d, {})["date"]     = d
            if row.get("ref_buy"):  rows_by_date[d]["ref_buy"]  = row["ref_buy"]
            if row.get("ref_sell"): rows_by_date[d]["ref_sell"] = row["ref_sell"]
        print(f"  ✅ {len(ref_rows)} rows tham khảo")

    if not central_rows and not ref_rows:
        print("\n❌ Không lấy được gì. Thoát.")
        return

    # ── Save ─────────────────────────────────────────────────────────
    total  = save(r2, bucket, rows_by_date)
    valid = [r for r in rows_by_date.values()
              if r.get("date") and len(r["date"]) == 10]
    if not valid:
        print("⚠️  Không có row hợp lệ")
        return
    latest = sorted(valid, key=lambda r: r["date"])[-1]
    print(f"\n✅ Done — {total} rows trong R2")
    print(f"   Mới nhất ({latest['date']}):")
    if latest.get("central"):  print(f"   Trung tâm:      {latest['central']:,} ₫")
    if latest.get("ref_buy"):  print(f"   Tham khảo Mua:  {latest['ref_buy']:,} ₫")
    if latest.get("ref_sell"): print(f"   Tham khảo Bán:  {latest['ref_sell']:,} ₫")

if __name__ == "__main__":
    main()
