"""
scripts/fetch_sbv.py
────────────────────────────────────────────────────────────────────
Bot: Fetch tỷ giá SBV (Ngân hàng Nhà nước VN) → lưu R2
Chạy hàng ngày qua GitHub Actions

Thu thập 2 loại:
  1. Tỷ giá tham khảo Cục QLNH (API Liferay, USD Mua/Bán)
     → pageSize=100, lấy lịch sử từng trang
  2. Tỷ giá trung tâm (scrape HTML trang chính)
     → chỉ có ngày hiện tại, nhưng append mỗi ngày

R2 file: sbv-data.json
Format:
{
  "v": 1,
  "updated": "...",
  "rows": [
    {
      "date": "2026-06-26",
      "central": 25195,       ← Tỷ giá trung tâm
      "ref_buy": 23986,       ← Tỷ giá tham khảo Mua
      "ref_sell": 26404       ← Tỷ giá tham khảo Bán
    },
    ...
  ]
}
────────────────────────────────────────────────────────────────────
"""

import os, json, time, re, boto3
from datetime import datetime, timezone, timedelta
from curl_cffi import requests

# ── Config ─────────────────────────────────────────────────────────
SBV_API_URL  = "https://sbv.gov.vn/o/headless-delivery/v1.0/content-structures/3450514/structured-contents"
SBV_HTML_URL = "https://sbv.gov.vn/vi/t%E1%BB%B7-gi%C3%A1"
R2_KEY       = "sbv-data.json"

HEADERS = {
    "User-Agent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":      "application/json, text/html, */*",
    "Accept-Lang": "vi-VN,vi;q=0.9,en;q=0.8",
}

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
        rows = data.get("rows", [])
        print(f"  📦 R2: {len(rows)} rows hiện có")
        return rows
    except Exception:
        print("  📄 sbv-data.json chưa có → tạo mới")
        return []

# ── Parse 1 item từ Liferay API ────────────────────────────────────
def parse_api_item(item):
    """
    Trả về dict {date, ref_buy, ref_sell} từ 1 item Liferay structured content
    """
    fields   = item.get("contentFields", [])
    date_str = None
    usd_buy  = None
    usd_sell = None

    for f in fields:
        name = f.get("name", "")
        val  = f.get("contentFieldValue", {})

        # Ngày áp dụng
        if name == "ngayApDung":
            raw = val.get("data", "")
            if raw:
                # "2026-06-25T17:00:00Z" → "2026-06-26" (UTC+7)
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                dt_vn = dt + timedelta(hours=7)
                date_str = dt_vn.strftime("%Y-%m-%d")

        # Tỷ giá tham khảo (repeatable field set)
        elif name == "tyGiaThamKhaos":
            nested = f.get("nestedContentFields", [])
            ngoai_te = None
            mua = sell = None
            for nf in nested:
                n = nf.get("name", "")
                v = nf.get("contentFieldValue", {}).get("data", "")
                if n == "ngoaiTe":     ngoai_te = str(v)
                elif n == "mua":
                    try: mua = int(float(v))
                    except: pass
                elif n == "ban":
                    try: sell = int(float(v))
                    except: pass
            if ngoai_te and "USD" in ngoai_te:
                usd_buy  = mua
                usd_sell = sell

    if date_str and (usd_buy or usd_sell):
        return {"date": date_str, "ref_buy": usd_buy, "ref_sell": usd_sell}
    return None

# ── Fetch tỷ giá tham khảo qua API (có lịch sử) ───────────────────
def fetch_api_history(session):
    """
    Paginate qua Liferay API để lấy toàn bộ lịch sử tỷ giá tham khảo
    """
    all_rows = {}
    page     = 1
    total_pages = 1

    print("  📡 Fetching SBV API (tỷ giá tham khảo)...")

    while page <= total_pages:
        try:
            res = session.get(SBV_API_URL, params={
                "pageSize": 100,
                "page":     page,
                "sort":     "datePublished:desc",
            }, timeout=20)

            if res.status_code != 200:
                print(f"  ⚠️  API page={page} HTTP {res.status_code}")
                break

            # Liferay trả về JSON hoặc XML tuỳ Accept header
            # Thử parse JSON trước
            try:
                data = res.json()
            except Exception:
                # Nếu trả về XML thì parse XML
                data = parse_xml_response(res.text)

            items      = data.get("items", [])
            last_page  = data.get("lastPage",    1)
            total_count= data.get("totalCount",  0)
            total_pages = last_page

            parsed = 0
            for item in items:
                row = parse_api_item(item)
                if row and row["date"]:
                    all_rows[row["date"]] = row
                    parsed += 1

            print(f"  📄 Page {page}/{total_pages} → {parsed}/{len(items)} rows parsed (total: {len(all_rows)})")
            page += 1
            time.sleep(0.5)  # rate limit nhẹ

        except Exception as e:
            print(f"  ⚠️  API page={page}: {e}")
            break

    return all_rows

# ── Parse XML từ Liferay nếu không trả về JSON ─────────────────────
def parse_xml_response(text):
    """
    Minimal XML parse cho Liferay response — chỉ cần lấy items cơ bản
    """
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(text)
        # Tìm lastPage và totalCount
        last_page   = int(root.findtext("lastPage",   "1"))
        total_count = int(root.findtext("totalCount", "0"))
        items       = []

        for item_el in root.findall(".//items"):
            # Tái tạo dict structure từ XML
            item = {
                "contentFields": [],
                "datePublished": item_el.findtext("datePublished", ""),
            }
            date_field = {
                "name": "ngayApDung",
                "contentFieldValue": {"data": item_el.findtext(".//contentFields[name='ngayApDung']/contentFieldValue/data", "")},
                "nestedContentFields": []
            }

            # Parse ngayApDung
            for cf in item_el.findall("contentFields"):
                name = cf.findtext("name", "")
                val  = cf.findtext("contentFieldValue/data", "")
                nested = []
                for nf in cf.findall("nestedContentFields"):
                    n = nf.findtext("name", "")
                    v = nf.findtext("contentFieldValue/data", "")
                    nested.append({"name": n, "contentFieldValue": {"data": v}})

                item["contentFields"].append({
                    "name": name,
                    "contentFieldValue": {"data": val},
                    "nestedContentFields": nested,
                })
            items.append(item)

        return {"items": items, "lastPage": last_page, "totalCount": total_count}
    except Exception as e:
        print(f"  ⚠️  XML parse error: {e}")
        return {"items": [], "lastPage": 1, "totalCount": 0}

# ── Scrape Tỷ giá trung tâm từ HTML ────────────────────────────────
def fetch_central_rate(session):
    """
    Scrape trang SBV để lấy Tỷ giá trung tâm hôm nay
    Trả về: {"date": "2026-06-27", "central": 25195}
    """
    try:
        res = session.get(SBV_HTML_URL, timeout=20)
        if res.status_code != 200:
            print(f"  ⚠️  HTML scrape HTTP {res.status_code}")
            return None

        html = res.text

        # Tìm số tỷ giá trung tâm: thường là 5 chữ số gần từ "VND" hoặc "Đồng Việt Nam"
        # Pattern: "25.195 VND" hoặc "25195" gần từ "trung tâm"
        patterns = [
            r'Đô la Mỹ\s*=\s*([\d,\.]+)\s*VND',     # "1 Đô la Mỹ = 25.195 VND"
            r'([\d\.]+)\s*VND\s*\n',                  # "25.195 VND"
            r'Tỷ giá trung tâm.*?(2[0-9][.,]\d{3})',  # any 5-digit near "trung tâm"
        ]

        central = None
        for pat in patterns:
            m = re.search(pat, html, re.DOTALL | re.IGNORECASE)
            if m:
                raw = m.group(1).replace(".", "").replace(",", "")
                try:
                    v = int(raw)
                    if 20000 < v < 35000:  # sanity check VND/USD
                        central = v
                        break
                except Exception:
                    pass

        # Tìm ngày áp dụng
        date_m = re.search(r'áp dụng.*?ngày\s+(\d{2}/\d{2}/\d{4})', html, re.IGNORECASE)
        if not date_m:
            date_m = re.search(r'(\d{2}/\d{2}/\d{4})', html)
        date_str = None
        if date_m:
            d, m_, y = date_m.group(1).split("/")
            date_str = f"{y}-{m_}-{d}"

        if central:
            print(f"  ✅ Trung tâm: {central:,} ₫  ({date_str or 'ngày không rõ'})")
            return {"date": date_str or datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d"),
                    "central": central}
        else:
            print("  ⚠️  Không parse được Tỷ giá trung tâm từ HTML")
            return None

    except Exception as e:
        print(f"  ⚠️  Scrape error: {e}")
        return None

# ── Merge và save ──────────────────────────────────────────────────
def save_to_r2(r2, bucket, rows_by_date):
    all_rows = sorted(rows_by_date.values(), key=lambda r: r["date"])
    payload  = {
        "v":       1,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":   len(all_rows),
        "rows":    all_rows,
    }
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    r2.put_object(
        Bucket      = bucket,
        Key         = R2_KEY,
        Body        = body,
        ContentType = "application/json",
        CacheControl= "max-age=3600",
    )
    return len(all_rows)

# ── Main ────────────────────────────────────────────────────────────
def main():
    print("🏦 SBV Rate Bot — Ngân hàng Nhà nước Việt Nam")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    session = requests.Session(impersonate="chrome116")

    # 1. Load existing R2 data
    r2, bucket = get_r2()
    existing   = load_existing(r2, bucket)
    rows_by_date = {r["date"]: r for r in existing}

    # 2. Fetch tỷ giá tham khảo (API) — lịch sử đầy đủ
    print("\n📡 Bước 1: Tỷ giá tham khảo Cục QLNH (API)...")
    api_rows = fetch_api_history(session)
    for date, row in api_rows.items():
        if date not in rows_by_date:
            rows_by_date[date] = row
        else:
            # Merge: cập nhật ref_buy/ref_sell nếu có
            rows_by_date[date].update({
                k: v for k, v in row.items()
                if v is not None and k in ("ref_buy", "ref_sell")
            })
    print(f"  → {len(api_rows)} rows từ API")

    # 3. Fetch tỷ giá trung tâm (HTML scrape) — chỉ hôm nay
    print("\n📡 Bước 2: Tỷ giá trung tâm (HTML scrape)...")
    central = fetch_central_rate(session)
    if central and central.get("central"):
        d = central["date"]
        if d not in rows_by_date:
            rows_by_date[d] = {"date": d}
        rows_by_date[d]["central"] = central["central"]
        print(f"  → Đã merge trung tâm vào ngày {d}")

    # 4. Save
    print(f"\n💾 Lưu {len(rows_by_date)} rows → R2...")
    total = save_to_r2(r2, bucket, rows_by_date)

    # Summary
    latest = sorted(rows_by_date.values(), key=lambda r: r["date"])[-1] if rows_by_date else {}
    print(f"✅ Done — {total} rows trong R2")
    if latest:
        print(f"   Mới nhất ({latest.get('date')}):")
        if latest.get("central"):  print(f"   Trung tâm: {latest['central']:,} ₫")
        if latest.get("ref_buy"):  print(f"   Tham khảo Mua: {latest['ref_buy']:,} ₫")
        if latest.get("ref_sell"): print(f"   Tham khảo Bán: {latest['ref_sell']:,} ₫")

if __name__ == "__main__":
    main()
