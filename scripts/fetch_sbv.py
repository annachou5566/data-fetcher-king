"""
scripts/fetch_sbv.py
────────────────────────────────────────────────────────────────────
Fetch tỷ giá trung tâm USD/VND từ SBV (Ngân hàng Nhà nước VN)
Nguồn: dttktt.sbv.gov.vn — portal chính thức của SBV

Lịch sử: từ 04/01/2016 (ngày SBV bắt đầu cơ chế tỷ giá trung tâm)
Tần suất: mỗi ngày làm việc (thứ 2–6), SBV công bố lúc ~8h sáng

R2 file: sbv-data.json
Format:  { "v":1, "updated":"...", "count":N,
           "rows": [{"date":"YYYY-MM-DD","central":N}, ...] }

Env vars: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
          R2_ENDPOINT_URL,  R2_BUCKET_NAME
────────────────────────────────────────────────────────────────────
"""

import os, json, re, time, boto3, requests
from datetime import datetime, timedelta, timezone, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

START_DATE   = "2016-01-04"   # Ngày SBV bắt đầu công bố TGTTT
R2_KEY       = "sbv-data.json"
SBV_JSF_URL  = "https://dttktt.sbv.gov.vn/TyGia/faces/TyGiaTrungTam.jspx"

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

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
# SCRAPER — dttktt.sbv.gov.vn (JSF/Oracle ADF)
# ─────────────────────────────────────────────────────────────────
def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    return s

def get_jsf_state(session):
    """GET trang JSF để lấy ViewState + tên các hidden field."""
    r = session.get(SBV_JSF_URL, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Thu thập tất cả hidden inputs (Oracle ADF cần nhiều field)
    hidden = {}
    form = soup.find("form")
    if form:
        for inp in form.find_all("input", {"type": "hidden"}):
            name = inp.get("name") or inp.get("id")
            if name:
                hidden[name] = inp.get("value", "")

    # Tìm input ngày từ/đến
    from_field, to_field = None, None
    for inp in soup.find_all("input"):
        name = (inp.get("name") or "").lower()
        iid  = (inp.get("id")   or "").lower()
        label = name + iid
        if any(k in label for k in ["from", "tungay", "batdau", "startdate", "tudate"]):
            from_field = inp.get("name") or inp.get("id")
        elif any(k in label for k in ["to", "denngay", "ketthuc", "enddate", "todate"]):
            to_field = inp.get("name") or inp.get("id")

    # Tìm submit button
    btn = soup.find("input", {"type": "submit"}) or soup.find("button", {"type": "submit"})
    btn_name  = btn.get("name",  "") if btn else ""
    btn_value = btn.get("value", "") if btn else ""

    return hidden, from_field, to_field, btn_name, btn_value, r.text

def parse_table(html: str) -> list:
    """Parse HTML table → list[{date, central}]."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    # Tìm table có chứa USD và số tỷ giá
    for tbl in soup.find_all("table"):
        text = tbl.get_text()
        if "USD" not in text:
            continue

        for tr in tbl.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 2:
                continue

            date_val = None
            rate_val = None

            for cell in cells:
                # Ngày dạng dd/MM/yyyy
                m = re.match(r"(\d{2}/\d{2}/\d{4})", cell)
                if m:
                    try:
                        date_val = datetime.strptime(m.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
                    except ValueError:
                        pass

                # Tỷ giá: số nguyên 5 chữ số trong khoảng 20000–35000
                clean = re.sub(r"[,.\s\xa0]", "", cell)
                if clean.isdigit() and 20_000 <= int(clean) <= 35_000:
                    rate_val = int(clean)

            if date_val and rate_val:
                rows.append({"date": date_val, "central": rate_val})

        if rows:
            break   # lấy table đầu tiên có data là đủ

    # Dedup theo date (giữ giá trị đầu tiên)
    seen = {}
    for r in rows:
        seen.setdefault(r["date"], r)
    return sorted(seen.values(), key=lambda x: x["date"])

def fetch_range(from_date: str, to_date: str, session) -> list:
    """Fetch 1 range ngày từ SBV JSF portal."""
    d_from = datetime.strptime(from_date, "%Y-%m-%d").strftime("%d/%m/%Y")
    d_to   = datetime.strptime(to_date,   "%Y-%m-%d").strftime("%d/%m/%Y")

    hidden, from_field, to_field, btn_name, btn_value, _ = get_jsf_state(session)

    payload = {**hidden}
    if from_field: payload[from_field] = d_from
    if to_field:   payload[to_field]   = d_to
    if btn_name:   payload[btn_name]   = btn_value

    r = session.post(
        SBV_JSF_URL, data=payload,
        headers={**HEADERS,
                 "Content-Type": "application/x-www-form-urlencoded",
                 "Referer": SBV_JSF_URL},
        timeout=30,
    )
    r.raise_for_status()
    return parse_table(r.text)

# ─────────────────────────────────────────────────────────────────
# BACKFILL — chia theo quý, song song 4 thread
# ─────────────────────────────────────────────────────────────────
def split_quarters(from_date: str, to_date: str) -> list[tuple]:
    quarters = []
    cur = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date,   "%Y-%m-%d")
    while cur <= end:
        # Ngày cuối quý
        q_month = ((cur.month - 1) // 3) * 3 + 3
        q_end   = datetime(cur.year, q_month, 1) + timedelta(days=32)
        q_end   = q_end.replace(day=1) - timedelta(days=1)
        q_end   = min(q_end, end)
        quarters.append((cur.strftime("%Y-%m-%d"), q_end.strftime("%Y-%m-%d")))
        cur = q_end + timedelta(days=1)
    return quarters

def backfill(from_date: str, to_date: str) -> list:
    quarters = split_quarters(from_date, to_date)
    print(f"  📅 Chia thành {len(quarters)} quý, fetch song song 4 thread...")

    all_rows = []

    def fetch_one(q):
        qf, qt = q
        s = make_session()
        try:
            rows = fetch_range(qf, qt, s)
            return rows, qf, qt, None
        except Exception as e:
            return [], qf, qt, str(e)

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fetch_one, q): q for q in quarters}
        done = 0
        for f in as_completed(futures):
            rows, qf, qt, err = f.result()
            all_rows.extend(rows)
            done += 1
            status = f"{len(rows)} rows" if not err else f"LỖI: {err[:60]}"
            print(f"   [{done:2d}/{len(quarters)}] {qf} → {qt}: {status}")
            time.sleep(0.3)

    return all_rows

# ─────────────────────────────────────────────────────────────────
# INCREMENTAL — vài ngày còn thiếu
# ─────────────────────────────────────────────────────────────────
def fetch_incremental(missing_dates: list) -> list:
    from_date = min(missing_dates)
    to_date   = max(missing_dates)
    s = make_session()
    try:
        rows = fetch_range(from_date, to_date, s)
        print(f"  ✅ {len(rows)} rows ({from_date} → {to_date})")
        return rows
    except Exception as e:
        print(f"  ❌ {e}")
        return []

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    print("🏦 SBV Central Rate — Tỷ giá trung tâm USD/VND")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"   Nguồn: dttktt.sbv.gov.vn | Lịch sử từ: {START_DATE}")

    r2, bucket = get_r2()
    existing   = load_existing(r2, bucket)
    known      = {row["date"] for row in existing}
    today      = date.today()

    # Ngày còn thiếu (bỏ qua cuối tuần — SBV không công bố)
    start = datetime.strptime(START_DATE if not known else max(known), "%Y-%m-%d").date()
    missing = [
        (start + timedelta(days=i)).isoformat()
        for i in range((today - start).days + 1)
        if (start + timedelta(days=i)).weekday() < 5          # thứ 2–6
        and (start + timedelta(days=i)).isoformat() not in known
    ]

    if not missing:
        last = existing[-1]
        print(f"✅ Up-to-date. TGTTT mới nhất ({last['date']}): {last['central']:,} VND/USD")
        return

    is_backfill = len(missing) > 10
    print(f"\n📡 [{'BACKFILL' if is_backfill else 'INCREMENTAL'}] "
          f"{len(missing)} ngày: {missing[0]} → {missing[-1]}")

    new_rows = backfill(missing[0], missing[-1]) if is_backfill \
               else fetch_incremental(missing)

    # Validate
    valid = [r for r in new_rows if 20_000 <= r.get("central", 0) <= 35_000]
    skip  = len(new_rows) - len(valid)
    print(f"\n   ✅ {len(valid)} rows hợp lệ"
          + (f", bỏ {skip} rows lỗi" if skip else "")
          + f", {len(missing) - len(new_rows)} ngày không có data (lễ/Tết)")

    if not valid and not existing:
        print("❌ Không có data, bỏ qua")
        return

    # Merge + dedup + sort
    seen = {row["date"]: row for row in existing}
    seen.update({row["date"]: row for row in valid})
    all_rows = sorted(seen.values(), key=lambda r: r["date"])

    print(f"💾 Lưu {len(all_rows):,} rows → R2...", flush=True)
    save_to_r2(r2, bucket, all_rows)

    last = all_rows[-1]
    print(f"✅ Done — {len(all_rows):,} rows trong R2")
    print(f"   TGTTT ({last['date']}): {last['central']:,} VND/USD")

if __name__ == "__main__":
    main()
