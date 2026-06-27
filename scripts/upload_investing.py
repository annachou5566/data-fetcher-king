"""
scripts/upload_investing.py
────────────────────────────────────────────────────────────────────
One-time script: merge CSV từ investing.com → upload R2
Chạy 1 lần thủ công: python scripts/upload_investing.py

Kết quả: investing-data.json trong R2
Format: { "v":1, "rows": [{"date":"YYYY-MM-DD","close":N,"high":N,"low":N,"open":N}, ...] }
────────────────────────────────────────────────────────────────────
"""

import os, csv, json, boto3
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────
CSV_FILES = [
    "data/Dữ liệu Lịch sử USD_VND.csv",
    "data/Dữ liệu Lịch sử USD_VND (4).csv",
    "data/Dữ liệu Lịch sử USD_VND (2).csv",
]
R2_KEY = "investing-data.json"

# ── Parse ──────────────────────────────────────────────────────────
def parse_vn_number(s):
    """'26,300.0' → 26300"""
    try:
        return int(float(s.replace(',', '')))
    except:
        return None

def parse_date(s):
    """'26/06/2026' → '2026-06-26'"""
    try:
        return datetime.strptime(s.strip(), '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        return None

def parse_csv(filepath):
    rows = {}
    try:
        with open(filepath, encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                date  = parse_date(row.get('Ngày',''))
                close = parse_vn_number(row.get('Lần cuối',''))
                high  = parse_vn_number(row.get('Cao',''))
                low   = parse_vn_number(row.get('Thấp',''))
                open_ = parse_vn_number(row.get('Mở',''))
                if date and close and close > 5000:  # sanity check
                    rows[date] = {
                        'date':  date,
                        'close': close,
                        'high':  high or close,
                        'low':   low  or close,
                        'open':  open_ or close,
                    }
        print(f"  ✅ {os.path.basename(filepath)}: {len(rows)} rows")
    except FileNotFoundError:
        print(f"  ⚠️  {filepath} not found — skip")
    return rows

# ── R2 ─────────────────────────────────────────────────────────────
def get_r2():
    return boto3.client('s3',
        aws_access_key_id     = os.environ['R2_ACCESS_KEY_ID'],
        aws_secret_access_key = os.environ['R2_SECRET_ACCESS_KEY'],
        endpoint_url          = os.environ['R2_ENDPOINT_URL'],
    ), os.environ['R2_BUCKET_NAME']

# ── Main ────────────────────────────────────────────────────────────
def main():
    print("📊 Investing.com CSV → R2 uploader")

    # Parse tất cả files, merge (file sau override file trước nếu trùng date)
    merged = {}
    for path in CSV_FILES:
        rows = parse_csv(path)
        # Daily (file 1 & 4) override weekly (file 3) khi trùng date
        merged.update(rows)

    all_rows = sorted(merged.values(), key=lambda r: r['date'])
    print(f"\n📈 Tổng sau merge: {len(all_rows)} rows")
    print(f"   Từ: {all_rows[0]['date']}  →  {all_rows[-1]['date']}")
    print(f"   Close range: {min(r['close'] for r in all_rows):,} – {max(r['close'] for r in all_rows):,} VND")

    # Upload R2
    payload = {
        'v':       1,
        'source':  'investing.com',
        'updated': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'count':   len(all_rows),
        'rows':    all_rows,
    }
    body = json.dumps(payload, separators=(',',':')).encode('utf-8')
    print(f"\n💾 File size: {len(body)/1024:.0f} KB")

    r2, bucket = get_r2()
    r2.put_object(
        Bucket       = bucket,
        Key          = R2_KEY,
        Body         = body,
        ContentType  = 'application/json',
        CacheControl = 'max-age=86400',  # cache 1 ngày, không đổi
    )
    print(f"✅ Uploaded → R2/{R2_KEY}")
    print(f"   {len(all_rows):,} data points từ {all_rows[0]['date']} đến {all_rows[-1]['date']}")

if __name__ == '__main__':
    main()
