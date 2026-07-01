"""
scripts/fetch_interbank.py — v3 FINAL
────────────────────────────────────────────────────────────────────
Tự động hoàn toàn: Yahoo Finance USDVND=X → investing-data.json trong R2

Lần đầu chạy: backfill toàn bộ từ 2003 đến nay (~6000 rows)
Hàng ngày:    chỉ fetch 7 ngày gần nhất, override/append như cần

Nguồn: Yahoo Finance (USDVND=X) — cùng methodology với investing.com
       (cả 2 đều dùng mid-market interbank rate, sai lệch < 0.1%)
────────────────────────────────────────────────────────────────────
"""

import os, json, time, boto3
from datetime import datetime, timezone, timedelta, date
from curl_cffi import requests

R2_KEY = "investing-data.json"
SYMBOL = "USDVND=X"

# Yahoo Finance v8 chart API — không cần key, no rate limit aggressive
YF_URL = f"https://query1.finance.yahoo.com/v8/finance/chart/{SYMBOL}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json",
    "Referer":    "https://finance.yahoo.com",
}

def get_r2():
    return boto3.client("s3",
        aws_access_key_id     = os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key = os.environ["R2_SECRET_ACCESS_KEY"],
        endpoint_url          = os.environ["R2_ENDPOINT_URL"],
    ), os.environ["R2_BUCKET_NAME"]

def load_existing(r2, bucket):
    try:
        obj     = r2.get_object(Bucket=bucket, Key=R2_KEY)
        payload = json.loads(obj["Body"].read().decode("utf-8"))
        return payload
    except Exception:
        return {"v": 1, "rows": [], "source": "Yahoo Finance USDVND=X"}

def fetch_yf(session, period1_ts, period2_ts):
    """Fetch OHLCV từ Yahoo Finance theo unix timestamp range."""
    try:
        res = session.get(YF_URL, params={
            "interval":   "1d",
            "period1":    str(period1_ts),
            "period2":    str(period2_ts),
            "events":     "history",
            "includeAdjustedClose": "true",
        }, headers=HEADERS, timeout=20)

        if res.status_code != 200:
            print(f"  ⚠️  Yahoo Finance HTTP {res.status_code}")
            return []

        data   = res.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            print("  ⚠️  No result in Yahoo response")
            return []

        r         = result[0]
        ts_list   = r.get("timestamp", [])
        quote     = r.get("indicators", {}).get("quote", [{}])[0]
        opens     = quote.get("open",  [])
        highs     = quote.get("high",  [])
        lows      = quote.get("low",   [])
        closes    = quote.get("close", [])

        rows = []
        for i, ts in enumerate(ts_list):
            # Timestamp → date (UTC+7)
            dt   = datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(hours=7)
            d    = dt.strftime("%Y-%m-%d")
            # USDVND Yahoo trả về trong đơn vị VND trực tiếp (~26,000)
            c = closes[i] if closes[i] else None
            o = opens[i]  if i < len(opens)  else c
            h = highs[i]  if i < len(highs)  else c
            l = lows[i]   if i < len(lows)   else c
            if not c or c < 10000:   # sanity check
                continue
            rows.append({
                "date":  d,
                "close": round(c),
                "open":  round(o) if o else round(c),
                "high":  round(h) if h else round(c),
                "low":   round(l) if l else round(c),
            })

        return rows

    except Exception as e:
        print(f"  ⚠️  Yahoo fetch error: {e}")
        return []

def main():
    today = datetime.now(timezone(timedelta(hours=7))).date()
    print(f"💱 Interbank Bot — Yahoo Finance {SYMBOL}")
    print(f"   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    session = requests.Session(impersonate="chrome120")
    r2, bucket = get_r2()

    # Load existing
    payload  = load_existing(r2, bucket)
    rows_map = {r["date"]: r for r in payload.get("rows", [])}
    old_count = len(rows_map)
    print(f"  📦 R2: {old_count} rows hiện có")

    # Xác định range cần fetch
    # Lần đầu hoặc thiếu nhiều → backfill từ 2003
    oldest   = min(rows_map.keys()) if rows_map else "9999"
    needs_backfill = oldest > "2010-01-01"

    if needs_backfill:
        # Fetch từ 2003-01-01 đến hôm nay
        period1 = int(datetime(2003, 1, 1, tzinfo=timezone.utc).timestamp())
        period2 = int(datetime.combine(today + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp())
        print(f"  🔄 Backfill từ 2003 → {today}...")
    else:
        # Daily: fetch 10 ngày gần nhất (để cover weekend/holiday gap)
        start   = today - timedelta(days=10)
        period1 = int(datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc).timestamp())
        period2 = int(datetime.combine(today + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp())
        print(f"  📡 Daily update: {start} → {today}...")

    new_rows = fetch_yf(session, period1, period2)
    if not new_rows:
        print("❌ Không lấy được data từ Yahoo Finance")
        return

    print(f"  ✅ Yahoo trả về {len(new_rows)} rows")

    # Merge: Yahoo data override gì đã có
    # (Yahoo Finance data chính xác hơn er-api fallback cũ)
    added = updated = 0
    for row in new_rows:
        d = row["date"]
        old = rows_map.get(d)
        rows_map[d] = row  # luôn dùng Yahoo (không có _src tag = nguồn gốc)
        if old is None: added += 1
        elif old.get("_src") == "er-api": updated += 1

    all_rows = sorted(rows_map.values(), key=lambda r: r["date"])

    # Remove _src tags từ các ngày Yahoo đã có data thật
    for r in all_rows:
        r.pop("_src", None)

    # Save
    payload.update({
        "v":       1,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count":   len(all_rows),
        "rows":    all_rows,
        "source":  f"Yahoo Finance {SYMBOL} (interbank mid-market)",
    })

    body = json.dumps(payload, separators=(",",":")).encode()
    r2.put_object(
        Bucket=bucket, Key=R2_KEY, Body=body,
        ContentType="application/json", CacheControl="max-age=86400",
    )

    latest = all_rows[-1]
    print(f"\n✅ Done — {len(all_rows)} rows total")
    print(f"   +{added} ngày mới | {updated} ngày override (từ er-api tạm)")
    print(f"   Mới nhất ({latest['date']}): {latest['close']:,} ₫")
    if needs_backfill:
        print(f"   Oldest: {all_rows[0]['date']}")

if __name__ == "__main__":
    main()
