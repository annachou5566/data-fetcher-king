"""
seed_klines.py v2
-----------------
Download historical klines từ Binance Vision (data.binance.vision)
- Top 100 token theo market cap
- Pairs: USDT, USDC, BTC, ETH
- Tự động skip nếu pair không tồn tại
- Tự detect tháng đầu tiên có data (không hardcode)
- Convert CSV → SQL batch → Import vào Cloudflare D1
"""

import urllib.request
import urllib.error
import zipfile
import io
import os
import sys
import time
import json

# ─── TOP 100 BASE SYMBOLS (không có USDT/USDC suffix) ────────────────────────
# Xếp theo market cap tại 2025, update thủ công mỗi quý nếu cần
TOP100_BASES = [
    "BTC","ETH","XRP","BNB","SOL","DOGE","ADA","TRX","AVAX","LINK",
    "TON","SHIB","SUI","DOT","BCH","LTC","UNI","HBAR","NEAR","APT",
    "XLM","CRO","ONDO","ICP","ETC","TAO","VET","RENDER","ARB","FIL",
    "ATOM","OP","FET","ALGO","BONK","MKR","IMX","PEPE","INJ","SEI",
    "GRT","FLOKI","JASMY","LDO","SAND","MANA","AXS","THETA","CHZ","ENS",
    "QNT","EGLD","CAKE","FTM","KAS","BLUR","1INCH","AAVE","SNX","COMP",
    "ZEC","XTZ","BAT","YFI","SUSHI","ZIL","IOTA","WAVES","DASH","NEO",
    "EOS","ONT","BTT","HOT","WIN","SC","XEM","STORJ","SKL","CKB",
    "RSR","DYDX","STX","CFX","ACH","ANKR","OMG","RVN","CELR","CELO",
    "KAVA","BNX","LUNC","LUNA","MASK","PEOPLE","SPELL","GALA","ENJ","CHR",
]

# Pairs sẽ thử cho mỗi base symbol
QUOTE_ASSETS = ["USDT", "USDC", "BTC", "ETH"]

# Intervals: (interval, start_year, start_month)
# start là fallback — script sẽ tự tìm tháng đầu thực sự có data
INTERVALS = [
    ("1w",  2017, 7),
    ("1d",  2017, 7),
    ("4h",  2017, 7),
    ("2h",  2017, 7),
    ("1h",  2017, 7),
    ("30m", 2022, 1),
    ("15m", 2022, 1),
    ("5m",  2024, 1),
    ("1m",  2025, 1),
]

BASE_URL   = "https://data.binance.vision/data/spot/monthly/klines"
OUTPUT_DIR = "./klines_sql"
BATCH_SIZE = 500
END_YEAR   = 2026
END_MONTH  = 5

# ─── UTILS ───────────────────────────────────────────────────────────────────

def month_range(start_year, start_month):
    result = []
    y, m = start_year, start_month
    while (y, m) <= (END_YEAR, END_MONTH):
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def try_download(url, timeout=20):
    """Tải URL, trả về bytes hoặc None."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status == 200:
                return resp.read()
    except Exception:
        pass
    return None


def pair_exists(symbol, interval):
    """Kiểm tra pair có tồn tại trên Binance Vision không (thử 2017-08 và 2021-01)."""
    for y, m in [(2021, 1), (2023, 6), (2024, 1)]:
        url = f"{BASE_URL}/{symbol}/{interval}/{symbol}-{interval}-{y}-{m:02d}.zip"
        data = try_download(url, timeout=10)
        if data:
            return True
    return False


def find_start_month(symbol, interval, fallback_year, fallback_month):
    """
    Tìm tháng đầu tiên có data bằng binary search đơn giản.
    Nếu không tìm được thì dùng fallback.
    """
    months = month_range(fallback_year, fallback_month)
    # Thử tháng đầu tiên trong list
    for y, m in months[:3]:
        url = f"{BASE_URL}/{symbol}/{interval}/{symbol}-{interval}-{y}-{m:02d}.zip"
        if try_download(url, timeout=10):
            return y, m
    return fallback_year, fallback_month


def parse_csv_from_zip(zip_bytes, symbol, interval):
    rows = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        content = z.read(z.namelist()[0]).decode("utf-8")
        for line in content.splitlines():
            if not line.strip():
                continue
            cols = line.split(",")
            if not cols[0].isdigit():
                continue
            try:
                rows.append({
                    "symbol":    symbol,
                    "interval":  interval,
                    "open_time": int(cols[0]),
                    "open":      float(cols[1]),
                    "high":      float(cols[2]),
                    "low":       float(cols[3]),
                    "close":     float(cols[4]),
                    "volume":    float(cols[5]),
                })
            except (ValueError, IndexError):
                continue
    return rows


def rows_to_sql(rows, table="klines"):
    sql_lines = []
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        values = ",\n  ".join(
            f"('{r['symbol']}','{r['interval']}',"
            f"{r['open_time']},{r['open']},{r['high']},"
            f"{r['low']},{r['close']},{r['volume']})"
            for r in batch
        )
        sql_lines.append(
            f"INSERT OR IGNORE INTO klines "
            f"(symbol,interval,open_time,open,high,low,close,volume) VALUES\n"
            f"  {values};\n"
        )
    return "\n".join(sql_lines)


# ─── LOGIC CHÍNH ─────────────────────────────────────────────────────────────

def build_symbol_list(filter_symbol=None, filter_quote=None):
    """Tạo danh sách tất cả symbol pairs cần download."""
    pairs = []
    bases = [filter_symbol] if filter_symbol else TOP100_BASES
    quotes = [filter_quote] if filter_quote else QUOTE_ASSETS
    for base in bases:
        for quote in quotes:
            # BTC không cần pair BTCBTC hay BTCETH
            if base == quote:
                continue
            if base == "ETH" and quote == "ETH":
                continue
            pairs.append(f"{base}{quote}")
    return pairs


def seed_pair_interval(symbol, interval, start_year, start_month):
    """Download 1 symbol + 1 interval, ghi file SQL."""
    months = month_range(start_year, start_month)
    all_rows = []
    skipped_start = 0  # đếm số tháng đầu không có data

    for idx, (y, m) in enumerate(months, 1):
        url = f"{BASE_URL}/{symbol}/{interval}/{symbol}-{interval}-{y}-{m:02d}.zip"
        data = try_download(url)

        if data is None:
            # Nếu chưa có data ở đầu, tiếp tục tìm
            if not all_rows:
                skipped_start += 1
            # Nếu đã có data rồi mà bị None → tháng hiện tại chưa publish, dừng
            else:
                break
            continue

        rows = parse_csv_from_zip(data, symbol, interval)
        all_rows.extend(rows)
        print(f"    {y}-{m:02d} — {len(rows):,} candles ✓")
        time.sleep(0.08)

    if not all_rows:
        return 0

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_file = f"{OUTPUT_DIR}/{symbol}_{interval}.sql"
    # Append vào file nếu đã tồn tại (chạy lại một phần)
    mode = "a" if os.path.exists(out_file) else "w"
    with open(out_file, mode) as f:
        f.write(rows_to_sql(all_rows))

    print(f"  ✅ {symbol} [{interval}] — {len(all_rows):,} candles → {out_file}")
    return len(all_rows)


def generate_import_script():
    sql_files = sorted(f for f in os.listdir(OUTPUT_DIR) if f.endswith(".sql"))
    if not sql_files:
        print("⚠ Không có file SQL nào")
        return

    lines = [
        "#!/bin/bash",
        "# Import klines SQL vào Cloudflare D1",
        "set -e",
        "DB_NAME='wave-alpha-klines'",
        "",
    ]
    for f in sql_files:
        lines.append(
            f"echo '📦 {f}...'\n"
            f"wrangler d1 execute $DB_NAME --file={OUTPUT_DIR}/{f} --remote\n"
        )
    lines.append("echo '✅ Tất cả import xong!'")

    with open("./import_to_d1.sh", "w") as f:
        f.write("\n".join(lines))
    os.chmod("./import_to_d1.sh", 0o755)
    print(f"\n✅ Import script → ./import_to_d1.sh")


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

def main():
    """
    Cách dùng:
      python seed_klines.py                     → tất cả
      python seed_klines.py BTCUSDT             → 1 symbol, tất cả interval
      python seed_klines.py BTCUSDT 1d          → 1 symbol, 1 interval
      python seed_klines.py "" "" USDT          → tất cả symbol USDT pair
    """
    filter_symbol   = sys.argv[1].upper() if len(sys.argv) > 1 and sys.argv[1] else None
    filter_interval = sys.argv[2]         if len(sys.argv) > 2 and sys.argv[2] else None
    filter_quote    = sys.argv[3].upper() if len(sys.argv) > 3 and sys.argv[3] else None
    resume_from     = sys.argv[4].upper() if len(sys.argv) > 4 and sys.argv[4] else None

    print("=" * 60)
    print("  Binance Vision Klines Seeder v2")
    print(f"  Symbol filter : {filter_symbol or 'ALL'}")
    print(f"  Interval filter: {filter_interval or 'ALL'}")
    print(f"  Quote filter  : {filter_quote or 'ALL (USDT/USDC/BTC/ETH)'}")
    print(f"  Resume from   : {resume_from or 'beginning'}")
    print("=" * 60)

    # Nếu filter theo symbol cụ thể thì dùng thẳng, không scan
    if filter_symbol:
        symbols = [filter_symbol]
    else:
        # Scan xem pair nào thực sự tồn tại (dùng interval 1d để check nhanh)
        print("\n🔍 Đang kiểm tra pair nào tồn tại trên Binance Vision...")
        all_pairs = build_symbol_list(filter_quote=filter_quote)
        symbols = []
        check_interval = filter_interval or "1d"
        for pair in all_pairs:
            exists = pair_exists(pair, check_interval)
            status = "✓" if exists else "✗"
            print(f"  {status} {pair}")
            if exists:
                symbols.append(pair)
            time.sleep(0.05)

        print(f"\n✅ Tìm thấy {len(symbols)} pairs hợp lệ / {len(all_pairs)} pairs kiểm tra")

        # Lưu danh sách để debug
        with open("./valid_pairs.json", "w") as f:
            json.dump(symbols, f, indent=2)
        print("   Danh sách → ./valid_pairs.json")

    # Download data
    total_candles = 0
    intervals_to_run = [
        (iv, sy, sm) for iv, sy, sm in INTERVALS
        if not filter_interval or iv == filter_interval
    ]

    for interval, start_year, start_month in intervals_to_run:
        print(f"\n{'═'*60}")
        print(f"  Interval: {interval}")
        print(f"{'═'*60}")
        for symbol in symbols:
            print(f"\n▶ {symbol} [{interval}]")
            n = seed_pair_interval(symbol, interval, start_year, start_month)
            total_candles += n

    generate_import_script()
    print(f"\n🏁 Hoàn tất! Tổng cộng {total_candles:,} candles")


if __name__ == "__main__":
    main()
