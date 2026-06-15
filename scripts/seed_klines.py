"""
seed_klines.py
--------------
Download historical klines từ Binance Vision (data.binance.vision)
Convert CSV → SQL batch → Import vào Cloudflare D1

Chỉ dùng thư viện chuẩn Python, không cần cài thêm gì.
"""

import urllib.request
import zipfile
import io
import csv
import os
import sys
import time

# ─── CẤU HÌNH ────────────────────────────────────────────────────────────────

SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]

# Thứ tự từ nhỏ → lớn, làm lần lượt
INTERVALS = [
    ("1w",  2017, 8),   # full history, rất nhỏ
    ("1d",  2017, 8),   # full history, nhỏ
    ("4h",  2017, 8),   # full history, vừa
    ("2h",  2017, 8),
    ("1h",  2017, 8),
    ("30m", 2022, 1),   # 3 năm gần
    ("15m", 2022, 1),
    ("5m",  2024, 1),   # 2 năm gần
    ("1m",  2025, 1),   # 6 tháng gần
]

BASE_URL   = "https://data.binance.vision/data/spot/monthly/klines"
OUTPUT_DIR = "./klines_sql"
BATCH_SIZE = 500   # số rows mỗi INSERT batch

# ─── HÀM CHÍNH ───────────────────────────────────────────────────────────────

def month_range(start_year, start_month, end_year=2026, end_month=5):
    """Sinh ra list (year, month) từ start đến end."""
    result = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        result.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def download_zip(symbol, interval, year, month):
    """Tải file zip từ Binance Vision, trả về bytes hoặc None nếu không tồn tại."""
    filename = f"{symbol}-{interval}-{year}-{month:02d}.zip"
    url = f"{BASE_URL}/{symbol}/{interval}/{filename}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            if resp.status == 200:
                return resp.read()
    except Exception:
        pass
    return None


def parse_csv_from_zip(zip_bytes, symbol, interval):
    """Đọc CSV từ zip, trả về list dict rows."""
    rows = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        csv_name = z.namelist()[0]
        content = z.read(csv_name).decode("utf-8")
        for line in content.splitlines():
            if not line.strip():
                continue
            cols = line.split(",")
            if not cols[0].isdigit():
                continue  # skip header nếu có
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
    return rows


def rows_to_sql(rows, table="klines"):
    """Convert list rows → SQL INSERT statements theo batch."""
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
            f"INSERT OR IGNORE INTO {table} "
            f"(symbol,interval,open_time,open,high,low,close,volume) VALUES\n"
            f"  {values};\n"
        )
    return "\n".join(sql_lines)


def seed_symbol_interval(symbol, interval, start_year, start_month):
    """Download toàn bộ 1 symbol + 1 interval, ghi ra file SQL."""
    months = month_range(start_year, start_month)
    all_rows = []
    total = len(months)

    print(f"\n{'─'*50}")
    print(f"▶ {symbol} [{interval}] — {total} tháng cần xử lý")

    for idx, (y, m) in enumerate(months, 1):
        label = f"{y}-{m:02d}"
        zip_bytes = download_zip(symbol, interval, y, m)

        if zip_bytes is None:
            print(f"  [{idx}/{total}] {label} — bỏ qua (chưa có data)")
            continue

        rows = parse_csv_from_zip(zip_bytes, symbol, interval)
        all_rows.extend(rows)
        print(f"  [{idx}/{total}] {label} — {len(rows):,} candles ✓")
        time.sleep(0.1)  # tránh bị rate limit

    if not all_rows:
        print(f"  ⚠ Không có data, bỏ qua")
        return

    # Ghi file SQL
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_file = f"{OUTPUT_DIR}/{symbol}_{interval}.sql"
    with open(out_file, "w") as f:
        f.write(rows_to_sql(all_rows, table="klines"))

    print(f"  ✅ Tổng {len(all_rows):,} candles → {out_file}")


def generate_import_script():
    """Tạo shell script để import toàn bộ SQL files vào D1."""
    sql_files = sorted(f for f in os.listdir(OUTPUT_DIR) if f.endswith(".sql"))
    if not sql_files:
        print("⚠ Không có file SQL nào để tạo import script")
        return

    lines = [
        "#!/bin/bash",
        "# Auto-generated: import tất cả klines SQL vào Cloudflare D1",
        "# Chạy: bash import_to_d1.sh",
        "# Yêu cầu: wrangler đã login, đúng account",
        "",
        "DB_NAME='wave-alpha-klines'",
        "",
    ]
    for f in sql_files:
        lines.append(
            f"echo '📦 Importing {f}...'\n"
            f"wrangler d1 execute $DB_NAME --file={OUTPUT_DIR}/{f} --remote\n"
        )
    lines.append("echo '✅ Done!'")

    script_path = "./import_to_d1.sh"
    with open(script_path, "w") as f:
        f.write("\n".join(lines))
    os.chmod(script_path, 0o755)
    print(f"\n✅ Import script → {script_path}")
    print("   Chạy: bash import_to_d1.sh")


# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

def main():
    # Cho phép filter từ command line: python seed_klines.py BTCUSDT 1d
    filter_symbol   = sys.argv[1].upper() if len(sys.argv) > 1 else None
    filter_interval = sys.argv[2]         if len(sys.argv) > 2 else None

    print("=" * 50)
    print("  Binance Vision Klines Seeder")
    print("=" * 50)

    for interval, start_year, start_month in INTERVALS:
        if filter_interval and interval != filter_interval:
            continue
        for symbol in SYMBOLS:
            if filter_symbol and symbol != filter_symbol:
                continue
            seed_symbol_interval(symbol, interval, start_year, start_month)

    generate_import_script()
    print("\n🏁 Seeding hoàn tất!")


if __name__ == "__main__":
    main()
