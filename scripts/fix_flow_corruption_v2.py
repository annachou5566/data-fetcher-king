#!/usr/bin/env python3
"""fix_flow_corruption_v2.py — sửa TOÀN DIỆN hơn bản trước:
  1. Tự quét HẾT mọi file etf-history/ + etf-flows.json — không cần khai
     ngày tay nữa (bản trước đoán sai ngày, bỏ sót 2026-07-28).
  2. Sửa CẢ fund.holdings/fund.aum (không chỉ flow) — bản trước bỏ sót AUM
     sai trong 2026-07-27.json (còn sót từ lần chạy ra holdings=24).
  3. Với flow bất thường: TỰ TẢI LẠI Farside thật cho đúng ngày đó (không
     hardcode số từ log cũ nữa — bản trước hardcode sai/thiếu vì đoán nhầm
     ngày). Import thẳng fetch_farside_html/parse_farside_table_full từ
     chính fetch_etf.py để chắc chắn dùng đúng logic parse, không viết lại.

Chạy: python scripts/fix_flow_corruption_v2.py
Cần đủ 4 biến môi trường R2 giống hệt fetch_etf.py. Phải chạy TRONG thư mục
scripts/ (hoặc cùng thư mục với fetch_etf.py) vì cần import fetch_etf.py.
"""
import os, sys, json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_etf as fe  # tái sử dụng get_r2/r2_get_json/r2_put_json/Farside parser gốc

# 3 ticker đã biết bị ảnh hưởng bởi bug holdings=24 (regex Fidelity cũ).
# holdings THẬT gần nhất đã xác nhận qua chính trang Fidelity + lần chạy
# đúng sau đó — dùng làm giá trị khôi phục khi thấy holdings bất thường.
FIDELITY_UNDERLYING = {"FBTC": "BTC", "FETH": "ETH", "FSOL": "SOL"}
KNOWN_GOOD_HOLDINGS = {"FBTC": 172278.80, "FETH": 481939.54, "FSOL": 1709750.60}
MIN_PLAUSIBLE_HOLDINGS = 1000  # holdings thật của cả 3 ticker này luôn > 100k


def list_keys(r2, prefix):
    keys, token = [], None
    while True:
        kwargs = {"Bucket": fe.R2_BUCKET_NAME, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = r2.list_objects_v2(**kwargs)
        keys += [o["Key"] for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return sorted(keys)


def implied_price(blob, underlying, exclude_ticker):
    """Suy giá coin/token từ 1 ticker CÙNG loại tài sản khác trong CÙNG file
    (vd IBIT cho BTC) — tránh phải hardcode giá lịch sử có thể sai."""
    for e in blob.get("etfs", []):
        if e.get("underlying") == underlying and e.get("ticker") != exclude_ticker:
            fund = e.get("fund") or {}
            aum, hold = fund.get("aum"), fund.get("holdings")
            if aum and hold and hold >= 100 and aum > 0:
                return aum / hold
    return None


def fetch_farside_row_for_date(asset, date_str):
    """Tải lại Farside THẬT cho asset (BTC/ETH/SOL) và tìm đúng dòng khớp
    date_str ("YYYY-MM-DD"). Dùng chính hàm gốc fetch_etf.fetch_farside_html
    + parse_farside_table_full, không viết lại logic parse."""
    url = fe.FARSIDE_URLS.get(asset)
    if not url:
        return None
    html = fe.fetch_farside_html(url)
    if not html:
        return None
    _, rows = fe.parse_farside_table_full(html, asset)
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    for row in rows:
        try:
            rdate = datetime.strptime(row["date"], "%d %b %Y").date()
        except Exception:
            continue
        if rdate == target:
            return row
    return None


def get_entry(blob, ticker):
    for e in blob.get("etfs", []):
        if e.get("ticker") == ticker:
            return e
    return None


def fix_holdings_aum(blob, ticker, underlying, changes):
    e = get_entry(blob, ticker)
    if not e:
        return
    fund = e.get("fund") or {}
    hold = fund.get("holdings")
    if hold is None or hold >= MIN_PLAUSIBLE_HOLDINGS:
        return
    price = implied_price(blob, underlying, ticker)
    good_hold = KNOWN_GOOD_HOLDINGS.get(ticker)
    if price and good_hold:
        old_aum = fund.get("aum")
        new_aum = good_hold * price
        fund["holdings"] = good_hold
        fund["aum"] = new_aum
        e["fund"] = fund
        changes.append(f"{ticker}: holdings {hold}->{good_hold:.2f}, aum {old_aum}->{new_aum:,.0f} "
                        f"(giá suy từ ticker cùng loại trong file: ${price:,.2f})")
    else:
        changes.append(f"{ticker}: holdings={hold} BẤT THƯỜNG nhưng KHÔNG sửa được tự động "
                        f"(không tìm được ticker cùng loại '{underlying}' hợp lệ trong file để suy giá) — CẦN KIỂM TRA TAY")


def fix_flow(blob, ticker, underlying, date_str, changes, farside_cache):
    e = get_entry(blob, ticker)
    if not e:
        return
    flow = (e.get("flow") or {}).get("daily_usd")
    aum = (e.get("fund") or {}).get("aum")
    if flow is None or not aum or aum <= 0 or abs(flow) <= 0.5 * aum:
        return

    cache_key = (underlying, date_str)
    if cache_key not in farside_cache:
        print(f"    -> đang tải lại Farside thật cho {underlying} ngày {date_str}...")
        farside_cache[cache_key] = fetch_farside_row_for_date(underlying, date_str)
    row = farside_cache[cache_key]

    if row and ticker in row:
        new_flow = row[ticker] * 1_000_000
        e["flow"] = {"daily_usd": new_flow, "is_inflow": new_flow > 0, "source": "farside", "date": row["date"]}
        changes.append(f"{ticker}: flow {flow/1e6:+.2f}M -> {new_flow/1e6:+.2f}M (Farside thật, {row['date']})")
    else:
        e["flow"] = None
        changes.append(f"{ticker}: flow {flow/1e6:+.2f}M BẤT THƯỜNG, không tìm được số Farside cho ngày này -> đặt null")


def recompute_totals(blob):
    totals = {}
    for e in blob.get("etfs", []):
        u = e.get("underlying", "???")
        totals.setdefault(u, {"aum": 0.0, "flow": 0.0, "count": 0})
        totals[u]["aum"] += (e.get("fund") or {}).get("aum") or 0
        totals[u]["flow"] += (e.get("flow") or {}).get("daily_usd") or 0
        totals[u]["count"] += 1
    blob["totals"] = totals


def process(blob, label, date_str, farside_cache):
    if not blob or "etfs" not in blob:
        print(f"[{label}] rỗng/sai cấu trúc, bỏ qua")
        return None, False

    changes = []
    for ticker, underlying in FIDELITY_UNDERLYING.items():
        fix_holdings_aum(blob, ticker, underlying, changes)
        fix_flow(blob, ticker, underlying, date_str, changes, farside_cache)

    if not changes:
        print(f"[{label}] Không thấy gì bất thường")
        return blob, False

    recompute_totals(blob)
    print(f"[{label}] Đã sửa {len(changes)} chỗ:")
    for c in changes:
        print(f"    - {c}")
    return blob, True


def main():
    missing = [k for k in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT_URL", "R2_BUCKET_NAME")
               if not os.getenv(k)]
    if missing:
        print(f"THIẾU biến môi trường: {missing}")
        sys.exit(1)

    r2 = fe.get_r2()
    farside_cache = {}  # (asset, date_str) -> row | None, tránh tải lại Farside nhiều lần cho cùng ngày

    print("=== 1) etf-flows.json (trạng thái hiện tại / live) ===")
    flows = fe.r2_get_json(r2, "etf-flows.json")
    if flows:
        fetched_at = flows.get("fetched_at", "")
        date_str = fetched_at[:10] if fetched_at else datetime.utcnow().strftime("%Y-%m-%d")
        fixed, changed = process(flows, "etf-flows.json", date_str, farside_cache)
        if changed:
            fe.r2_put_json(r2, "etf-flows.json", fixed, "max-age=120")
            print("  -> đã ghi lại etf-flows.json\n")

    print("\n=== 2) etf-history/ (TOÀN BỘ file, tự quét — không khai ngày tay) ===")
    keys = list_keys(r2, "etf-history/")
    print(f"  Tìm thấy {len(keys)} file\n")
    for key in keys:
        date_str = key.split("/")[-1].replace(".json", "")
        blob = fe.r2_get_json(r2, key)
        fixed, changed = process(blob, key, date_str, farside_cache)
        if changed:
            fe.r2_put_json(r2, key, fixed, "max-age=86400")
            print(f"  -> đã ghi lại {key}\n")

    print("\nXong. Nên chạy lại scripts/audit_r2_data.py để xác nhận sạch hoàn toàn.")


if __name__ == "__main__":
    main()
