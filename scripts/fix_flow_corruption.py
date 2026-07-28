#!/usr/bin/env python3
"""fix_flow_corruption.py — sửa 1 lần: dọn các flow bị nhiễm trên R2 do bug
holdings_prev bị ghi sai (qty=24) từ lần chạy trước khi có sanity check.
Đã fix tận gốc trong compute_self_flow() của fetch_etf.py (guard >50% AUM) —
script này chỉ dọn dữ liệu ĐàGHI RA R2 từ TRƯỚC khi có bản vá đó.

Dùng lại NGUYÊN VẸN cách kết nối R2 / tên biến môi trường / cấu trúc JSON từ
chính fetch_etf.py — không tự chế thêm cơ chế mới, không gọi lại Farside qua
mạng (dùng đúng số Farside đã thấy TRONG LOG CI thật bạn gửi, tránh sai lệch
do fetch lại có thể ra ngày/số khác thời điểm gốc).

Chạy: python fix_flow_corruption.py
Cần đủ 4 biến môi trường giống hệt fetch_etf.py:
  R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME

⚠️ TRƯỚC KHI CHẠY: sửa danh sách AFFECTED_DATES bên dưới cho đúng (các) ngày
UTC mà lần chạy hỏng đã ghi ra etf-history/ — script không tự đoán được ngày
đó, chỉ bạn biết chính xác server chạy job vào lúc nào.
"""
import os, sys, json
import boto3
from botocore.config import Config

R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL      = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME       = os.getenv("R2_BUCKET_NAME")

# ⚠️ SỬA CHO ĐÚNG NGÀY THẬT (định dạng YYYY-MM-DD, theo UTC) trước khi chạy.
# Dựa theo log bạn gửi (Farside "Latest: 27 Jul 2026"), khả năng cao là ngày
# này — nhưng tự kiểm tra lại cho chắc trên R2 (liệt kê key etf-history/).
AFFECTED_DATES = ["2026-07-27"]

# Số Farside THẬT lấy trực tiếp từ chính log CI bạn đã dán (mục [2/4] Farside
# flows) — không gọi lại mạng, tránh lấy nhầm ngày/số khác thời điểm gốc.
KNOWN_GOOD_FARSIDE = {
    "FBTC": {"daily_usd": -2.8e6,  "date": "27 Jul 2026"},  # Farside BTC latest
    "FETH": {"daily_usd": -27.8e6, "date": "24 Jul 2026"},  # Farside ETH latest (không có số mới hơn lúc đó)
    "FSOL": {"daily_usd": 0.0,     "date": "27 Jul 2026"},  # Farside SOL latest
}


def get_r2():
    return boto3.client("s3", endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID, aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"))


def r2_get_json(r2, key):
    try:
        resp = r2.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"  (không đọc được {key}: {e})")
        return None


def r2_put_json(r2, key, data, cc="max-age=120"):
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    r2.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=body, ContentType="application/json", CacheControl=cc)


def flow_is_corrupted(entry):
    """Cùng đúng guard mới trong compute_self_flow() của fetch_etf.py: flow
    1 ngày vượt quá 50% AUM cùng ngày là dấu hiệu holdings_prev bị nhiễm."""
    flow = (entry.get("flow") or {}).get("daily_usd")
    aum = (entry.get("fund") or {}).get("aum")
    if flow is None or not aum or aum <= 0:
        return False
    return abs(flow) > 0.5 * aum


def fix_blob(blob, label):
    if not blob or "etfs" not in blob:
        print(f"[{label}] rỗng hoặc sai cấu trúc, bỏ qua")
        return blob, 0

    fixed = []
    for e in blob["etfs"]:
        t = e["ticker"]
        if flow_is_corrupted(e):
            if t in KNOWN_GOOD_FARSIDE:
                good = KNOWN_GOOD_FARSIDE[t]
                e["flow"] = {"daily_usd": good["daily_usd"], "is_inflow": good["daily_usd"] > 0,
                             "source": "farside", "date": good["date"]}
                fixed.append(f"{t} -> thay bằng Farside thật ({good['daily_usd']/1e6:+.1f}M)")
            else:
                old_flow = (e.get("flow") or {}).get("daily_usd")
                e["flow"] = None
                fixed.append(f"{t} -> không có số Farside biết trước, đặt null (cũ: {old_flow/1e6:+.1f}M)")

    # Tính lại totals theo underlying từ danh sách etfs SAU khi đã sửa —
    # đúng công thức run() dùng ở cuối fetch_etf.py.
    totals = {}
    for e in blob["etfs"]:
        u = e["underlying"]
        totals.setdefault(u, {"aum": 0.0, "flow": 0.0, "count": 0})
        totals[u]["aum"] += (e.get("fund") or {}).get("aum") or 0
        totals[u]["flow"] += (e.get("flow") or {}).get("daily_usd") or 0
        totals[u]["count"] += 1
    blob["totals"] = totals

    if fixed:
        print(f"[{label}] Đã sửa {len(fixed)} ticker:")
        for line in fixed:
            print(f"    - {line}")
    else:
        print(f"[{label}] Không thấy gì bị nhiễm (guard >50% AUM không kích hoạt)")
    return blob, len(fixed)


def main():
    missing = [k for k, v in [("R2_ACCESS_KEY_ID", R2_ACCESS_KEY_ID), ("R2_SECRET_ACCESS_KEY", R2_SECRET_ACCESS_KEY),
                               ("R2_ENDPOINT_URL", R2_ENDPOINT_URL), ("R2_BUCKET_NAME", R2_BUCKET_NAME)] if not v]
    if missing:
        print(f"THIẾU biến môi trường: {missing}")
        print("Export đủ 4 biến R2 giống hệt fetch_etf.py rồi chạy lại script này.")
        sys.exit(1)

    r2 = get_r2()

    print("=== 1) etf-flows.json (trạng thái hiện tại / live) ===")
    flows = r2_get_json(r2, "etf-flows.json")
    if flows:
        flows, n1 = fix_blob(flows, "etf-flows.json")
        if n1:
            r2_put_json(r2, "etf-flows.json", flows, "max-age=120")
            print("  -> đã ghi lại etf-flows.json\n")
        else:
            print("  -> không cần ghi lại (có thể lần chạy full sau đó đã tự ghi đè đúng rồi)\n")

    print("=== 2) etf-history/<ngày>.json (lưu vĩnh viễn) ===")
    for date_str in AFFECTED_DATES:
        key = f"etf-history/{date_str}.json"
        hist = r2_get_json(r2, key)
        if not hist:
            print(f"  {key}: không tồn tại hoặc không đọc được, bỏ qua")
            continue
        hist, n2 = fix_blob(hist, key)
        if n2:
            r2_put_json(r2, key, hist, "max-age=86400")
            print(f"  -> đã ghi lại {key}\n")
        else:
            print(f"  -> không cần ghi lại {key}\n")

    print("Xong. Kiểm tra lại dashboard/API để chắc chắn số đã về bình thường.")


if __name__ == "__main__":
    main()
