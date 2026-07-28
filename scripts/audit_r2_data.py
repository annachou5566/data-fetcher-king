#!/usr/bin/env python3
"""audit_r2_data.py — CHỈ ĐỌC, KHÔNG GHI GÌ LÊN R2. An toàn tuyệt đối, chạy
bao nhiêu lần cũng được. Quét toàn bộ etf-flows.json + TẤT CẢ file trong
etf-history/ để tìm bất thường — không giả định chỉ có 3 ticker/1 ngày đã
biết bị ảnh hưởng, quét rộng để chắc chắn không sót gì.

Chạy: python audit_r2_data.py
Cần đủ 4 biến môi trường R2 giống hệt fetch_etf.py.
"""
import os, sys, json
import boto3
from botocore.config import Config

R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL      = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME       = os.getenv("R2_BUCKET_NAME")


def get_r2():
    return boto3.client("s3", endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID, aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"))


def r2_get_json(r2, key):
    try:
        resp = r2.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception as e:
        return None, str(e)
    return None


def list_keys(r2, prefix):
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = r2.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return sorted(keys)


def audit_blob(blob, label, problems):
    """Kiểm tra nhiều loại bất thường, không chỉ riêng flow > 50% AUM:
    1. flow bất hợp lý so với AUM (guard cũ)
    2. AUM âm hoặc bằng 0 cho ticker self_computed (không nên xảy ra)
    3. totals[u]['flow'] không khớp tổng cộng dồn từ etfs (lệch do sửa tay
       thiếu sót, hoặc bug tính totals)
    4. holdings âm (không thể có holdings âm thật)
    """
    if not blob or "etfs" not in blob:
        problems.append(f"[{label}] rỗng hoặc thiếu key 'etfs'")
        return

    computed_totals = {}
    for e in blob["etfs"]:
        t = e.get("ticker", "???")
        flow = (e.get("flow") or {}).get("daily_usd")
        aum = (e.get("fund") or {}).get("aum")
        holdings = (e.get("fund") or {}).get("holdings")
        u = e.get("underlying", "???")

        if flow is not None and aum and aum > 0 and abs(flow) > 0.5 * aum:
            pct = abs(flow) / aum * 100
            problems.append(f"[{label}] {t}: flow={flow/1e6:+.2f}M chiếm {pct:.0f}% AUM ({aum/1e6:.2f}M) — NGHI NGỜ")

        if e.get("flow", {}) and (e.get("flow") or {}).get("source") == "self_computed":
            if aum is not None and aum <= 0:
                problems.append(f"[{label}] {t}: self_computed nhưng AUM={aum} (không hợp lệ)")
            if holdings is not None and holdings < 0:
                problems.append(f"[{label}] {t}: holdings âm ({holdings})")

        computed_totals.setdefault(u, {"aum": 0.0, "flow": 0.0, "count": 0})
        computed_totals[u]["aum"] += aum or 0
        computed_totals[u]["flow"] += flow or 0
        computed_totals[u]["count"] += 1

    stored_totals = blob.get("totals", {})
    for u, comp in computed_totals.items():
        stored = stored_totals.get(u, {})
        stored_flow = stored.get("flow", 0)
        # So sánh với sai số nhỏ (làm tròn float)
        if abs((stored_flow or 0) - comp["flow"]) > 1.0:
            problems.append(f"[{label}] totals['{u}']['flow']={stored_flow} KHÔNG khớp tổng thật ({comp['flow']:.2f}) — totals bị lệch")
        if stored.get("count") != comp["count"]:
            problems.append(f"[{label}] totals['{u}']['count']={stored.get('count')} KHÔNG khớp số ticker thật ({comp['count']})")


def main():
    missing = [k for k, v in [("R2_ACCESS_KEY_ID", R2_ACCESS_KEY_ID), ("R2_SECRET_ACCESS_KEY", R2_SECRET_ACCESS_KEY),
                               ("R2_ENDPOINT_URL", R2_ENDPOINT_URL), ("R2_BUCKET_NAME", R2_BUCKET_NAME)] if not v]
    if missing:
        print(f"THIẾU biến môi trường: {missing}")
        sys.exit(1)

    r2 = get_r2()
    problems = []

    print("=== etf-flows.json ===")
    flows = r2_get_json(r2, "etf-flows.json")
    if isinstance(flows, tuple):
        print(f"  không đọc được: {flows[1]}")
    else:
        audit_blob(flows, "etf-flows.json", problems)

    print("\n=== etf-history/ (toàn bộ file, không chỉ ngày đã biết) ===")
    keys = list_keys(r2, "etf-history/")
    print(f"  Tìm thấy {len(keys)} file: {keys}\n")
    for key in keys:
        blob = r2_get_json(r2, key)
        if isinstance(blob, tuple):
            print(f"  {key}: không đọc được ({blob[1]})")
            continue
        before = len(problems)
        audit_blob(blob, key, problems)
        if len(problems) == before:
            print(f"  {key}: OK, không thấy bất thường")

    print("\n=== KẾT QUẢ ===")
    if problems:
        print(f"Tìm thấy {len(problems)} bất thường:")
        for p in problems:
            print(f"  ⚠️  {p}")
    else:
        print("✅ Không tìm thấy bất thường nào trong toàn bộ dữ liệu đã quét.")


if __name__ == "__main__":
    main()
