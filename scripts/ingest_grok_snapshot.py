#!/usr/bin/env python3
"""ingest_grok_snapshot.py — đọc file JSON Grok bạn dán tay vào repo
(data/grayscale_grok_snapshot.json), KIỂM TRA KỸ trước khi tin bất kỳ số nào,
rồi mới ghi lên R2 để fetch_etf.py đọc lại.

Chạy tự động qua GitHub Actions mỗi khi bạn commit file JSON mới (xem
.github/workflows/ingest-grok-snapshot.yml đi kèm) — không cần bấm gì thêm
ngoài việc paste JSON vào file rồi commit.

4 lớp kiểm tra trước khi 1 ticker được chấp nhận:
  1. Grok tự báo success=false → loại (đã tự nhận không chắc/bị chặn)
  2. holdings_qty quá nhỏ (<100) → nghi hallucination, loại
  3. raw_evidence KHÔNG chứa đúng số holdings_qty → nghi Grok bịa số, loại
     (đây là lớp quan trọng nhất — bắt được cả trường hợp Grok tự mâu thuẫn)
  4. Lệch quá xa baseline đã xác nhận thật (nếu có) → loại

Ticker nào bị loại thì fetch_etf.py tự fallback Farside cho ticker đó —
không có rủi ro số sai lọt vào, chỉ mất phần "tốt hơn" chứ không mất an toàn.
"""
import os, sys, json, re
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_etf as fe  # tái sử dụng get_r2/r2_put_json — không viết lại logic

SNAPSHOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "grayscale_grok_snapshot.json")

# Chỉ các ticker ta thực sự dùng trong registry hiện tại — bỏ qua
# BCOR/BTCC/MNRS/BPI/ETCO (không nắm coin trực tiếp) và các coin chưa track
# (AVAX/LINK/DOGE/SUI/XRP/multi-asset) cho tới khi mở rộng registry.
TRACKED_TICKERS = {"BTC", "GBTC", "ETHE", "ETH", "HYPG", "GSOL"}

MIN_PLAUSIBLE_QTY = 100  # không quỹ nào trong nhóm này giữ dưới 100 coin thật

# Baseline đã XÁC NHẬN THẬT qua trang gốc (user tự paste) — chỉ dùng để chặn
# số lệch bất thường, biên đủ rộng để không chặn nhầm drift bình thường.
KNOWN_BASELINE = {
    "GBTC": 133450.3482,  # xác nhận 28/07/2026 qua page thật user paste
}


def raw_evidence_matches(qty, raw_evidence):
    """Kiểm tra raw_evidence có chứa đúng số holdings_qty không — chống Grok
    tự mâu thuẫn (số parse ra khác số trong câu trích chính nó đưa ra)."""
    if qty is None or not raw_evidence:
        return False
    for n in re.findall(r"[\d,]+\.?\d*", raw_evidence):
        try:
            if abs(float(n.replace(",", "")) - qty) < 0.01:
                return True
        except ValueError:
            continue
    return False


def validate_fund(fund):
    """(ok, reason) — kiểm tra kỹ trước khi tin bất kỳ số nào Grok trả về."""
    t = fund.get("ticker")
    if t not in TRACKED_TICKERS:
        return False, "ticker không thuộc danh sách đang track"
    if not fund.get("success", False):
        return False, f"Grok tự báo thất bại: {fund.get('error_message')}"
    qty = fund.get("holdings_qty")
    if qty is None:
        return False, "holdings_qty = null"
    if not isinstance(qty, (int, float)) or qty < MIN_PLAUSIBLE_QTY:
        return False, f"holdings_qty={qty} quá nhỏ/không hợp lệ, nghi hallucination"
    if not raw_evidence_matches(qty, fund.get("raw_evidence")):
        return False, f"raw_evidence không khớp holdings_qty={qty} — nghi Grok bịa số"
    baseline = KNOWN_BASELINE.get(t)
    if baseline and not (0.3 * baseline <= qty <= 3.0 * baseline):
        return False, f"holdings_qty={qty} lệch quá xa baseline đã xác nhận ({baseline})"
    as_of = fund.get("as_of_date")
    if not as_of or not re.match(r"^\d{4}-\d{2}-\d{2}$", str(as_of)):
        return False, f"as_of_date không hợp lệ: {as_of!r}"
    return True, "OK"


def main():
    if not os.path.exists(SNAPSHOT_PATH):
        print(f"Không thấy {SNAPSHOT_PATH} — không có gì để ingest, thoát.")
        sys.exit(0)

    with open(SNAPSHOT_PATH, encoding="utf-8") as f:
        data = json.load(f)

    accepted, rejected = {}, []
    for fund in data.get("funds", []):
        ok, reason = validate_fund(fund)
        t = fund.get("ticker", "???")
        if ok:
            accepted[t] = {
                "holdings_qty": fund["holdings_qty"],
                "aum_usd": fund.get("assets_under_management_usd"),
                "as_of_date": fund["as_of_date"],
                "raw_evidence": fund.get("raw_evidence"),
            }
            print(f"  ✓ {t}: holdings={fund['holdings_qty']} — chấp nhận")
        else:
            rejected.append((t, reason))
            print(f"  ✗ {t}: TỪ CHỐI — {reason}")

    if rejected:
        print(f"\n⚠️ {len(rejected)} ticker bị từ chối — fetch_etf.py sẽ tự fallback Farside cho các ticker đó, không có rủi ro số sai.")

    if not accepted:
        print("Không có ticker nào qua được kiểm tra — không ghi gì lên R2.")
        sys.exit(0)

    r2 = fe.get_r2()
    payload = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "grok_manual_snapshot",
        "tickers": accepted,
    }
    fe.r2_put_json(r2, "grayscale-grok-snapshot.json", payload, cc="max-age=3600")
    print(f"\n✓ Đã ghi {len(accepted)} ticker lên R2: grayscale-grok-snapshot.json")


if __name__ == "__main__":
    main()
