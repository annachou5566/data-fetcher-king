"""
backfill_multitoken_tiers.py
──────────────────────────────
Migration MỘT LẦN — quét lại raw_text của TOÀN BỘ event bằng regex tách
tier (copy y hệt logic _parse_multi_token_tiers trong alpha_parser.py —
KHÔNG import cross-repo, vì alpha_parser.py nằm ở repo khác, không phải
repo data-fetcher-king này).

Bắt các trường hợp event Alpha Box nhiều token (VD: EDGE+BEE, ON+MPLX...)
bị lưu THIẾU hoặc SAI lúc parser cũ (bản đang chạy tại thời điểm event đó
được tạo) chưa tách tier đúng — tokens_detail bị null/thiếu token,
symbols_all null, amount_per_user lấy nhầm tier cao thay vì tier_common.

Với mỗi event, nếu re-parse raw_text ra tokens_detail KHÁC với bản đang
lưu (thiếu token, hoặc tier_common khác amount_per_user đang lưu) → cập
nhật lại: tokens_detail, symbols_all, symbol (token đầu), amount_per_user,
value_usd.

KHÔNG tự enrich contract/giá cho token phụ mới thêm (VD BEE) — việc đó để
job_enrich_prices() (chạy mỗi 5 phút trên Render, ở repo kia) tự nhặt
tiếp, vì nó đã có sẵn logic loop qua tokens_detail để enrich từng token
con rồi.

Cách chạy:
    python scripts/backfill_multitoken_tiers.py             # dry-run
    python scripts/backfill_multitoken_tiers.py --apply
    python scripts/backfill_multitoken_tiers.py --apply --refresh
      (--refresh gọi HTTP GET tới REFRESH_URL, KHÔNG import storage.py
       — vì storage.py cũng nằm ở repo khác)
"""

import os
import re
import sys
import json
import requests
from supabase import create_client


def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


# Copy y hệt regex trong alpha_parser.py::_parse_multi_token_tiers — giữ
# nguyên logic (kể cả cách xử lý dấu phẩy) để kết quả khớp 100% với
# những gì parser thật sẽ ra nếu chạy trên cùng raw_text này.
_TIER_PATTERN = re.compile(
    r'(\d[\d,]*)\s*,\s*(\d[\d,]*)\s*,?\s*or\s*(\d[\d,]*)\s+([A-Z]{2,10})\s+tokens?',
    re.IGNORECASE
)


def parse_multi_token_tiers(text: str) -> list:
    """→ [{"symbol":"EDGE","tier_common":69,"tier_rare":86,"tier_super_rare":244}, ...]"""
    if not text:
        return []
    out = []
    seen = set()
    for m in _TIER_PATTERN.finditer(text):
        low, mid, high, sym = m.groups()
        sym = sym.upper()
        if sym in seen or sym == "OR":  # "or" đôi khi bị regex khớp nhầm là symbol
            continue
        seen.add(sym)
        try:
            out.append({
                "symbol": sym,
                "tier_common": float(low.replace(",", "")),
                "tier_rare": float(mid.replace(",", "")),
                "tier_super_rare": float(high.replace(",", "")),
            })
        except Exception:
            continue
    return out


def main():
    apply = "--apply" in sys.argv
    do_refresh = "--refresh" in sys.argv

    supa_url = os.environ.get("SUPABASE_URL", "(chưa set)")
    print(f"Đang kết nối Supabase project: {supa_url}\n")

    supabase = get_supabase()

    rows = []
    page_size = 1000
    offset = 0
    while True:
        batch = supabase.table("alpha_events") \
            .select("id, symbol, symbols_all, amount_per_user, price_snapshot, "
                    "value_usd, tokens_detail, raw_text, event_type") \
            .range(offset, offset + page_size - 1) \
            .execute().data
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    print(f"Tổng số event: {len(rows)}")

    to_fix = []
    for row in rows:
        raw_text = row.get("raw_text")
        if not raw_text or row.get("event_type") != "airdrop":
            continue

        reparsed = parse_multi_token_tiers(raw_text)
        if len(reparsed) < 2:
            continue  # không phải Alpha Box nhiều token — bỏ qua

        current_td = row.get("tokens_detail")
        if isinstance(current_td, str):
            try:
                current_td = json.loads(current_td)
            except Exception:
                current_td = None

        new_tier_common = reparsed[0]["tier_common"]
        current_amount = row.get("amount_per_user")
        try:
            current_amount_f = float(current_amount) if current_amount is not None else None
        except Exception:
            current_amount_f = None

        needs_fix = False
        if not current_td or not isinstance(current_td, list):
            needs_fix = True
        elif len(current_td) < len(reparsed):
            needs_fix = True  # thiếu token (case BEE)
        elif current_amount_f is None or abs(current_amount_f - new_tier_common) > 1e-9:
            needs_fix = True

        if not needs_fix:
            continue

        price = row.get("price_snapshot")
        new_value_usd = None
        if price:
            try:
                new_value_usd = round(new_tier_common * float(price), 4)
            except Exception:
                pass

        to_fix.append({
            "id": row["id"],
            "old_symbol": row.get("symbol"),
            "old_symbols_all": row.get("symbols_all"),
            "old_tokens_detail": current_td,
            "new_tokens_detail": reparsed,
            "new_symbol": reparsed[0]["symbol"],
            "new_symbols_all": ",".join(t["symbol"] for t in reparsed),
            "old_amount": current_amount,
            "new_amount": new_tier_common,
            "old_value_usd": row.get("value_usd"),
            "new_value_usd": new_value_usd,
        })

    if not to_fix:
        print("Không có event Alpha Box nào cần sửa — mọi thứ đã khớp với parser hiện tại ✓")
        return

    print(f"\n{'DRY-RUN — sẽ sửa' if not apply else 'ĐANG SỬA'} {len(to_fix)} event:\n")
    for f in to_fix:
        print(f"  id={f['id']:<6} {f['old_symbol']} → symbols_all: {f['old_symbols_all']!r} → {f['new_symbols_all']!r}")
        print(f"           amount_per_user: {f['old_amount']} → {f['new_amount']}  |  value_usd: {f['old_value_usd']} → {f['new_value_usd']}")
        print(f"           tokens_detail cũ: {f['old_tokens_detail']}")
        print(f"           tokens_detail mới: {f['new_tokens_detail']}\n")

    if not apply:
        print("(Đây chỉ là dry-run — chạy lại với --apply để ghi thật vào Supabase)")
        return

    updated = 0
    for f in to_fix:
        update_data = {
            "tokens_detail": f["new_tokens_detail"],
            "symbol": f["new_symbol"],
            "symbols_all": f["new_symbols_all"],
            "amount_per_user": f["new_amount"],
        }
        if f["new_value_usd"] is not None:
            update_data["value_usd"] = f["new_value_usd"]
        supabase.table("alpha_events").update(update_data).eq("id", f["id"]).execute()
        updated += 1

    print(f"\n✓ Đã sửa {updated}/{len(to_fix)} event trên Supabase")

    if do_refresh:
        refresh_url = os.environ.get("REFRESH_URL")
        if not refresh_url:
            print("\n⚠️  Chưa set REFRESH_URL (secret/env) nên KHÔNG tự refresh được.")
            print("   Tự mở https://<app-của-bạn>.onrender.com/refresh trên trình duyệt.")
        else:
            try:
                r = requests.get(refresh_url, timeout=30)
                print(f"✓ Đã gọi {refresh_url} → {r.status_code} {r.text[:200]}")
            except Exception as e:
                print(f"⚠️  Gọi REFRESH_URL lỗi: {e}")
    else:
        print("\nLƯU Ý: R2 chưa được đồng bộ — mở https://<app-của-bạn>.onrender.com/refresh")


if __name__ == "__main__":
    main()
