"""
backfill_multitoken_tiers.py
──────────────────────────────
Migration MỘT LẦN — quét lại raw_text của TOÀN BỘ event bằng ĐÚNG hàm
_parse_multi_token_tiers() hiện tại trong alpha_parser.py (import thẳng,
không copy lại regex — đảm bảo logic giống 100% với production).

Bắt các trường hợp event Alpha Box nhiều token (VD: EDGE+BEE, ON+MPLX...)
bị lưu THIẾU hoặc SAI lúc parser cũ (bản đang chạy tại thời điểm event đó
được tạo) chưa tách tier đúng — tokens_detail bị null/thiếu token,
symbols_all null, amount_per_user lấy nhầm tier cao thay vì tier_common.

Với mỗi event, nếu parser HIỆN TẠI re-parse raw_text ra tokens_detail
KHÁC với bản đang lưu (thiếu token, hoặc tier_common khác amount_per_user
đang lưu) → cập nhật lại: tokens_detail, symbols_all, symbol (token đầu),
amount_per_user, value_usd.

KHÔNG tự enrich contract/giá cho token phụ mới thêm (VD BEE) — việc đó để
job_enrich_prices() (chạy mỗi 5 phút trên Render) tự nhặt tiếp, vì nó đã
có sẵn logic loop qua tokens_detail để enrich từng token con rồi.

Cách chạy (giống backfill_tier_common.py):
    python scripts/backfill_multitoken_tiers.py             # dry-run
    python scripts/backfill_multitoken_tiers.py --apply --refresh
"""

import os
import sys
import json
from supabase import create_client


def _add_alpha_parser_dir_to_path():
    """[SỬA — BUG] Trước đây giả định alpha_parser.py nằm ở gốc repo
    (dirname(dirname(__file__))) — sai cấu trúc thư mục thật, gây lỗi
    "ModuleNotFoundError: No module named 'alpha_parser'". Giờ TỰ TÌM
    file alpha_parser.py nằm ở đâu trong repo (quét từ gốc repo xuống),
    add đúng thư mục chứa nó vào sys.path — không cần đoán vị trí nữa.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(here)  # scripts/ nằm ngay dưới gốc repo
    for root in (repo_root, here, os.getcwd()):
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in (".git", "node_modules", "__pycache__")]
            if "alpha_parser.py" in filenames:
                if dirpath not in sys.path:
                    sys.path.insert(0, dirpath)
                return
    print("⚠️  Không tìm thấy alpha_parser.py trong repo — import sẽ lỗi ngay sau đây.")


_add_alpha_parser_dir_to_path()

# Import ĐÚNG hàm regex đang chạy thật trong parser — không viết lại.
from alpha_parser import _parse_multi_token_tiers


def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def main():
    apply = "--apply" in sys.argv
    do_refresh = "--refresh" in sys.argv

    supa_url = os.environ.get("SUPABASE_URL", "(chưa set)")
    print(f"Đang kết nối Supabase project: {supa_url}\n")

    supabase = get_supabase()

    # Lấy toàn bộ event có raw_text (phân trang, phòng khi >1000 dòng).
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

        reparsed = _parse_multi_token_tiers(raw_text)
        if len(reparsed) < 2:
            continue  # không phải Alpha Box nhiều token — bỏ qua

        # So sánh với dữ liệu đang lưu
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

        # Coi là "cần sửa" nếu: tokens_detail đang thiếu hẳn, HOẶC thiếu
        # token nào đó so với bản re-parse, HOẶC amount_per_user lệch
        # tier_common.
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
        from storage import refresh_r2_snapshot
        refresh_r2_snapshot()
        print("✓ Đã refresh_r2_snapshot() — R2 đã đồng bộ lại")
    else:
        print("\nLƯU Ý: R2 chưa được đồng bộ — mở https://<app-của-bạn>.onrender.com/refresh")


if __name__ == "__main__":
    main()
