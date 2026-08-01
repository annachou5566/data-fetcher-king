"""
backfill_tier_common.py
────────────────────────
Migration MỘT LẦN — sửa lại các event Alpha Box (nhiều tier: Common/Rare/
Super Rare) đã bị lưu SAI amount_per_user từ trước khi parser có logic
tách tier (_parse_multi_token_tiers trong alpha_parser.py).

Bug cũ: parser fallback regex generic từng khớp nhầm SỐ CUỐI trong câu
kiểu "receive one of the following rewards: 315, 395, or 1125 ON tokens"
(khớp "1125 ON tokens" thay vì "315") → amount_per_user bị lưu = tier
Super Rare (1125) thay vì tier Common (315) — trong khi thực tế ~85%
người dùng chỉ nhận đúng mức Common, Rare/Super Rare chỉ là thưởng CỘNG
THÊM ngẫu nhiên (315 + 80 = 395, 315 + 810 = 1125 — khớp với ảnh chụp
app Binance chính chủ).

Parser hiện tại (parse_with_regex, dòng ~207) đã tự lấy đúng tier_common
cho event MỚI — script này CHỈ backfill lại các event CŨ đã lỡ lưu sai.

Cách chạy:
    python backfill_tier_common.py            # dry-run, chỉ in ra, KHÔNG ghi
    python backfill_tier_common.py --apply     # ghi thật vào Supabase
    python backfill_tier_common.py --apply --refresh   # ghi xong tự gọi
                                                        # refresh_r2_snapshot()
"""

import os
import sys
import json
from supabase import create_client


def _add_storage_dir_to_path():
    """[SỬA — BUG] Trước đây giả định storage.py nằm ở gốc repo — có thể
    sai cấu trúc thư mục thật, gây lỗi ModuleNotFoundError khi chạy
    --refresh. Giờ TỰ TÌM file storage.py nằm ở đâu trong repo, add đúng
    thư mục chứa nó vào sys.path.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(here)
    for root in (repo_root, here, os.getcwd()):
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in (".git", "node_modules", "__pycache__")]
            if "storage.py" in filenames:
                if dirpath not in sys.path:
                    sys.path.insert(0, dirpath)
                return


_add_storage_dir_to_path()


def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def main():
    apply = "--apply" in sys.argv
    do_refresh = "--refresh" in sys.argv

    # [MỚI] In ra ĐANG kết nối tới project Supabase nào (URL không nhạy
    # cảm, an toàn để in ra log) — để đối chiếu trực tiếp với URL đang
    # dùng thật trên Render. Nếu 2 cái khác nhau → đó chính là lý do
    # "Tổng số event = 0" (đang hỏi nhầm project trống/khác).
    supa_url = os.environ.get("SUPABASE_URL", "(chưa set)")
    print(f"Đang kết nối Supabase project: {supa_url}")
    key_preview = os.environ.get("SUPABASE_KEY", "")
    print(f"SUPABASE_KEY length: {len(key_preview)} ký tự (service_role key thật thường dài ~200+ ký tự, bắt đầu bằng 'eyJ')")
    print(f"SUPABASE_KEY preview: {key_preview[:15]}...{key_preview[-6:] if len(key_preview) > 21 else ''}\n")

    supabase = get_supabase()

    # Sanity check thô trước — query 1 dòng bất kỳ, không lọc gì, để biết
    # ngay là do kết nối/quyền hay do filter phía sau.
    try:
        probe = supabase.table("alpha_events").select("id", count="exact").limit(1).execute()
        print(f"Sanity check: bảng alpha_events báo tổng cộng {probe.count} dòng (theo Supabase đếm)")
    except Exception as ex:
        print(f"⚠️  Sanity check LỖI khi query alpha_events: {ex}")
        print("   → Rất có thể sai tên bảng, sai schema, hoặc SUPABASE_KEY không có quyền đọc bảng này.")

    # [SỬA — BUG] Filter .not_.is_("tokens_detail", "null") của PostgREST
    # trả về 0 kết quả dù DB thật sự có event tokens_detail (case ON) —
    # nhiều khả năng do kiểu cột (jsonb lưu string JSON lồng bên trong,
    # hoặc rỗng "" thay vì SQL NULL). Để không phụ thuộc filter phía
    # server nữa, giờ LẤY TOÀN BỘ bảng (có phân trang, phòng khi >1000
    # dòng — mặc định Supabase chỉ trả tối đa 1000 dòng/lần) rồi tự lọc
    # tokens_detail bằng Python cho chắc ăn.
    rows = []
    page_size = 1000
    offset = 0
    while True:
        batch = supabase.table("alpha_events") \
            .select("id, symbol, project_name, amount_per_user, price_snapshot, value_usd, tokens_detail, source_msg_id") \
            .range(offset, offset + page_size - 1) \
            .execute().data
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    print(f"Tổng số event trong bảng alpha_events: {len(rows)}")

    has_tokens_detail = [r for r in rows if r.get("tokens_detail")]
    print(f"Số event có tokens_detail (không rỗng/không null): {len(has_tokens_detail)}\n")

    # Debug: nếu vẫn 0, in thử vài dòng bất kỳ để xem cấu trúc thật của
    # cột tokens_detail đang là gì (giúp chẩn đoán tiếp nếu còn lỗi).
    if not has_tokens_detail:
        print("⚠️  Không tìm thấy event nào có tokens_detail. Xem thử 3 event có symbol chứa nhiều token (source_msg_id gần AEON/ON) để debug:")
        sample = [r for r in rows if r.get("symbol") == "ON"]
        for r in sample[:3]:
            print(f"  id={r.get('id')} symbol={r.get('symbol')} tokens_detail={r.get('tokens_detail')!r} (type={type(r.get('tokens_detail')).__name__})")

    rows = has_tokens_detail

    to_fix = []
    for row in rows:
        td = row.get("tokens_detail")
        if isinstance(td, str):
            try:
                td = json.loads(td)
            except Exception:
                continue
        if not isinstance(td, list) or not td:
            continue

        main_token = td[0]
        tier_common = main_token.get("tier_common")
        if tier_common is None:
            continue

        current_amt = row.get("amount_per_user")
        try:
            current_amt_f = float(current_amt) if current_amt is not None else None
        except Exception:
            current_amt_f = None

        if current_amt_f is not None and abs(current_amt_f - float(tier_common)) < 1e-9:
            continue  # đã đúng rồi, bỏ qua

        price = row.get("price_snapshot")
        new_value_usd = None
        if price:
            try:
                new_value_usd = round(float(tier_common) * float(price), 4)
            except Exception:
                pass

        to_fix.append({
            "id": row["id"],
            "symbol": row.get("symbol"),
            "old_amount": current_amt,
            "new_amount": tier_common,
            "old_value_usd": row.get("value_usd"),
            "new_value_usd": new_value_usd,
        })

    if not to_fix:
        print("Không có event nào cần sửa — mọi thứ đã đúng chuẩn tier_common ✓")
        return

    print(f"{'DRY-RUN — sẽ sửa' if not apply else 'ĐANG SỬA'} {len(to_fix)} event:\n")
    for f in to_fix:
        print(f"  id={f['id']:<6} {f['symbol']:<8} amount_per_user: {f['old_amount']} → {f['new_amount']}"
              f"   |  value_usd: {f['old_value_usd']} → {f['new_value_usd']}")

    if not apply:
        print("\n(Đây chỉ là dry-run — chạy lại với --apply để ghi thật vào Supabase)")
        return

    updated = 0
    for f in to_fix:
        update_data = {"amount_per_user": f["new_amount"]}
        if f["new_value_usd"] is not None:
            update_data["value_usd"] = f["new_value_usd"]
        supabase.table("alpha_events").update(update_data).eq("id", f["id"]).execute()
        updated += 1

    print(f"\n✓ Đã sửa {updated}/{len(to_fix)} event trên Supabase")

    if do_refresh:
        from storage import refresh_r2_snapshot
        refresh_r2_snapshot()
        print("✓ Đã refresh_r2_snapshot() — R2 (all.json/history.json) đã đồng bộ lại")
    else:
        print("\nLƯU Ý: R2 (all.json/history.json) CHƯA được đồng bộ lại.")
        print("Chạy lại với --refresh, hoặc tự gọi endpoint GET /refresh trên app Render.")


if __name__ == "__main__":
    main()
