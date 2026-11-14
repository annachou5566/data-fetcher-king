# PNJ V31 — Clean Source Cursor Boundary (14/11/2026)

## Sự cố xác minh
- GitHub Actions run **43403309919 / attempt 3 / job 132246161987**, `Historical clean build only` FAILED: `STATUS=HOLD reason=SOURCE_DAY_MISSING_2026-11-13`.
- Trước lỗi, R2 đã có đủ **3.969/3.969** object gốc và **3.969/3.969** object clean theo ngày cho phạm vi **2016-01-01 → 2026-11-12**. Global source và clean `next_date=2026-11-13`. Day source 2026-11-13 chưa được thu thập, không phải object mất.
- Nguồn dữ liệu mới chỉ chạy cron daily `13 3 * * *` (10:13 ICT). Lượt clean sau 00:00 ICT dùng `today_vn()-1` và yêu cầu ngày nguồn chưa được commit. Trạng thái FAIL không phản ánh hư hỏng kho sạch.

## Bản sửa nhỏ
- `scripts/pnj_clean_v1.py:build_history` đọc và xác minh cursor nguồn `metal/pnj/history-v1/_state.json`, sau đó đặt `last=min(requested_last, source_next_date - 1 day)`.
- Khi clean đã khớp source, xuất `caught_up=true, processed=0`, không yêu cầu ngày nguồn chưa có. Khi source tự cập nhật hôm trước, clean xử lý đúng ngày vừa được commit.
- Schema/cursor nguồn thiếu, sai hoặc ngoài phạm vi thì fail closed; không bỏ qua gap bên trong vùng đã commit.
- Không chỉnh sửa object raw, clean hay checkpoint thủ công; không đổi cron; không đổi `wave-alpha` Production, Oracle hoặc các workflow khác.
- Tests: **64/64 offline PASS** (61 cũ + 3 mới) trên exact GitHub candidate. Chưa được gọi hosted PASS trước khi xem GitHub Actions chạy thật.

## Chốt vận hành
- Nếu GitHub push job chạy khi source chưa cập nhật ngày 13/11, nó phải **Success** với 0 ngày cần xử lý; R2 clean vẫn next=2026-11-13.
- Cron 10:13 ICT tiếp theo phải thu thập source 13/11 trước, sau đó materialize clean 13/11; **chỉ khi** R2 `source.next_date` và `clean.next_date` sang 2026-11-14, mới chốt ngày 13/11 PASS.
- Không bấm lại các backfill 366 ngày. Giữ chung một bucket R2, một lịch và một concurrency group PNJ.
