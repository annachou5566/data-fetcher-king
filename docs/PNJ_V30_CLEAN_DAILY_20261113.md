# PNJ V30 — Clean V1 + Daily 10:13 ICT (13/11/2026)

## Nguồn xác thực và phạm vi

- Source of truth GitHub: `annachou5566/data-fetcher-king/main`; duy nhất một workflow PNJ và một R2 history writer.
- PNJ raw history: bucket đã có `wave-alpha-data`, prefix **immutable** `metal/pnj/history-v1/days/YYYY/MM/YYYY-MM-DD.json`.
- R2 audit đầu 13/11/2026: **3.969/3.969** ngày lịch từ 01/01/2016 đến 12/11/2026 có object ngày; **3.357** ngày có ít nhất một giá hợp lệ, **612** ngày trống/chưa xác minh; **3.303** ngày có đủ cả hai sản phẩm, **54** ngày có giá nhưng thiếu ít nhất một sản phẩm mục tiêu; **243** hàng dữ liệu bị đánh dấu lỗi nguồn. Đây không phải cam kết đủ mọi lần thay đổi giá trong ngày hay chứng nhận dữ liệu đúng 100%.
- Không chỉnh sửa private `annachou5566/wave-alpha`, Production, Oracle, Cloudflare Worker, Page, hay schema của các job khác.

## Phase 1 — Immutable clean V1

- Runner: `scripts/pnj_clean_v1.py`; khóa `PNJ_CLEAN_R2_WRITE_APPROVED=1` chỉ trong workflow chuyên dụng.
- **R2 clean**: `metal/pnj/clean-v1/snapshots/YYYY/MM/YYYY-MM-DD/<source_sha256>.json` là immutable. `metal/pnj/clean-v1/days/YYYY/MM/YYYY-MM-DD.json` là **derived latest view** có thể được cập nhật sau khi lưu snapshot bất biến. `metal/pnj/clean-v1/_state.json` checkpoint riêng: đọc lại và verify R2 sau mỗi write.
- Chỉ nhận sản phẩm nguồn `SJC` ở các khu vực PNJ hợp lệ, phân loại `PNJ_SJC_QUOTE`; `Nhẫn Trơn PNJ 999.9` ở `Giá vàng nữ trang`, phân loại `PNJ_RING_9999`. **Không** đổi sản phẩm `PNJ` vùng sang nhẫn, không ghép với BTMH/SJC trực tiếp.
- Giá: nguồn `thousand_VND_per_luong` chuyển bằng số nguyên sang `VND_per_luong` (**×1.000**), yêu cầu cả mua và bán, kiểm tra giá, ngày, spread, trùng thời gian. Giữ timestamp nguồn theo Asia/Ho_Chi_Minh (không thêm offset giả).
- Chỉ tạo quote bán lẻ bid/ask theo thời điểm, **không** tạo nến OHLC, volume, forward-fill hoặc giá ngày nghỉ. Phân loại trạng thái `BOTH_TARGETS_OBSERVED`, `PARTIAL_TARGET_COVERAGE`, `NO_TARGET_QUOTES`, và lưu quarantine cho lỗi target. Mọi bản gốc giữ nguyên.
- Lớp clean mới có `publication_status=INTERNAL_REVIEW_ONLY`: không đưa lên frontend trước QA nghiệm thu độc lập.

## Phase 2 — Daily + source revision provenance

- Workflow duy nhất: `.github/workflows/pnj-history-backfill.yml`. Thay lịch chạy hàng giờ bằng `13 3 * * *` **UTC = 10:13 sáng Việt Nam (ICT)**. GitHub Free không đảm bảo thời điểm chính xác.
- Event `push` (chỉ file V30 được allowlist, chỉ branch main) chạy **một lần clean backfill 3.969 ngày** hoàn toàn từ R2, KHÔNG gọi PNJ. Nếu vượt runtime, checkpointer tự ngắt tại 135 phút và daily cron resume.
- Event `schedule` hoặc `workflow_dispatch: daily`: catch-up tối đa 7 ngày từ global source cursor, chỉ đến **hôm qua ICT**; recheck 3 ngày đã đóng gần nhất với 10 giây giãn cách. R2 source gốc không bao giờ overwrite.
- PNJ thay đổi raw: tạo phiên bản bất biến `metal/pnj/history-v1/revisions/YYYY/MM/YYYY-MM-DD/<source_sha256>.json` và pointer `history-v1/latest/.../YYYY-MM-DD.json`, đồng thời tạo clean snapshot mới. Nếu phản hồi mới trống hoặc mất target cũ, chỉ lưu candidate/review để kiểm tra, **không** promote pointer hay ghi đè clean view.
- 401/403/429/redirect: persisted pause vào state nguồn; không dùng proxy, xoay IP, CAPTCHA bypass hoặc retry trên HTTP. Lỗi mạng tối đa 2 retries như V29, checkpoint giữ nguyên.
- Một concurrency group cố định `pnj-history-backfill`, `cancel-in-progress: false`, `contents: read`, Ubuntu GitHub public standard runner. Emergency pause: repository variable `WA_PNJ_HISTORY_PAUSED=1`.
- R2 Standard Free snapshot gần nhất: tài khoản ~2,53GB / 10GB-month, 89.446 PutObject, 309.810 GetObject trong tháng 11, dưới hạn mức miễn phí tại thời điểm kiểm tra. Tiếp tục theo dõi vì các job khác cũng dùng chung tài khoản; không khẳng định chi phí tương lai tuyệt đối bằng 0.

## QA và nghiệm thu

- Tối thiểu 61 bài test offline từ 2 suite PNJ đều PASS trên exact GitHub branch; 5 mẫu PNJ R2 **đầy đủ, không bị API trả kết quả rút gọn**, xác minh SHA256 và trạng thái đúng (Chủ nhật, thiếu nhẫn, giá đang cập nhật, inverted SJC, quote mới nhất).
- Sau triển khai `main`, **chỉ công bố Phase 1 runtime PASS khi** GitHub hosted clean Actions `success` và `metal/pnj/clean-v1/_state.json.next_date >= 2026-11-13`, tổng số clean object là 3.969, không thiếu ngày/đụng raw.
- **Chỉ công bố Phase 2 hosted PASS sau lượt daily đầu tiên** với một ngày mới, recheck + provenance, và xác nhận cron chỉ có 1 lịch/ngày. Cho tới khi đó, status Phase 2 là source/schedule deployed; runtime chưa chứng minh.
- Next: kiểm toán khoảng trống 612 ngày, lỗi target ở 54 ngày, 243 bản ghi nguồn bất thường; thiết lập chính sách sản phẩm/tiếp cận dữ liệu trước khi dùng vào website staging và tiếp tục giữ `INTERNAL_REVIEW_ONLY`.
