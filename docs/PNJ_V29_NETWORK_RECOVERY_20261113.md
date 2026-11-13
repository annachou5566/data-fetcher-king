# PNJ V29 — Network Recovery (2026-11-13)

## Điều tra hai GitHub Actions FAIL gần nhất

- Run 43332449329 (workflow_dispatch): lưu thành công đến 2023-08-31, tiếp theo `UPSTREAM_NETWORK_ERROR` vào 2023-09-01.
- Run 43336682548 (schedule): tự nối checkpoint, lưu 2023-09-01 → 2023-09-05, tiếp theo `UPSTREAM_NETWORK_ERROR` vào 2023-09-06.
- Các bước checkout, Python, unit tests và pip đều PASS; **lỗi chỉ tại bước lấy dữ liệu PNJ**. Log không cung cấp root cause TCP/DNS/timeout chính xác, nên không được khẳng định nguyên nhân chi tiết.
- Cloudflare R2 đọc trực tiếp: `_state.json.next_date=2023-09-06`, `days_processed=2805`, không bị pause; năm 2023 có 248 ngày đã commit. Object 2023-09-05 tồn tại và 2023-09-06 chưa tồn tại.
- Năm 2019, 2020, 2021, 2022 đã scan xong; giữ nguyên mọi dữ liệu nguồn và checkpoint.

## Thay đổi cực nhỏ V29

- `fetch_day` thử lại **chính xác cùng một ngày**, chỉ khi phát sinh `URLError`, `TimeoutError` hoặc `ConnectionError`.
- Tổng tối đa **3 lần gọi**: lần đầu, thử lại sau 10 giây, thử lại sau 25 giây. Không đổi IP, không proxy, không vòng lặp vô hạn.
- **Không thử lại bất kỳ phản hồi HTTP nào**, nhất là 401/403/429. Nếu 403/429, cơ chế pause hiện hành vẫn giữ nguyên. Không thử lại trường hợp JSON/schema/giá dữ liệu không hợp lệ.
- Không đổi `max_days=366`, 10 giây giữa các ngày, 150 phút graceful cutoff, 180 phút GitHub job timeout, một writer, R2 Standard hiện tại, hoặc website WaveAlpha.
- Một ngày chỉ được ghi lên R2 sau khi đọc đủ JSON hợp lệ. Ghi từng object + readback + năm + state như V28, không overwrite lại ngày đã hoàn tất.
- Hết ba lần mà vẫn lỗi: giữ `STATUS=HOLD reason=UPSTREAM_NETWORK_ERROR`, GitHub job FAIL hữu ý. Cron kế tiếp hoặc manual sau khi nguồn ổn định tự resume. Không che lỗi thành Success.

## QA và tiếp tục

- GitHub source + test đã được nạp nguyên bản vào VM cô lập: **38/38 PASS**, gồm 4 test mới cho retry đúng ngày, exhausted attempts, không retry 403/429/503, dữ liệu và cursor cũ giữ nguyên khi lỗi.
- Đây là **offline QA** cho logic V29; phải đọc log hosted run tiếp theo trước khi tuyên bố chạy ổn thực tế.
- Nếu không có job PNJ đang chạy, có thể để schedule tự thử lại hoặc manual `main → max_days=366` đúng một lượt. Checkpoint tại 2023-09-06. Nếu ba lần vẫn không kết nối được, ngừng bấm liên tục và kiểm tra quyền truy cập / lỗi mạng phía PNJ.
- Dữ liệu lưu là raw, vẫn `INTERNAL_REVIEW_ONLY`, chưa chứng nhận sạch 100% hoặc cho phép bulk redistribution; tách xử lý chất lượng dữ liệu và publication.
