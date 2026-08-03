#!/usr/bin/env python3
"""fetch_grok_email_and_ingest.py — TỰ ĐỘNG HOÀN TOÀN, không cần copy-paste tay.

Luồng: Grok Automation (lên lịch sẵn trên grok.com) chạy xong -> gửi email
kết quả tới 1 hộp Gmail riêng -> workflow này (chạy theo lịch cron trên
GitHub Actions) tự kết nối Gmail qua IMAP -> tìm email Grok mới nhất chưa xử
lý -> trích JSON ra khỏi nội dung email -> kiểm tra 4 lớp (giống hệt
ingest_grok_snapshot.py) -> ghi lên R2 nếu qua hết -> đánh dấu email đã đọc.

Cần 1 lần duy nhất, KHÔNG lặp lại mỗi ngày:
  1. Tạo 1 Gmail (khuyên dùng riêng, không dùng gmail chính) để nhận email
     Grok automation.
  2. Bật "App Password" cho Gmail đó: myaccount.google.com/apppasswords
     (cần bật 2FA trước nếu chưa có) -> tạo 1 app password riêng cho việc này.
  3. Trong Grok (grok.com) tạo Automation: prompt = nội dung
     grok_grayscale_prompt.txt, lịch chạy = mỗi ngày/2 ngày vào giờ Grayscale
     hay cập nhật, output = gửi qua EMAIL tới đúng Gmail ở bước 1.
     ⚠️ QUAN TRỌNG: dặn rõ trong phần mô tả Automation là email PHẢI chứa
     NGUYÊN VĂN JSON, không tóm tắt/diễn giải lại — nếu Grok tự ý viết email
     dạng "digest" thân thiện thay vì JSON thô, script này sẽ không trích
     được gì và tự bỏ qua an toàn (không báo lỗi giả, chỉ fallback Farside).
  4. Thêm 2 GitHub Secrets: GMAIL_ADDRESS, GMAIL_APP_PASSWORD (mật khẩu ứng
     dụng ở bước 2, KHÔNG phải mật khẩu Gmail thật).

Từ đó về sau: hoàn toàn không cần đụng tay vào bước nào nữa.
"""
import os, sys, re, json, imaplib, email
from email.header import decode_header
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_etf as fe                       # get_r2 / r2_put_json
from ingest_grok_snapshot import validate_fund, TRACKED_TICKERS  # tái dùng đúng 4 lớp kiểm tra

GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
# Xác nhận thật 31/07/2026: email Grok automation gửi từ noreply@x.ai,
# subject "Grayscale ETF Crypto Data Extraction JSON" — subject đã đủ đặc
# trưng nên SUBJECT_FILTER để trống, chỉ lọc theo sender.
SENDER_FILTER = os.getenv("GROK_EMAIL_SENDER_FILTER", "x.ai")
SUBJECT_FILTER = os.getenv("GROK_EMAIL_SUBJECT_FILTER", "")  # để trống nếu không lọc theo subject


def decode_mime_words(s):
    parts = decode_header(s)
    return "".join(
        (p.decode(enc or "utf-8", errors="replace") if isinstance(p, bytes) else p)
        for p, enc in parts
    )


def get_email_body_text(msg):
    """Lấy phần text/plain (ưu tiên) hoặc text/html (fallback, strip tag thô)."""
    if msg.is_multipart():
        plain, html = None, None
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain" and plain is None:
                plain = part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8", errors="replace")
            elif ctype == "text/html" and html is None:
                html = part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8", errors="replace")
        if plain:
            return plain
        if html:
            return re.sub(r"<[^>]+>", " ", html)  # strip tag HTML thô, đủ dùng để tìm JSON
        return ""
    else:
        payload = msg.get_payload(decode=True)
        if payload is None:
            return msg.get_payload()
        return payload.decode(msg.get_content_charset() or "utf-8", errors="replace")


def extract_json(text):
    """Tìm khối JSON trong nội dung email — chịu được trường hợp Grok bọc
    thêm code fence ```json ... ``` hoặc vài dòng mô tả trước/sau."""
    text = re.sub(r"```(?:json)?", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    candidate = text[start:end + 1]
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        return None


def main():
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        print("THIẾU GMAIL_ADDRESS / GMAIL_APP_PASSWORD — chưa cấu hình, thoát an toàn (không phải lỗi, có thể user chưa setup xong).")
        sys.exit(0)

    # ⚠️ CHẨN ĐOÁN AN TOÀN — không in secret thật, chỉ in ĐỘ DÀI và có/không
    # khoảng trắng/newline lạ, để tự phát hiện 2 lỗi hay gặp nhất (secret rỗng
    # do gõ sai tên, hoặc dính dấu cách/newline khi copy-paste) mà KHÔNG cần
    # bạn dán secret thật cho tôi xem.
    print(f"[chẩn đoán] GMAIL_ADDRESS: độ dài={len(GMAIL_ADDRESS)}, có khoảng trắng đầu/cuối={GMAIL_ADDRESS != GMAIL_ADDRESS.strip()}, có @gmail.com={'@gmail.com' in GMAIL_ADDRESS.lower()}")
    print(f"[chẩn đoán] GMAIL_APP_PASSWORD: độ dài={len(GMAIL_APP_PASSWORD)} (App Password chuẩn Google = ĐÚNG 16 ký tự, không dấu cách), có dấu cách={' ' in GMAIL_APP_PASSWORD}, có khoảng trắng đầu/cuối={GMAIL_APP_PASSWORD != GMAIL_APP_PASSWORD.strip()}")

    imap = imaplib.IMAP4_SSL("imap.gmail.com")
    try:
        imap.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
    except imaplib.IMAP4.error as e:
        print(f"\n❌ LOGIN THẤT BẠI: {e}")
        print("Nếu 2 dòng [chẩn đoán] ở trên đều bình thường (độ dài đúng, không")
        print("dấu cách thừa) thì đây GẦN NHƯ CHẮC CHẮN là do Google chặn đăng")
        print("nhập từ IP lạ (GitHub Actions) — không phải lỗi secret/code.")
        print("Cách sửa: đăng nhập Gmail bằng trình duyệt (đúng tài khoản này),")
        print("mở https://accounts.google.com/DisplayUnlockCaptcha , bấm")
        print("'Continue' — việc này 'nới lỏng' chặn đăng nhập lạ trong ít phút,")
        print("chạy lại workflow NGAY sau đó để test. Nếu vẫn lỗi sau bước này,")
        print("khả năng cao đây là Google Workspace (không phải Gmail cá nhân)")
        print("và admin đã tắt IMAP/less-secure-app ở cấp tổ chức — cần dùng")
        print("Gmail cá nhân (@gmail.com) thay vì email công ty/trường học.")
        sys.exit(1)

    imap.select("INBOX")

    criteria = ['UNSEEN', 'FROM', f'"{SENDER_FILTER}"']
    if SUBJECT_FILTER:
        criteria += ['SUBJECT', f'"{SUBJECT_FILTER}"']
    status, msg_ids = imap.search(None, *criteria)
    if status != "OK" or not msg_ids[0]:
        print("Không có email Grok mới (chưa đọc) nào — không có gì để ingest, thoát.")
        imap.logout()
        sys.exit(0)

    ids = msg_ids[0].split()
    latest_id = ids[-1]  # mới nhất nếu có nhiều email chưa đọc dồn lại
    status, msg_data = imap.fetch(latest_id, "(RFC822)")
    raw_email = msg_data[0][1]
    msg = email.message_from_bytes(raw_email)
    subject = decode_mime_words(msg.get("Subject", ""))
    print(f"Đang xử lý email: {subject!r}")

    body = get_email_body_text(msg)
    data = extract_json(body)

    if data is None:
        print("KHÔNG trích được JSON hợp lệ từ email — có thể Grok gửi dạng digest thay vì JSON thô.")
        print(f"  300 ký tự đầu nội dung email: {body[:300]!r}")
        print("Không đánh dấu đã đọc, để lần chạy sau thử lại (phòng khi lỗi tạm thời).")
        imap.logout()
        sys.exit(0)

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
        print(f"\n⚠️ {len(rejected)} ticker bị từ chối — fallback Farside cho các ticker đó.")

    if accepted:
        r2 = fe.get_r2()
        payload = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source": "grok_email_automation",
            "email_subject": subject,
            "tickers": accepted,
        }
        fe.r2_put_json(r2, "grayscale-grok-snapshot.json", payload, cc="max-age=3600")
        print(f"\n✓ Đã ghi {len(accepted)} ticker lên R2: grayscale-grok-snapshot.json")
    else:
        print("Không có ticker nào qua được kiểm tra — không ghi gì lên R2.")

    # Đánh dấu đã đọc CHỈ SAU KHI xử lý xong (thành công hoặc thất bại rõ ràng
    # do dữ liệu, không phải do lỗi tạm thời) — tránh bỏ sót nếu script crash
    # giữa chừng ở lần chạy trước.
    imap.store(latest_id, '+FLAGS', '\\Seen')
    imap.logout()


if __name__ == "__main__":
    main()
