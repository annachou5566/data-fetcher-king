"""
scripts/sync_listing_prices.py
───────────────────────────────
Backfill "listing_price" (giá ngày đầu niêm yết) cho từng event trong
alpha-events/*.json trên R2, dùng lại API_AGG_KLINES nội bộ — nguồn này
hoạt động cho MỌI token Alpha (kể cả token chưa từng lên CEX), khác với
endpoint /api/v3/klines công khai chỉ có token đã spot-listed.

Idempotent: chỉ fetch những event CHƯA có listing_price. Chạy lại bao
nhiêu lần cũng an toàn, không tốn thêm request cho token đã xử lý.

Env cần (đã có sẵn trong GitHub Secrets, dùng chung với fetch_alpha.py):
  BINANCE_INTERNAL_KLINES_API, PROXY_WORKER_URL,
  R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME
"""

import json
import os
import time
import threading
import random
import urllib.parse
import requests
import zipfile
import io
import csv as _csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from dotenv import load_dotenv
import boto3
from botocore.config import Config

load_dotenv()

# Tái sử dụng fetch_smart() + get_session() đã có sẵn (proxy, retry, jitter, 429-backoff)
import fetch_alpha as fa

MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "4"))
fa._request_semaphore = threading.Semaphore(MAX_CONCURRENT)

API_AGG_KLINES = os.getenv("BINANCE_INTERNAL_KLINES_API")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# [MỚI] Danh sách token bạn TỰ XÁC NHẬN đã delisted (kiểm tra trực tiếp
# trên Binance), nhưng token-list API của Binance CHƯA cập nhật
# fullyDelisted=true kịp thời — cross-check tự động không bắt được nên
# vẫn cứ retry vô ích mỗi 30 phút. Thêm symbol vào đây để dừng hẳn, xoá
# đi bất cứ lúc nào nếu Binance list lại / bạn xác nhận sai.
MANUAL_CONFIRMED_DEAD = {"MIRROR", "CYPR", "RDAC"}

# [MỚI] Token bạn TỰ XÁC NHẬN giá đang lưu bị sai (do bug đã sửa) — thêm
# symbol vào đây để xoá giá cũ, code sẽ tự tính lại đúng ở lần chạy kế
# tiếp. Xoá khỏi danh sách này sau khi đã xác nhận giá mới đúng.
MANUAL_RESET_PRICES = {"SLX"}

# [SỬA] fetch_listing_price chạy đa luồng (ThreadPoolExecutor) — lý do fail
# phải lưu theo TỪNG THREAD riêng (threading.local), KHÔNG được gắn lên
# function object dùng chung, nếu không các luồng sẽ ghi đè lẫn nhau và
# log sẽ hiện SAI lý do (của token khác) cho từng token.
_fail_reason_local = threading.local()

# Một số chain non-EVM dùng prefix CT_ trong API nội bộ (giống logic trong fetch_alpha.py)
NO_LOWER_CHAINS = {"CT_501", "CT_784", "501", "784"}


def get_r2():
    return boto3.client(
        's3',
        endpoint_url=os.getenv("R2_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version='s3v4'),
    )


def load_json(r2, key):
    try:
        obj = r2.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def upload_json(r2, key, data):
    body = json.dumps(data, default=str, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    r2.put_object(
        Bucket=R2_BUCKET_NAME, Key=key, Body=body,
        ContentType='application/json', CacheControl='public, max-age=60',
    )
    print(f"  [R2] {key} updated ({len(data)} events, {len(body)//1024}KB)")


def _find_day_candles_1d(k_infos, target_date_str):
    """
    Tìm nến 1d đúng ngày CỦA EVENT ĐANG XỬ LÝ (target_date_str) — có thể là
    ngày niêm yết (TGE) hoặc ngày của một đợt airdrop/claim sau đó, vì mỗi
    event trong all.json có event_time riêng và được gọi hàm này riêng.

    QUAN TRỌNG: KHÔNG được fallback về k_infos[0] khi không tìm thấy đúng
    ngày — trước đây code làm vậy và nó âm thầm trả về nến ĐẦU TIÊN trong
    mảng (thường gần ngày niêm yết gốc), khiến các event airdrop lần 2/3
    của cùng token bị gán nhầm giá của ngày niêm yết đầu tiên. Bug này rất
    khó phát hiện vì field "date" trong kết quả trông vẫn hợp lệ.

    Thay vào đó: cho phép dung sai ±1 ngày (lệch timezone/UTC-cutoff nhẹ)
    nhưng phải log rõ khi dùng dung sai, và trả None (không đoán bừa) nếu
    hoàn toàn không có nến nào gần target_date_str.
    """
    try:
        target_dt = datetime.strptime(target_date_str, '%Y-%m-%d')
    except Exception:
        return None

    exact = None
    nearest = None
    nearest_diff = None

    for k in k_infos:
        try:
            k_dt = datetime.utcfromtimestamp(int(k[0]) / 1000)
        except Exception:
            continue
        day = k_dt.strftime('%Y-%m-%d')
        if day == target_date_str:
            exact = k
            break
        diff = abs((k_dt - target_dt).total_seconds())
        if nearest_diff is None or diff < nearest_diff:
            nearest_diff = diff
            nearest = k

    if exact:
        return exact

    # Dung sai tối đa 1 ngày (86400s) — dùng cho lệch UTC-cutoff nhẹ, KHÔNG
    # phải để che lấp việc thiếu dữ liệu ở ngày event thực sự xa hơn nhiều.
    if nearest is not None and nearest_diff is not None and nearest_diff <= 86400:
        if DEBUG:
            near_day = datetime.utcfromtimestamp(int(nearest[0]) / 1000).strftime('%Y-%m-%d')
            print(f"[debug] không có nến đúng ngày {target_date_str}, dùng nến gần nhất {near_day} (lệch {nearest_diff/3600:.1f}h)", end=" ")
        return nearest

    return None


def _compute_vwap(hourly_candles, target_date_str=None, start_ms=None, end_ms=None):
    """
    VWAP = Σ(typical_price × volume) / Σ(volume), chỉ tính trong đúng 24h
    của ngày niêm yết. typical_price = (high+low+close)/3 — phản ánh đúng
    vùng giá mà phần lớn volume diễn ra, không bị lệch về giá mở cửa thấp
    như cách dùng open đơn thuần.

    [SỬA] BUG THẬT: trước đây lọc nến bằng cách so KHỚP CHUỖI NGÀY LỊCH
    (vd "2026-05-25") — nhưng cửa sổ 24h thực tế của mình neo theo đúng
    giờ listingTime (vd 12:00 UTC ngày 25/5 → 12:00 UTC ngày 26/5), KHÔNG
    canh theo nửa đêm UTC. Kết quả: nến thuộc nửa SAU của cửa sổ (00:00–
    12:00 UTC ngày 26/5) bị so sánh với target_date_str="2026-05-25" →
    không khớp → bị loại khỏi VWAP dù vẫn nằm trong đúng 24h listing thật.
    Với SLX (event listingTime=25/5 12:00 UTC), lỗi này khiến VWAP chỉ
    tính trên nửa đầu ngày, gây sai lệch nghiêm trọng.

    Giờ lọc bằng KHOẢNG MILI-GIÂY TƯỜNG MINH [start_ms, end_ms] — luôn
    khớp đúng cửa sổ 24h thực tế đã fetch, bất kể có canh nửa đêm UTC hay
    không. Giữ tương thích ngược: nếu không truyền start_ms/end_ms, vẫn
    dùng target_date_str như cũ (cho code cũ chưa cập nhật call site).
    """
    num, den = 0.0, 0.0
    for k in hourly_candles:
        try:
            ot = int(k[0])
        except Exception:
            continue
        if start_ms is not None and end_ms is not None:
            if ot < start_ms or ot > end_ms:
                continue
        elif target_date_str is not None:
            try:
                day = datetime.utcfromtimestamp(ot / 1000).strftime('%Y-%m-%d')
            except Exception:
                continue
            if day != target_date_str:
                continue
        high, low, close, vol = fa.safe_float(k[2]), fa.safe_float(k[3]), fa.safe_float(k[4]), fa.safe_float(k[5])
        if vol <= 0:
            continue
        typical = (high + low + close) / 3
        num += typical * vol
        den += vol
    return (num / den) if den > 0 else None


def _max_price_since(k_infos, since_date_str, ref_price=None):
    """
    Giá cao nhất kể từ since_date_str (ngày event — listing hoặc airdrop)
    đến hiện tại MÀ NGƯỜI DÙNG THỰC SỰ CÓ THỂ CHỐT LỜI ĐƯỢC. Dùng để trả
    lời câu hỏi "nếu bán đúng đỉnh thì lời bao nhiêu", đối chiếu với việc
    bán ngay lúc claim (vwap 5 phút đầu) hoặc hold tới hiện tại (giá now).

    [SỬA] TRƯỚC ĐÂY dùng "high" của nến ngày — đây CHÍNH LÀ "cái râu nến
    dài thòn" mà không ai chốt lời thực sự được: "high" trên Binance chỉ
    cần 1 LỆNH DUY NHẤT (có thể do market maker cố tình bơm/xả giá trong
    tích tắc, thanh khoản gần như bằng 0) chạm mức đó là được ghi nhận,
    dù 99.99% volume cả ngày giao dịch ở vùng giá thấp hơn nhiều — không
    ai (trừ đúng người khớp lệnh đó) có cơ hội bán ở mức giá này.

    → Đổi sang dùng "close" (giá ĐÓNG CỬA) của mỗi ngày — đây là mức giá
    ĐÃ THỰC SỰ ỔN ĐỊNH/KHỚP LỆNH THẬT tại 1 thời điểm cụ thể (cuối ngày
    UTC), phản ánh đúng "vùng giá mà thị trường đã đồng thuận", không thể
    bị 1 lệnh đơn lẻ thao túng như "high". Lấy giá đóng cửa CAO NHẤT trong
    số tất cả các ngày kể từ lúc listing — đây là mức "chốt lời thực tế"
    cao nhất mà 1 người bán bình thường (không phải chính người tạo ra
    wick) có thể đạt được, nếu theo dõi giá đóng cửa mỗi ngày.

    (Đánh đổi: có thể bỏ lỡ 1 đỉnh THẬT xảy ra giữa ngày rồi tụt trước khi
    đóng cửa — nhưng đây là đánh đổi ĐÚNG HƯỚNG theo yêu cầu: thà báo thấp
    hơn giá thật có thể đạt, còn hơn báo 1 con số không ai chốt lời được.)

    Tái sử dụng LUÔN mảng nến 1d đã fetch sẵn cho fetch_listing_price
    (limit=1500 ngày, thường đã phủ từ lúc token mới list tới hiện tại)
    — không tốn thêm request nào.

    SANITY CHECK: API klines nội bộ đôi khi trả về 1 nến bị lỗi decimal/
    trùng địa chỉ (vd TAIKO ngày 2026-01-06 trả close ở mức phi lý so với
    ngày trước). Nếu KHÔNG lọc, 1 nến rác này khiến "peak return" của
    riêng token đó là số vô nghĩa (hàng tỷ %), và vì "Avg peak" ở frontend
    là trung bình cộng thuần trên toàn bộ token, 1 outlier đủ để kéo sập
    cả con số trung bình của toàn chart.

    Quy tắc: 1 nến close chỉ được chấp nhận là đỉnh mới nếu nó không vượt
    quá MAX_JUMP_MULT (mặc định 50x) so với close của nến liền trước (hoặc
    ref_price = giá lúc event, nếu chưa có nến nào trước đó). Nến bất
    thường sẽ bị bỏ qua và log ra, không âm thầm nuốt.
    """
    try:
        since_dt = datetime.strptime(since_date_str, '%Y-%m-%d')
    except Exception:
        return None

    MAX_JUMP_MULT = 50  # không token nào x50 trong 1 ngày rồi giữ nguyên mãi — đó là lỗi data

    relevant = []
    for k in k_infos:
        try:
            k_dt = datetime.utcfromtimestamp(int(k[0]) / 1000)
        except Exception:
            continue
        if k_dt < since_dt:
            continue
        relevant.append((k_dt, k))
    relevant.sort(key=lambda x: x[0])

    best_price, best_date = None, None
    prev_close = ref_price  # điểm neo đầu tiên: giá lúc event (vwap 5 phút)

    for k_dt, k in relevant:
        close = fa.safe_float(k[4])
        if close <= 0:
            continue

        if prev_close and prev_close > 0 and close > prev_close * MAX_JUMP_MULT:
            if DEBUG:
                print(f"[debug] bỏ qua nến bất thường {k_dt.date()}: close={close} vs prev_close={prev_close} (>{MAX_JUMP_MULT}x)", end=" ")
            continue  # KHÔNG cập nhật prev_close bằng giá trị bất thường này — neo lại giá trị cũ để tiếp tục so sánh nến sau

        if best_price is None or close > best_price:
            best_price = close
            best_date = k_dt.strftime('%Y-%m-%d')

        prev_close = close

    return {"price": best_price, "date": best_date} if best_price is not None else None


ALPHA_TRADE_KLINES_URL = "https://www.binance.com/bapi/defi/v1/public/alpha-trade/klines"


def fetch_alpha_trade_klines_official(alpha_id, interval, start_ms=None, end_ms=None, limit=1500):
    """
    Gọi API Alpha Klines CHÍNH THỨC, có docs công khai của Binance:
    https://developers.binance.com/docs/alpha/market-data/rest-api/klines

    [SỬA] KHÁC BIỆT SỐNG CÒN so với API nội bộ cũ (chainId+tokenAddress+
    dataType=aggregate): API này CÓ HỖ TRỢ THẬT startTime/endTime (docs
    xác nhận rõ). API nội bộ kia âm thầm LỜ HẲN startTime dù không báo
    lỗi gì — đã kiểm chứng bằng thực nghiệm: 2 lần chạy cách nhau 1 ngày,
    số nến trả về của MỌI token đều tăng đúng +1, bất kể startTime truyền
    vào là gì → chứng minh nó luôn trả "N nến gần nhất tính từ lúc gọi",
    khiến mọi event cũ hơn ~300 ngày luôn "rỗng" dù token vẫn còn sống.

    symbol format: "{alphaId}USDT" (vd "ALPHA_1011USDT") — alphaId lấy
    từ chính token list API (fetch_alpha_token_status_map trả về).

    Trả về list [[open_time, open, high, low, close, volume, close_time,
    ...], ...] — CÙNG FORMAT với klines Binance chuẩn (open_time ở mili-
    giây), tương thích thẳng với _compute_vwap/_max_price_since hiện có,
    không cần đổi gì thêm. Trả None nếu lỗi/không có dữ liệu.
    """
    if not alpha_id:
        return None
    params = {"symbol": f"{alpha_id}USDT", "interval": interval, "limit": min(limit, 1500)}
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    try:
        res = requests.get(
            ALPHA_TRADE_KLINES_URL, params=params, timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        if res.status_code != 200:
            return None
        payload = res.json()
        if payload.get("code") != "000000":
            return None
        data = payload.get("data")
        return data if isinstance(data, list) and data else None
    except Exception:
        return None


CLAIM_WINDOW_MINUTES = 5
"""
[MỚI] Độ dài cửa sổ tính "giá lúc claim" — CHỈ tính VWAP trong N phút đầu
tiên có giao dịch thật, KHÔNG PHẢI cả ngày (24h) như trước đây.

LÝ DO: "giá lúc claim" về bản chất là giá bạn nhận được khi CLAIM VÀ HÀNH
ĐỘNG NGAY (bán/giữ) — chỉ có ý nghĩa trong vài phút đầu khi thị trường vừa
mở, mọi người đổ xô claim cùng lúc. Sau khoảng đó, biến động giá phản ánh
hành vi HOLD (giữ tiếp), không còn liên quan gì tới "giá lúc claim" nữa.
Tính VWAP trên cả 24h sẽ TRỘN LẪN 2 khái niệm khác nhau (giá claim + biến
động cả ngày do hold), khiến con số không còn đại diện đúng cho "giá lúc
claim" — có thể sai lệch nếu ngày đó có biến động mạnh muộn hơn (như case
LINEA/SLX từng gặp, dù nguyên nhân khác nhưng cùng bản chất: khung thời
gian tính toán không khớp với khái niệm muốn đo).

max_since (đỉnh giá) KHÔNG bị ảnh hưởng bởi thay đổi này — vẫn quét từ
đúng lúc listing tới HIỆN TẠI như cũ, vì "đỉnh" đúng nghĩa là so sánh với
toàn bộ lịch sử kể từ đó, không giới hạn 10 phút.

Đổi số phút ở đây nếu muốn (5-10 phút là hợp lý theo yêu cầu thực tế).
"""


def fetch_listing_price(chain_id, contract, target_date_str, alpha_id=None, alpha_listing_time_ms=None):
    """
    Trả về {vwap, open, close, date, max_since} của ngày event (listing
    hoặc airdrop), hoặc None nếu không tìm được dữ liệu nào / token chưa
    có klines nội bộ.

    vwap: giá đại diện cho "lúc claim" — [SỬA] tính VWAP CHỈ trong
          CLAIM_WINDOW_MINUTES phút đầu tiên có giao dịch thật (mặc định
          5 phút), KHÔNG PHẢI cả ngày 24h như bản trước. Xem docstring
          của CLAIM_WINDOW_MINUTES ở trên để biết lý do — tóm tắt: VWAP
          24h trộn lẫn "giá lúc claim" với biến động do HOLD cả ngày,
          không còn đúng ý nghĩa "giá lúc claim" nữa.
    open / close: giá mở/đóng của khung CLAIM_WINDOW_MINUTES đó — giữ lại
                  để tham khảo / fallback khi vwap không tính được (vd
                  volume = 0 suốt cả khung).
    max_since: {price, date} — [SỬA] giá ĐÓNG CỬA (close) cao nhất trong
               số tất cả các ngày kể từ lúc listing đến HIỆN TẠI — KHÔNG
               dùng "high" (giá cao nhất trong ngày) nữa, vì "high" chỉ
               cần 1 lệnh chớp nhoáng chạm mức đó là được ghi nhận (kiểu
               "râu nến" do market maker bơm/xả, thanh khoản gần như bằng
               0 tại đó) — không ai chốt lời thực sự được. "close" là mức
               giá ĐÃ THỰC SỰ ỔN ĐỊNH tại 1 thời điểm cụ thể, đại diện
               đúng cho "nếu theo dõi giá mỗi ngày và bán ở ngày tốt nhất,
               tối đa lời được bao nhiêu" — con số THỰC TẾ CHỐT LỜI ĐƯỢC,
               không phải con số lý thuyết không ai chạm tới. Không bị
               giới hạn bởi CLAIM_WINDOW_MINUTES — đây vẫn là "đỉnh trong
               suốt lịch sử nắm giữ", để so sánh "bán lúc claim" vs "hold"
               vs "bán đúng đỉnh".

    alpha_id: nếu có (từ fetch_alpha_token_status_map), ƯU TIÊN dùng API
    Alpha klines CHÍNH THỨC trước — hỗ trợ startTime thật, không giới hạn
    "N nến gần nhất" như API nội bộ. Chỉ fallback về API nội bộ (chainId/
    tokenAddress) khi không có alpha_id hoặc API chính thức không có data
    (vd token đã fullyDelisted khỏi Alpha, API official không còn trả).

    alpha_listing_time_ms: [SỬA] mốc "listingTime" THẬT lấy từ chính API
    token-list Binance — QUAN TRỌNG vì target_date_str (event_time trong
    data của mình) thực ra là ngày CÔNG BỐ Pre-TGE, KHÔNG PHẢI ngày
    listing thật để giao dịch. Khoảng cách 2 mốc này thay đổi tuỳ token
    (đã xác minh thực tế: SENT cách 3 ngày, PIEVERSE cách 16 ngày, BTW
    cách ~70 ngày) — không có cửa sổ dò cố định nào đủ tin cậy. Nếu có
    mốc này, dùng THẲNG làm ngày mục tiêu, khỏi cần dò mò.
    """
    official_attempted = False
    official_note = "không có alphaId (symbol không có trong 637 token đối chiếu, hoặc chưa graduate/chưa từng lên Alpha)"

    if alpha_id:
        official_attempted = True
        try:
            if alpha_listing_time_ms:
                # Có mốc listingTime thật — dùng thẳng, chính xác tuyệt đối
                target_ms = int(alpha_listing_time_ms)
                target_date_str_real = datetime.utcfromtimestamp(target_ms / 1000).strftime('%Y-%m-%d')
            else:
                target_ms = int(datetime.strptime(target_date_str, "%Y-%m-%d").timestamp() * 1000)
                target_date_str_real = target_date_str
            day_start, day_end = target_ms, target_ms + 86400000 - 1

            # [SỬA] Dùng nến 1 PHÚT thay vì 1 giờ để tính VWAP — nến 1h gộp
            # cả giờ thành 1 open/high/low/close, nên công thức
            # (high+low+close)/3 coi 1 cú "wick" chỉ tồn tại vài giây (vd
            # thanh khoản mỏng, 1-2 lệnh nhỏ đẩy giá vọt lên rồi rơi ngay)
            # có trọng số ngang với volume CẢ GIỜ — kéo lệch VWAP nghiêm
            # trọng dù về bản chất chỉ 1 lượng volume rất nhỏ giao dịch ở
            # mức giá đó. Đã xác minh thực tế qua LINEA: nến 1h có
            # high=$2.007 (gấp ~87 lần vùng giá xung quanh $0.023-0.036)
            # kéo VWAP lên $0.336 dù giá thật đa số giao dịch quanh $0.03.
            # Xuống nến 1 phút, cú wick đó (nếu chỉ tồn tại vài giây/phút)
            # sẽ bị cô lập vào đúng 1-2 nến phút có volume nhỏ tương ứng,
            # không còn "mượn" volume của cả giờ nữa — VWAP phản ánh đúng
            # vùng giá NHIỀU NGƯỜI THỰC SỰ MUA BÁN ĐƯỢC nhất.
            #
            # Vẫn fetch NGUYÊN NGÀY (không chỉ 10 phút) vì cần dữ liệu này
            # để: (a) tìm chính xác PHÚT ĐẦU TIÊN có giao dịch thật (có
            # thể trễ vài phút so với listingTime do độ trễ hệ thống), và
            # (b) fallback dò ngày nếu chưa có listingTime (nhánh bên dưới).
            k_intraday = fetch_alpha_trade_klines_official(alpha_id, "1m", start_ms=day_start, end_ms=day_end, limit=1440)

            actual_target_date_str = target_date_str_real
            if not k_intraday and not alpha_listing_time_ms:
                # Chỉ dò mò khi KHÔNG có listingTime thật để dựa vào —
                # nhiều token thực tế chỉ có giao dịch vài ngày SAU ngày
                # công bố. Dò tiến 1d trong 14 ngày kế tiếp để tìm ngày
                # thật có nến đầu tiên (giống kỹ thuật dùng cho Vision).
                k_daily_probe = fetch_alpha_trade_klines_official(
                    alpha_id, "1d", start_ms=day_start,
                    end_ms=day_start + 14 * 86400000, limit=14
                )
                if k_daily_probe:
                    first_ms = int(float(k_daily_probe[0][0]))
                    actual_target_date_str = datetime.utcfromtimestamp(first_ms / 1000).strftime('%Y-%m-%d')
                    day_start2, day_end2 = first_ms, first_ms + 86400000 - 1
                    k_intraday = fetch_alpha_trade_klines_official(alpha_id, "1m", start_ms=day_start2, end_ms=day_end2, limit=1440)
                    day_start, day_end = day_start2, day_end2  # [SỬA] thiếu cập nhật day_end trước đây

            if k_intraday:
                # [MỚI] Tìm PHÚT ĐẦU TIÊN thật sự có giao dịch (volume>0)
                # trong ngày — không giả định listingTime/day_start chính
                # xác tới từng phút (có thể trễ vài phút do độ trễ hệ
                # thống matching engine). Từ đó lấy đúng CLAIM_WINDOW_MINUTES
                # phút kế tiếp để tính "giá lúc claim".
                first_trade_ms = None
                for k in k_intraday:
                    try:
                        if fa.safe_float(k[5]) > 0:  # volume > 0
                            first_trade_ms = int(k[0])
                            break
                    except Exception:
                        continue
                if first_trade_ms is None:
                    first_trade_ms = day_start  # không có nến nào có volume — dùng mốc gốc, vwap sẽ ra None ở bước sau

                claim_end_ms = first_trade_ms + CLAIM_WINDOW_MINUTES * 60_000 - 1
                claim_candles = [k for k in k_intraday if first_trade_ms <= int(k[0]) <= claim_end_ms]

                open_price  = fa.safe_float(claim_candles[0][1]) if claim_candles else fa.safe_float(k_intraday[0][1])
                close_price = fa.safe_float(claim_candles[-1][4]) if claim_candles else fa.safe_float(k_intraday[-1][4])
                vwap = _compute_vwap(k_intraday, start_ms=first_trade_ms, end_ms=claim_end_ms)
                ref_price = vwap if vwap is not None else close_price

                # Nến ngày để tính max_since — kể từ ĐÚNG LÚC LISTING (không
                # phải chỉ CLAIM_WINDOW_MINUTES) tới hiện tại, KHÔNG đổi.
                k_daily = fetch_alpha_trade_klines_official(alpha_id, "1d", start_ms=day_start, limit=1500)
                max_since = _max_price_since(k_daily, actual_target_date_str, ref_price=ref_price) if k_daily else None

                _fail_reason_local.value = None
                return {
                    "vwap": ref_price,
                    "open": open_price,
                    "close": close_price,
                    "date": actual_target_date_str,
                    "max_since": max_since,
                }
            if alpha_listing_time_ms:
                official_note = f"API chính thức KHÔNG có nến 1 phút nào cho alphaId={alpha_id} tại đúng listingTime thật ({target_date_str_real}) — token có thể chưa từng thật sự khớp lệnh dù đã lên lịch listing"
            else:
                official_note = f"API chính thức KHÔNG có nến 1 phút nào cho alphaId={alpha_id} trong ngày {target_date_str} lẫn 14 ngày sau đó (đã thử, trả rỗng, không có listingTime thật để dùng)"
        except Exception as ex:
            official_note = f"API chính thức lỗi: {ex}"
            if DEBUG: print(f"[debug] {official_note}", end=" ")

    if not API_AGG_KLINES:
        _fail_reason_local.value = f"[official: {official_note}] API_AGG_KLINES secret rỗng/chưa truyền vào script"
        if DEBUG: print(f"[debug] {_fail_reason_local.value}", end=" ")
        return None
    if not chain_id or not contract:
        _fail_reason_local.value = f"[official: {official_note}] thiếu chain_id={chain_id!r} hoặc contract={contract!r} trong event data"
        if DEBUG: print(f"[debug] {_fail_reason_local.value}", end=" ")
        return None

    addr = str(contract)
    chain_variants = [str(chain_id)]
    if str(chain_id) in ("501", "784"):
        chain_variants.append(f"CT_{chain_id}")  # thử biến thể Solana/Sui

    fail_reason = "không có chain nào để thử (chain_id/contract rỗng?)"

    # [SỬA] BUG THẬT: không truyền startTime thì API (giống mọi API kiểu
    # Binance klines) mặc định trả về N nến GẦN NHẤT TÍNH TỪ BÂY GIỜ, chứ
    # không phải N nến kể từ ngày event. Với event càng cũ, cửa sổ trả về
    # càng không chạm tới được ngày cần tìm — đúng như log thực tế cho
    # thấy: token trả về đều đặn ~270-320 nến (không phải 1000 như đã xin)
    # và luôn KHÔNG khớp ngày với các event quá 300 ngày trước. Neo
    # startTime vào target_date_str (trừ đệm vài ngày) để cửa sổ trả về
    # LUÔN bao trùm đúng ngày cần tìm, bất kể event cũ bao lâu.
    try:
        target_ms = int(datetime.strptime(target_date_str, "%Y-%m-%d").timestamp() * 1000)
    except (ValueError, TypeError):
        target_ms = None
    day_start_ms = target_ms - 5 * 86400000 if target_ms is not None else None  # đệm 5 ngày trước

    for cid in chain_variants:
        clean_addr = addr if cid in NO_LOWER_CHAINS else addr.lower()
        base = f"{API_AGG_KLINES}?chainId={cid}&tokenAddress={clean_addr}&dataType=aggregate"

        # 1) Nến ngày — luôn cần để có open/close + xác định đúng ngày
        day_url = f"{base}&interval=1d&limit=1000"
        if day_start_ms is not None:
            day_url += f"&startTime={day_start_ms}"
        try:
            res_day = fa.fetch_smart(day_url, retries=2)
        except Exception as ex:
            fail_reason = f"fetch_smart exception (1d, chain={cid}): {ex}"
            if DEBUG: print(f"[debug] {fail_reason}", end=" ")
            res_day = None

        if res_day is None:
            fail_reason = f"API aggregator không trả dữ liệu (1d, chain={cid}) — proxy lỗi, hoặc token/contract không được API nhận diện"
            if DEBUG: print(f"[debug] fetch_smart trả None (1d, chain={cid}) — proxy/secret/network lỗi", end=" ")
            continue

        k_day = (res_day.get("data") or {}).get("klineInfos")
        if not k_day:
            fail_reason = f"API trả về NHƯNG không có klineInfos (chain={cid}) — token/contract này API aggregator không có data (dù token có thể vẫn đang sống trên Alpha)"
            if DEBUG: print(f"[debug] res_day có trả về nhưng không có klineInfos (chain={cid}). keys={list(res_day.keys())}", end=" ")
            continue

        day_match = _find_day_candles_1d(k_day, target_date_str)
        if not day_match:
            fail_reason = f"API có {len(k_day)} nến ngày (chain={cid}) nhưng KHÔNG khớp ngày {target_date_str} — có thể ngoài phạm vi lịch sử API giữ, hoặc target_date_str sai"
            if DEBUG: print(f"[debug] có {len(k_day)} nến nhưng không khớp ngày {target_date_str}", end=" ")
            continue

        actual_date = datetime.utcfromtimestamp(int(day_match[0]) / 1000).strftime('%Y-%m-%d')
        open_price  = fa.safe_float(day_match[1])
        close_price = fa.safe_float(day_match[4])

        # 2) Nến 5 PHÚT trong đúng ngày đó — để tính VWAP chính xác hơn.
        # [SỬA] Trước đây dùng nến 1 GIỜ — 1 cú "wick" chỉ tồn tại vài
        # giây/phút (thanh khoản mỏng) có thể kéo lệch cả giờ dù volume
        # thật ở mức giá đó rất nhỏ (đã xác minh qua LINEA). Xuống 5 phút
        # giúp cô lập wick vào đúng khung nhỏ của nó, không "mượn" volume
        # của cả giờ — vẫn nằm trong giới hạn limit=1000 của API này
        # (1000 nến 5' ≈ 3.47 ngày, đủ phủ 24h mục tiêu + đệm 1 ngày).
        #
        # [SỬA THÊM] "giá lúc claim" chỉ nên tính trong CLAIM_WINDOW_MINUTES
        # phút đầu tiên có giao dịch thật — không phải cả 24h (xem docstring
        # CLAIM_WINDOW_MINUTES ở đầu file để biết lý do đầy đủ).
        vwap = None
        try:
            day_start_ms = int(day_match[0])
            day_end_ms   = day_start_ms + 86400000 - 1
            hour_start_ms = day_start_ms - 86400000  # đệm 1 ngày trước cho chắc
            hourly_url = f"{base}&interval=5m&limit=1000&startTime={hour_start_ms}"
            res_hourly = fa.fetch_smart(hourly_url, retries=1)
            k_hourly = (res_hourly or {}).get("data", {}).get("klineInfos") if res_hourly else None
            if k_hourly:
                first_trade_ms = None
                for k in k_hourly:
                    try:
                        ot = int(k[0])
                        if day_start_ms <= ot <= day_end_ms and fa.safe_float(k[5]) > 0:
                            first_trade_ms = ot
                            break
                    except Exception:
                        continue
                if first_trade_ms is None:
                    first_trade_ms = day_start_ms
                claim_end_ms = first_trade_ms + CLAIM_WINDOW_MINUTES * 60_000 - 1
                # [SỬA] Dùng khoảng ms tường minh thay vì so chuỗi ngày —
                # nhất quán với fix ở fetch_alpha_trade_klines_official,
                # tránh lặp lại cùng loại lỗi nếu ngày không canh nửa đêm.
                vwap = _compute_vwap(k_hourly, start_ms=first_trade_ms, end_ms=claim_end_ms)
        except Exception:
            pass

        ref_price = vwap if vwap is not None else close_price
        return {
            "vwap":  vwap if vwap is not None else close_price,  # fallback: dùng close nếu không có 1h data
            "open":  open_price,
            "close": close_price,
            "date":  actual_date,
            "max_since": _max_price_since(k_day, actual_date, ref_price=ref_price),
        }

    _fail_reason_local.value = f"[official: {official_note}] {fail_reason}"
    return None


BINANCE_VISION_BASE = "https://data.binance.vision"


def _download_vision_zip(url, retries=2):
    """
    Tải trực tiếp 1 file zip klines từ Binance Vision.
    Đây là file TĨNH trên CDN (S3), KHÔNG phải REST API — nên:
      - KHÔNG bị Binance chặn geo (451) như /api/v3/klines
      - KHÔNG cần proxy qua Render → KHÔNG tốn bandwidth Render
      - KHÔNG cần x-api-key
    Trả về bytes nếu có, None nếu file chưa được publish (404, bình thường
    với dữ liệu quá mới — ví dụ hôm nay/tháng này) hoặc lỗi mạng.
    """
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=20)
            if res.status_code == 200:
                return res.content
            if res.status_code == 404:
                return None  # chưa publish — không phải lỗi, cứ fallback proxy
        except Exception:
            pass
        time.sleep(0.5)
    return None


def _normalize_kline_row_ts(r):
    """
    Chuẩn hoá open_time (r[0]) và close_time (r[6], nếu có) về MILI-giây.

    [SỬA] Từ 1/1/2025, Binance Vision xuất timestamp ở MICRO-giây (16 chữ
    số) thay vì MILI-giây (13 chữ số) như REST API cũ:
    https://github.com/binance/binance-public-data
    ("The timestamp for SPOT Data from January 1st 2025 onwards will be
    in microseconds.")

    Không chuẩn hoá sẽ khiến int(ts)/1000 ra sai đơn vị 1000 lần →
    datetime.utcfromtimestamp() nhận giá trị khổng lồ → "year XXXXX is
    out of range". Ngưỡng 1e14 phân biệt an toàn: epoch mili-giây cho
    mọi ngày thực tế (1970–2100) luôn < 5e12, còn epoch micro-giây từ
    2025 trở đi luôn > 1.7e15 — không thể nhầm lẫn.
    """
    try:
        ot = float(r[0])
        if ot > 1e14:
            r[0] = str(ot / 1000.0)
        if len(r) > 6:
            ct = float(r[6])
            if ct > 1e14:
                r[6] = str(ct / 1000.0)
    except (ValueError, IndexError):
        pass
    return r


def _parse_vision_klines_csv(zip_bytes):
    """
    Giải nén + parse CSV klines từ Binance Vision thành list dạng
    [[open_time, open, high, low, close, volume, close_time, ...], ...]
    — CÙNG THỨ TỰ CỘT và CÙNG ĐƠN VỊ (mili-giây) như klines trả về từ
    REST API /api/v3/klines, nên toàn bộ code xử lý phía sau (VWAP,
    max-price-since,...) dùng index k[2]/k[3]/k[4]/k[5] không cần đổi gì.
    """
    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding='utf-8')
                reader = _csv.reader(text)
                for r in reader:
                    if not r or not r[0].strip():
                        continue
                    try:
                        float(r[0])  # bỏ dòng header (nếu có) không phải số
                    except ValueError:
                        continue
                    rows.append(_normalize_kline_row_ts(r))
    except Exception:
        return []
    return rows


def _binance_vision_daily_klines(symbol, interval, date_str):
    """1 file = 1 ngày. Dùng cho khung nhỏ (1h,...) — ví dụ VWAP 24h tại ngày listing."""
    pair = f"{symbol}USDT"
    url = f"{BINANCE_VISION_BASE}/data/spot/daily/klines/{pair}/{interval}/{pair}-{interval}-{date_str}.zip"
    content = _download_vision_zip(url)
    return _parse_vision_klines_csv(content) if content else None


def _binance_vision_monthly_klines(symbol, interval, year_month):
    """1 file = 1 tháng. Dùng cho khung lớn (1d,...) — tránh phải tải hàng trăm file ngày."""
    pair = f"{symbol}USDT"
    url = f"{BINANCE_VISION_BASE}/data/spot/monthly/klines/{pair}/{interval}/{pair}-{interval}-{year_month}.zip"
    content = _download_vision_zip(url)
    return _parse_vision_klines_csv(content) if content else None


def _try_vision_klines(symbol, interval, start_ms, limit):
    """
    Cố lấy klines từ Binance Vision trước. Trả None nếu không đủ dữ liệu
    (ví dụ khoảng thời gian quá mới, file chưa publish) để caller fallback
    sang proxy Render.
    """
    if start_ms is None:
        return None  # Vision cần biết đúng ngày/tháng — không hỗ trợ kiểu "N nến gần nhất tính từ giờ"
    if start_ms <= 0:
        # Trick "startTime=0 → nến sớm nhất" chỉ REST API Binance hỗ trợ,
        # Vision không có cách tương đương (không muốn dò từ 1970) → bỏ
        # qua Vision, để caller fallback thẳng qua proxy Render.
        return None

    start_dt = datetime.utcfromtimestamp(start_ms / 1000)
    rows = []

    if interval == "1d":
        # Gom các file THÁNG từ tháng listing tới tháng hiện tại
        cur = start_dt.replace(day=1)
        today = datetime.utcnow()
        months_tried = 0
        while cur <= today and months_tried < 36:  # chặn tối đa 3 năm, tránh vòng lặp vô hạn
            ym = cur.strftime("%Y-%m")
            part = _binance_vision_monthly_klines(symbol, interval, ym)
            if part:
                rows.extend(part)
            months_tried += 1
            cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)  # sang tháng kế tiếp
    else:
        # Gom các file NGÀY, đủ số ngày để phủ hết "limit" nến của khung nhỏ
        cur = start_dt.date()
        days_needed = max(1, (limit // 24) + 2)
        for _ in range(days_needed):
            part = _binance_vision_daily_klines(symbol, interval, cur.strftime("%Y-%m-%d"))
            if part:
                rows.extend(part)
            cur += timedelta(days=1)

    if not rows:
        return None

    rows = [r for r in rows if float(r[0]) >= start_ms]
    if not rows:
        return None
    rows.sort(key=lambda r: float(r[0]))
    return rows[:limit]


def _binance_public_klines(symbol, interval, start_ms=None, limit=1000, retries=2):
    """
    Lấy klines public Binance cho {symbol}USDT.

    [SỬA] KHÔNG còn dùng Render nữa — chỉ dùng Binance Vision
    (data.binance.vision, file tĩnh trên CDN). Lý do bỏ hẳn Render:
      - Binance ban theo IP. Render dùng IP chia sẻ (shared pool) — chỉ
        cần 1-2 request "xui" trúng lúc Binance đang nhạy cảm là dính 418,
        và ban đó ảnh hưởng LUÔN service Render khác (kể cả fetch_alpha.py
        cùng chạy trên đó).
      - Vision đã đủ dùng cho gần như mọi trường hợp thực tế (dữ liệu quá
        khứ tại ngày listing luôn có sẵn từ lâu, trừ token vừa list trong
        vài giờ/ngày gần nhất — trường hợp đó đơn giản là chưa có dữ liệu
        để tính, trả None và tự động thử lại ở lần chạy sau khi Vision đã
        publish, KHÔNG cần proxy chữa cháy).
    retries giữ lại trong signature để không phải sửa call site, nhưng
    không còn dùng (Vision không cần retry theo kiểu rate-limit).
    """
    return _try_vision_klines(symbol, interval, start_ms, limit)


def fetch_listing_price_public_spot(symbol, target_date_str):
    """
    Fallback cho token đã graduate khỏi Alpha (spot_listed=true): API klines
    DEX nội bộ thường không còn lịch sử cho nhóm này vì volume đã chuyển hẳn
    sang CEX order book. Dùng /api/v3/klines công khai của Binance qua proxy
    nội bộ (_binance_public_klines) — bắt buộc phải qua proxy vì Binance
    chặn IP GitHub Actions khi gọi thẳng.
    """
    try:
        start_ms = int(datetime.strptime(target_date_str, "%Y-%m-%d").timestamp() * 1000)
    except Exception:
        return None

    try:
        rows = _binance_public_klines(symbol, "1h", start_ms=start_ms, limit=24)
        if not isinstance(rows, list) or not rows:
            print(f"  [warn] public spot klines rỗng/lỗi cho {symbol} (kiểm tra proxy hoặc symbol có thật cặp USDT không)")
            return None

        num, den = 0.0, 0.0
        for k in rows:
            high, low, close, vol = float(k[2]), float(k[3]), float(k[4]), float(k[5])
            if vol <= 0:
                continue
            typical = (high + low + close) / 3
            num += typical * vol
            den += vol

        if den <= 0:
            return None

        max_since = None
        MAX_JUMP_MULT = 50  # cùng ngưỡng sanity-check như _max_price_since ở trên
        try:
            daily_rows = _binance_public_klines(symbol, "1d", start_ms=start_ms, limit=1000)
            if isinstance(daily_rows, list) and daily_rows:
                    best_price, best_date = None, None
                    prev_close = num / den if den > 0 else None  # neo bằng vwap ngày listing
                    for k in daily_rows:
                        high = float(k[2])
                        close = float(k[4])
                        if high <= 0:
                            continue
                        if prev_close and prev_close > 0 and high > prev_close * MAX_JUMP_MULT:
                            if DEBUG: print(f"[debug] bỏ qua nến bất thường public spot cho {symbol}: high={high} vs prev_close={prev_close}", end=" ")
                            if close > 0:
                                prev_close = close
                            continue
                        if best_price is None or high > best_price:
                            best_price = high
                            best_date = datetime.utcfromtimestamp(int(k[0]) / 1000).strftime('%Y-%m-%d')
                        if close > 0:
                            prev_close = close
                    if best_price is not None:
                        max_since = {"price": best_price, "date": best_date}
        except Exception as ex:
            if DEBUG: print(f"[debug] public spot max_since exception cho {symbol}: {ex}", end=" ")

        return {
            "vwap":  num / den,
            "open":  float(rows[0][1]),
            "close": float(rows[-1][4]),
            "date":  target_date_str,
            "max_since": max_since,
            "source": "public_spot",   # đánh dấu nguồn khác để dễ debug sau này
        }
    except Exception as ex:
        if DEBUG: print(f"[debug] public spot fallback exception cho {symbol}: {ex}", end=" ")
        return None



def fetch_spot_listing_date(symbol, since_date_str=None):
    """
    Lấy NGÀY THẬT token bắt đầu có lệnh khớp trên Binance spot — không đoán,
    không phụ thuộc cron job, không cần tra CMC/CoinGecko.

    [SỬA] KHÔNG còn dùng mẹo "startTime=0 → nến cũ nhất" nữa — đó là hành
    vi riêng của REST API Binance, phải đi qua Render nên có rủi ro bị
    Binance ban IP (đã dính thật ở lần chạy trước). Thay bằng: dò các file
    THÁNG (1d) trên Binance Vision, bắt đầu từ tháng token được list Alpha
    (since_date_str, nếu có) tiến dần về sau — vì ngày spot-listing luôn
    SAU ngày Alpha-listing nên không cần dò từ gốc lịch sử Binance (2017).
    File tháng đầu tiên có dữ liệu → nến đầu tiên trong đó chính là ngày
    spot-listing thật (Binance Vision chỉ có data từ ngày cặp bắt đầu giao
    dịch, không có nến rỗng phía trước).
    """
    try:
        if since_date_str:
            start_dt = datetime.strptime(since_date_str[:10], "%Y-%m-%d")
        else:
            start_dt = datetime.utcnow() - timedelta(days=730)  # không có hint: lùi tối đa 2 năm

        cur = start_dt.replace(day=1)
        today = datetime.utcnow()
        months_tried = 0
        while cur <= today and months_tried < 36:  # chặn tối đa 3 năm tìm kiếm
            ym = cur.strftime("%Y-%m")
            part = _binance_vision_monthly_klines(symbol, "1d", ym)
            if part:
                part.sort(key=lambda r: float(r[0]))
                open_ms = int(float(part[0][0]))
                return datetime.utcfromtimestamp(open_ms / 1000).strftime('%Y-%m-%d')
            months_tried += 1
            cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)

        print(f"  [warn] fetch_spot_listing_date: không tìm thấy dữ liệu Vision cho {symbol} (chưa có cặp {symbol}USDT hoặc chưa publish)")
        return None
    except Exception as ex:
        print(f"  [warn] fetch_spot_listing_date lỗi cho {symbol}: {ex}")
        return None




_ALPHA_STATUS_MAP = {}  # {SYMBOL: {"listingCex", "fullyDelisted", "alphaId", "contractAddress", "chainId"}} — set trong main()


def _get_alpha_info(symbol, contract_address=None):
    """
    Trả về (alpha_id, listing_time_ms) hoặc (None, None).

    [SỬA] Ticker Alpha có thể bị TÁI SỬ DỤNG cho token khác sau khi token
    cũ rời Alpha — contract_address (không thể trùng giữa 2 token khác
    nhau) dùng để đối chiếu an toàn trước khi tin alphaId.

    listing_time_ms lấy từ field "listingTime" của chính API — đây là
    mốc CHÍNH XÁC Binance ghi nhận ngày token bắt đầu giao dịch Alpha.
    QUAN TRỌNG: event_time trong data của mình ghi ngày CÔNG BỐ Pre-TGE,
    KHÔNG PHẢI ngày listing thật — khoảng cách 2 mốc này thay đổi tuỳ
    token (đã xác minh: SENT cách 3 ngày, PIEVERSE cách 16 ngày, BTW
    cách ~70 ngày) nên không thể đoán bằng 1 cửa sổ cố định — phải dùng
    listingTime thật thay vì dò mò.
    """
    st = _ALPHA_STATUS_MAP.get((symbol or "").upper())
    if not st or not st.get("alphaId"):
        return None, None
    if contract_address and st.get("contractAddress"):
        if str(contract_address).lower() != st["contractAddress"]:
            print(f"  [warn] {symbol}: alphaId={st['alphaId']} hiện tại là TICKER KHÁC "
                  f"(contract hiện tại {st['contractAddress'][:10]}... != event contract "
                  f"{str(contract_address)[:10]}...) — bỏ qua, không dùng nhầm")
            return None, None  # ticker bị tái sử dụng cho token khác — không tin alphaId này
    return st["alphaId"], st.get("listingTime")


def _process_one(e, idx, total, is_first_occurrence=True):
    """
    Worker cho 1 token — chạy trong thread pool. Trả về (event, result|None).

    is_first_occurrence: [MỚI] token có thể có NHIỀU event cùng symbol
    (nhiều đợt airdrop/claim khác nhau — "P1", "P2",...). listingTime lấy
    từ Binance CHỈ có 1 giá trị DUY NHẤT cho mỗi symbol (mốc list Alpha
    LẦN ĐẦU) — nếu áp dụng y hệt giá trị đó cho MỌI event cùng symbol, các
    đợt airdrop SAU (P2, P3...) sẽ bị tính giá SAI ngày (dùng nhầm ngày
    list lần đầu, có thể cách xa hàng tuần/tháng so với ngày claim thật
    của đợt đó) — kéo theo "đỉnh giá" bị tính từ SAI mốc thời gian, ra số
    vô nghĩa (đã xác minh thực tế qua ACU/WMTX round 2: đỉnh hiển thị hoá
    ra là ATH từ lần list ĐẦU TIÊN, không phải từ ngày airdrop lần 2).

    Chỉ event XƯA NHẤT của mỗi symbol mới được tin dùng listingTime chung
    này (vì nó khớp với lần list đầu); các event SAU dùng đúng ngày riêng
    của chính nó (date_str) — để fetch_listing_price tự dò/tính theo ngày
    đó, không bị ghi đè bởi listingTime của lần list đầu.
    """
    contract = e.get("contract_address")
    chain_id = e.get("chain_id")
    date_str = (e.get("event_time") or e.get("date") or "")[:10]
    symbol = e.get("symbol") or e.get("token") or "?"

    _alpha_id, _listing_ms = _get_alpha_info(symbol, contract_address=contract)
    if not is_first_occurrence:
        _listing_ms = None  # không tin listingTime chung cho đợt airdrop SAU lần đầu
    result = fetch_listing_price(chain_id, contract, date_str, alpha_id=_alpha_id, alpha_listing_time_ms=_listing_ms)
    dex_fail_reason = getattr(_fail_reason_local, "value", None) if not result else None

    if not result and e.get("spot_listed") and not e.get("listing_price_unavailable"):
        # [SỬA] Token đã graduate khỏi Alpha — trước đây dùng NHẦM date_str
        # (ngày list Alpha) để tra giá spot, trong khi cặp SYMBOLUSDT
        # thường chỉ bắt đầu có nến THẬT SỰ nhiều tuần/tháng SAU đó → luôn
        # "rỗng" dù symbol đúng và có cặp thật (đã xác minh HEMI, SAPIEN,
        # HOLO đều đang giao dịch spot thật trên Binance). Giờ tìm NGÀY
        # SPOT-LISTING THẬT trước (từ chính klines Binance), rồi mới lấy
        # giá đúng ngày đó.
        # listing_price_unavailable=True nghĩa là đã đối chiếu API Alpha
        # và xác nhận token CHẾT HẲN (fullyDelisted, chưa từng lên CEX) —
        # bỏ qua nhánh này vì chắc chắn Binance spot sẽ không có gì, tránh
        # tốn request/thời gian dò ngày spot-listing vô ích.
        spot_date = fetch_spot_listing_date(symbol, since_date_str=date_str)
        if spot_date:
            result = fetch_listing_price_public_spot(symbol, spot_date)
            if result:
                e["spot_listed_at"] = spot_date

    tag = f"OK  vwap=${result['vwap']:.6f}  (open=${result['open']:.6f})" if result else "no data"
    print(f"  [{idx}/{total}] {symbol} ({date_str})... {tag}", flush=True)
    if not result and dex_fail_reason:
        print(f"      ↳ lý do DEX/aggregator fail: {dex_fail_reason}", flush=True)

    return e, result


def assign_air_numbers(events):
    """
    [MỚI] Tính và LƯU THẲNG số thứ tự airdrop ("Air N") vào từng event,
    thay vì để frontend tự đoán bằng cách đếm/group lại mỗi lần render.

    Lý do làm ở backend, không phải frontend:
    - Đây là nơi DUY NHẤT có đủ toàn bộ lịch sử (all.json = backfill cũ +
      event realtime từ wa-listener gộp lại) để đếm chính xác "đây là lần
      airdrop thứ mấy của token này" — wa-listener (storage.py) không đủ
      thông tin để tự tính vì nó không biết các lần airdrop xảy ra TRƯỚC
      khi nó tồn tại.
    - Tính 1 lần, lưu vào data → mọi nơi đọc ra (frontend, API, export)
      đều thấy cùng 1 con số, không lệch nhau do khác cách nhóm/parse.

    Cách tính: nhóm theo symbol (viết hoa), loại "tge" ra khỏi phép đếm
    (TGE là phát hành/bán riêng, không phải đợt airdrop — dùng chung logic
    loại trừ với _first_occurrence_ids ở trên), sort theo event_time tăng
    dần, đánh số 1,2,3... theo đúng thứ tự thời gian thật.

    Idempotent + rẻ: luôn tính lại toàn bộ mỗi lần chạy (không cần cờ
    chống chạy lại), vì đây chỉ là phép sort+đếm trong RAM, không tốn
    request mạng nào.
    """
    groups = {}
    for e in events:
        event_type = (e.get("event_type") or e.get("type") or "").lower()
        if event_type == "tge":
            continue  # TGE không tính vào số thứ tự airdrop
        sym = (e.get("symbol") or e.get("token") or "").upper()
        if not sym:
            continue
        groups.setdefault(sym, []).append(e)

    changed = 0
    for sym, evs in groups.items():
        evs.sort(key=lambda ev: str(ev.get("event_time") or ev.get("date") or ev.get("created_at") or ""))
        for i, e in enumerate(evs, start=1):
            if e.get("air_number") != i:
                e["air_number"] = i
                changed += 1

    return changed


def _first_occurrence_ids(events):
    """
    [MỚI] Trả về set id() của event là lần xuất hiện SỚM NHẤT (theo
    event_time/date) của mỗi symbol — dùng để quyết định event nào được
    tin dùng listingTime chung từ Binance (chỉ lần đầu), event nào (các
    đợt airdrop sau — P2, P3...) phải dùng ngày riêng của chính nó.

    Tính trên TOÀN BỘ events (không chỉ phần "todo" chưa có giá) — vì
    lần đầu tiên của 1 symbol có thể ĐÃ được xử lý xong từ trước (không
    còn trong todo nữa), trong khi đợt sau (P2) mới là cái đang cần xử
    lý — vẫn cần biết nó KHÔNG PHẢI lần đầu để không áp nhầm listingTime.

    [SỬA] BUG THẬT phát hiện qua "LAB": event loại "tge" (Token Generation
    Event — phát hành/bán riêng token) có thể xảy ra RẤT LÂU TRƯỚC khi
    token thật sự lên Alpha (đã xác minh: LAB có TGE ngày 14/10/2025,
    nhưng mãi tới ~10/03/2026 mới lên Alpha — cách nhau gần 5 tháng).
    Trước đây coi "sớm nhất theo ngày" = "lần list đầu", nên TGE (luôn
    sớm nhất) bị NHẦM là lần list đầu, bị ghi đè ngày thật (14/10/2025)
    bằng listingTime của Binance (~10/03/2026) — trùng khớp y hệt ngày
    của sự kiện Airdrop, khiến 2 event khác nhau hiện CÙNG 1 NGÀY trên
    frontend dù dữ liệu gốc có 2 ngày khác nhau hoàn toàn.

    → TGE không bao giờ được coi là "lần list đầu" — luôn loại khỏi diện
    này, dùng đúng ngày riêng của nó (không bị ghi đè bởi listingTime).
    "Lần đầu" giờ chỉ xét trong số các event KHÔNG PHẢI TGE (grab/claim/
    airdrop — các loại thật sự liên quan tới việc lên Alpha).
    """
    earliest = {}
    for e in events:
        sym = (e.get("symbol") or e.get("token") or "").upper()
        if not sym:
            continue
        event_type = (e.get("event_type") or e.get("type") or "").lower()
        if event_type == "tge":
            continue  # TGE không liên quan tới ngày lên Alpha — luôn giữ nguyên ngày riêng
        d = (e.get("event_time") or e.get("date") or "")
        if sym not in earliest or d < earliest[sym][0]:
            earliest[sym] = (d, e)
    return {id(v[1]) for v in earliest.values()}


def invalidate_wrong_tge_dates(events):
    """
    [MỚI] Dọn dữ liệu ĐÃ TÍNH SAI bởi đúng bug vừa tìm ra ở "LAB": TGE
    (Token Generation Event) trước đây có thể bị nhầm là "lần list Alpha
    đầu tiên" (vì luôn xảy ra sớm nhất theo ngày), khiến ngày thật của nó
    bị GHI ĐÈ bằng listingTime của Binance — trùng hệt ngày của sự kiện
    Airdrop/claim thật sự, dù 2 event hoàn toàn khác nhau và cách nhau
    có thể tới vài tháng (LAB: TGE 14/10/2025, Alpha listing ~10/03/2026).

    Không dùng chung cờ _multiround_checked với invalidate_multi_round_
    listing_prices() — cờ đó đã được set (=True) từ TRƯỚC KHI có fix này,
    nên sẽ bị bỏ qua nếu tái sử dụng. Cần cờ RIÊNG cho đúng bug này.

    Phát hiện: event type="tge" có listing_price.date KHÁC với ngày gốc
    (event_time/date) của chính nó — đây là bằng chứng rõ ràng nó đã bị
    ghi đè bằng listingTime của 1 event khác (chỉ xảy ra do bug, TGE thật
    ra không bao giờ nên đổi ngày so với ngày gốc).
    """
    invalidated = 0
    for e in events:
        if e.get("_tge_date_checked"):
            continue
        e["_tge_date_checked"] = True
        event_type = (e.get("event_type") or e.get("type") or "").lower()
        if event_type != "tge":
            continue
        lp = e.get("listing_price")
        if not lp or not isinstance(lp, dict) or not lp.get("date"):
            continue
        own_date = (e.get("event_time") or e.get("date") or "")[:10]
        if own_date and lp["date"] != own_date:
            sym = e.get("symbol") or e.get("token") or "?"
            print(f"  [invalidate-tge] {sym}: TGE có listing_price.date={lp['date']} nhưng ngày gốc={own_date} "
                  f"— xoá để tính lại đúng ngày TGE thật")
            e["listing_price"] = None
            e["ath_since_listing_price"] = None
            e["ath_since_listing_date"] = None
            invalidated += 1
    return invalidated


def invalidate_multi_round_listing_prices(events):
    """
    [MỚI] Dọn lại dữ liệu ĐÃ TÍNH SAI theo đúng bug vừa tìm ra: các đợt
    airdrop/claim SAU lần đầu (P2, P3...) của cùng 1 symbol trước đây bị
    tính giá bằng listingTime CHUNG của symbol đó (mốc list Alpha LẦN
    ĐẦU), thay vì đúng ngày riêng của chính đợt đó — khiến "đỉnh giá"
    hiển thị sai hoàn toàn về mặt ý nghĩa (ATH từ lần list đầu, không
    phải từ ngày airdrop đang xem). Đã xác minh thực tế qua ACU/WMTX.

    An toàn để tính lại: dù trước đó tính bằng đường nào, cho tính lại
    theo logic đã sửa (dùng đúng ngày riêng của event) không gây hại gì,
    chỉ tốn thêm request. Có cờ chống quét lại vô hạn — is_first_occurrence
    là đặc điểm CỐ ĐỊNH theo dữ liệu hiện có, không tự đổi.
    """
    invalidated = 0
    first_ids = _first_occurrence_ids(events)
    for e in events:
        if e.get("_multiround_checked"):
            continue
        e["_multiround_checked"] = True
        if id(e) in first_ids:
            continue  # lần đầu — không thuộc diện lỗi này
        if e.get("listing_price"):
            e["listing_price"] = None
            e["ath_since_listing_price"] = None
            e["ath_since_listing_date"] = None
            invalidated += 1
    return invalidated


def enrich_events(events):
    """
    Mutates events in-place, returns count of newly-filled entries.
    Chạy song song có giới hạn (MAX_CONCURRENT) thay vì tuần tự —
    416 token tuần tự dễ vượt timeout 15 phút của GitHub Actions,
    chạy song song giảm thời gian xuống còn ~1/N.
    """
    todo = [
        e for e in events
        if not e.get("listing_price")
        and (e.get("symbol") or e.get("token") or "").upper() not in MANUAL_CONFIRMED_DEAD
        and e.get("contract_address")
        and (e.get("event_time") or e.get("date"))
    ]
    total = len(todo)
    filled = 0

    if not todo:
        return 0

    first_ids = _first_occurrence_ids(events)

    print(f"  Xử lý {total} token, chạy song song tối đa {MAX_CONCURRENT} luồng...")

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = {
            pool.submit(_process_one, e, i + 1, total, id(e) in first_ids): e
            for i, e in enumerate(todo)
        }
        for fut in as_completed(futures):
            e, result = fut.result()
            if result:
                e["listing_price"] = result
                # [MỚI] Đưa ATH (max_since) ra field top-level cho dễ dùng
                # — trước đây bị lồng bên trong listing_price.max_since,
                # dễ bị bỏ sót khi đọc data.
                ms = result.get("max_since")
                e["ath_since_listing_price"] = ms.get("price") if ms else None
                e["ath_since_listing_date"]  = ms.get("date") if ms else None
                filled += 1
            else:
                e["listing_price"] = None  # đánh dấu đã thử, lần sau vẫn tự retry vì None là falsy

    return filled


def _process_spot_one(e, idx, total):
    """Worker: xác định ngày spot-listing THẬT (từ klines Binance, không đoán),
    rồi fetch giá đúng ngày đó (tái dùng fetch_listing_price / public spot)."""
    contract = e.get("contract_address")
    chain_id = e.get("chain_id")
    symbol   = e.get("symbol") or e.get("token") or "?"
    alpha_date = (e.get("event_time") or e.get("date") or "")[:10] or None

    spot_date = fetch_spot_listing_date(symbol, since_date_str=alpha_date)
    if not spot_date:
        print(f"  [spot {idx}/{total}] {symbol}... không tìm được ngày spot-listing (symbol lệch/chưa có cặp USDT?)", flush=True)
        return e, None, None

    # [SỬA] KHÔNG truyền alpha_listing_time_ms ở đây — spot_date đã là
    # ngày CHÍNH XÁC tự tìm ra riêng cho việc lên spot (khác hẳn ngày
    # list Alpha ban đầu). Nếu truyền alpha_listing_time_ms, nó sẽ ghi đè
    # nhầm spot_date bằng ngày list Alpha không liên quan — cùng loại bug
    # vừa sửa ở _process_one cho các đợt airdrop lần 2 trở lên.
    _alpha_id2, _ = _get_alpha_info(symbol, contract_address=contract)
    result = fetch_listing_price(chain_id, contract, spot_date, alpha_id=_alpha_id2, alpha_listing_time_ms=None)
    if not result:
        result = fetch_listing_price_public_spot(symbol, spot_date)

    tag = f"OK  price=${result['vwap']:.6f}" if result else "no data"
    print(f"  [spot {idx}/{total}] {symbol} @ {spot_date}... {tag}", flush=True)
    return e, spot_date, result


def enrich_spot_listing_prices(events):
    """
    Với các event có spot_listed=true: tự tra ngày spot-listing THẬT của
    token trực tiếp từ Binance (fetch_spot_listing_date — nến sớm nhất của
    cặp SYMBOLUSDT), rồi fetch giá đúng ngày đó — để so sánh 3 chiến lược:
    bán trước spot (giá claim) / hold tới lúc spot (spot_listing_price) /
    hold dài hơn (giá hiện tại, tính ở frontend).

    Không phụ thuộc market-data.json hay cron-job diff nữa — chính xác hơn
    VÀ backfill được ngay cho token đã list từ lâu, không cần chờ transition.

    Idempotent: chỉ fetch event nào CHƯA có spot_listing_price.
    """
    todo = [
        e for e in events
        if e.get("spot_listed")
        and not e.get("spot_listing_price")
        and not e.get("listing_price_unavailable")
        and e.get("contract_address")
        and (e.get("symbol") or e.get("token"))
    ]
    total = len(todo)
    if not total:
        return 0

    print(f"  Xử lý {total} token cần giá lúc spot-listing, chạy song song tối đa {MAX_CONCURRENT} luồng...")
    filled = 0
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = {
            pool.submit(_process_spot_one, e, i + 1, total): e
            for i, e in enumerate(todo)
        }
        for fut in as_completed(futures):
            e, spot_date, result = fut.result()
            if spot_date:
                e["spot_listed_at"] = spot_date
            if result:
                e["spot_listing_price"] = result
                ms = result.get("max_since")
                e["spot_ath_price"] = ms.get("price") if ms else None
                e["spot_ath_date"]  = ms.get("date") if ms else None
                filled += 1
            else:
                e["spot_listing_price"] = None  # đánh dấu đã thử, None là falsy -> tự retry lần sau

    return filled


def _load_json_dict(r2, key):
    """
    [MỚI] load_json() ở trên chỉ hỗ trợ LIST (ép về [] nếu không phải
    list) — dùng riêng cho alpha-events. Cache Futures là DICT
    ({"ts":..., "symbols":[...]}) nên cần hàm đọc riêng, không ép kiểu.
    """
    try:
        obj = r2.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _upload_json_dict(r2, key, data):
    body = json.dumps(data, default=str, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    r2.put_object(
        Bucket=R2_BUCKET_NAME, Key=key, Body=body,
        ContentType='application/json', CacheControl='public, max-age=60',
    )


FUTURES_SYMBOLS_CACHE_KEY = "alpha-events/_futures_symbols_cache.json"
FUTURES_SYMBOLS_CACHE_MAX_AGE_SEC = 24 * 3600  # 1 ngày — Futures hiếm khi list mới, không cần mới hơn


def fetch_futures_symbols(r2):
    """
    Lấy danh sách symbol ĐANG có hợp đồng Futures (USDT-M) trên Binance —
    dùng để trả lời câu hỏi "token có lên Future không".

    [SỬA] fapi.binance.com/fapi/v1/exchangeInfo bị Binance chặn 451 với
    IP GitHub Actions (xác nhận qua log thật). data.binance.vision cũng
    không dùng được (web app JS, không phải REST/XML tĩnh).

    → Dùng lại route CÓ SẴN trên Render: GET /api/futures-tickers (vốn
    để hiện bảng ticker Futures ở frontend) — Render gọi fapi.binance.com
    bằng IP KHÔNG bị chặn, và response ticker/24hr đã sẵn danh sách MỌI
    symbol Futures đang giao dịch (mỗi symbol 1 dòng), không cần route
    exchangeInfo riêng → KHÔNG cần sửa/deploy lại Render.

    [QUAN TRỌNG — tiết kiệm bandwidth Render] response ticker/24hr vẫn
    nặng ~100-150KB. Futures gần như không đổi theo ngày, nên KHÔNG gọi
    mỗi lần chạy (mỗi 30 phút = ~430MB/tháng nếu dùng exchangeInfo, vẫn
    đáng kể dù nhẹ hơn) — cache kết quả trong R2, chỉi gọi lại Render nếu
    cache cũ hơn 24h. Giảm xuống còn ~1 lần/ngày, không đáng kể.
    """
    cached = _load_json_dict(r2, FUTURES_SYMBOLS_CACHE_KEY)
    now = time.time()
    if cached and isinstance(cached, dict) and cached.get("symbols"):
        age = now - cached.get("ts", 0)
        if age < FUTURES_SYMBOLS_CACHE_MAX_AGE_SEC:
            print(f"  [futures] dùng cache R2 (mới {age/3600:.1f}h trước, {len(cached['symbols'])} symbol) — không gọi Render")
            return set(cached["symbols"])

    if not fa.PROXY_WORKER_URL:
        print("  [warn] fetch_futures_symbols: PROXY_WORKER_URL rỗng, không gọi được Render — coi như chưa biết")
        return set(cached["symbols"]) if cached and cached.get("symbols") else set()

    parsed = urllib.parse.urlparse(fa.PROXY_WORKER_URL)
    url = f"{parsed.scheme}://{parsed.netloc}/api/futures-tickers"
    try:
        session = fa.get_session()  # có sẵn header x-api-key cho middleware bảo mật Render
        res = session.get(url, timeout=20)
        if res.status_code != 200:
            print(f"  [warn] fetch_futures_symbols (qua Render): HTTP {res.status_code}")
            return set(cached["symbols"]) if cached and cached.get("symbols") else set()
        data = res.json()
        if not isinstance(data, list):
            print("  [warn] fetch_futures_symbols: response không phải list")
            return set(cached["symbols"]) if cached and cached.get("symbols") else set()
        out = {t["symbol"].upper() for t in data if isinstance(t, dict) and t.get("symbol", "").upper().endswith("USDT")}
        if out:
            _upload_json_dict(r2, FUTURES_SYMBOLS_CACHE_KEY, {"ts": now, "symbols": sorted(out)})
        return out
    except Exception as ex:
        print(f"  [warn] fetch_futures_symbols (qua Render) lỗi: {ex}")
        return set(cached["symbols"]) if cached and cached.get("symbols") else set()


ALPHA_TOKEN_LIST_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"


def fetch_alpha_token_status_map():
    """
    Đối chiếu với danh sách token Alpha THẬT từ chính Binance — để phân
    biệt rạch ròi 2 trường hợp "no data" hoàn toàn khác nhau, mà log text
    (tên tag "DEX pair not found") không phân biệt được:

    1. Token đã fullyDelisted khỏi Alpha NHƯNG listingCex=true → đã
       GRADUATE lên spot thật (rời Alpha vì THÀNH CÔNG, không phải chết).
       Ví dụ thực tế: AIGENSYN, GENIUS, CHIP đều đúng case này.
    2. Token đã fullyDelisted khỏi Alpha VÀ listingCex=false → CHẾT THẬT
       (rút khỏi Alpha, chưa từng lên spot) — sẽ KHÔNG BAO GIỜ có klines
       Binance nào cả, retry mỗi 30 phút chỉ tốn request vô ích.

    Trả về dict {SYMBOL: {"listingCex": bool, "fullyDelisted": bool, "alphaId": str|None}}.
    Trả về {} nếu lỗi mạng/API — code gọi PHẢI coi thiếu dữ liệu là
    "chưa biết", tuyệt đối KHÔNG suy diễn thành "token chết", vì endpoint
    này chưa chắc đầy đủ 100% (không rõ có giới hạn/xoay vòng dữ liệu).
    """
    try:
        res = requests.get(
            ALPHA_TOKEN_LIST_URL, timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        )
        if res.status_code != 200:
            print(f"  [warn] fetch_alpha_token_status_map: HTTP {res.status_code}")
            return {}
        payload = res.json()
        data = payload.get("data") or []
        out = {}
        for t in data:
            sym = (t.get("symbol") or "").upper()
            if not sym:
                continue
            out[sym] = {
                "listingCex": bool(t.get("listingCex")),
                "fullyDelisted": bool(t.get("fullyDelisted")),
                "alphaId": t.get("alphaId") or None,
                # [SỬA] Ticker có thể bị TÁI SỬ DỤNG cho token Alpha khác
                # sau khi token cũ rời Alpha (đã xác minh thực tế: "BTW"
                # hiện tại trỏ tới token bắt đầu giao dịch 2026-03, không
                # phải token event 2025-12-22 trong data). contractAddress
                # là thứ DUY NHẤT không thể trùng giữa 2 token khác nhau —
                # dùng để đối chiếu, không match alphaId nếu contract khác.
                "contractAddress": (t.get("contractAddress") or "").lower(),
                "chainId": str(t.get("chainId") or ""),
                # [SỬA] event_time trong data của mình = ngày công bố
                # Pre-TGE, KHÔNG PHẢI ngày listing thật để giao dịch trên
                # Alpha. Khoảng cách 2 mốc này thay đổi tuỳ token — đã xác
                # minh thực tế: SENT chỉ cách 3 ngày, PIEVERSE cách 16
                # ngày (tin tức xác nhận Pre-TGE 29/10 nhưng Alpha trading
                # mở 14/11/2025), BTW cách tới ~70 ngày. Không thể đoán
                # bằng 1 cửa sổ cố định. May mắn field "listingTime" của
                # chính API này là mốc CHÍNH XÁC Binance ghi nhận — dùng
                # thẳng, không cần dò mò nữa.
                "listingTime": t.get("listingTime"),
            }
        return out
    except Exception as ex:
        print(f"  [warn] fetch_alpha_token_status_map lỗi (bỏ qua, coi như chưa biết): {ex}")
        return {}


def invalidate_at_risk_listing_prices(events, status_map):
    """
    [MỚI] Kiểm tra AN TOÀN hơn nhiều so với heuristic bị bỏ trước đó (so
    listing_price.date với listingTime — dễ nhầm với token qua nhánh DEX
    có "date" khác biệt hợp lệ).

    Cách này KHÔNG nhìn vào "date" cũ đã lưu — mà hỏi thẳng: "nếu tính
    lại NGAY BÂY GIỜ qua API Alpha chính thức, token này có ở diện dễ
    dính bug ranh giới ngày (đã sửa ở _compute_vwap) hay không?" Bug đó
    CHỈ xảy ra khi cửa sổ 24h không canh nửa đêm UTC — tức listingTime
    thật có giờ khác 00:00:00. Đã xác minh: BTW (08:00 UTC), SLX (12:00
    UTC) — cả 2 mốc thực tế gặp đều KHÔNG canh nửa đêm, nên đây nhiều khả
    năng là trường hợp PHỔ BIẾN, không phải ngoại lệ.

    An toàn vì: dù event đó trước đây được tính qua nhánh KHÁC (DEX/
    aggregator nội bộ, không phải API chính thức) — cho tính lại cũng
    KHÔNG gây hại gì, chỉ tốn thêm 1 lượt gọi API, kết quả cuối cùng vẫn
    đúng hoặc đúng hơn. Khác hẳn heuristic cũ (so ngày) vốn có thể XOÁ
    NHẦM dữ liệu ĐÃ ĐÚNG.
    """
    invalidated = 0
    if not status_map:
        return 0
    for e in events:
        if e.get("_vwap_daybound_checked"):
            continue  # [SỬA] đã quét 1 lần rồi — nếu không đánh dấu, token
                      # có listingTime không canh nửa đêm (đặc điểm CỐ ĐỊNH,
                      # không tự hết) sẽ bị xoá-tính lại VÔ HẠN mỗi 30 phút
        e["_vwap_daybound_checked"] = True   # đánh dấu NGAY, dù có invalidate hay không
        if not e.get("listing_price"):
            continue
        sym = (e.get("symbol") or e.get("token") or "").upper()
        contract = e.get("contract_address")
        alpha_id, listing_ms = _get_alpha_info(sym, contract_address=contract)
        if not alpha_id or not listing_ms:
            continue  # không có alphaId đáng tin cho symbol này -> không thuộc diện rủi ro này
        try:
            dt = datetime.utcfromtimestamp(int(listing_ms) / 1000)
        except Exception:
            continue
        if dt.hour == 0 and dt.minute == 0:
            continue  # canh đúng nửa đêm UTC -> không dính bug ranh giới ngày
        e["listing_price"] = None
        e["ath_since_listing_price"] = None
        e["ath_since_listing_date"] = None
        invalidated += 1
    return invalidated


def invalidate_manual_reset_prices(events):
    """
    [AN TOÀN, THỦ CÔNG] Bổ sung cho invalidate_at_risk_listing_prices ở
    trên — dùng khi bạn tự phát hiện 1 token giá sai nhưng không thuộc
    diện tự động phát hiện được (vd không có alphaId hiện tại để kiểm
    tra). Thêm symbol vào MANUAL_RESET_PRICES, chạy 1 lần để xoá giá cũ,
    code tự tính lại ở lần chạy kế tiếp — rồi xoá khỏi danh sách.
    """
    invalidated = 0
    for e in events:
        sym = (e.get("symbol") or e.get("token") or "").upper()
        if sym not in MANUAL_RESET_PRICES:
            continue
        if e.get("listing_price"):
            e["listing_price"] = None
            e["ath_since_listing_price"] = None
            e["ath_since_listing_date"] = None
            invalidated += 1
        if e.get("spot_listing_price"):
            e["spot_listing_price"] = None
            e["spot_ath_price"] = None
            e["spot_ath_date"] = None
    return invalidated


def apply_alpha_status(events, status_map, futures_symbols=None):
    """
    Mutates events in-place dựa trên status_map (từ fetch_alpha_token_status_map)
    và futures_symbols (từ fetch_futures_symbols). Trả về (số token tự phát
    hiện graduate lên spot, số token đánh dấu chết hẳn).

    [MỚI] Các field MÔ TẢ trạng thái (alpha_delisted, is_spot_listed,
    futures_listed) được cập nhật cho MỌI event có symbol khớp —
    không chỉ event còn thiếu giá — vì đây là thông tin trạng thái hiện
    tại, cần đúng cho cả event đã có giá từ trước. Phần TỰ ĐỘNG HÀNH ĐỘNG
    (đánh dấu spot_listed để kích hoạt dò giá / đánh dấu chết hẳn để dừng
    retry) thì vẫn chỉ chạy cho event chưa có listing_price, để không
    tốn công lặp lại vô ích.
    """
    marked_spot, marked_dead = 0, 0
    futures_symbols = futures_symbols or set()

    for e in events:
        sym = (e.get("symbol") or e.get("token") or "").upper()

        # Future — độc lập với Alpha token list, cập nhật cho mọi event
        if futures_symbols:
            e["futures_listed"] = f"{sym}USDT" in futures_symbols

        # [MỚI] Override thủ công — ưu tiên cao hơn cross-check tự động,
        # vì bạn đã tự xác nhận trực tiếp trên Binance, đáng tin hơn API
        # token-list (có thể cập nhật trễ).
        if sym in MANUAL_CONFIRMED_DEAD:
            e["alpha_delisted"] = True
            if not e.get("listing_price") and not e.get("listing_price_unavailable"):
                e["listing_price_unavailable"] = True
                marked_dead += 1
            continue

        if not status_map:
            continue
        st = status_map.get(sym)
        if not st:
            continue  # không có trong danh sách API -> chưa biết, không suy diễn

        # Field mô tả — luôn cập nhật, kể cả event đã có giá từ trước
        e["alpha_delisted"] = st["fullyDelisted"]
        e["is_spot_listed"] = bool(st["listingCex"])

        if e.get("listing_price"):
            continue  # đã có giá — không cần chạy tiếp phần tự động dưới

        if st["fullyDelisted"] and st["listingCex"] and not e.get("spot_listed"):
            e["spot_listed"] = True
            marked_spot += 1
        elif st["fullyDelisted"] and not st["listingCex"]:
            if not e.get("listing_price_unavailable"):
                e["listing_price_unavailable"] = True
                marked_dead += 1

    return marked_spot, marked_dead


def main():
    print(f"🔑 API_AGG_KLINES configured: {bool(API_AGG_KLINES)}")
    print(f"🔑 PROXY_WORKER_URL configured: {bool(fa.PROXY_WORKER_URL)}  (chỉ dùng cho Alpha aggregator API — klines public đã chuyển hẳn sang Binance Vision, KHÔNG còn qua Render)")
    if DEBUG:
        print(f"   API_AGG_KLINES = {API_AGG_KLINES}")
        print(f"   PROXY_WORKER_URL = {fa.PROXY_WORKER_URL}")

    r2 = get_r2()
    print("⏳ Loading alpha-events/all.json from R2...")
    all_events = load_json(r2, "alpha-events/all.json")
    print(f"   {len(all_events)} events loaded")

    if not all_events:
        print("⚠️  No events found — nothing to backfill.")
        return

    print("⏳ Đối chiếu với danh sách token Alpha thật từ Binance...")
    status_map = fetch_alpha_token_status_map()
    global _ALPHA_STATUS_MAP
    _ALPHA_STATUS_MAP = status_map
    print("⏳ Đối chiếu danh sách hợp đồng Futures (USDT-M) từ Binance...")
    futures_symbols = fetch_futures_symbols(r2)
    marked_spot, marked_dead = apply_alpha_status(all_events, status_map, futures_symbols)
    print(f"   Đối chiếu {len(status_map)} token Alpha + {len(futures_symbols)} symbol Futures — "
          f"tự phát hiện {marked_spot} token đã graduate lên spot, "
          f"đánh dấu {marked_dead} token đã chết hẳn (fullyDelisted, chưa từng lên CEX — sẽ không retry nữa)")

    at_risk_count = invalidate_at_risk_listing_prices(all_events, status_map)
    if at_risk_count:
        print(f"   [invalidate] Phát hiện {at_risk_count} event có nguy cơ dính bug ranh giới ngày VWAP "
              f"(listingTime không canh nửa đêm UTC) — đã xoá giá cũ để tính lại bằng logic đã sửa")

    tge_date_count = invalidate_wrong_tge_dates(all_events)
    if tge_date_count:
        print(f"   [invalidate-tge] Phát hiện {tge_date_count} event loại TGE bị ghi đè sai ngày "
              f"(trùng ngày với event Alpha-listing khác) — đã xoá giá cũ để tính lại đúng ngày TGE thật")

    multiround_count = invalidate_multi_round_listing_prices(all_events)
    if multiround_count:
        print(f"   [invalidate] Phát hiện {multiround_count} event là đợt airdrop SAU lần đầu (P2, P3...) "
              f"bị tính nhầm bằng listingTime của lần list đầu — đã xoá giá cũ để tính lại đúng ngày riêng")

    # [MỚI] Reset TOÀN BỘ 1 lần — dùng khi đổi Ý NGHĨA công thức (vd đổi
    # VWAP 24h -> VWAP 5 phút đầu, đổi peak từ "high" -> "close") khiến
    # TOÀN BỘ dữ liệu cũ không còn đúng ý nghĩa mới, dù không phải "bug".
    # Bật bằng biến môi trường FORCE_FULL_RESET=true CHO ĐÚNG 1 LẦN CHẠY,
    # rồi TẮT NGAY (xoá biến hoặc set về false) — nếu để quên bật, mỗi lần
    # chạy sau sẽ xoá sạch giá vừa tính lại, tính đi tính lại vô nghĩa.
    if os.getenv("FORCE_FULL_RESET", "false").lower() == "true":
        full_reset_count = 0
        for e in all_events:
            if e.get("listing_price") or e.get("spot_listing_price"):
                e["listing_price"] = None
                e["spot_listing_price"] = None
                e["ath_since_listing_price"] = None
                e["ath_since_listing_date"] = None
                e["spot_ath_price"] = None
                e["spot_ath_date"] = None
                full_reset_count += 1
        print(f"   [FORCE_FULL_RESET] Đã xoá giá của TOÀN BỘ {full_reset_count} event — sẽ tính lại hết theo "
              f"công thức mới (VWAP {CLAIM_WINDOW_MINUTES} phút đầu + đỉnh theo giá đóng cửa). "
              f"NHỚ TẮT biến FORCE_FULL_RESET sau lần chạy này!")

    reset_count = invalidate_manual_reset_prices(all_events)
    if reset_count:
        print(f"   [reset] Đã xoá giá cũ của {reset_count} event trong MANUAL_RESET_PRICES — sẽ tính lại ngay bên dưới")

    filled = enrich_events(all_events)
    print(f"\n✅ Backfilled {filled} new listing prices")

    filled_spot = enrich_spot_listing_prices(all_events)
    print(f"✅ Backfilled {filled_spot} giá lúc spot-listing (ngày lấy trực tiếp từ klines Binance)")

    # [SỬA — GIAI ĐOẠN 0 tái cấu trúc kiến trúc, ĐÃ SỬA LẠI LẦN 2]
    # Lần sửa trước tôi bỏ luôn cả history.json — SAI, vì vừa xác nhận
    # qua code route /api/alpha-events (Cloudflare Function): tab=history
    # đọc DUY NHẤT history.json (không liên quan gì all.json) — nếu
    # ngừng ghi, TOÀN BỘ dữ liệu giá VWAP/đỉnh sẽ biến mất khỏi tab
    # History trên frontend. Sửa lại đúng phạm vi:
    #   - upcoming.json / live.json: NGỪNG ghi — route /api/alpha-events
    #     xác nhận tab=active đọc live+upcoming+pending.json TRỰC TIẾP từ
    #     wa-listener (Telegram real-time listener), không qua all.json.
    #     Script này chạy mỗi 30 phút, trước đây re-derive 2 file này từ
    #     all.json (LUÔN RỖNG vì all.json không chứa event upcoming/live)
    #     — ghi đè mất dữ liệu THẬT của wa-listener. Đã xác nhận qua mọi
    #     log: "upcoming.json updated (0 events, 0KB)" — luôn luôn.
    #   - history.json + all.json: TIẾP TỤC ghi như cũ — đây đúng là nơi
    #     cần field giá đã enrich (listing_price, spot_listing_price,
    #     ath_*,...), và không có bằng chứng có hệ thống khác cùng ghi
    #     đè 2 file này (khác hẳn upcoming/live — có bằng chứng rõ ràng
    #     wa-listener ghi liên tục).
    air_number_changed = assign_air_numbers(all_events)
    if air_number_changed:
        print(f"   [air_number] Cập nhật số thứ tự airdrop (Air N) cho {air_number_changed} event ✓")

    history = [e for e in all_events if e.get("status") in ("ended", None) or e.get("status") not in ("upcoming", "live")]

    # [MỚI] Sort theo thời gian MỚI NHẤT lên đầu. Cần thiết vì giờ
    # wa-listener (storage.py) cũng append event realtime thẳng vào
    # all.json (xem _upsert_ended_to_all_events) để script này tự nhận
    # diện và enrich giá — nhưng nó append vào CUỐI mảng, không theo thứ
    # tự ngày. Nếu không sort ở đây, mỗi lần script này chạy lại (theo
    # lịch GitHub Actions) sẽ ghi đè mất thứ tự đã sort bởi wa-listener,
    # khiến event mới lại rớt xuống cuối danh sách History trên frontend.
    def _history_sort_key(ev):
        ts = ev.get("event_time") or ev.get("created_at") or ""
        return str(ts)
    history.sort(key=_history_sort_key, reverse=True)

    upload_json(r2, "alpha-events/all.json",     all_events)
    upload_json(r2, "alpha-events/history.json", history)

    print("🏁 DONE")


if __name__ == "__main__":
    main()
