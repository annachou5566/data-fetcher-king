"""
scripts/fetch_etf.py  v22
- Lấy TOÀN BỘ lịch sử từ Farside (tất cả ngày từ ngày ra mắt) — CHỈ CÒN dùng làm
  fallback CUỐI CÙNG cho các ticker chưa tìm được nguồn nào khác.
- Lưu vào R2: etf-flows.json (daily latest) + etf-farside-history.json (full history)
- self_computed = TỰ TÍNH Flow (Δholdings×price), lấy thẳng từ issuer, GỐC RỄ,
  không qua bên thứ 3 — ĐÃ XÁC NHẬN hoạt động qua log thật (18 ticker):
    IBIT/ETHA (iShares API) · ARKB (ARK CSV) · HODL/ETHV/VSOL/VBNB (VanEck qua
    Playwright+scroll) · BITB/ETHW/BSOL/BHYP (site riêng Bitwise, SSR tĩnh) ·
    TSOL/THYP (21shares.com, AUM thật) · EZBC/EZET/SOEZ (Franklin Templeton,
    Playwright+scroll+role-gate) · BTCO/QETH (Invesco, Playwright+scroll+role-gate)
- BRRR: MỚI THÊM 27/07/2026, CHƯA CHẠY THẬT trên CI (19th self_computed nhưng
  chưa được xác nhận qua log tự động — chỉ mới xác nhận qua nội dung user paste
  từ trình duyệt thật). Valkyrie đã đổi thương hiệu thành "CoinShares Bitcoin
  ETF" tại coinshares.com/etf/brrr/ (domain HOÀN TOÀN khác valkyrieinvesti.com).
  robots.txt của coinshares.com CHO PHÉP crawl path này (khác valkyrieinvesti.com
  chặn toàn bộ) — nhưng site dùng DataDome bot-management nên vẫn có rủi ro bị
  chặn kỹ thuật dù robots.txt cho phép. Xem fetch_coinshares_holdings() để biết
  chi tiết 3 tầng fallback. Cần theo dõi log lần chạy CI đầu tiên.
- Fidelity (FBTC/FETH/FSOL): ĐÃ TẮT — trang digital.fidelity.com CÓ đủ dữ liệu
  thật ("Total {coin} in fund"), xác nhận qua ảnh chụp user từ điện thoại thật
  (IP thường, vào bình thường). NHƯNG từ GitHub Actions (IP datacenter) LUÔN bị
  chặn ngay ở tầng kết nối (net::ERR_HTTP2_PROTOCOL_ERROR, thử --disable-http2
  vẫn timeout) — WAF chặn theo dải IP datacenter/cloud, cùng loại vấn đề như
  Grayscale (Vercel Security Checkpoint). Quyết định KHÔNG lách chặn IP, áp
  dụng nhất quán nguyên tắc đã dùng cho Grayscale.
- On-chain (theo dõi ví custodian): KHÔNG khả thi — custodian (Coinbase Custody,
  Fidelity Digital Assets...) chủ động KHÔNG công bố địa chỉ ví chính thức vì lý
  do bảo mật. Mọi "on-chain tracker" (Arkham, TheBlock) đều tự suy đoán, không
  phải quỹ xác nhận.
- SEC EDGAR: các quỹ này là Delaware Trust MIỄN TRỪ Investment Company Act 1940
  nên không nộp N-PORT (holdings định kỳ chuẩn). Chỉ có 10-K/10-Q (Schedule of
  Investments) theo QUÝ/NĂM — không đủ tần suất cho Flow hàng ngày.
- CÒN LẠI chưa tìm được nguồn daily công khai hoặc bị chặn (dùng Farside):
  FBTC/FETH/FSOL (Fidelity — WAF chặn IP datacenter), BTCW (WisdomTree —
  không công bố), BRRR (Valkyrie — có trang holdings nhưng robots.txt CHẶN
  crawl, tôn trọng không lách), MSBT (Morgan Stanley — etfdb.com xác nhận
  "Holdings data not available"), GBTC/BTC/ETHE/HYPG/GSOL (Grayscale — Vercel
  Security Checkpoint bot-detection, quyết định không lách).
- YÊU CẦU CÀI THÊM cho VanEck/Franklin/Invesco: pip install playwright &&
  playwright install --with-deps chromium (đã có trong requirements.txt +
  workflow .yml, chỉ chạy khi RUN_MODE=full).
- AUM: iShares live → ARK/Bitwise/VanEck/Franklin/Invesco (holdings thật) →
  21Shares (AUM thật) → static on-chain → Nasdaq "Net Assets" (SOL/HYP/BNB còn
  lại) → cache cũ.
"""

import json, os, re, time, csv, io
from datetime import datetime, timezone
from urllib.parse import quote

import boto3, cloudscraper, requests
from botocore.config import Config
try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

RUN_MODE             = os.getenv("RUN_MODE", "full")
R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL      = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME       = os.getenv("R2_BUCKET_NAME")
ETHA_PRODUCT_ID_ENV  = os.getenv("ETHA_PRODUCT_ID", "")

FAKE_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
           "AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36")

ISHARES_IDS = {
    "IBIT": "333011",
    "ETHA": ETHA_PRODUCT_ID_ENV or "337614",
}

# BTC holdings per ETF (on-chain snapshot từ etf_holdings.json)
# Dùng để tính AUM khi không có live data
# AUM = holdings × BTC_price hiện tại → tự động update theo giá
STATIC_BTC_HOLDINGS = {
    "FBTC": 204870.57,   # Fidelity
    "GBTC": 203601.41,   # Grayscale
    "ARKB": 157218.40,   # ARK/21Shares
    "BITB": 141486.62,   # Bitwise
    "HODL":  22924.98,   # VanEck
    "EZBC":  17942.63,   # Franklin
    "BTCW":  15745.38,   # WisdomTree
    "BTCO":  14510.33,   # Invesco
    "BRRR":   6939.32,   # Valkyrie
}

# Farside URLs — dùng full history URL
FARSIDE_URLS = {
    "BTC": "https://farside.co.uk/bitcoin-etf-flow-all-data/",
    "ETH": "https://farside.co.uk/ethereum-etf-flow-all-data/",
    "SOL": "https://farside.co.uk/sol/",
    "HYP": "https://farside.co.uk/hyp/",
}
FARSIDE_KEYWORDS = {
    "BTC": ["IBIT","FBTC"],
    "ETH": ["ETHA","FETH"],
    "SOL": ["BSOL","VSOL","FSOL"],
    "HYP": ["HYP","GHYP","FHYP"],
}

ETF_REGISTRY = [
    # self_computed=True: Flow = Δholdings × price, tính TỰ, KHÔNG cần Farside cho
    # ticker đó. Chỉ đánh dấu cho các nguồn đã XÁC NHẬN THẬT (không đoán mò):
    #   - "ishares": đã tích hợp sẵn API chính thức của iShares (IBIT, ETHA)
    #   - "ark_csv": file CSV holdings công khai của ARK, pattern URL xác nhận
    #     qua 2 nguồn độc lập (assets.ark-funds.com/fund-documents/funds-etf-csv/
    #     ARK_{TÊN_QUỸ}_ETF_{TICKER}_HOLDINGS.csv)
    # Các ticker CHƯA có self_computed vẫn dùng Farside như cũ — đây là migration
    # TỪNG BƯỚC, không phải xoá Farside 1 lần, để không bao giờ bị mất dữ liệu quỹ
    # nào giữa chừng nếu 1 nguồn tự tính bị lỗi/thay đổi định dạng.
    {"ticker":"IBIT","name":"iShares Bitcoin Trust ETF","issuer":"BlackRock","underlying":"BTC","fee":0.25,"src":"ishares","self_computed":True},
    {"ticker":"FBTC","name":"Fidelity Wise Origin Bitcoin Fund","issuer":"Fidelity","underlying":"BTC","fee":0.25,"src":"nasdaq","self_computed":True,"fidelity_symbol":"FBTC"},
    {"ticker":"GBTC","name":"Grayscale Bitcoin Trust ETF","issuer":"Grayscale","underlying":"BTC","fee":1.50,"src":"nasdaq","self_computed":True,"grayscale_url":"https://etfs.grayscale.com/gbtc"},
    {"ticker":"ARKB","name":"ARK 21Shares Bitcoin ETF","issuer":"ARK/21Shares","underlying":"BTC","fee":0.21,"src":"nasdaq","self_computed":True,"ark_fund_name":"21SHARES_BITCOIN"},
    {"ticker":"BITB","name":"Bitwise Bitcoin ETF","issuer":"Bitwise","underlying":"BTC","fee":0.20,"src":"nasdaq","self_computed":True,"bitwise_domain":"bitbetf.com"},
    {"ticker":"HODL","name":"VanEck Bitcoin ETF","issuer":"VanEck","underlying":"BTC","fee":0.20,"src":"nasdaq","self_computed":True,"vaneck_slug":"bitcoin-etf-hodl","vaneck_asset_word":"Bitcoin"},
    {"ticker":"EZBC","name":"Franklin Bitcoin ETF","issuer":"Franklin","underlying":"BTC","fee":0.19,"src":"nasdaq","self_computed":True,"franklin_url":"https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/39639/SINGLCLASS/franklin-bitcoin-etf/EZBC"},
    # BRRR: Valkyrie đã ĐỔI THƯƠNG HIỆU hoàn toàn thành "CoinShares Bitcoin ETF"
    # (CoinShares mua lại mảng bitcoin ETF của Valkyrie) — domain mới hoàn toàn
    # khác: coinshares.com/etf/brrr/ (không còn valkyrieinvesti.com). XÁC NHẬN
    # qua robots.txt thật của coinshares.com (07/2026): các Disallow chỉ nhắm
    # path theo locale (/at-en/*, /be-en/*...) và /*/d/*, /lookups/* — KHÔNG
    # disallow /etf/brrr/, khác hẳn valkyrieinvesti.com (Disallow: / toàn bộ).
    # Trang có sẵn bảng Holdings cập nhật hàng ngày: "BITCOIN XBTUSD <shares>
    # <market value>" + "As of date: <ngày>" + AUM chính xác tới cent.
    {"ticker":"BRRR","name":"CoinShares Bitcoin ETF","issuer":"CoinShares","underlying":"BTC","fee":0.25,"src":"nasdaq","self_computed":True,"coinshares_url":"https://coinshares.com/etf/brrr/"},
    {"ticker":"BTCO","name":"Invesco Galaxy Bitcoin ETF","issuer":"Invesco","underlying":"BTC","fee":0.25,"src":"nasdaq","self_computed":True,"invesco_url":"https://www.invesco.com/us/financial-products/etfs/holdings?audienceType=Investor&ticker=BTCO"},
    {"ticker":"BTCW","name":"WisdomTree Bitcoin Fund","issuer":"WisdomTree","underlying":"BTC","fee":0.25,"src":"nasdaq"},
    {"ticker":"MSBT","name":"Morgan Stanley Bitcoin Trust","issuer":"Morgan Stanley","underlying":"BTC","fee":0.14,"src":"nasdaq"},
    {"ticker":"BTC","name":"Grayscale Bitcoin Mini Trust ETF","issuer":"Grayscale","underlying":"BTC","fee":0.15,"src":"nasdaq","self_computed":True,"grayscale_url":"https://etfs.grayscale.com/btc"},
    {"ticker":"ETHA","name":"iShares Ethereum Trust ETF","issuer":"BlackRock","underlying":"ETH","fee":0.25,"src":"ishares","self_computed":True},
    {"ticker":"FETH","name":"Fidelity Ethereum Fund","issuer":"Fidelity","underlying":"ETH","fee":0.25,"src":"nasdaq","self_computed":True,"fidelity_symbol":"FETH"},
    {"ticker":"ETHE","name":"Grayscale Ethereum Trust ETF","issuer":"Grayscale","underlying":"ETH","fee":2.50,"src":"nasdaq","self_computed":True,"grayscale_url":"https://etfs.grayscale.com/ethe"},
    {"ticker":"ETHW","name":"Bitwise Ethereum ETF","issuer":"Bitwise","underlying":"ETH","fee":0.20,"src":"nasdaq","self_computed":True,"bitwise_domain":"ethwetf.com"},
    {"ticker":"ETHV","name":"VanEck Ethereum ETF","issuer":"VanEck","underlying":"ETH","fee":0.20,"src":"nasdaq","self_computed":True,"vaneck_slug":"ethereum-etf-ethv","vaneck_asset_word":"Ether"},
    # CETH: XÁC NHẬN đã ngừng hoạt động (etfdb.com: "This ETF is no longer active",
    # issuer thật là Amun Holdings — tên cũ của 21Shares trước khi đổi thương hiệu).
    # Đây là lý do Nasdaq luôn trả $None cho CETH — không phải lỗi code, mà do quỹ
    # đã chết. Giữ lại trong registry để không phá vỡ dữ liệu lịch sử cũ, nhưng
    # đánh dấu inactive để loại khỏi mọi tính toán/fetch (tránh lãng phí request
    # và tránh AUM/Flow=0 giả tạo lẫn vào tổng ETH).
    {"ticker":"CETH","name":"21Shares Core Ethereum ETF","issuer":"21Shares","underlying":"ETH","fee":0.21,"src":"nasdaq","inactive":True},
    {"ticker":"EZET","name":"Franklin Ethereum ETF","issuer":"Franklin","underlying":"ETH","fee":0.19,"src":"nasdaq","self_computed":True,"franklin_url":"https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/40521/SINGLCLASS/franklin-ethereum-etf/EZET"},
    {"ticker":"QETH","name":"Invesco Galaxy Ethereum ETF","issuer":"Invesco","underlying":"ETH","fee":0.25,"src":"nasdaq","self_computed":True,"invesco_url":"https://www.invesco.com/us/financial-products/etfs/holdings?audienceType=Investor&ticker=QETH"},
    # Solana ETFs — xác nhận issuer/fee qua search 07/2026
    {"ticker":"BSOL","name":"Bitwise Solana Staking ETF","issuer":"Bitwise","underlying":"SOL","fee":0.20,"src":"nasdaq","self_computed":True,"bitwise_domain":"bsoletf.com"},
    {"ticker":"VSOL","name":"VanEck Solana ETF","issuer":"VanEck","underlying":"SOL","fee":0.30,"src":"nasdaq","self_computed":True,"vaneck_slug":"solana-etf-vsol","vaneck_asset_word":"Solana"},
    {"ticker":"FSOL","name":"Fidelity Solana Fund","issuer":"Fidelity","underlying":"SOL","fee":0.25,"src":"nasdaq","self_computed":True,"fidelity_symbol":"FSOL"},
    {"ticker":"TSOL","name":"21Shares Solana ETF","issuer":"21Shares","underlying":"SOL","fee":0.21,"src":"nasdaq","self_computed":True,"shares21_slug":"tsol"},
    {"ticker":"SOEZ","name":"Franklin Solana ETF","issuer":"Franklin","underlying":"SOL","fee":0.19,"src":"nasdaq","self_computed":True,"franklin_url":"https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/47315/SINGLCLASS/franklin-solana-etf/SOEZ"},
    # GSOL: KHÁC cấu trúc — xác nhận qua Grayscale tweet chính thức "GSOL is not
    # an ETP and is quoted on OTC Markets Group" (không niêm yết sàn như GBTC/
    # ETHE/HYPG, URL cũng ở domain khác: grayscale.com/funds/ thay vì
    # etfs.grayscale.com/). Loại OTC-trust này thường KHÔNG có trang "Total X in
    # Trust" công khai như các ETP thật — chưa tìm được nguồn gốc rễ, tạm giữ
    # Farside cho ticker này.
    {"ticker":"GSOL","name":"Grayscale Solana Trust ETF","issuer":"Grayscale","underlying":"SOL","fee":0.19,"src":"nasdaq"},
    # Hyperliquid ETFs — xác nhận issuer/fee qua search 07/2026
    {"ticker":"BHYP","name":"Bitwise Hyperliquid ETF","issuer":"Bitwise","underlying":"HYP","fee":0.34,"src":"nasdaq","self_computed":True,"bitwise_domain":"bhypetf.com"},
    {"ticker":"THYP","name":"21Shares Hyperliquid ETF","issuer":"21Shares","underlying":"HYP","fee":0.30,"src":"nasdaq","self_computed":True,"shares21_slug":"thyp"},
    {"ticker":"HYPG","name":"Grayscale Hyperliquid Staking ETF","issuer":"Grayscale","underlying":"HYP","fee":0.29,"src":"nasdaq","self_computed":True,"grayscale_url":"https://etfs.grayscale.com/hypg"},
    # BNB ETF — mới thêm, hiện chỉ có VanEck (VBNB). self_computed đã VERIFY THẬT
    # bằng cách fetch trực tiếp trang https://www.vaneck.com/us/en/investments/
    # bnb-etf-vbnb/ — có đúng bảng "ETF Statistics" với dòng "BNB in Fund:
    # 3,886.175", fee 0.39% xác nhận qua nhiều nguồn. Fund còn rất nhỏ (mới ra
    # mắt 07/05/2026, AUM ~$2.27M) nhưng dữ liệu hợp lệ để track.
    {"ticker":"VBNB","name":"VanEck BNB ETF","issuer":"VanEck","underlying":"BNB","fee":0.39,"src":"nasdaq","self_computed":True,"vaneck_slug":"bnb-etf-vbnb","vaneck_asset_word":"BNB"},
]
ETF_TICKERS = [e["ticker"] for e in ETF_REGISTRY if not e.get("inactive")]

def fmt_aum(aum):
    """Định dạng AUM: dùng $B nếu ≥1B, còn lại dùng $M — tránh hiện '$0.00B' gây
    hiểu lầm mất dữ liệu cho các quỹ nhỏ (vd VBNB ~$2.2M, TSOL ~$3.1M)."""
    aum = aum or 0
    return f"${aum/1e9:.2f}B" if aum >= 1e9 else f"${aum/1e6:.2f}M"


def parse_num(v):
    if v is None or str(v).strip() in ("","N/A","--","null","None"): return None
    if isinstance(v,(int,float)): return float(v)
    s = re.sub(r"[$,%\s]","",str(v))
    try: return float(s)
    except: return None

def parse_money(v):
    """'$123.4M' -> 123400000.0, '(1.2B)' -> -1200000000.0, 'N/A' -> None."""
    if v is None: return None
    s = str(v).strip()
    if not s or s in ("N/A","--","null","None"): return None
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace("$","").replace(",","").strip()
    mult = 1.0
    if s and s[-1].upper() in ("K","M","B","T"):
        mult = {"K":1e3,"M":1e6,"B":1e9,"T":1e12}[s[-1].upper()]
        s = s[:-1]
    try:
        val = float(s) * mult
        return -val if neg else val
    except: return None

def get_session():
    s = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"windows","desktop":True})
    s.headers.update({"User-Agent":FAKE_UA,"Accept":"application/json,*/*","Accept-Language":"en-US,en;q=0.9"})
    return s

def get_r2():
    return boto3.client("s3",endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"))

def r2_get_json(r2,key):
    try:
        resp=r2.get_object(Bucket=R2_BUCKET_NAME,Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except: return None

def r2_put_json(r2,key,data,cc="max-age=120"):
    body=json.dumps(data,ensure_ascii=False,separators=(",",":")).encode("utf-8")
    r2.put_object(Bucket=R2_BUCKET_NAME,Key=key,Body=body,ContentType="application/json",CacheControl=cc)

def load_crypto_prices():
    prices={}
    try:
        r=requests.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,hyperliquid,binancecoin&vs_currencies=usd",
            headers={"User-Agent":FAKE_UA},timeout=10)
        if r.status_code==200:
            d=r.json()
            if "bitcoin"     in d: prices["BTC"]=float(d["bitcoin"]["usd"])
            if "ethereum"    in d: prices["ETH"]=float(d["ethereum"]["usd"])
            if "solana"      in d: prices["SOL"]=float(d["solana"]["usd"])
            if "hyperliquid" in d: prices["HYP"]=float(d["hyperliquid"]["usd"])
            if "binancecoin" in d: prices["BNB"]=float(d["binancecoin"]["usd"])
    except Exception as e: print(f"  [Crypto] {e}")
    print(f"  [Crypto] BTC=${prices.get('BTC')}  ETH=${prices.get('ETH')}  SOL=${prices.get('SOL')}  HYP=${prices.get('HYP')}  BNB=${prices.get('BNB')}")
    return prices

def parse_aum_from_label(label, val):
    """parse_money() ra số thô, nhưng Nasdaq ghi rõ trong TÊN label đơn vị thật:
    'Assets Under Management (,000)' nghĩa là giá trị đang tính bằng NGHÌN USD
    (đã xác nhận qua log thật 25/07 — field không phải 'Net Assets' như đoán ban
    đầu). Phải nhân 1000 mới ra đúng USD, nếu không AUM sẽ hụt 1000 lần."""
    parsed = parse_money(val)
    if parsed is None: return None
    if label and "(,000)" in label.replace(" ", ""):
        parsed *= 1000
    return parsed

def fetch_nasdaq_summary_net_assets(session, ticker):
    """Lấy AUM trực tiếp từ Nasdaq summary API. Dùng làm fallback AUM cho các ETF
    chưa có nguồn holdings riêng (SOL/HYP/BNB hiện chỉ có Farside cho Flow, KHÔNG
    có iShares/ARK/VanEck-verified nào cho holdings) — khác với BTC (có static
    on-chain snapshot) và IBIT/ETHA (có iShares live).

    XÁC NHẬN THẬT qua log 25/07: field không tên 'Net Assets' như đoán ban đầu,
    mà là 'Assets Under Management (,000)' — đơn vị NGHÌN USD, không phải USD
    thô hay có suffix K/M/B như info fields khác."""
    url=f"https://api.nasdaq.com/api/quote/{ticker}/summary?assetclass=etf&limit=25"
    try:
        r=session.get(url,headers={"Referer":f"https://www.nasdaq.com/market-activity/funds-and-etfs/{ticker.lower()}",
                     "Accept":"application/json,*/*"},timeout=12)
        if r.status_code!=200:
            print(f"    summary {ticker}: HTTP {r.status_code}")
            return None
        data=r.json().get("data") or {}
        summary=data.get("summaryData") or {}
        if not summary:
            print(f"    summary {ticker}: summaryData rỗng | top-level keys của data: {list(data.keys())[:15]}")
            return None
        label,val=find_by_label(summary,"assets under management")
        if not label:
            label,val=find_by_label(summary,"net assets")
        if not label:
            print(f"    summary {ticker}: không có label AUM | các label có sẵn: {list({str((it or {}).get('label','')) for it in summary.values()})}")
            return None
        parsed=parse_aum_from_label(label,val)
        if parsed is None:
            print(f"    summary {ticker}: thấy label '{label}'='{val}' nhưng parse ra None")
        return parsed
    except Exception as e:
        print(f"    summary ✗ {ticker}: {e}")
    return None

def find_by_label(d, keyword):
    """Tìm (label, value) theo label chứa keyword (không phân biệt hoa/thường)
    trong dict dạng {key: {"label":..., "value":...}}. Dùng chung cho info.keyStats
    và summary.summaryData vì Nasdaq trả 2 endpoint theo cùng 1 shape này."""
    if not isinstance(d, dict): return None, None
    for item in d.values():
        if not isinstance(item, dict): continue
        label = str(item.get("label",""))
        if keyword.lower() in label.lower():
            return label, item.get("value")
    return None, None

def fetch_nasdaq_all(session):
    results={}
    underlying_map={e["ticker"]:e["underlying"] for e in ETF_REGISTRY}
    for ticker in ETF_TICKERS:
        try:
            r=session.get(f"https://api.nasdaq.com/api/quote/{ticker}/info?assetclass=etf",
                headers={"Referer":f"https://www.nasdaq.com/market-activity/funds-and-etfs/{ticker.lower()}"},timeout=12)
            if r.status_code!=200: continue
            d=r.json().get("data") or {}
            p=d.get("primaryData") or {}
            price=parse_num(p.get("lastSalePrice"))
            results[ticker]={"price":price,"change":parse_num(p.get("netChange")),
                "change_pct":parse_num((p.get("percentageChange") or "").replace("%","")),
                "volume":parse_num((p.get("volume") or "").replace(",",""))}
            print(f"  {ticker}: ${price}")
            # SOL/HYP/BNB: không có nguồn holdings on-chain nào → cần AUM để không
            # bị $0. Thử MIỄN PHÍ trước: keyStats trong response info đã fetch sẵn
            # (không tốn request thêm) — field thật tên "Assets Under Management
            # (,000)" (xác nhận qua log 25/07, đơn vị NGHÌN USD).
            if underlying_map.get(ticker) in ("SOL","HYP","BNB"):
                keystats=d.get("keyStats") or {}
                label,val=find_by_label(keystats,"assets under management")
                net_assets=parse_aum_from_label(label,val) if label else None
                if net_assets:
                    results[ticker]["net_assets"]=net_assets
                    print(f"    ↳ Net Assets (keyStats.'{label}'): ${net_assets/1e6:.2f}M")
                else:
                    if keystats:
                        print(f"    (keyStats {ticker}, không có 'assets under management') labels: {[str((it or {}).get('label','')) for it in keystats.values()][:15]}")
                    net_assets=fetch_nasdaq_summary_net_assets(session,ticker)
                    if net_assets:
                        results[ticker]["net_assets"]=net_assets
                        print(f"    ↳ Net Assets (summary API): ${net_assets/1e6:.2f}M")
        except Exception as e: print(f"  ✗ {ticker}: {e}")
        time.sleep(0.3)
    return results

# ── FARSIDE ───────────────────────────────────────────────────────
def fetch_farside_html(url):
    """Direct → AllOrigins fallback"""
    try:
        s=cloudscraper.create_scraper()
        r=s.get(url,headers={"User-Agent":FAKE_UA},timeout=20)
        if r.status_code==200 and len(r.text)>3000:
            print(f"    Direct OK ({len(r.text)} chars)")
            return r.text
    except Exception as e: print(f"    Direct: {e}")
    try:
        proxy=f"https://api.allorigins.win/get?url={quote(url)}"
        r=requests.get(proxy,timeout=25)
        if r.status_code==200:
            html=r.json().get("contents","")
            if html and len(html)>3000:
                print(f"    AllOrigins OK ({len(html)} chars)")
                return html
    except Exception as e: print(f"    AllOrigins: {e}")
    return None

def parse_val(s):
    """Parse Farside value: '(12.5)' → -12.5, '0.0' → 0, '-' → 0"""
    s=str(s).replace(",","").strip()
    if not s or s=="-" or s=="": return 0.0
    if s.startswith("(") and s.endswith(")"):
        try: return -abs(float(s[1:-1]))
        except: return 0.0
    try: return float(s)
    except: return 0.0

def parse_farside_table_full(html, asset):
    """
    Parse TOÀN BỘ bảng Farside.
    Return:
      headers: [ticker1, ticker2, ...]
      rows: [{"date": "26 Jun 2026", "IBIT": -444.5, "Total": -444.5, ...}, ...]
    """
    if not html: return None, []

    keywords = FARSIDE_KEYWORDS.get(asset, [])

    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")

        # Tìm bảng chứa keyword
        target_table = None
        for table in soup.find_all("table"):
            text = table.get_text().upper()
            if any(k in text for k in keywords):
                target_table = table
                break
        if not target_table:
            print(f"    ✗ No table for {asset}")
            return None, []

        all_rows = target_table.find_all("tr")

        # Tìm header row (chứa ticker names)
        headers = []
        header_idx = 0
        for i, row in enumerate(all_rows):
            cells = [c.get_text().strip() for c in row.find_all(["th","td"])]
            cells_upper = " ".join(cells).upper()
            if any(k in cells_upper for k in keywords):
                headers = [c.strip() for c in cells]
                header_idx = i
                break

        if not headers:
            print(f"    ✗ No headers for {asset}")
            return None, []

        # Lọc bỏ empty headers ở đầu, giữ ticker names
        # Headers format: ['', 'IBIT', 'FBTC', ..., 'Total']
        clean_headers = headers  # giữ nguyên để dùng index

        print(f"    Headers ({len(headers)}): {headers[:12]}")

        # Parse TẤT CẢ data rows
        rows = []
        for row in all_rows[header_idx+1:]:
            cells = [c.get_text().strip() for c in row.find_all("td")]
            if not cells: continue

            first = cells[0]
            # Bỏ qua dòng không phải ngày
            if not re.match(r"^\d{1,2}\s+[A-Za-z]{3}", first):
                continue

            row_obj = {"date": first}
            for i, hdr in enumerate(clean_headers):
                if i == 0 or i >= len(cells): continue
                ticker = hdr.strip().upper()
                if ticker in ("FEE", "SEED"):
                    continue
                # QUAN TRỌNG: cột Tổng (cuối bảng) ở một số trang (SOL, HYP...) Farside
                # không đặt tên (header text rỗng) — TRƯỚC ĐÂY code coi "not hdr" là cột rác
                # và bỏ qua luôn, khiến SOL/HYP mất hẳn dữ liệu tổng. Giờ gán key "TOTAL"
                # cho mọi cột có header rỗng (trừ cột date ở index 0 đã skip riêng).
                if not ticker:
                    ticker = "TOTAL"
                row_obj[ticker] = parse_val(cells[i]) if i < len(cells) else 0.0

            if len(row_obj) > 1:  # có ít nhất 1 cột data
                rows.append(row_obj)

        # Sort theo NGÀY THỰC TẾ (không tin thứ tự HTML — trước đây reverse() mù quáng
        # đã làm rows[-1] trở thành dòng CŨ NHẤT thay vì mới nhất, khiến "flow hôm nay"
        # bị ghi nhầm thành flow của ngày ETF ra mắt).
        def _parse_date(d):
            try: return datetime.strptime(d, "%d %b %Y")
            except: return datetime.min
        rows.sort(key=lambda r: _parse_date(r["date"]))  # cũ → mới, đảm bảo rows[-1] luôn là mới nhất

        print(f"    ✓ {len(rows)} historical rows (first: {rows[0]['date'] if rows else 'N/A'} → last: {rows[-1]['date'] if rows else 'N/A'})")
        return headers, rows
    else:
        print(f"    bs4 not installed")
        return None, []

def fetch_farside_all(session):
    """
    Lấy toàn bộ lịch sử từ Farside cho tất cả asset.
    Return:
      daily_latest: { IBIT: flow_usd_today, ... }
      full_history: { BTC: [{date, IBIT, FBTC, ...}], ETH: [...], ... }
    """
    daily_latest = {}
    full_history = {}

    for asset, url in FARSIDE_URLS.items():
        print(f"\n  Farside {asset}: {url}")
        html = fetch_farside_html(url)
        if not html:
            print(f"    ✗ Failed")
            continue

        headers, rows = parse_farside_table_full(html, asset)
        if not rows:
            print(f"    ✗ No rows")
            continue

        # Lưu full history
        full_history[asset] = {
            "headers": headers,
            "rows": rows,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        # Lấy dòng mới nhất CÓ DỮ LIỆU THỰC (bỏ qua dòng cuối nếu Farside hiện "–" hết,
        # vd ngày hôm nay chưa đóng cửa → parse ra {} rỗng → lấy ngày hôm trước thay thế)
        latest = None
        for row in reversed(rows):
            has_data = any(v != 0 for k, v in row.items() if k not in ("date", "TOTAL"))
            if has_data:
                latest = row
                break
        if not latest:
            latest = rows[-1]  # fallback: lấy dòng cuối dù rỗng

        # BUG FIX: daily_latest tích lũy KHÔNG RESET giữa các asset → ticker của asset
        # trước (vd BSOL) bị "thấm" vào latest dict của asset sau (HYP).
        # Sửa: mỗi asset chỉ đóng góp đúng các ticker của nó vào daily_latest.
        # BUG FIX (nghiêm trọng): trước đây "if val != 0" coi flow=$0.0 là "thiếu dữ liệu"
        # và bỏ qua ticker đó khỏi daily_latest. Ở run(), khi daily_flows.get(t) trả về
        # None, code fallback dùng flow của LẦN CHẠY TRƯỚC (prev["flow"], cache trên R2).
        # Hậu quả: mỗi khi 1 quỹ nhỏ (BITB/BRRR/BTCW...) có ngày flow=0 THẬT (không giao
        # dịch), giá trị cache CŨ (có thể là rác từ tận ngày ETF ra mắt, do bug reverse()
        # trước đây) bị "hồi sinh" và đóng băng mãi — trong khi các quỹ lớn (IBIT, FBTC)
        # hầu như ngày nào cũng flow≠0 nên không bao giờ dính, luôn cập nhật đúng.
        # → Kết quả: bảng hiển thị 2 nhóm ticker lệch nhau nhiều tháng/năm dữ liệu.
        # Sửa: flow=0 LÀ dữ liệu thật, phải ghi vào daily_latest luôn, không bỏ qua.
        asset_latest = {}
        print(f"    Latest: {latest.get('date')} → ", end="")
        for ticker, val in latest.items():
            if ticker in ("date", "TOTAL"): continue
            asset_latest[ticker] = val * 1_000_000  # $M → $ (bao gồm cả giá trị 0)
        # Merge vào daily_latest, nhưng KHÔNG overwrite ticker đã có từ asset khác
        for k, v in asset_latest.items():
            if k not in daily_latest:
                daily_latest[k] = v
        print({k: v/1e6 for k,v in asset_latest.items()})

        # Convert tất cả flows sang USD (nhân 1M)
        for row in full_history[asset]["rows"]:
            for k in list(row.keys()):
                if k != "date":
                    row[k] = row[k] * 1_000_000

    return daily_latest, full_history

# ── iShares ───────────────────────────────────────────────────────
VARNISH="https://www.ishares.com/varnish-api/blk-one01-product-data/product-data/api/v2/get-product-data"

def _url(pid,excl,incl,as_of=None):
    p=(f"component=holdings.all&portfolioId={pid}&appSubType=ISHARES&appType=PRODUCT_PAGE"
       f"&locale=en_US&targetSite=us-ishares&userType=individual"
       f"&excludeContent={'true' if excl else 'false'}"
       f"&includeConfig={'true' if incl else 'false'}")
    if as_of: p+=f"&asOfDate={as_of}"
    return f"{VARNISH}?{p}"

def fetch_ishares(session,ticker,product_id,crypto_price=None):
    hdrs={"Referer":f"https://www.ishares.com/us/products/{product_id}/",
           "Accept":"application/json,*/*","User-Agent":FAKE_UA}
    latest_date=None
    try:
        r=session.get(_url(product_id,True,True),headers=hdrs,timeout=15)
        if r.status_code!=200: return None
        d=r.json()
        if ticker=="ETHA" and "ethereum" not in d.get("fundName","").lower(): return None
        comp=(d.get("componentsByNameMap") or {}).get("holdings",{})
        cont=(comp.get("containersByNameMap") or {}).get("all",{})
        dmap=cont.get("dataPointsByNameMap",{})
        dates=dmap.get("dateList",{}).get("value") or []
        if dates: latest_date=str(dates[0])
    except Exception as e: print(f"    config: {e}"); return None
    try:
        r=session.get(_url(product_id,False,False,as_of=latest_date),headers=hdrs,timeout=20)
        if r.status_code!=200: return None
        d=r.json()
        comp=(d.get("componentsByNameMap") or {}).get("holdings",{})
        cont=(comp.get("containersByNameMap") or {}).get("all",{})
        dmap=cont.get("dataPointsByNameMap",{})
        mv=dmap.get("marketValue",{}).get("value",[])
        aum=max((v for v in mv if isinstance(v,(int,float)) and v>0),default=None)
        holdings=None
        for key in ["unitsHeld","sharesHeld","quantity"]:
            arr=dmap.get(key,{}).get("value",[])
            if arr:
                h=parse_num(arr[0] if isinstance(arr,list) else arr)
                if h and 100<h<1_000_000_000: holdings=h; break
        if not holdings and aum and crypto_price and crypto_price>0:
            holdings=aum/crypto_price
        ao=dmap.get("asOfDate",{}).get("value")
        if aum or holdings:
            return {"aum":aum,"holdings":holdings,"nav_date":str(ao) if ao else latest_date}
        return None
    except Exception as e: print(f"    data: {e}"); return None

def fetch_ark_holdings(session, fund_name, ticker):
    """Fetch holdings từ CSV public của ARK. Trả về (holdings_coin_qty, as_of_date) hoặc None.
    URL pattern xác nhận qua search: assets.ark-funds.com/fund-documents/funds-etf-csv/
    ARK_{TÊN_QUỸ}_ETF_{TICKER}_HOLDINGS.csv — file này ARK cập nhật public mỗi ngày
    giao dịch, không cần key/auth gì cả.
    """
    url = f"https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_{fund_name}_ETF_{ticker}_HOLDINGS.csv"
    try:
        r = session.get(url, headers={"User-Agent": FAKE_UA}, timeout=15)
        if r.status_code != 200:
            print(f"    ARK CSV HTTP {r.status_code}: {url}")
            return None
        text = r.content.decode("utf-8-sig", errors="ignore")  # ARK CSV có BOM
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            return None
        # Quỹ Bitcoin/Ethereum trust của ARK chỉ nắm 1 tài sản duy nhất → chỉ có
        # đúng 1 dòng holding thật (bỏ qua dòng "cash"/tổng nếu CSV có thêm).
        # Thử nhiều tên cột khác nhau vì ARK có thể đổi format theo thời gian.
        as_of = None
        for row in rows:
            name = (row.get("company") or row.get("Company") or row.get("fund") or "").lower()
            if "cash" in name or "total" in name:
                continue
            for key in ["shares", "Shares", "shares_held", "Shares Held"]:
                if key in row and row[key]:
                    qty = parse_num(row[key])
                    if qty and qty > 0:
                        as_of = row.get("date") or row.get("Date")
                        return (qty, as_of)
        return None
    except Exception as e:
        print(f"    ARK CSV error ({ticker}): {e}")
        return None


def fetch_21shares_aum(session, slug, ticker):
    """Lấy AUM + NAV trực tiếp từ trang sản phẩm chính thức 21shares.com
    (https://www.21shares.com/en-us/products-us/{slug}).

    XÁC NHẬN THẬT qua fetch trực tiếp 07/2026: trang này (nền Webflow) có 2 loại
    nội dung khác nhau:
      - AUM, NAV, Management fee, Daily volume: SỐ THẬT, không phải placeholder
        (verify chéo: AUM/NAV ≈ Shares Outstanding hiển thị cùng trang, khớp nhau
        → không phải số giả lập trong template).
      - Bảng "Holdings" chi tiết (coin quantity, weight): CHỈ hiện placeholder
        ("NAME AAAA 00,000.00"...) vì phần này load qua JS client-side, không lấy
        được bằng fetch tĩnh (giống VanEck/Grayscale).
    → Dùng được AUM (đơn vị USD thật), KHÔNG có coin quantity chính xác. Suy ra
    holdings ước lượng = AUM / giá coin hiện tại (chấp nhận sai số nhỏ do
    premium/discount, tương tự cách xử lý Nasdaq Net Assets fallback trước đó —
    nhưng đây là SOURCE GỐC (issuer), không qua Nasdaq/Farside/bên thứ 3 nào).
    """
    url = f"https://www.21shares.com/en-us/products-us/{slug}"
    try:
        r = session.get(url, headers={"User-Agent": FAKE_UA}, timeout=15)
        if r.status_code != 200:
            print(f"    21Shares HTTP {r.status_code}: {url}")
            return None
        text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text)
        aum_m = re.search(r"\$\s*([\d,]+\.?\d*)\s*AUM", text)
        if not aum_m:
            print(f"    21Shares: không tìm thấy 'AUM' trên trang {ticker} | độ dài trang: {len(text)} ký tự")
            return None
        aum = parse_money(aum_m.group(1))
        date_m = re.search(r"Value as of ([A-Za-z]+ \d{1,2}, \d{4})", text)
        as_of = date_m.group(1) if date_m else None
        return (aum, as_of)
    except Exception as e:
        print(f"    21Shares error ({ticker}): {e}")
        return None


def fetch_rendered_text(url, click_texts=None, wait_ms=4000, extra_wait_selector=None, screenshot_path=None, scroll=False):
    """Render trang bằng Chromium THẬT qua Playwright — giải pháp GỐC RỄ cho các
    trang React/Next SPA (VanEck, Grayscale) mà requests/cloudscraper không lấy
    được nội dung vì cần chạy JS.

    KHÔNG PHẢI bên thứ 3 nào cả — đây là browser chạy ngay trong CI/máy chủ của
    chính bạn (GitHub Actions có internet đầy đủ, khác với sandbox giới hạn domain
    của tôi lúc code — nên tôi KHÔNG tự test trực tiếp được hàm này, chỉ viết theo
    đúng Playwright API chuẩn).

    click_texts: danh sách text nút cần bấm qua TRƯỚC khi đọc nội dung, vd modal
    "Personalize Your Experience" của VanEck có nút kiểu "Continue"/"United
    States"/"Individual Investor" — thử bấm lần lượt, cái nào không thấy thì bỏ
    qua (không có nút đó không phải lỗi, có thể trang không hiện modal lần này).

    scroll: nếu True, cuộn trang từ từ xuống hết chiều dài — nhiều trang chỉ
    load các widget (chart, bảng holdings...) khi widget đó vào viewport
    (lazy-load để tiết kiệm tài nguyên), nếu không cuộn thì các widget đó sẽ
    mãi trống trơn dù trang đã "render xong" về mặt kỹ thuật.

    screenshot_path: nếu có, LƯU ẢNH CHỤP MÀN HÌNH đúng lúc đọc nội dung (sau khi
    đã thử bấm hết click_texts) — để biết CHÍNH XÁC Playwright đang thấy gì, thay
    vì đoán mù qua text thô. Ảnh lưu vào thư mục này sẽ được workflow .yml upload
    làm artifact để bạn tải về xem trực tiếp.

    YÊU CẦU CÀI ĐẶT (không có sẵn trong sandbox của tôi, bạn cần thêm vào CI):
        pip install playwright
        playwright install --with-deps chromium
    Nếu chưa cài, hàm tự trả None (không crash), code gọi vẫn fallback được.
    """
    if not HAS_PLAYWRIGHT:
        return None
    # Thử 2 lần: lần 1 bình thường, lần 2 (nếu lần 1 lỗi kết nối kiểu
    # ERR_HTTP2_PROTOCOL_ERROR — xác nhận qua log thật với Fidelity 07/2026)
    # tắt hẳn HTTP/2, buộc dùng HTTP/1.1 — 1 số server/WAF không tương thích
    # HTTP/2 với Chromium headless dù trình duyệt thật vẫn vào bình thường.
    for attempt, extra_args in enumerate([[], ["--disable-http2"]]):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=extra_args)
                page = browser.new_page(user_agent=FAKE_UA)
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                page.wait_for_timeout(1500)
                for txt in (click_texts or []):
                    try:
                        page.get_by_text(txt, exact=False).first.click(timeout=2000)
                        page.wait_for_timeout(800)
                    except Exception:
                        pass  # không thấy nút này — bỏ qua, thử nút tiếp theo
                if scroll:
                    try:
                        height = page.evaluate("document.body.scrollHeight")
                        step = 600
                        y = 0
                        while y < height:
                            y += step
                            page.evaluate(f"window.scrollTo(0, {y})")
                            page.wait_for_timeout(400)  # đủ thời gian trigger IntersectionObserver
                        page.evaluate("window.scrollTo(0, 0)")  # về đầu trang cho screenshot đẹp
                    except Exception as e:
                        print(f"    Playwright: cuộn trang lỗi: {e}")
                if extra_wait_selector:
                    try:
                        page.wait_for_selector(extra_wait_selector, timeout=8000)
                    except Exception:
                        pass
                page.wait_for_timeout(wait_ms)
                if screenshot_path:
                    try:
                        page.screenshot(path=screenshot_path, full_page=True)
                    except Exception as e:
                        print(f"    Playwright: chụp màn hình lỗi ({screenshot_path}): {e}")
                text = page.inner_text("body")
                browser.close()
                return text
        except Exception as e:
            if attempt == 0:
                print(f"    Playwright lỗi lần 1 ({url}): {e} — thử lại với --disable-http2")
            else:
                print(f"    Playwright lỗi lần 2 (đã thử --disable-http2) ({url}): {e}")
    return None


def fetch_fidelity_holdings(session, symbol, ticker):
    """digital.fidelity.com/prgw/digital/research/quote/dashboard/summary —
    trang quote công khai (không cần login), có sẵn field:
      "Total bitcoin in fund — As of Jul-24-2026: 172,278.8049"

    XÁC NHẬN QUA VIEW-SOURCE THẬT 27/07/2026 (user tự lấy, không phải tôi
    đoán): nội dung app CHỈ LÀ "<resexp-app-root ...></resexp-app-root>" —
    custom element RỖNG, số liệu do JS bơm vào sau khi load. Nghĩa là fetch
    trực tiếp (requests/curl_cffi) KHÔNG BAO GIỜ lấy được số liệu, bất kể có
    vượt qua được chặn IP hay không — đây là bản chất kiến trúc trang, không
    phải lỗi tạm thời. Vì vậy bỏ hẳn tầng "direct fetch" (khác CoinShares —
    trang đó có SSR một phần).

    XÁC NHẬN QUA RAW OUTPUT THẬT 27/07/2026 (user tự lấy r.jina.ai, không
    phải tôi đoán) — 2 phát hiện:
      (a) FBTC: r.jina.ai đôi khi chụp quá sớm, trả về trang còn "Loading
          This could take a moment." — không có field nào. → thêm X-Timeout
          ở tầng dưới để đợi lâu hơn.
      (b) FETH/FSOL: r.jina.ai trả về ĐẦY ĐỦ dữ liệu thật, nhưng cấu trúc
          thật là "Total ether in fund [tooltip ~50 ký tự] As of <ngày>
          <số>" — khoảng cách "fund"→số thật ~57 ký tự, vượt giới hạn regex
          cũ (\\D{0,30}) nên KHÔNG BAO GIỜ khớp đúng. Đây là nguyên nhân thật
          của việc CI ra qty=24 giống nhau ở cả 3 ticker trước đó — không
          phải chặn IP hay DataDome, mà là bug parse. Đã sửa: neo "Total
          <coin> in fund" trước, sau đó tìm cặp "As of <ngày> <số>" ĐẦU TIÊN
          xuất hiện sau neo (không phải "Ether per share"/"Shares per ether"
          — các field đó cũng có dạng "As of ... <số>" nhưng đứng SAU, nên
          lấy cặp đầu tiên là đúng).
    """
    url = f"https://digital.fidelity.com/prgw/digital/research/quote/dashboard/summary?symbol={symbol}"
    text = None

    # 1) r.jina.ai — ĐÃ XÁC NHẬN CHẠY QUA ĐƯỢC (HTTP 200) theo log CI thật,
    # nhưng đôi khi chụp quá sớm lúc trang còn "Loading" (xác nhận qua raw
    # output thật của FBTC user paste 27/07/2026 — chỉ có chữ "Loading This
    # could take a moment.", không có field nào). Thêm X-Timeout để jina đợi
    # lâu hơn trước khi trả về, giảm khả năng chụp trúng lúc chưa render xong.
    try:
        rj = session.get(f"https://r.jina.ai/{url}",
            headers={"X-Return-Format":"text","Accept":"text/plain","X-Timeout":"20"}, timeout=30)
        if rj.status_code == 200 and re.search(r"Total\s+[A-Za-z]+\s+in\s+fund", rj.text, re.IGNORECASE):
            text = rj.text
        else:
            print(f"    Fidelity (r.jina.ai) {ticker}: HTTP {rj.status_code}, khớp field: {bool(re.search(r'Total .{1,20} in fund', rj.text, re.IGNORECASE)) if rj.status_code==200 else 'N/A'}")
    except Exception as e:
        print(f"    Fidelity (r.jina.ai) lỗi ({ticker}): {e}")

    # 2) Playwright — dự phòng cuối. Đợi CÓ ĐIỀU KIỆN (chờ chữ "Total" xuất
    # hiện) thay vì chỉ chờ cố định 2500ms — vì đã xác nhận trang này có thể
    # load chậm hơn mức đó (xem lý do ở tầng 1).
    if not text:
        os.makedirs("debug_screenshots", exist_ok=True)
        text = fetch_rendered_text(url, wait_ms=4000, scroll=True,
            extra_wait_selector="text=Total",
            screenshot_path=f"debug_screenshots/fidelity_{ticker}.png")
        if not text:
            print(f"    Fidelity: Playwright không lấy được nội dung cho {ticker} (chặn kết nối hoặc chưa cài playwright)")

    if not text:
        print(f"    Fidelity: cả 2 tầng (r.jina.ai/Playwright) đều thất bại cho {ticker}")
        return None

    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)

    # ⚠️ SỬA 27/07/2026 sau khi user paste raw r.jina.ai output thật cho cả
    # FBTC/FETH/FSOL. Phát hiện: cấu trúc trang thật là
    #   "Total ether in fund [tooltip dài ~50 ký tự chen giữa] As of <ngày> <số>"
    # — khoảng cách giữa "fund" và số thật ~57 ký tự, VƯỢT giới hạn cũ \D{0,30}
    # → regex cũ KHÔNG BAO GIỜ khớp đúng, và có thể đã vơ nhầm số rác ở chỗ
    # khác trên trang (giải thích được vì sao log CI ra 24 giống hệt cả 3
    # ticker). Cách mới: neo "Total <coin> in fund" trước để xác định đúng
    # khu vực, sau đó tìm cặp "As of <ngày> <số>" ĐẦU TIÊN xuất hiện sau neo
    # đó (trong cửa sổ 400 ký tự) — khớp đúng cấu trúc thật đã thấy ở cả
    # FETH ("As of Jul-24-2026 481,939.5378") và FSOL ("As of Jul-24-2026
    # 1,709,750.6034"). Lấy cặp ĐẦU TIÊN là bắt buộc — các field sau đó như
    # "Ether per share" cũng có dạng "As of <ngày> <số>" nhưng là tỷ lệ nhỏ
    # (~0.00996), không phải holdings.
    anchor = re.search(r"Total\s+([A-Za-z]+)\s+in\s+fund", text, re.IGNORECASE)
    if not anchor:
        print(f"    Fidelity: không tìm thấy neo 'Total <coin> in fund' trên trang {ticker}")
        print(f"      → độ dài trang: {len(text)} ký tự | 300 ký tự đầu: {text[:300]!r}")
        return None

    window = text[anchor.end(): anchor.end() + 400]
    m = re.search(r"As of\s+([A-Za-z]{3}-\d{1,2}-\d{4})\s+([\d,]+\.?\d*)", window)
    if not m:
        print(f"    Fidelity: tìm thấy neo '{anchor.group(0)}' nhưng không thấy 'As of <ngày> <số>' theo sau trên trang {ticker}")
        print(f"      → 400 ký tự sau neo: {window!r}")
        return None
    as_of = m.group(1)
    qty = float(m.group(2).replace(",", ""))

    # ⚠️ SANITY CHECK — THÊM 27/07/2026 sau khi log CI thật cho ra holdings=24
    # cho CẢ 3 ticker (FBTC/FETH/FSOL, khác coin hoàn toàn) — dấu hiệu chắc
    # chắn của false positive: r.jina.ai/Playwright khớp nhầm 1 đoạn text nào
    # đó tình cờ đúng dạng "Total <chữ> in fund ... 24" nhưng KHÔNG PHẢI số
    # holdings thật. Không quỹ ETF nào trong danh sách này giữ dưới 100 coin —
    # nếu qty thấp bất thường, coi là parse sai, KHÔNG trả kết quả (để tự
    # fallback Farside) thay vì âm thầm ghi AUM sai lệch hàng trăm/nghìn lần.
    MIN_PLAUSIBLE_QTY = 100
    if qty < MIN_PLAUSIBLE_QTY:
        print(f"    Fidelity: {ticker} parse ra qty={qty} — QUÁ NHỎ so với ngưỡng hợp lý ({MIN_PLAUSIBLE_QTY}), nghi false positive regex, coi như thất bại")
        print(f"      → đoạn khớp: {window[max(0,m.start()-100):m.end()+100]!r}")
        return None

    baseline = STATIC_BTC_HOLDINGS.get(ticker)
    if baseline and not (0.3 * baseline <= qty <= 3.0 * baseline):
        print(f"    Fidelity: {ticker} parse ra qty={qty:.2f} — lệch quá xa baseline tĩnh ({baseline:.2f}), nghi false positive regex, coi như thất bại")
        print(f"      → đoạn khớp: {window[max(0,m.start()-100):m.end()+100]!r}")
        return None

    return (qty, as_of)


def fetch_franklin_holdings(session, url, ticker):
    """✅ ĐÃ XÁC NHẬN hoạt động qua log thật 07/2026 (EZBC/EZET/SOEZ đều lấy được
    holdings ngay lần thử đầu tiên, không có lỗi).

    Franklin Templeton (franklintempleton.com) là SPA (fetch tĩnh chỉ ra
    "Loading..." — đã xác nhận qua fetch trực tiếp). robots.txt CHO PHÉP scrape
    (chỉ disallow /llm.txt). Trang sản phẩm có mục "Additional Fund Info" với
    dòng "{Coin} in Fund — Updated Daily: <số>" sau khi JS render xong, có cổng
    xác nhận vai trò nhà đầu tư/quốc gia trước đó (giống VanEck).

    Dùng chung kỹ thuật đã THÀNH CÔNG với VanEck: Playwright + thử bấm qua các
    nút xác nhận phổ biến + cuộn trang trigger lazy-load, rồi regex tổng quát
    "<coin> in Fund" (không cố định tên coin, tránh lặp lỗi ghi nhãn không nhất
    quán như đã gặp ở VanEck/VSOL).
    """
    os.makedirs("debug_screenshots", exist_ok=True)
    text = fetch_rendered_text(url,
        click_texts=["Individual Investor","Continue","Accept","Agree","I Agree",
                     "Yes","Confirm","United States","Enter","OK","Proceed"],
        wait_ms=2000, scroll=True, screenshot_path=f"debug_screenshots/franklin_{ticker}.png")
    if not text:
        print(f"    Franklin: Playwright không lấy được nội dung cho {ticker} (chưa cài playwright hoặc lỗi mạng)")
        return None
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    m = re.search(r"[A-Za-z]+\s+in\s+Fund\D{0,20}?([\d,]+\.?\d*)", text, re.IGNORECASE)
    if not m:
        snippet = text[:300]
        print(f"    Franklin: không tìm thấy '<coin> in Fund' trên trang {ticker}")
        print(f"      → độ dài trang: {len(text)} ký tự | 300 ký tự đầu: {snippet!r}")
        return None
    qty = float(m.group(1).replace(",", ""))
    return (qty, None)


def fetch_invesco_holdings(session, url, ticker):
    """✅ ĐÃ XÁC NHẬN hoạt động qua log thật 07/2026 (BTCO/QETH đều lấy được
    "Total units of crypto" ngay lần thử đầu tiên, khớp đúng ảnh chụp màn hình).

    Invesco (invesco.com) là SPA + cổng xác nhận vai trò nhà đầu tư (Individual/
    Financial Professional/Institutional) — đã xác nhận qua fetch trực tiếp
    (thấy rõ "Confirm your role to continue"). robots.txt CHO PHÉP scrape trang
    sản phẩm ETF (chỉ disallow vài query-param như asOfDate=). Có dòng
    "Total units of crypto <số>" sau khi qua cổng + JS render xong.
    """
    os.makedirs("debug_screenshots", exist_ok=True)
    text = fetch_rendered_text(url,
        click_texts=["Individual Investor","Confirm","Continue","Accept","Agree",
                     "I Agree","Yes","United States","Enter","OK","Proceed"],
        wait_ms=2000, scroll=True, screenshot_path=f"debug_screenshots/invesco_{ticker}.png")
    if not text:
        print(f"    Invesco: Playwright không lấy được nội dung cho {ticker} (chưa cài playwright hoặc lỗi mạng)")
        return None
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    m = re.search(r"Total\s+units?\s+of\s+crypto\D{0,20}?([\d,]+\.?\d*)", text, re.IGNORECASE)
    if not m:
        snippet = text[:300]
        print(f"    Invesco: không tìm thấy 'Total units of crypto' trên trang {ticker}")
        print(f"      → độ dài trang: {len(text)} ký tự | 300 ký tự đầu: {snippet!r}")
        return None
    qty = float(m.group(1).replace(",", ""))
    return (qty, None)


def fetch_grayscale_holdings(session, url, ticker):
    """⚠️ CHƯA CHẠY LẠI THẬT sau khi sửa — trước đây (07/2026) Playwright gặp
    "Vercel Security Checkpoint / Failed to verify your browser" (Kasada Deep
    Analysis, xem ghi chú registry). Đã sửa 2 lỗi ĐÃ BIẾT (rút kinh nghiệm từ
    Fidelity) trước khi thử lại, KHÔNG đợi lỗi xảy ra rồi mới vá:
      1) Thứ tự tầng cũ là Playwright trước, r.jina.ai sau — ngược pattern đã
         chứng minh hiệu quả ở Fidelity/CoinShares (direct rẻ nhất trước,
         Playwright tốn tài nguyên nhất để cuối). Đổi lại cho nhất quán.
      2) Regex cũ `\\D{0,30}?` giữa "Total X in Fund" và số — ĐÚNG lỗi đã gặp
         ở Fidelity (tooltip chen giữa làm khoảng cách >30 ký tự, không bao
         giờ khớp được số thật). Đổi sang tìm số PLAUSIBLE (≥3 chữ số) trong
         cửa sổ rộng hơn (300 ký tự) sau neo, thay vì giới hạn hẹp.
    Vẫn CHƯA thử vượt qua Kasada bằng kỹ thuật chuyên biệt nào (không giả
    fingerprint, không giải CAPTCHA, không stealth-plugin) — chỉ dùng lại
    đúng 3 tầng thường đã dùng cho Fidelity/CoinShares. Nếu vẫn thất bại ở cả
    3 tầng, đúng như kết luận cũ: Kasada quá mạnh, quay lại Farside, không
    đầu tư thêm.
    """
    text = None

    # 0) Fetch trực tiếp — rẻ nhất, thử trước
    try:
        r = session.get(url, headers={"User-Agent": FAKE_UA}, timeout=15)
        if r.status_code == 200:
            candidate = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            if re.search(r"in\s+Fund", candidate, re.IGNORECASE):
                text = candidate
            else:
                print(f"    Grayscale (direct) {ticker}: HTTP 200 nhưng không thấy 'in Fund' — nghi Vercel Checkpoint chặn hoặc cần JS")
        else:
            print(f"    Grayscale (direct) {ticker}: HTTP {r.status_code}")
    except Exception as e:
        print(f"    Grayscale (direct) lỗi ({ticker}): {e}")

    # 1) r.jina.ai — đã chứng minh hiệu quả với CoinShares (DataDome) dù
    # request trực tiếp bị chặn — đáng thử trước Playwright vì rẻ hơn nhiều
    if not text:
        try:
            rj = session.get(f"https://r.jina.ai/{url}",
                headers={"X-Return-Format":"text","Accept":"text/plain","X-Timeout":"20"}, timeout=30)
            if rj.status_code == 200 and re.search(r"in\s+Fund", rj.text, re.IGNORECASE):
                text = rj.text
            else:
                print(f"    Grayscale (r.jina.ai) {ticker}: HTTP {rj.status_code}, có 'in Fund': {('in Fund' in rj.text) if rj.status_code==200 else 'N/A'}")
        except Exception as e:
            print(f"    Grayscale (r.jina.ai) lỗi ({ticker}): {e}")

    # 2) Playwright — cuối cùng, tốn tài nguyên CI nhất, và đã biết trước khả
    # năng cao vẫn dính Vercel Security Checkpoint (xem docstring)
    if not text:
        text = fetch_rendered_text(url, wait_ms=4000, extra_wait_selector="text=in Fund")
        if not text:
            print(f"    Grayscale: Playwright không lấy được nội dung cho {ticker} (Vercel Checkpoint chặn hoặc chưa cài playwright)")

    if not text:
        print(f"    Grayscale: cả 3 tầng (direct/r.jina.ai/Playwright) đều thất bại cho {ticker}")
        return None

    text = text.replace("\xa0", " ")
    text = re.sub(r"[*_#|]", " ", text)
    text = re.sub(r"\s+", " ", text)

    # Neo "Total <coin> in Fund" trước, sau đó tìm số PLAUSIBLE (≥3 chữ số,
    # có thể có dấu phẩy/thập phân) trong cửa sổ 300 ký tự sau neo — không
    # giới hạn cứng 30 ký tự như bản cũ (bài học từ Fidelity).
    anchor = re.search(r"Total\s+([A-Za-z]+)\s+in\s+Fund", text, re.IGNORECASE)
    if not anchor:
        print(f"    Grayscale: không tìm thấy neo 'Total <coin> in Fund' trên trang {ticker}")
        print(f"      → độ dài trang: {len(text)} ký tự | 300 ký tự đầu: {text[:300]!r}")
        return None

    window = text[anchor.end(): anchor.end() + 300]
    m = re.search(r"([\d,]{3,}\.?\d*)", window)
    if not m:
        print(f"    Grayscale: tìm thấy neo '{anchor.group(0)}' nhưng không thấy số hợp lệ theo sau trên trang {ticker}")
        print(f"      → 300 ký tự sau neo: {window!r}")
        return None
    qty = float(m.group(1).replace(",", ""))

    # Sanity check — cùng nguyên tắc đã thêm cho Fidelity: không quỹ nào giữ
    # dưới 100 coin, và so với baseline tĩnh nếu có (chỉ GBTC có trong dict).
    MIN_PLAUSIBLE_QTY = 100
    if qty < MIN_PLAUSIBLE_QTY:
        print(f"    Grayscale: {ticker} parse ra qty={qty} — QUÁ NHỎ, nghi false positive, coi như thất bại")
        print(f"      → đoạn khớp: {window[max(0,m.start()-100):m.end()+100]!r}")
        return None
    baseline = STATIC_BTC_HOLDINGS.get(ticker)
    if baseline and not (0.3 * baseline <= qty <= 3.0 * baseline):
        print(f"    Grayscale: {ticker} parse ra qty={qty:.2f} — lệch quá xa baseline tĩnh ({baseline:.2f}), coi như thất bại")
        return None

    return (qty, None)


def fetch_bitwise_holdings(session, domain, ticker):
    """Fetch holdings từ site riêng của từng quỹ Bitwise (BITB→bitbetf.com,
    ETHW→ethwetf.com, BSOL→bsoletf.com, BHYP→bhypetf.com).

    XÁC NHẬN THẬT qua fetch trực tiếp 07/2026 (không đoán mò): tất cả các site
    này dùng CHUNG 1 nền tảng Next.js, và quan trọng nhất — SERVER-RENDERED
    (SSR), khác hẳn VanEck (React SPA client-render + màn hình chặn khu vực).
    Nội dung "Fund Holdings" nằm thẳng trong HTML trả về từ request đầu tiên,
    curl/requests lấy được ngay, không cần JS/proxy render gì cả.

    Format thấy được (giống hệt nhau qua cả 4 site, chỉ khác tên coin):
      "Bitcoin in Fund  36,678.89" (BITB) / "ETH in Fund  106,365.71" (ETHW) /
      "Solana in Fund  8,278,700.00" (BSOL) / "Hyperliquid in Fund  2,044,448.48"
      (BHYP — lưu ý BHYP có 1 chỗ bị TYPO thành "Hyyperliquid in Fund", nên
      regex KHÔNG cố định chữ đầu, chỉ bắt "<từ bất kỳ> in Fund <số>" rồi lấy
      match đầu tiên — mỗi trust chỉ có đúng 1 dòng holding thật, không rủi ro
      khớp nhầm).

    Trang còn có sẵn "Net Assets (AUM)" ở mục Fund Details — chính xác hơn cả
    Nasdaq (không bị làm tròn theo (,000)), lấy kèm luôn khi có.
    """
    url = f"https://{domain}/"
    try:
        r = session.get(url, headers={"User-Agent": FAKE_UA}, timeout=15)
        if r.status_code != 200:
            print(f"    Bitwise HTTP {r.status_code}: {url}")
            return None
        text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text)
        m = re.search(r"[A-Za-z]+\s+in\s+Fund\D{0,20}?([\d,]+\.?\d*)", text)
        if not m:
            has_marker = "in Fund" in text
            snippet = text[:200]
            print(f"    Bitwise: không tìm thấy '<coin> in Fund' trên trang {ticker}")
            print(f"      → độ dài trang: {len(text)} ký tự | có 'in Fund': {has_marker}")
            print(f"      → 200 ký tự đầu: {snippet!r}")
            return None
        qty = float(m.group(1).replace(",", ""))
        date_m = re.search(r"Data as of (\d{2}/\d{2}/\d{4})", text)
        as_of = date_m.group(1) if date_m else None
        aum_m = re.search(r"Net Assets \(AUM\)\s*\$?([\d,]+\.?\d*)", text)
        aum = parse_money(aum_m.group(1)) if aum_m else None
        return (qty, as_of, aum)
    except Exception as e:
        print(f"    Bitwise error ({ticker}): {e}")
        return None


def fetch_vaneck_holdings(session, url_slug, asset_word, ticker):
    """VanEck's fund pages RENDER BẰNG JAVASCRIPT (đã xác nhận THẬT qua log
    24-25/07: fetch trực tiếp chỉ ra ~8-9K ký tự, KHÔNG có 'ETF Statistics' —
    y hệt ARK/Grayscale SPA. Nhận định "static" ban đầu là SAI, do tool dùng để
    verify thủ công lúc đó tự chạy JS như trình duyệt thật nên bị đánh lừa.

    Giải pháp: đi qua r.jina.ai — dịch vụ proxy công khai, MIỄN PHÍ, không cần
    API key (https://r.jina.ai/<url>), tự render trang bằng browser thật ở phía
    họ rồi trả về text đã "hydrate" xong. Trang VanEck vốn công khai, không có
    auth/paywall gì — đây không phải né chặn gì cả, chỉ là mượn 1 browser thật
    để lấy đúng nội dung mà curl/requests không tự chạy JS được.

    Có 2 lớp fallback để không bao giờ mất dữ liệu nếu r.jina.ai lỗi:
    1) r.jina.ai (chính, render JS thật)
    2) fetch trực tiếp như cũ (gần như luôn ra shell rỗng, nhưng thử cho chắc,
       phòng khi VanEck đổi lại sang static hoặc r.jina.ai đang down)
    3) nếu cả 2 đều fail → trả None → hàm gọi tự fallback sang Farside.

    asset_word đúng theo cách VanEck ghi trên trang: "Bitcoin" (HODL), "Ether"
    (ETHV — KHÔNG PHẢI "Ethereum"), "Solana" (VSOL), "BNB" (VBNB)."""
    url = f"https://www.vaneck.com/us/en/investments/{url_slug}/"
    text = None

    # 0) Playwright — render JS thật, tự bấm qua modal (nếu có) + cuộn trang để
    # trigger lazy-load. XÁC NHẬN qua ảnh chụp thật 07/2026: modal "Personalize"
    # KHÔNG còn là vấn đề chính (Playwright đã qua được, NAV/AUM hiện đúng số
    # thật) — vấn đề THẬT là mục "Holdings" (chứa "ETF Statistics"/"X in Fund")
    # nằm DƯỚI màn hình đầu, chỉ load khi cuộn tới (lazy-load, giống Performance/
    # Fees cũng trống tương tự cho tới khi cuộn qua).
    os.makedirs("debug_screenshots", exist_ok=True)
    shot_path = f"debug_screenshots/vaneck_{ticker}.png"
    pw_text = fetch_rendered_text(url,
        click_texts=["Continue","United States","Individual Investor","Accept","Agree",
                     "I Agree","Yes","Confirm","Enter","Get Started","OK","Proceed"],
        wait_ms=2000, extra_wait_selector="text=ETF Statistics", screenshot_path=shot_path, scroll=True)
    if pw_text and "ETF Statistics" not in pw_text:
        # Vẫn chưa thấy dữ liệu Holdings dù đã cuộn — có thể cần cuộn chậm hơn/
        # chờ lâu hơn nữa. In ra để biết CHÍNH XÁC, VÀ đã lưu ảnh chụp thật lúc
        # đó vào debug_screenshots/ — workflow .yml sẽ upload làm artifact để
        # xem trực tiếp, không cần đoán mù qua text nữa.
        print(f"    VanEck (Playwright) {ticker}: đã cuộn nhưng vẫn chưa thấy Holdings | đã lưu ảnh {shot_path} | 300 ký tự đầu: {pw_text[:300]!r}")
    text = pw_text  # dùng luôn dù chưa chắc có Holdings — vẫn có thể có AUM

    # 1) r.jina.ai — render JS qua proxy (dự phòng nếu chưa cài Playwright)
    if not text:
        try:
            rj = session.get(f"https://r.jina.ai/{url}",
                headers={"X-Return-Format":"text","Accept":"text/plain"}, timeout=25)
            if rj.status_code == 200 and len(rj.text) > 1000:
                text = rj.text
            else:
                print(f"    VanEck (r.jina.ai) {ticker}: HTTP {rj.status_code}, độ dài {len(rj.text)}")
        except Exception as e:
            print(f"    VanEck (r.jina.ai) lỗi ({ticker}): {e}")

    # 2) Fallback: fetch trực tiếp như cũ (rẻ, không hại gì khi thử thêm)
    if not text:
        try:
            r = session.get(url, headers={"User-Agent": FAKE_UA}, timeout=15)
            if r.status_code == 200:
                text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
        except Exception as e:
            print(f"    VanEck direct lỗi ({ticker}): {e}")

    if not text:
        print(f"    VanEck: cả Playwright/r.jina.ai lẫn fetch trực tiếp đều thất bại cho {ticker}")
        return None

    # Chuẩn hoá: \xa0 (nbsp) → space thường; dọn ký tự markdown (*_#|) mà
    # r.jina.ai có thể chèn vào (in đậm/heading/bảng) — nếu không dọn, regex
    # "\D{0,20}?" có thể vẫn khớp qua vì \D chấp nhận mọi ký tự không phải số,
    # nhưng dọn cho sạch để log snippet dễ đọc hơn khi debug.
    text = text.replace("\xa0", " ")
    text = re.sub(r"[*_#|]", " ", text)
    text = re.sub(r"\s+", " ", text)
    # Regex tổng quát "<từ bất kỳ> in Fund" thay vì cố định asset_word — XÁC
    # NHẬN qua ảnh chụp thật 07/2026: VanEck ghi nhãn KHÔNG NHẤT QUÁN giữa các
    # quỹ! HODL/ETHV/VBNB dùng tên coin ("Bitcoin/Ether/BNB in Fund") nhưng
    # VSOL lại dùng TICKER ("VSOL in Fund", không phải "Solana in Fund") —
    # asset_word cố định đã trượt vì lý do này. Mỗi trust chỉ có đúng 1 dòng
    # holding thật nên regex tổng quát không rủi ro khớp nhầm.
    m = re.search(r"[A-Za-z]+\s+in\s+Fund\D{0,20}?([\d,]+\.?\d*)", text, re.IGNORECASE)
    if not m:
        has_marker = "ETF Statistics" in text
        has_word = asset_word.lower() in text.lower()
        snippet = text[:200]
        print(f"    VanEck: không tìm thấy '{asset_word} in Fund' trên trang {ticker}")
        print(f"      → độ dài text: {len(text)} ký tự | có 'ETF Statistics': {has_marker} | có '{asset_word}': {has_word}")
        print(f"      → 200 ký tự đầu: {snippet!r}")
        # Dự phòng: không có Holdings (coin quantity) thì vẫn thử lấy AUM qua
        # "Total Net Assets" — mục này KHÔNG bị lazy-load (nằm ngay đầu trang,
        # xác nhận qua ảnh chụp thật 07/2026 luôn hiện số đúng dù Holdings trống).
        aum_m = re.search(r"TOTAL NET ASSETS\D{0,20}?\$?\s*([\d,]+\.?\d*[BMKT]?)", text, re.IGNORECASE)
        if aum_m:
            aum = parse_money(aum_m.group(1))
            if aum:
                print(f"    VanEck: không có Holdings nhưng lấy được Total Net Assets = ${aum:,.0f} cho {ticker}")
                return (None, None, aum)
        return None
    qty = float(m.group(1).replace(",", ""))
    date_m = re.search(r"ETF Statistics as of (\d{2}/\d{2}/\d{4})", text)
    as_of = date_m.group(1) if date_m else None
    return (qty, as_of, None)


def fetch_coinshares_holdings(session, url, ticker, asset_ticker="XBTUSD"):
    """⚠️ CHƯA XÁC MINH bằng fetch tự động thật từ GitHub Actions (chỉ mới xác
    nhận qua nội dung user tự paste từ trình duyệt thật 27/07/2026, KHÔNG phải
    qua request tự động của tôi) — viết theo đúng bằng chứng đã có, cần theo
    dõi log lần chạy CI đầu tiên để biết CHÍNH XÁC tier nào (0/1/2 bên dưới)
    thực sự lấy được dữ liệu.

    coinshares.com/etf/brrr/ (trước đây valkyrieinvesti.com/brrr.html, đã đổi
    thương hiệu — xem chú thích registry). robots.txt CHO PHÉP scrape path
    /etf/brrr/ (đã tự xác nhận qua robots.txt thật, chỉ disallow path locale
    và /*/d/*, /lookups/*). Trang có bảng Holdings dạng:
        "BITCOIN XBTUSD 5,890.21 377,868,595.57"
    (cột 3 = số BTC nắm giữ, cột 4 = market value) + "As of date: MM/DD/YYYY"
    + AUM chính xác tới cent ở mục Key Information ("AuM USD ...").

    LƯU Ý QUAN TRỌNG: cookie declaration của chính trang coinshares.com (xem
    ở trang /etf/brrr/product-guide/) liệt kê cookie "datadome" — tức site
    dùng DataDome bot-management. Không có gì đảm bảo digitalocean/GitHub
    Actions IP sẽ qua được dù robots.txt cho phép — robots.txt chỉ là quy ước
    "được phép crawl", không phải "sẽ không bị chặn kỹ thuật". Vì vậy hàm này
    thử theo 3 tầng tăng dần chi phí, dừng ngay khi tầng nào ra kết quả:
      0) requests/cloudscraper trực tiếp — RẺ NHẤT, đáng thử trước vì nội
         dung do user paste có số liệu thật ngay trong text (dấu hiệu có thể
         là SSR/Next.js render sẵn ở server, không cần JS như VanEck) — nhưng
         nếu DataDome chặn ở tầng này thì sẽ ra shell rỗng hoặc trang challenge.
      1) r.jina.ai — proxy render JS công khai, miễn phí, đã dùng cho VanEck/
         Grayscale trong chính file này.
      2) Playwright (Chromium thật) — cuối cùng, tốn tài nguyên CI nhất.
    """
    text = None
    site_aum = None

    # 0) Fetch trực tiếp — rẻ nhất, thử trước
    try:
        r = session.get(url, headers={"User-Agent": FAKE_UA}, timeout=15)
        if r.status_code == 200:
            candidate = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            if asset_ticker in candidate:
                text = candidate
            else:
                print(f"    CoinShares (direct) {ticker}: HTTP 200 nhưng không thấy '{asset_ticker}' — có thể bị DataDome chặn/challenge hoặc cần JS")
        else:
            print(f"    CoinShares (direct) {ticker}: HTTP {r.status_code}")
    except Exception as e:
        print(f"    CoinShares (direct) lỗi ({ticker}): {e}")

    # 1) r.jina.ai — dự phòng nếu fetch trực tiếp không ra số
    if not text:
        try:
            rj = session.get(f"https://r.jina.ai/{url}",
                headers={"X-Return-Format":"text","Accept":"text/plain"}, timeout=25)
            if rj.status_code == 200 and asset_ticker in rj.text:
                text = rj.text
            else:
                print(f"    CoinShares (r.jina.ai) {ticker}: HTTP {rj.status_code}, có '{asset_ticker}': {asset_ticker in rj.text if rj.status_code==200 else 'N/A'}")
        except Exception as e:
            print(f"    CoinShares (r.jina.ai) lỗi ({ticker}): {e}")

    # 2) Playwright — cuối cùng, tốn tài nguyên nhất
    if not text:
        os.makedirs("debug_screenshots", exist_ok=True)
        text = fetch_rendered_text(url, wait_ms=3000, scroll=True,
            extra_wait_selector=f"text={asset_ticker}",
            screenshot_path=f"debug_screenshots/coinshares_{ticker}.png")
        if not text:
            print(f"    CoinShares: Playwright không lấy được nội dung cho {ticker} (chưa cài playwright hoặc lỗi mạng)")

    if not text:
        print(f"    CoinShares: cả 3 tầng (direct/r.jina.ai/Playwright) đều thất bại cho {ticker}")
        return None

    text = text.replace("\xa0", " ")
    text = re.sub(r"[*_#|]", " ", text)
    text = re.sub(r"\s+", " ", text)

    m = re.search(re.escape(asset_ticker) + r"\D{0,20}?([\d,]+\.?\d*)", text)
    if not m:
        has_marker = asset_ticker in text
        snippet = text[:250]
        print(f"    CoinShares: không tìm thấy holdings sau '{asset_ticker}' trên trang {ticker}")
        print(f"      → độ dài trang: {len(text)} ký tự | có '{asset_ticker}': {has_marker}")
        print(f"      → 250 ký tự đầu: {snippet!r}")
        return None
    qty = float(m.group(1).replace(",", ""))

    date_m = re.search(r"As of date:\s*(\d{2}/\d{2}/\d{4})", text, re.IGNORECASE)
    as_of = date_m.group(1) if date_m else None

    # AUM chính xác từ mục Key Information ("AuM USD 377,876,360.14") — ưu
    # tiên hơn qty×price vì không bị làm tròn theo giá coin snapshot khác giờ.
    aum_m = re.search(r"AuM\s*(?:\(US\$\))?\s*USD?\s*\$?\s*([\d,]+\.?\d*)", text, re.IGNORECASE)
    if aum_m:
        try:
            site_aum = float(aum_m.group(1).replace(",", ""))
        except ValueError:
            site_aum = None

    return (qty, as_of, site_aum)


def compute_self_flow(holdings_today, holdings_prev, price_today):
    """Flow tự tính = Δholdings × giá — CHÍNH XÁC cùng phương pháp Farside/mọi bên
    tracker khác dùng (Shares Outstanding/Holdings đổi × NAV hoặc giá tài sản),
    chỉ khác là mình tính trực tiếp từ dữ liệu holdings gốc của issuer, không qua
    trung gian nào. Trả về None nếu thiếu bất kỳ input nào (không đoán/không giả).

    ⚠️ SANITY CHECK THÊM 27/07/2026 — xác nhận qua log CI thật: lần chạy sớm
    hơn (trước khi có sanity check ở fetch_fidelity_holdings) đã lỡ GHI SỐ
    HOLDINGS SAI (qty=24, do lỗi regex cũ) vào holdings_history — tức "mốc
    hôm qua" bị nhiễm. Lần chạy sau đó tính đúng holdings thật (172,278.80)
    nhưng trừ cho mốc hỏng (24) → ra flow ~$10.9 TỶ (gần bằng nguyên AUM),
    lọt thẳng vào output vì trước đây compute_self_flow không kiểm tra độ
    lớn của flow, chỉ kiểm tra có đủ input hay không.

    Giờ chặn thêm: flow 1 ngày của các quỹ ETF này trong thực tế hiếm khi
    vượt quá ~50% AUM (kể cả ngày biến động mạnh nhất lịch sử). Nếu vượt,
    coi là dấu hiệu holdings_prev (mốc hôm qua) bị hỏng/nhiễm — trả None để
    tự fallback Farside cho NGÀY ĐÓ, thay vì đẩy con số sai vào output. Mốc
    holdings_history vẫn được ghi đè bằng số ĐÚNG ở cuối lần chạy này (xem
    run()), nên lần chạy KẾ TIẾP sẽ tự khỏi, không cần can thiệp thủ công."""
    if holdings_today is None or holdings_prev is None or price_today is None:
        return None
    if holdings_prev <= 0:
        return None  # tránh trường hợp dữ liệu holdings cũ bị lỗi/rỗng
    flow = (holdings_today - holdings_prev) * price_today
    aum_today = holdings_today * price_today
    if aum_today > 0 and abs(flow) > 0.5 * aum_today:
        return None  # nghi holdings_prev (mốc "hôm qua") bị nhiễm số liệu sai
    return flow


HOLDINGS_HISTORY_KEY = "etf-holdings-history.json"

def load_holdings_history(r2):
    """{ "TICKER": {"date": "YYYY-MM-DD", "holdings": 123.45}, ... } — lưu holdings
    của LẦN CHẠY FULL GẦN NHẤT cho mỗi ticker tự tính, dùng làm mốc "hôm qua" để
    tính Δholdings ở lần chạy full tiếp theo."""
    data = r2_get_json(r2, HOLDINGS_HISTORY_KEY)
    return data if isinstance(data, dict) else {}

def save_holdings_history(r2, history):
    r2_put_json(r2, HOLDINGS_HISTORY_KEY, history, "max-age=3600")


def run(r2):
    now_utc=datetime.now(timezone.utc)
    today_str=now_utc.strftime("%Y-%m-%d")
    session=get_session()

    prev_etfs={e["ticker"]:e for e in (r2_get_json(r2,"etf-flows.json") or {}).get("etfs",[])}
    crypto_prices=load_crypto_prices()
    holdings_history = load_holdings_history(r2)  # {"TICKER": {"date":..., "holdings":...}} của lần full trước

    print("\n📈 [1/4] Nasdaq prices...")
    nasdaq=fetch_nasdaq_all(session)
    print(f"  → {sum(1 for v in nasdaq.values() if v.get('price'))} tickers")

    daily_flows={}; full_history={}
    if RUN_MODE=="full":
        print("\n📊 [2/4] Farside flows (full history)...")
        daily_flows, full_history = fetch_farside_all(session)
        # Save full history to R2
        if full_history:
            r2_put_json(r2,"etf-farside-history.json",{
                "data": full_history,
                "updated_at": now_utc.isoformat()
            },"max-age=3600")
            total_rows=sum(len(v.get("rows",[])) for v in full_history.values())
            print(f"\n  ✓ History saved: {total_rows} total rows across {len(full_history)} assets")
    else:
        print("⏭️  Skip Farside")

    print("\n🏦 [3/4] Issuer holdings (iShares + ARK + VanEck + Bitwise + 21Shares + Franklin + Invesco) — nguồn TỰ TÍNH flow, không qua Farside...")
    issuer={}
    holdings_today={}  # ticker -> holdings mới fetch được lần chạy này (để lưu lại làm mốc "hôm qua" cho lần sau)
    if RUN_MODE=="full":
        for etf_ticker,pid in ISHARES_IDS.items():
            u=next((e["underlying"] for e in ETF_REGISTRY if e["ticker"]==etf_ticker),"")
            raw=fetch_ishares(session,etf_ticker,pid,crypto_prices.get(u))
            if raw:
                nav=nasdaq.get(etf_ticker,{}).get("price")
                aum=raw.get("aum") or (raw["holdings"]*crypto_prices[u] if raw.get("holdings") and u in crypto_prices else None)
                issuer[etf_ticker]={**raw,"nav":nav,"aum":aum}
                if raw.get("holdings"): holdings_today[etf_ticker]=raw["holdings"]
                print(f"  ✓ {etf_ticker}: AUM={fmt_aum(aum)}  holdings={raw.get('holdings',0):.0f}")
            time.sleep(0.5)

        for etf in ETF_REGISTRY:
            if etf.get("src")=="nasdaq" and etf.get("self_computed") and etf.get("ark_fund_name"):
                t=etf["ticker"]
                res=fetch_ark_holdings(session, etf["ark_fund_name"], t)
                if res:
                    qty, as_of = res
                    holdings_today[t]=qty
                    u=etf["underlying"]
                    aum=qty*crypto_prices[u] if u in crypto_prices else None
                    issuer[t]={"holdings":qty,"aum":aum,"nav":nasdaq.get(t,{}).get("price"),"nav_date":as_of}
                    print(f"  ✓ {t} (ARK CSV): holdings={qty:.2f}  AUM={fmt_aum(aum)}")
                else:
                    print(f"  ✗ {t}: không lấy được holdings từ ARK CSV — fallback Farside cho ticker này")
                time.sleep(0.3)

        for etf in ETF_REGISTRY:
            if etf.get("src")=="nasdaq" and etf.get("self_computed") and etf.get("vaneck_slug"):
                t=etf["ticker"]
                res=fetch_vaneck_holdings(session, etf["vaneck_slug"], etf["vaneck_asset_word"], t)
                if res:
                    qty, as_of, site_aum = res
                    u=etf["underlying"]
                    if qty:
                        holdings_today[t]=qty
                        aum=qty*crypto_prices[u] if u in crypto_prices else None
                        issuer[t]={"holdings":qty,"aum":aum,"nav":nasdaq.get(t,{}).get("price"),"nav_date":as_of}
                        print(f"  ✓ {t} (VanEck): holdings={qty:.2f}  AUM={fmt_aum(aum)}")
                    elif site_aum:
                        # Không lấy được coin quantity (Holdings bị lazy-load
                        # chưa trigger được) nhưng có Total Net Assets thật →
                        # suy ngược holdings ước lượng, vẫn tốt hơn Farside vì
                        # AUM lấy thẳng từ VanEck, không qua bên thứ 3.
                        qty_est=site_aum/crypto_prices[u] if u in crypto_prices and crypto_prices[u] else None
                        if qty_est: holdings_today[t]=qty_est
                        issuer[t]={"holdings":qty_est,"aum":site_aum,"nav":nasdaq.get(t,{}).get("price"),"nav_date":as_of}
                        print(f"  ✓ {t} (VanEck, chỉ có AUM): AUM={fmt_aum(site_aum)}  holdings≈{(qty_est or 0):.2f}")
                    else:
                        print(f"  ✗ {t}: không lấy được holdings/AUM từ VanEck — fallback Farside cho ticker này")
                else:
                    print(f"  ✗ {t}: không lấy được holdings từ VanEck — fallback Farside cho ticker này")
                time.sleep(3.0)  # r.jina.ai free tier giới hạn ~20 req/phút

        for etf in ETF_REGISTRY:
            if etf.get("src")=="nasdaq" and etf.get("self_computed") and etf.get("bitwise_domain"):
                t=etf["ticker"]
                res=fetch_bitwise_holdings(session, etf["bitwise_domain"], t)
                if res:
                    qty, as_of, site_aum = res
                    holdings_today[t]=qty
                    u=etf["underlying"]
                    # Ưu tiên AUM lấy thẳng từ site (chính xác tới đơn vị), chỉ
                    # tính qty×giá làm dự phòng nếu site không có field AUM.
                    aum=site_aum or (qty*crypto_prices[u] if u in crypto_prices else None)
                    issuer[t]={"holdings":qty,"aum":aum,"nav":nasdaq.get(t,{}).get("price"),"nav_date":as_of}
                    print(f"  ✓ {t} (Bitwise): holdings={qty:.2f}  AUM={fmt_aum(aum)}")
                else:
                    print(f"  ✗ {t}: không lấy được holdings từ Bitwise — fallback Farside cho ticker này")
                time.sleep(0.5)

        for etf in ETF_REGISTRY:
            if etf.get("src")=="nasdaq" and etf.get("self_computed") and etf.get("shares21_slug"):
                t=etf["ticker"]
                res=fetch_21shares_aum(session, etf["shares21_slug"], t)
                if res:
                    aum, as_of = res
                    u=etf["underlying"]
                    # 21shares.com không lộ coin quantity thật (bảng Holdings bị
                    # JS-block) → suy ngược holdings ước lượng từ AUM/giá coin,
                    # để vẫn dùng chung được compute_self_flow(Δholdings×price).
                    qty=aum/crypto_prices[u] if u in crypto_prices and crypto_prices[u] else None
                    if qty: holdings_today[t]=qty
                    issuer[t]={"holdings":qty,"aum":aum,"nav":nasdaq.get(t,{}).get("price"),"nav_date":as_of}
                    print(f"  ✓ {t} (21Shares): AUM={fmt_aum(aum)}  holdings≈{(qty or 0):.2f}")
                else:
                    print(f"  ✗ {t}: không lấy được AUM từ 21Shares — fallback Farside cho ticker này")
                time.sleep(0.5)

        # ✅ Franklin Templeton — ĐÃ XÁC NHẬN hoạt động (EZBC/EZET/SOEZ, log
        # thật 07/2026, không có lỗi).
        for etf in ETF_REGISTRY:
            if etf.get("src")=="nasdaq" and etf.get("self_computed") and etf.get("franklin_url"):
                t=etf["ticker"]
                res=fetch_franklin_holdings(session, etf["franklin_url"], t)
                if res:
                    qty, as_of=res
                    holdings_today[t]=qty
                    u=etf["underlying"]
                    aum=qty*crypto_prices[u] if u in crypto_prices else None
                    issuer[t]={"holdings":qty,"aum":aum,"nav":nasdaq.get(t,{}).get("price"),"nav_date":as_of}
                    print(f"  ✓ {t} (Franklin): holdings={qty:.2f}  AUM={fmt_aum(aum)}")
                else:
                    print(f"  ✗ {t}: không lấy được holdings từ Franklin — fallback Farside cho ticker này")
                time.sleep(1.0)

        # ✅ Invesco — ĐÃ XÁC NHẬN hoạt động (BTCO/QETH, log thật 07/2026).
        for etf in ETF_REGISTRY:
            if etf.get("src")=="nasdaq" and etf.get("self_computed") and etf.get("invesco_url"):
                t=etf["ticker"]
                res=fetch_invesco_holdings(session, etf["invesco_url"], t)
                if res:
                    qty, as_of=res
                    holdings_today[t]=qty
                    u=etf["underlying"]
                    aum=qty*crypto_prices[u] if u in crypto_prices else None
                    issuer[t]={"holdings":qty,"aum":aum,"nav":nasdaq.get(t,{}).get("price"),"nav_date":as_of}
                    print(f"  ✓ {t} (Invesco): holdings={qty:.2f}  AUM={fmt_aum(aum)}")
                else:
                    print(f"  ✗ {t}: không lấy được holdings từ Invesco — fallback Farside cho ticker này")
                time.sleep(1.0)

        # ⚠️ CoinShares (BRRR) — MỚI THÊM 27/07/2026, CHƯA CHẠY THẬT LẦN NÀO
        # trên CI. Theo dõi log lần chạy đầu để biết tầng nào (direct/r.jina.ai/
        # Playwright) thực sự lấy được dữ liệu — xem chú thích trong
        # fetch_coinshares_holdings(). Nếu cả 3 tầng đều fail vì DataDome chặn
        # cứng, ticker này tự fallback Farside như cũ, không mất dữ liệu.
        for etf in ETF_REGISTRY:
            if etf.get("src")=="nasdaq" and etf.get("self_computed") and etf.get("coinshares_url"):
                t=etf["ticker"]
                res=fetch_coinshares_holdings(session, etf["coinshares_url"], t)
                if res:
                    qty, as_of, site_aum=res
                    holdings_today[t]=qty
                    u=etf["underlying"]
                    aum=site_aum or (qty*crypto_prices[u] if u in crypto_prices else None)
                    issuer[t]={"holdings":qty,"aum":aum,"nav":nasdaq.get(t,{}).get("price"),"nav_date":as_of}
                    print(f"  ✓ {t} (CoinShares): holdings={qty:.2f}  AUM={fmt_aum(aum)}")
                else:
                    print(f"  ✗ {t}: không lấy được holdings từ CoinShares — fallback Farside cho ticker này")
                time.sleep(1.0)

        # ⚠️ Fidelity — BẬT LẠI 27/07/2026 với 3 tầng fallback (xem docstring
        # fetch_fidelity_holdings). Trước đó tắt vì Playwright bị chặn ở tầng
        # kết nối (net::ERR_HTTP2_PROTOCOL_ERROR) — CHƯA thử r.jina.ai (tầng
        # có IP khác GitHub Actions, đã lấy được BRRR dù DataDome chặn request
        # trực tiếp). Nếu log CI lần này vẫn ra "cả 3 tầng đều thất bại" cho
        # cả FBTC/FETH/FSOL → xác nhận chắc chắn là chặn IP thuần, không phải
        # thiếu kỹ thuật gì thêm có thể thử miễn phí — lúc đó residential
        # proxy là bước hợp lý duy nhất còn lại.
        for etf in ETF_REGISTRY:
            if etf.get("src")=="nasdaq" and etf.get("self_computed") and etf.get("fidelity_symbol"):
                t=etf["ticker"]
                res=fetch_fidelity_holdings(session, etf["fidelity_symbol"], t)
                if res:
                    qty, as_of=res
                    holdings_today[t]=qty
                    u=etf["underlying"]
                    aum=qty*crypto_prices[u] if u in crypto_prices else None
                    issuer[t]={"holdings":qty,"aum":aum,"nav":nasdaq.get(t,{}).get("price"),"nav_date":as_of}
                    print(f"  ✓ {t} (Fidelity): holdings={qty:.2f}  AUM={fmt_aum(aum)}")
                else:
                    print(f"  ✗ {t}: không lấy được holdings từ Fidelity — fallback Farside cho ticker này")
                time.sleep(1.0)

        # ⚠️ Grayscale — BẬT LẠI 28/07/2026 sau khi xác nhận robots.txt CHO
        # PHÉP hoàn toàn (Allow: *, khác giả định ban đầu) và cấu trúc trang
        # thật (nhãn "TOTAL <COIN> IN FUND", không phải "in Trust" như đoán
        # trước — user tự Ctrl+A copy trang GBTC thật để xác nhận). GSOL vẫn
        # giữ Farside — tài liệu cũ ghi rõ đây là sản phẩm OTC, domain khác
        # (grayscale.com/funds/ chứ không phải etfs.grayscale.com/), CHƯA xác
        # nhận lại. URL của BTC/ETHE/HYPG là ĐOÁN theo pattern GBTC (domain +
        # ticker viết thường) — CHƯA xác nhận từng URL riêng, nếu sai sẽ tự
        # fallback Farside an toàn (không có rủi ro sai số, đã có sanity check).
        for etf in ETF_REGISTRY:
            if etf.get("src")=="nasdaq" and etf.get("self_computed") and etf.get("grayscale_url"):
                t=etf["ticker"]
                res=fetch_grayscale_holdings(session, etf["grayscale_url"], t)
                if res:
                    qty, as_of = res
                    holdings_today[t]=qty
                    u=etf["underlying"]
                    aum=qty*crypto_prices[u] if u in crypto_prices else None
                    issuer[t]={"holdings":qty,"aum":aum,"nav":nasdaq.get(t,{}).get("price"),"nav_date":as_of}
                    print(f"  ✓ {t} (Grayscale): holdings={qty:.2f}  AUM={fmt_aum(aum)}")
                else:
                    print(f"  ✗ {t}: không lấy được holdings từ Grayscale — fallback Farside cho ticker này")
                time.sleep(1.0)

    print("\n🔧 [4/4] Building output...")
    etfs=[]; totals={}
    self_computed_count=0; farside_count=0; cached_count=0
    for etf in ETF_REGISTRY:
        if etf.get("inactive"): continue
        t=etf["ticker"]; u=etf["underlying"]
        mkt=nasdaq.get(t) or {}; iss=issuer.get(t) or {}; prev=prev_etfs.get(t) or {}
        price=mkt.get("price")
        nav=iss.get("nav") or (prev.get("fund") or {}).get("nav")
        holdings=iss.get("holdings") or (prev.get("fund") or {}).get("holdings")
        aum=iss.get("aum")
        # Fallback AUM: live holdings × price
        if not aum and holdings and u in crypto_prices: aum=holdings*crypto_prices[u]
        # Fallback AUM: static on-chain × price (BTC ETFs)
        if not aum and u=="BTC" and t in STATIC_BTC_HOLDINGS:
            holdings=holdings or STATIC_BTC_HOLDINGS[t]
            aum=STATIC_BTC_HOLDINGS[t]*crypto_prices.get("BTC",0)
        # Fallback AUM: Nasdaq "Net Assets" (SOL/HYP/BNB — chưa có nguồn holdings
        # riêng nào như BTC/ETH, nên trước đây aum luôn None → hiện $0.00B).
        # Suy ngược holdings từ AUM/giá coin để premium & hiển thị holdings cũng có.
        if not aum:
            na=mkt.get("net_assets")
            if na:
                aum=na
                if not holdings and u in crypto_prices and crypto_prices[u]:
                    holdings=aum/crypto_prices[u]
        # Fallback AUM: cache
        if not aum: aum=(prev.get("fund") or {}).get("aum")

        premium={"usd":price-nav,"pct":(price-nav)/nav*100} if price and nav and nav>0 else None

        # ── Flow: ƯU TIÊN tự tính (Δholdings × giá coin) nếu có đủ dữ liệu, sau đó
        # mới fallback Farside, cuối cùng mới fallback cache cũ. Đây là migration
        # TỪNG BƯỚC khỏi Farside — ticker nào tự tính được thì KHÔNG còn phụ thuộc
        # Farside nữa, ticker nào chưa có nguồn xác nhận thì vẫn dùng Farside như cũ.
        flow=None
        if etf.get("self_computed") and RUN_MODE=="full":
            holdings_today_val = holdings_today.get(t)
            holdings_prev_val = (holdings_history.get(t) or {}).get("holdings")
            self_flow_usd = compute_self_flow(holdings_today_val, holdings_prev_val, crypto_prices.get(u))
            if self_flow_usd is not None:
                flow={"daily_usd":self_flow_usd,"is_inflow":self_flow_usd>0,"source":"self_computed","date":today_str}
                self_computed_count+=1
        if flow is None:
            flow_usd=daily_flows.get(t)
            if flow_usd is not None:
                flow={"daily_usd":flow_usd,"is_inflow":flow_usd>0,"source":"farside","date":today_str}
                farside_count+=1
            elif prev.get("flow"):
                flow=prev["flow"]
                cached_count+=1

        etfs.append({"ticker":t,"name":etf["name"],"issuer":etf["issuer"],"underlying":u,"fee":etf["fee"],
            "market":{"price":price,"change":mkt.get("change"),"change_pct":mkt.get("change_pct"),"volume":mkt.get("volume")} if mkt else None,
            "fund":{"nav":nav,"nav_date":iss.get("nav_date"),"shares":None,"aum":aum,"holdings":holdings,"premium":premium},
            "flow":flow,"onchain":None})
        totals.setdefault(u,{"aum":0.0,"flow":0.0,"count":0})
        totals[u]["aum"]+=aum or 0
        totals[u]["flow"]+=(flow or {}).get("daily_usd") or 0
        totals[u]["count"]+=1

    out={"etfs":etfs,"totals":totals,"run_mode":RUN_MODE,"fetched_at":now_utc.isoformat()}
    r2_put_json(r2,"etf-flows.json",out,"max-age=120")
    if RUN_MODE=="full": r2_put_json(r2,f"etf-history/{today_str}.json",out,"max-age=86400")

    # Lưu holdings hôm nay làm mốc "hôm qua" cho lần full chạy kế tiếp
    if RUN_MODE=="full" and holdings_today:
        for t,qty in holdings_today.items():
            holdings_history[t]={"date":today_str,"holdings":qty}
        save_holdings_history(r2, holdings_history)

    print("✅ Done")
    if RUN_MODE=="full":
        print(f"   Flow source: {self_computed_count} self-computed · {farside_count} farside · {cached_count} cached")
    for u,t in totals.items():
        s="+" if t["flow"]>=0 else ""
        aum_str=f"${t['aum']/1e9:.2f}B" if t["aum"]>=1e9 else f"${t['aum']/1e6:.2f}M"
        print(f"   {u}: AUM={aum_str}  Flow={s}${t['flow']/1e6:.1f}M  ({t['count']} ETFs)")

if __name__=="__main__":
    import time as _t; t0=_t.time()
    print(f"⚙️  ETF Fetcher v15 — RUN_MODE={RUN_MODE}")
    r2=get_r2(); run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
