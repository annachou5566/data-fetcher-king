"""
scripts/fetch_etf.py  v15
- Lấy TOÀN BỘ lịch sử từ Farside (tất cả ngày từ ngày ra mắt)
- Lưu vào R2: etf-flows.json (daily latest) + etf-farside-history.json (full history)
- Thêm HYP (Hyperliquid) từ farside.co.uk/hyp/
- AUM: iShares live cho IBIT/ETHA, static holdings cho BTC ETF còn lại
"""

import json, os, re, time
from datetime import datetime, timezone
from urllib.parse import quote

import boto3, cloudscraper, requests
from botocore.config import Config
try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

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
    {"ticker":"IBIT","name":"iShares Bitcoin Trust ETF","issuer":"BlackRock","underlying":"BTC","fee":0.25,"src":"ishares"},
    {"ticker":"FBTC","name":"Fidelity Wise Origin Bitcoin Fund","issuer":"Fidelity","underlying":"BTC","fee":0.25,"src":"nasdaq"},
    {"ticker":"GBTC","name":"Grayscale Bitcoin Trust ETF","issuer":"Grayscale","underlying":"BTC","fee":1.50,"src":"nasdaq"},
    {"ticker":"ARKB","name":"ARK 21Shares Bitcoin ETF","issuer":"ARK/21Shares","underlying":"BTC","fee":0.21,"src":"nasdaq"},
    {"ticker":"BITB","name":"Bitwise Bitcoin ETF","issuer":"Bitwise","underlying":"BTC","fee":0.20,"src":"nasdaq"},
    {"ticker":"HODL","name":"VanEck Bitcoin ETF","issuer":"VanEck","underlying":"BTC","fee":0.20,"src":"nasdaq"},
    {"ticker":"EZBC","name":"Franklin Bitcoin ETF","issuer":"Franklin","underlying":"BTC","fee":0.19,"src":"nasdaq"},
    {"ticker":"BRRR","name":"Valkyrie Bitcoin Fund","issuer":"Valkyrie","underlying":"BTC","fee":0.25,"src":"nasdaq"},
    {"ticker":"BTCO","name":"Invesco Galaxy Bitcoin ETF","issuer":"Invesco","underlying":"BTC","fee":0.25,"src":"nasdaq"},
    {"ticker":"BTCW","name":"WisdomTree Bitcoin Fund","issuer":"WisdomTree","underlying":"BTC","fee":0.25,"src":"nasdaq"},
    {"ticker":"ETHA","name":"iShares Ethereum Trust ETF","issuer":"BlackRock","underlying":"ETH","fee":0.25,"src":"ishares"},
    {"ticker":"FETH","name":"Fidelity Ethereum Fund","issuer":"Fidelity","underlying":"ETH","fee":0.25,"src":"nasdaq"},
    {"ticker":"ETHE","name":"Grayscale Ethereum Trust ETF","issuer":"Grayscale","underlying":"ETH","fee":2.50,"src":"nasdaq"},
    {"ticker":"ETHW","name":"Bitwise Ethereum ETF","issuer":"Bitwise","underlying":"ETH","fee":0.20,"src":"nasdaq"},
    {"ticker":"ETHV","name":"VanEck Ethereum ETF","issuer":"VanEck","underlying":"ETH","fee":0.20,"src":"nasdaq"},
    {"ticker":"CETH","name":"21Shares Core Ethereum ETF","issuer":"21Shares","underlying":"ETH","fee":0.21,"src":"nasdaq"},
    {"ticker":"EZET","name":"Franklin Ethereum ETF","issuer":"Franklin","underlying":"ETH","fee":0.19,"src":"nasdaq"},
    {"ticker":"QETH","name":"Invesco Galaxy Ethereum ETF","issuer":"Invesco","underlying":"ETH","fee":0.25,"src":"nasdaq"},
]
ETF_TICKERS = [e["ticker"] for e in ETF_REGISTRY]

def parse_num(v):
    if v is None or str(v).strip() in ("","N/A","--","null","None"): return None
    if isinstance(v,(int,float)): return float(v)
    s = re.sub(r"[$,%\s]","",str(v))
    try: return float(s)
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
        r=requests.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd",
            headers={"User-Agent":FAKE_UA},timeout=10)
        if r.status_code==200:
            d=r.json()
            if "bitcoin"  in d: prices["BTC"]=float(d["bitcoin"]["usd"])
            if "ethereum" in d: prices["ETH"]=float(d["ethereum"]["usd"])
    except Exception as e: print(f"  [Crypto] {e}")
    print(f"  [Crypto] BTC=${prices.get('BTC')}  ETH=${prices.get('ETH')}")
    return prices

def fetch_nasdaq_all(session):
    results={}
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
                if i == 0 or not hdr or i >= len(cells): continue
                ticker = hdr.strip().upper()
                if not ticker or ticker in ("FEE","SEED",""):
                    continue
                # "Total" → giữ nguyên
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

        # Lấy dòng mới nhất (cuối list sau khi đã reverse)
        latest = rows[-1]
        print(f"    Latest: {latest.get('date')} → ", end="")
        for ticker, val in latest.items():
            if ticker == "date": continue
            if val != 0:
                daily_latest[ticker] = val * 1_000_000  # $M → $
        print({k: v/1e6 for k,v in daily_latest.items() if v != 0})

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

# ── Pipeline ──────────────────────────────────────────────────────
def run(r2):
    now_utc=datetime.now(timezone.utc)
    today_str=now_utc.strftime("%Y-%m-%d")
    session=get_session()

    prev_etfs={e["ticker"]:e for e in (r2_get_json(r2,"etf-flows.json") or {}).get("etfs",[])}
    crypto_prices=load_crypto_prices()

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

    print("\n🏦 [3/4] iShares (IBIT + ETHA)...")
    issuer={}
    if RUN_MODE=="full":
        for etf_ticker,pid in ISHARES_IDS.items():
            u=next((e["underlying"] for e in ETF_REGISTRY if e["ticker"]==etf_ticker),"")
            raw=fetch_ishares(session,etf_ticker,pid,crypto_prices.get(u))
            if raw:
                nav=nasdaq.get(etf_ticker,{}).get("price")
                aum=raw.get("aum") or (raw["holdings"]*crypto_prices[u] if raw.get("holdings") and u in crypto_prices else None)
                issuer[etf_ticker]={**raw,"nav":nav,"aum":aum}
                print(f"  ✓ {etf_ticker}: AUM=${(aum or 0)/1e9:.2f}B  holdings={raw.get('holdings',0):.0f}")
            time.sleep(0.5)

    print("\n🔧 [4/4] Building output...")
    etfs=[]; totals={}
    for etf in ETF_REGISTRY:
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
        # Fallback AUM: cache
        if not aum: aum=(prev.get("fund") or {}).get("aum")

        premium={"usd":price-nav,"pct":(price-nav)/nav*100} if price and nav and nav>0 else None

        # Flow từ Farside hôm nay
        flow_usd=daily_flows.get(t)
        flow=None
        if flow_usd is not None:
            flow={"daily_usd":flow_usd,"is_inflow":flow_usd>0,"source":"farside","date":now_utc.strftime("%Y-%m-%d")}
        elif prev.get("flow"):
            flow=prev["flow"]

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
    print("✅ Done")
    for u,t in totals.items():
        s="+" if t["flow"]>=0 else ""
        print(f"   {u}: AUM=${t['aum']/1e9:.2f}B  Flow={s}${t['flow']/1e6:.1f}M  ({t['count']} ETFs)")

if __name__=="__main__":
    import time as _t; t0=_t.time()
    print(f"⚙️  ETF Fetcher v15 — RUN_MODE={RUN_MODE}")
    r2=get_r2(); run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
