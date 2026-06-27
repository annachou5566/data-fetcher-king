"""
scripts/fetch_etf.py  v13
Thêm Farside daily flow scraper — fill toàn bộ flow column cho BTC + ETH ETFs.
IBIT/ETHA AUM vẫn từ iShares (confirmed working).
Các ETF khác: price từ Nasdaq + flow từ Farside = đủ dùng.
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
    "IBIT": "333011",          # confirmed ✅
    "ETHA": ETHA_PRODUCT_ID_ENV or "337614",  # confirmed ✅
}

# Farside URL per asset
FARSIDE_URLS = {
    "BTC": [
        "https://farside.co.uk/btc/",
        "https://farside.co.uk/bitcoin-etf-flow-all-data/",
    ],
    "ETH": [
        "https://farside.co.uk/eth/",
        "https://farside.co.uk/eth-etf-flow-all-data/",
    ],
    "SOL": [
        "https://farside.co.uk/sol/",
        "https://farside.co.uk/solana-etf-flow-all-data/",
    ],
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

def r2_get_json(r2, key):
    try:
        resp = r2.get_object(Bucket=R2_BUCKET_NAME,Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except: return None

def r2_put_json(r2, key, data, cc="max-age=120"):
    body = json.dumps(data,ensure_ascii=False,separators=(",",":")).encode("utf-8")
    r2.put_object(Bucket=R2_BUCKET_NAME,Key=key,Body=body,ContentType="application/json",CacheControl=cc)

def load_crypto_prices():
    prices = {}
    try:
        r = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd",
            headers={"User-Agent":FAKE_UA},timeout=10)
        if r.status_code == 200:
            d = r.json()
            if "bitcoin"  in d: prices["BTC"] = float(d["bitcoin"]["usd"])
            if "ethereum" in d: prices["ETH"] = float(d["ethereum"]["usd"])
    except Exception as e: print(f"  [Crypto] {e}")
    print(f"  [Crypto] BTC=${prices.get('BTC')}  ETH=${prices.get('ETH')}")
    return prices

def fetch_nasdaq_all(session):
    results = {}
    for ticker in ETF_TICKERS:
        try:
            r = session.get(f"https://api.nasdaq.com/api/quote/{ticker}/info?assetclass=etf",
                headers={"Referer":f"https://www.nasdaq.com/market-activity/funds-and-etfs/{ticker.lower()}"},timeout=12)
            print(f"  Nasdaq {ticker}: {r.status_code}",end="")
            if r.status_code != 200: print(); continue
            d = r.json().get("data") or {}
            p = d.get("primaryData") or {}
            price = parse_num(p.get("lastSalePrice"))
            results[ticker] = {"price":price,"change":parse_num(p.get("netChange")),
                "change_pct":parse_num((p.get("percentageChange") or "").replace("%","")),
                "volume":parse_num((p.get("volume") or "").replace(",",""))}
            print(f"  ${price}")
        except Exception as e: print(f"\n  ✗ {ticker}: {e}")
        time.sleep(0.3)
    return results

# ── FARSIDE SCRAPER ───────────────────────────────────────────────
def fetch_farside_html(url):
    """Thử trực tiếp rồi fallback AllOrigins (bypass Cloudflare)"""
    # Thử 1: cloudscraper trực tiếp
    try:
        s = cloudscraper.create_scraper()
        r = s.get(url, headers={"User-Agent":FAKE_UA}, timeout=20)
        if r.status_code == 200 and len(r.text) > 3000:
            print(f"    Direct OK ({len(r.text)} chars)")
            return r.text
    except Exception as e:
        print(f"    Direct failed: {e}")

    # Thử 2: AllOrigins proxy (giống JS scraper của user)
    try:
        proxy = f"https://api.allorigins.win/get?url={quote(url)}"
        r = requests.get(proxy, timeout=25)
        if r.status_code == 200:
            html = r.json().get("contents","")
            if html and len(html) > 3000:
                print(f"    AllOrigins OK ({len(html)} chars)")
                return html
    except Exception as e:
        print(f"    AllOrigins failed: {e}")

    return None

def parse_farside_table(html, asset):
    """
    Parse Farside HTML table → dict { TICKER: flow_usd_today }
    Values on Farside are in $M (millions USD).
    """
    if not html:
        return {}

    flows = {}

    if HAS_BS4:
        # Dùng BeautifulSoup nếu có
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Tìm bảng có chứa keyword của asset
        keywords = {"BTC":["IBIT","FBTC"],"ETH":["ETHA","FETH"],"SOL":["BSOL","VSOL"]}
        target_kws = keywords.get(asset, [])

        best_table = None
        for table in soup.find_all("table"):
            text = table.get_text().upper()
            if any(k in text for k in target_kws):
                best_table = table
                break

        if not best_table:
            print(f"    ✗ No table found for {asset}")
            return {}

        rows = best_table.find_all("tr")
        if len(rows) < 2:
            return {}

        # Header row: tìm dòng có tên cột ETF
        headers = []
        header_idx = 0
        for i, row in enumerate(rows):
            cells = [c.get_text().strip() for c in row.find_all(["th","td"])]
            if any(k in " ".join(cells).upper() for k in target_kws):
                headers = cells
                header_idx = i
                break

        if not headers:
            return {}

        # Lấy dòng data mới nhất (dòng cuối cùng có ngày tháng)
        latest_row = None
        for row in reversed(rows[header_idx+1:]):
            cells = [c.get_text().strip() for c in row.find_all("td")]
            if not cells:
                continue
            first = cells[0]
            # Kiểm tra dòng có ngày tháng (dd Mon, dd Mon yyyy)
            if re.match(r"^\d{1,2}\s+[A-Za-z]{3}", first):
                latest_row = cells
                latest_date = first
                break

        if not latest_row:
            return {}

        print(f"    Latest row date: {latest_date}")
        print(f"    Headers: {headers[:8]}")

        # Map header → value
        for i, header in enumerate(headers):
            if i >= len(latest_row) or i == 0:
                continue
            ticker = header.strip().upper()
            if not ticker or ticker in ("TOTAL","DATE",""):
                continue
            val_str = latest_row[i].replace(",","").strip()
            # Handle negatives in parentheses: (12.5) → -12.5
            if val_str.startswith("(") and val_str.endswith(")"):
                val = -abs(float(val_str[1:-1])) if val_str[1:-1] else 0
            else:
                val = float(val_str) if val_str and val_str != "-" else 0
            # Convert $M → $
            flows[ticker] = val * 1_000_000

        print(f"    Flows extracted: { {k: v/1e6 for k,v in flows.items()} }")

    else:
        # Fallback regex nếu không có bs4
        print("    Warning: beautifulsoup4 not installed, using regex fallback")
        # Tìm tất cả dòng <tr>
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.I)
        if not rows:
            return {}

        # Extract text từ cells
        def get_cells(row_html):
            cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.DOTALL | re.I)
            return [re.sub(r"<[^>]+>","",c).strip() for c in cells]

        keywords = {"BTC":["IBIT","FBTC"],"ETH":["ETHA","FETH"],"SOL":["BSOL","VSOL"]}
        target_kws = keywords.get(asset, [])

        headers = []
        header_idx = -1
        for i, row in enumerate(rows):
            cells = get_cells(row)
            if any(k in " ".join(cells).upper() for k in target_kws):
                headers = [c.upper() for c in cells]
                header_idx = i
                break

        if header_idx == -1:
            return {}

        # Dòng data mới nhất
        latest = None
        for row in reversed(rows[header_idx+1:]):
            cells = get_cells(row)
            if cells and re.match(r"^\d{1,2}\s+[A-Za-z]{3}", cells[0]):
                latest = cells
                break

        if not latest:
            return {}

        for i, hdr in enumerate(headers):
            if i == 0 or i >= len(latest) or hdr in ("TOTAL","DATE",""):
                continue
            val_str = latest[i].replace(",","").strip()
            if val_str.startswith("(") and val_str.endswith(")"):
                val = -abs(float(val_str[1:-1])) if val_str[1:-1] else 0.0
            else:
                try: val = float(val_str)
                except: val = 0.0
            flows[hdr] = val * 1_000_000

    return flows

def fetch_farside_all(session):
    """
    Lấy daily flow từ Farside cho BTC + ETH (+ SOL nếu được).
    Return: { IBIT: flow_usd, FBTC: flow_usd, ... }
    """
    all_flows = {}
    for asset, urls in FARSIDE_URLS.items():
        print(f"\n  Farside {asset}:")
        html = None
        for url in urls:
            print(f"    Trying {url}")
            html = fetch_farside_html(url)
            if html:
                break
        if not html:
            print(f"    ✗ Could not fetch {asset}")
            continue
        flows = parse_farside_table(html, asset)
        if flows:
            all_flows.update(flows)
            print(f"    ✓ Got {len(flows)} ETF flows for {asset}")
        else:
            print(f"    ✗ No flows parsed for {asset}")

    return all_flows

# ── iShares ───────────────────────────────────────────────────────
VARNISH = ("https://www.ishares.com/varnish-api/blk-one01-product-data"
           "/product-data/api/v2/get-product-data")

def _url(pid, excl, incl, as_of=None):
    p = (f"component=holdings.all&portfolioId={pid}"
         f"&appSubType=ISHARES&appType=PRODUCT_PAGE"
         f"&locale=en_US&targetSite=us-ishares&userType=individual"
         f"&excludeContent={'true' if excl else 'false'}"
         f"&includeConfig={'true' if incl else 'false'}")
    if as_of: p += f"&asOfDate={as_of}"
    return f"{VARNISH}?{p}"

def fetch_ishares(session, ticker, product_id, crypto_price=None):
    hdrs = {"Referer":f"https://www.ishares.com/us/products/{product_id}/",
             "Accept":"application/json,*/*","User-Agent":FAKE_UA}
    latest_date = None
    try:
        r = session.get(_url(product_id,True,True),headers=hdrs,timeout=15)
        if r.status_code != 200: return None
        d = r.json()
        name = d.get("fundName","")
        if ticker == "ETHA" and "ethereum" not in name.lower(): return None
        comp = (d.get("componentsByNameMap") or {}).get("holdings",{})
        cont = (comp.get("containersByNameMap") or {}).get("all",{})
        dmap = cont.get("dataPointsByNameMap",{})
        dates = dmap.get("dateList",{}).get("value") or []
        if dates: latest_date = str(dates[0])
    except Exception as e:
        print(f"    config error: {e}"); return None

    try:
        r = session.get(_url(product_id,False,False,as_of=latest_date),headers=hdrs,timeout=20)
        if r.status_code != 200: return None
        d = r.json()
        comp = (d.get("componentsByNameMap") or {}).get("holdings",{})
        cont = (comp.get("containersByNameMap") or {}).get("all",{})
        dmap = cont.get("dataPointsByNameMap",{})
        mv = dmap.get("marketValue",{}).get("value",[])
        aum = max((v for v in mv if isinstance(v,(int,float)) and v>0),default=None)
        holdings = None
        for key in ["unitsHeld","sharesHeld","quantity"]:
            arr = dmap.get(key,{}).get("value",[])
            if arr:
                h = parse_num(arr[0] if isinstance(arr,list) else arr)
                if h and 100 < h < 1_000_000_000: holdings = h; break
        if not holdings and aum and crypto_price and crypto_price > 0:
            holdings = aum / crypto_price
        ao = dmap.get("asOfDate",{}).get("value")
        if aum or holdings:
            return {"aum":aum,"holdings":holdings,"nav_date":str(ao) if ao else latest_date}
        return None
    except Exception as e:
        print(f"    data error: {e}"); return None

# ── Pipeline ──────────────────────────────────────────────────────
def run(r2):
    now_utc   = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    session   = get_session()

    prev_etfs = {e["ticker"]:e for e in (r2_get_json(r2,"etf-flows.json") or {}).get("etfs",[])}
    crypto_prices = load_crypto_prices()

    print("\n📈 [1/4] Nasdaq prices...")
    nasdaq = fetch_nasdaq_all(session)
    print(f"  → {sum(1 for v in nasdaq.values() if v.get('price'))} tickers")

    print("\n📊 [2/4] Farside daily flows...")
    farside_flows = {}
    if RUN_MODE == "full":
        farside_flows = fetch_farside_all(session)
        print(f"\n  → Farside total: {len(farside_flows)} tickers with flow data")
    else:
        print("  Skip (RUN_MODE=price)")

    print("\n🏦 [3/4] iShares fund data (IBIT + ETHA)...")
    issuer = {}
    if RUN_MODE == "full":
        for etf_ticker, pid in ISHARES_IDS.items():
            etf_meta = next((e for e in ETF_REGISTRY if e["ticker"]==etf_ticker),{})
            u = etf_meta.get("underlying","")
            raw = fetch_ishares(session, etf_ticker, pid, crypto_prices.get(u))
            if raw:
                nav = nasdaq.get(etf_ticker,{}).get("price")
                aum = raw.get("aum") or (raw["holdings"]*crypto_prices[u] if raw.get("holdings") and u in crypto_prices else None)
                issuer[etf_ticker] = {**raw,"nav":nav,"aum":aum}
                print(f"  ✓ {etf_ticker}: AUM=${(aum or 0)/1e9:.2f}B  holdings={raw.get('holdings',0):.0f}")
            time.sleep(0.5)

    print("\n🔧 [4/4] Building output...")
    etfs=[]; totals={}
    for etf in ETF_REGISTRY:
        t   = etf["ticker"]
        u   = etf["underlying"]
        mkt = nasdaq.get(t) or {}
        iss = issuer.get(t) or {}
        prev = prev_etfs.get(t) or {}

        price    = mkt.get("price")
        nav      = iss.get("nav") or (prev.get("fund") or {}).get("nav")
        holdings = iss.get("holdings") or (prev.get("fund") or {}).get("holdings")
        aum      = iss.get("aum")
        if not aum and holdings and u in crypto_prices: aum = holdings*crypto_prices[u]
        if not aum: aum = (prev.get("fund") or {}).get("aum")

        premium = {"usd":price-nav,"pct":(price-nav)/nav*100} if price and nav and nav>0 else None

        # Flow từ Farside (hôm nay)
        flow_today_m = farside_flows.get(t)  # in USD (đã convert từ $M)
        flow = None
        if flow_today_m is not None:
            flow = {
                "daily_usd":   flow_today_m,
                "is_inflow":   flow_today_m > 0,
                "source":      "farside",
                "computed_at": now_utc.isoformat(),
            }
        elif prev.get("flow"):
            flow = prev["flow"]  # giữ flow ngày hôm qua

        etfs.append({
            "ticker":t,"name":etf["name"],"issuer":etf["issuer"],
            "underlying":u,"fee":etf["fee"],
            "market":{"price":price,"change":mkt.get("change"),"change_pct":mkt.get("change_pct"),"volume":mkt.get("volume")} if mkt else None,
            "fund":{"nav":nav,"nav_date":iss.get("nav_date"),"shares":None,"aum":aum,"holdings":holdings,"premium":premium},
            "flow":flow,"onchain":None,
        })
        totals.setdefault(u,{"aum":0.0,"flow":0.0,"count":0})
        totals[u]["aum"]   += aum or 0
        totals[u]["flow"]  += flow_today_m or 0
        totals[u]["count"] += 1

    out = {"etfs":etfs,"totals":totals,"run_mode":RUN_MODE,"fetched_at":now_utc.isoformat()}
    r2_put_json(r2,"etf-flows.json",out,"max-age=120")
    if RUN_MODE=="full":
        r2_put_json(r2,f"etf-history/{today_str}.json",out,"max-age=86400")
    print("✅ Done")
    for u,t in totals.items():
        s = "+" if t["flow"]>=0 else ""
        print(f"   {u}: AUM=${t['aum']/1e9:.2f}B  Flow={s}${t['flow']/1e6:.1f}M  ({t['count']} ETFs)")

if __name__=="__main__":
    import time as _t; t0=_t.time()
    print(f"⚙️  ETF Fetcher v13 — RUN_MODE={RUN_MODE}")
    r2=get_r2(); run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
