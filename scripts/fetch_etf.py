"""
scripts/fetch_etf.py  v3
Fixes:
- iShares: dùng varnish-api endpoint thật (từ network tab)
- CF function: RENDER_API_KEY (fix tên env var)
- Crypto price: flexible parsing market-data.json
- ARK: thêm URL fallback
- Log đầy đủ response 200 chars đầu của iShares để debug structure
"""

import csv, io, json, os, re, time
from datetime import datetime, timezone

import boto3, cloudscraper, requests
from botocore.config import Config

RUN_MODE             = os.getenv("RUN_MODE", "full")
R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL      = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME       = os.getenv("R2_BUCKET_NAME")

FAKE_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
           "AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36")

ETF_REGISTRY = [
    {"ticker":"IBIT","name":"iShares Bitcoin Trust ETF","issuer":"BlackRock","underlying":"BTC","fee":0.25,"src":{"type":"ishares","product_id":"333011","slug":"ishares-bitcoin-trust-etf"}},
    {"ticker":"FBTC","name":"Fidelity Wise Origin Bitcoin Fund","issuer":"Fidelity","underlying":"BTC","fee":0.25,"src":{"type":"nasdaq_only"}},
    {"ticker":"GBTC","name":"Grayscale Bitcoin Trust ETF","issuer":"Grayscale","underlying":"BTC","fee":1.50,"src":{"type":"grayscale","slug":"bitcoin-trust-btc"}},
    {"ticker":"ARKB","name":"ARK 21Shares Bitcoin ETF","issuer":"ARK/21Shares","underlying":"BTC","fee":0.21,"src":{"type":"ark_csv","ticker":"ARKB"}},
    {"ticker":"BITB","name":"Bitwise Bitcoin ETF","issuer":"Bitwise","underlying":"BTC","fee":0.20,"src":{"type":"nasdaq_only"}},
    {"ticker":"HODL","name":"VanEck Bitcoin ETF","issuer":"VanEck","underlying":"BTC","fee":0.20,"src":{"type":"nasdaq_only"}},
    {"ticker":"EZBC","name":"Franklin Bitcoin ETF","issuer":"Franklin","underlying":"BTC","fee":0.19,"src":{"type":"franklin","ticker":"EZBC"}},
    {"ticker":"BRRR","name":"Valkyrie Bitcoin Fund","issuer":"Valkyrie","underlying":"BTC","fee":0.25,"src":{"type":"nasdaq_only"}},
    {"ticker":"BTCO","name":"Invesco Galaxy Bitcoin ETF","issuer":"Invesco","underlying":"BTC","fee":0.25,"src":{"type":"nasdaq_only"}},
    {"ticker":"BTCW","name":"WisdomTree Bitcoin Fund","issuer":"WisdomTree","underlying":"BTC","fee":0.25,"src":{"type":"nasdaq_only"}},
    {"ticker":"ETHA","name":"iShares Ethereum Trust ETF","issuer":"BlackRock","underlying":"ETH","fee":0.25,"src":{"type":"ishares","product_id":"333132","slug":"ishares-ethereum-trust-etf"}},
    {"ticker":"FETH","name":"Fidelity Ethereum Fund","issuer":"Fidelity","underlying":"ETH","fee":0.25,"src":{"type":"nasdaq_only"}},
    {"ticker":"ETHE","name":"Grayscale Ethereum Trust ETF","issuer":"Grayscale","underlying":"ETH","fee":2.50,"src":{"type":"grayscale","slug":"ethereum-trust-eth"}},
    {"ticker":"ETHW","name":"Bitwise Ethereum ETF","issuer":"Bitwise","underlying":"ETH","fee":0.20,"src":{"type":"nasdaq_only"}},
    {"ticker":"ETHV","name":"VanEck Ethereum ETF","issuer":"VanEck","underlying":"ETH","fee":0.20,"src":{"type":"nasdaq_only"}},
    {"ticker":"CETH","name":"21Shares Core Ethereum ETF","issuer":"21Shares","underlying":"ETH","fee":0.21,"src":{"type":"ark_csv","ticker":"CETH"}},
    {"ticker":"EZET","name":"Franklin Ethereum ETF","issuer":"Franklin","underlying":"ETH","fee":0.19,"src":{"type":"franklin","ticker":"EZET"}},
    {"ticker":"QETH","name":"Invesco Galaxy Ethereum ETF","issuer":"Invesco","underlying":"ETH","fee":0.25,"src":{"type":"nasdaq_only"}},
]
ETF_TICKERS = [e["ticker"] for e in ETF_REGISTRY]

# ── Helpers ───────────────────────────────────────────────────────
def parse_num(v):
    if v is None or str(v).strip() in ("","N/A","--","null"): return None
    if isinstance(v,(int,float)): return float(v)
    s = re.sub(r"[$,%\s]","",str(v))
    s = re.sub(r"B$","e9",s,flags=re.I); s = re.sub(r"M$","e6",s,flags=re.I)
    try: return float(s)
    except: return None

def get_session():
    s = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"windows","desktop":True})
    s.headers.update({"User-Agent":FAKE_UA,"Accept":"application/json, text/html, */*","Accept-Language":"en-US,en;q=0.9"})
    return s

def get_r2():
    return boto3.client("s3",endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"))

def r2_get_json(r2, key):
    try:
        resp = r2.get_object(Bucket=R2_BUCKET_NAME,Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"  [R2 GET {key}] {e}"); return None

def r2_put_json(r2, key, data, cc="max-age=120"):
    body = json.dumps(data,ensure_ascii=False,separators=(",",":")).encode("utf-8")
    r2.put_object(Bucket=R2_BUCKET_NAME,Key=key,Body=body,ContentType="application/json",CacheControl=cc)

# ── Crypto price từ R2 ────────────────────────────────────────────
def load_crypto_prices(r2):
    """Thử nhiều key R2 khác nhau để lấy BTC/ETH price"""
    prices = {}
    keys_to_try = ["market-data.json", "alpha-market-data.json", "snapshot.json", "prices.json"]

    for key in keys_to_try:
        data = r2_get_json(r2, key)
        if not data: continue
        print(f"  [Crypto] Found R2 key: {key}, type={type(data).__name__}")

        # Handle dict với field data/tokens/tickers
        if isinstance(data, dict):
            # Case 1: { BTCUSDT: {price:...}, ETHUSDT: {price:...} }
            for sym in ("BTCUSDT","BTC","btcusdt"):
                item = data.get(sym)
                if item and isinstance(item,dict):
                    p = parse_num(item.get("price") or item.get("lastPrice") or item.get("close") or item.get("c"))
                    if p: prices["BTC"] = p; break
            for sym in ("ETHUSDT","ETH","ethusdt"):
                item = data.get(sym)
                if item and isinstance(item,dict):
                    p = parse_num(item.get("price") or item.get("lastPrice") or item.get("close") or item.get("c"))
                    if p: prices["ETH"] = p; break
            # Case 2: { data: [...] } hoặc { tokens: [...] }
            items = data.get("data") or data.get("tokens") or data.get("tickers") or []
        elif isinstance(data, list):
            items = data
        else:
            items = []

        for item in (items if isinstance(items,list) else []):
            sym = str(item.get("symbol","") or item.get("s","")).upper()
            p   = parse_num(item.get("price") or item.get("lastPrice") or item.get("close") or item.get("c"))
            if not p: continue
            if sym in ("BTCUSDT","BTC") and "BTC" not in prices: prices["BTC"] = p
            if sym in ("ETHUSDT","ETH") and "ETH" not in prices: prices["ETH"] = p

        if prices:
            print(f"  [Crypto] BTC=${prices.get('BTC')}  ETH=${prices.get('ETH')}")
            return prices

    print("  [Crypto] Không tìm được BTC/ETH price từ R2")
    return prices

# ── Nasdaq ────────────────────────────────────────────────────────
def fetch_nasdaq_all(session):
    results = {}
    for ticker in ETF_TICKERS:
        try:
            r = session.get(
                f"https://api.nasdaq.com/api/quote/{ticker}/info?assetclass=etf",
                headers={"Referer":f"https://www.nasdaq.com/market-activity/funds-and-etfs/{ticker.lower()}"},
                timeout=12)
            print(f"  Nasdaq {ticker}: HTTP {r.status_code}", end="")
            if r.status_code != 200: print(); continue
            d       = r.json().get("data") or {}
            primary = d.get("primaryData") or {}
            summary = d.get("summaryData") or {}
            def sv(k): return (summary.get(k) or {}).get("value")
            price      = parse_num(primary.get("lastSalePrice"))
            change     = parse_num(primary.get("netChange"))
            change_pct = parse_num((primary.get("percentageChange") or "").replace("%",""))
            volume     = parse_num((primary.get("volume") or "").replace(",",""))
            nav        = parse_num(sv("Nav") or sv("NAV") or sv("Net Asset Value"))
            shares     = parse_num((sv("Shares Outstanding") or "").replace(",",""))
            aum_raw    = sv("Total Net Assets") or ""
            aum = None
            if aum_raw and aum_raw not in ("N/A","--"):
                aum = parse_num(re.sub(r"[^0-9\.]","",aum_raw)) * (1e9 if "B" in aum_raw.upper() else 1e6 if "M" in aum_raw.upper() else 1)
            results[ticker] = {"price":price,"nav":nav,"shares":shares,"aum":aum,"change":change,"change_pct":change_pct,"volume":volume}
            print(f"  price=${price}  nav={nav}  shares={shares}")
        except Exception as e:
            print(f"\n  ✗ Nasdaq {ticker}: {e}")
        time.sleep(0.35)
    return results

# ── iShares varnish-api (URL thật từ network tab) ─────────────────
def fetch_ishares(session, product_id, ticker, slug):
    BASE_PARAMS = (
        f"appSubType=ISHARES&appType=PRODUCT_PAGE"
        f"&locale=en_US&portfolioId={product_id}"
        f"&targetSite=us-ishares&userType=individual"
        f"&excludeContent=true&includeConfig=true"
    )
    VARNISH = "https://www.ishares.com/varnish-api/blk-one01-product-data/product-data/api/v2/get-product-data"
    referer = f"https://www.ishares.com/us/products/{product_id}/{slug}"
    hdrs    = {"Referer":referer,"Accept":"application/json, */*","User-Agent":FAKE_UA}

    nav, shares, aum, nav_date, holdings = None, None, None, None, None

    # ── Overview: NAV, shares, AUM ────────────────────────────────
    try:
        url = f"{VARNISH}?component=overview&{BASE_PARAMS}"
        r   = session.get(url, headers=hdrs, timeout=15)
        print(f"  iShares overview {ticker}: HTTP {r.status_code}")
        if r.status_code == 200:
            txt = r.text.strip()
            print(f"    response[:200]: {txt[:200]}")
            if txt.startswith("{") or txt.startswith("["):
                data = r.json()
                # Log toàn bộ keys ở level cao nhất để hiểu structure
                print(f"    top-level keys: {list(data.keys()) if isinstance(data,dict) else 'list'}")
                # Tìm giá trị trong mọi vị trí có thể
                flat = json.dumps(data)
                m_nav    = re.search(r'"navAmount"\s*:\s*\{[^}]*"raw"\s*:\s*([\d\.]+)',flat)
                m_shares = re.search(r'"sharesOutstanding"\s*:\s*\{[^}]*"raw"\s*:\s*([\d\.]+)',flat)
                m_aum    = re.search(r'"netAssets"\s*:\s*\{[^}]*"raw"\s*:\s*([\d\.]+)',flat)
                m_date   = re.search(r'"navDate"\s*:\s*"([^"]+)"',flat)
                nav      = parse_num(m_nav.group(1))    if m_nav    else None
                shares   = parse_num(m_shares.group(1)) if m_shares else None
                aum      = parse_num(m_aum.group(1))    if m_aum    else None
                nav_date = m_date.group(1)               if m_date   else None
                print(f"    → nav={nav}  shares={shares}  aum={aum}  date={nav_date}")
    except Exception as e:
        print(f"    overview error: {e}")

    # ── Holdings: số BTC/ETH ──────────────────────────────────────
    try:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        url   = f"{VARNISH}?component=holdings.all&asOfDate={today}&{BASE_PARAMS}"
        r     = session.get(url, headers=hdrs, timeout=15)
        print(f"  iShares holdings {ticker}: HTTP {r.status_code}")
        if r.status_code == 200:
            txt = r.text.strip()
            print(f"    response[:200]: {txt[:200]}")
            if txt.startswith("{") or txt.startswith("["):
                data = r.json()
                flat = json.dumps(data)
                # Tìm số shares của holding đầu tiên (BTC/ETH)
                m = re.search(r'"shares"\s*:\s*\{[^}]*"raw"\s*:\s*([\d\.]+)',flat)
                if not m:
                    m = re.search(r'"weight"\s*:\s*\{[^}]*"raw"\s*:\s*9[0-9\.]',flat)  # weight ~99%
                # Thử tìm trong rows/data array
                rows = (data.get("data") or {}).get("tableData",{}).get("rows") or \
                       data.get("rows") or data.get("holdings") or []
                if rows:
                    row0 = rows[0] if isinstance(rows,list) else {}
                    print(f"    first holding keys: {list(row0.keys())[:8]}")
                    sh = row0.get("shares") or row0.get("quantity") or row0.get("sharesHeld")
                    if isinstance(sh,dict): sh = sh.get("raw") or sh.get("value")
                    holdings = parse_num(sh)
                if not holdings and m:
                    holdings = parse_num(m.group(1))
                print(f"    → holdings={holdings}")
    except Exception as e:
        print(f"    holdings error: {e}")

    if any([nav, shares, aum, holdings]):
        return {"nav":nav,"shares":shares,"aum":aum,"nav_date":nav_date,"holdings":holdings}
    return None

# ── ARK/21Shares CSV ──────────────────────────────────────────────
ARK_CSV_URLS = {
    "ARKB": [
        "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_21SHARES_BITCOIN_ETF_ARKB_HOLDINGS.csv",
        "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_21SHARES_BITCOIN_ETF_HOLDINGS.csv",
        "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARKB_HOLDINGS.csv",
    ],
    "CETH": [
        "https://ark-funds.com/wp-content/uploads/funds-etf-csv/21SHARES_CORE_ETHEREUM_ETF_CETH_HOLDINGS.csv",
        "https://ark-funds.com/wp-content/uploads/funds-etf-csv/21SHARES_CORE_ETHEREUM_ETF_HOLDINGS.csv",
        "https://ark-funds.com/wp-content/uploads/funds-etf-csv/CETH_HOLDINGS.csv",
    ],
}

def fetch_ark_csv(ticker):
    for url in ARK_CSV_URLS.get(ticker, []):
        try:
            r = requests.get(url,headers={"User-Agent":FAKE_UA},timeout=12)
            print(f"  ARK {ticker}: HTTP {r.status_code} {url[-50:]}")
            if r.status_code != 200: continue
            if r.text.strip().startswith("<"): print("    → HTML response, skip"); continue
            lines = r.text.strip().split("\n")
            print(f"    → {len(lines)} lines, first: {lines[0][:80]}")
            # Tìm header row
            hdr_idx = 0
            for i,ln in enumerate(lines[:5]):
                if "date" in ln.lower() and ("shares" in ln.lower() or "ticker" in ln.lower()):
                    hdr_idx = i; break
            reader = csv.DictReader(io.StringIO("\n".join(lines[hdr_idx:])))
            rows   = [row for row in reader if any(v.strip() for v in row.values())]
            if not rows: continue
            print(f"    → headers: {list(rows[0].keys())}")
            row = rows[0]
            hlow = {k.lower().strip():k for k in row}
            def g(pats):
                for p in pats:
                    for kl,ko in hlow.items():
                        if p in kl:
                            v = row.get(ko,"").strip()
                            if v and v not in ("","N/A"): return v
                return None
            holdings = parse_num(g(["shares"]))
            aum      = parse_num((g(["market value"]) or "").replace(",",""))
            nav_date = g(["date"])
            print(f"    → holdings={holdings}  aum={aum}")
            return {"holdings":holdings,"aum":aum,"nav_date":nav_date}
        except Exception as e:
            print(f"    error: {e}")
    return None

# ── Franklin CSV ──────────────────────────────────────────────────
FRANKLIN_URLS = {
    "EZBC": [
        "https://www.franklintempleton.com/content-en_US/cms/assets/fund-resources/EZBC-holdings.csv",
        "https://www.franklintempleton.com/api/products/funds/EZBC/holdings",
    ],
    "EZET": [
        "https://www.franklintempleton.com/content-en_US/cms/assets/fund-resources/EZET-holdings.csv",
        "https://www.franklintempleton.com/api/products/funds/EZET/holdings",
    ],
}

def fetch_franklin(ticker):
    for url in FRANKLIN_URLS.get(ticker, []):
        try:
            r = requests.get(url,headers={"User-Agent":FAKE_UA},timeout=12)
            print(f"  Franklin {ticker}: HTTP {r.status_code} {url[-50:]}")
            if r.status_code != 200: continue
            txt = r.text.strip()
            if txt.startswith("<"): print(f"    → HTML, skip"); continue
            print(f"    first 200: {txt[:200]}")
            # JSON response
            if txt.startswith("{") or txt.startswith("["):
                data = r.json() if not isinstance(r.json(),str) else {}
                flat = json.dumps(data)
                m_holdings = re.search(r'"(?:shares|quantity|units)"\s*:\s*([\d\.]+)',flat,re.I)
                m_aum      = re.search(r'"(?:marketValue|market_value|value)"\s*:\s*([\d\.]+)',flat,re.I)
                return {"holdings":parse_num(m_holdings.group(1)) if m_holdings else None,
                        "aum":parse_num(m_aum.group(1)) if m_aum else None,
                        "nav_date":datetime.now(timezone.utc).strftime("%Y-%m-%d")}
            # CSV response
            lines  = txt.split("\n")
            hdr_idx = 0
            for i,ln in enumerate(lines[:10]):
                if "shares" in ln.lower() or "quantity" in ln.lower():
                    hdr_idx = i; break
            reader = csv.DictReader(io.StringIO("\n".join(lines[hdr_idx:])))
            rows   = [row for row in reader if any(v.strip() for v in row.values())]
            if not rows: continue
            row  = rows[0]; hlow = {k.lower().strip():k for k in row}
            def g(pats):
                for p in pats:
                    for kl,ko in hlow.items():
                        if p in kl:
                            v = row.get(ko,"").strip()
                            if v and v not in ("","N/A"): return v
                return None
            holdings = parse_num(g(["shares","quantity","units"]))
            aum      = parse_num((g(["market value","mkt val"]) or "").replace(",",""))
            print(f"    → holdings={holdings}  aum={aum}")
            return {"holdings":holdings,"aum":aum,"nav_date":datetime.now(timezone.utc).strftime("%Y-%m-%d")}
        except Exception as e:
            print(f"    error: {e}")
    return None

# ── Grayscale ─────────────────────────────────────────────────────
def fetch_grayscale(session, slug):
    try:
        r = session.get(f"https://www.grayscale.com/funds/{slug}",timeout=15)
        print(f"  Grayscale {slug}: HTTP {r.status_code}")
        if r.status_code != 200: return None
        flat = r.text
        m_nav    = re.search(r'"nav[_\s]?(?:per[_\s]?share)?"\s*:\s*"?([\d,\.]+)"?',flat,re.I)
        m_aum    = re.search(r'"(?:aum|net[_\s]?assets?)"\s*:\s*"?([\d,\.]+)"?',flat,re.I)
        m_shares = re.search(r'"shares[_\s]?outstanding"\s*:\s*"?([\d,]+)"?',flat,re.I)
        result   = {"nav":parse_num(m_nav.group(1)) if m_nav else None,
                    "aum":parse_num(m_aum.group(1)) if m_aum else None,
                    "shares":parse_num(m_shares.group(1)) if m_shares else None}
        print(f"    → {result}")
        return result if any(result.values()) else None
    except Exception as e:
        print(f"  ✗ Grayscale {slug}: {e}"); return None

# ── Pipeline ──────────────────────────────────────────────────────
def run(r2):
    now_utc   = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    session   = get_session()

    prev_etfs     = {e["ticker"]:e for e in (r2_get_json(r2,"etf-flows.json") or {}).get("etfs",[])}
    crypto_prices = load_crypto_prices(r2)

    print("\n📈 [1/3] Nasdaq prices...")
    nasdaq = fetch_nasdaq_all(session)
    print(f"  → price OK: {sum(1 for v in nasdaq.values() if v.get('price'))} tickers")

    issuer = {}
    if RUN_MODE == "full":
        print("\n🏦 [2/3] Issuer fund data...")
        for etf in ETF_REGISTRY:
            t = etf["ticker"]; src = etf["src"]; data = None
            if src["type"] == "ishares":
                data = fetch_ishares(session, src["product_id"], t, src["slug"])
            elif src["type"] == "ark_csv":
                raw = fetch_ark_csv(t)
                if raw:
                    nav = nasdaq.get(t,{}).get("price")
                    data = {"holdings":raw["holdings"],"aum":raw["aum"],"nav":nav,"nav_date":raw["nav_date"]}
            elif src["type"] == "franklin":
                raw = fetch_franklin(src["ticker"])
                if raw:
                    nav = nasdaq.get(t,{}).get("price")
                    data = {"holdings":raw["holdings"],"aum":raw["aum"],"nav":nav,"nav_date":raw["nav_date"]}
            elif src["type"] == "grayscale":
                data = fetch_grayscale(session, src["slug"])
            if data: issuer[t] = data
            time.sleep(0.5)
        print(f"\n  → issuer data: {list(issuer.keys()) or 'NONE'}")
    else:
        print("\n⏭️  Skip issuer (RUN_MODE=price)")

    print("\n🔧 Building output...")
    etfs = []; totals = {}
    for etf in ETF_REGISTRY:
        t   = etf["ticker"]; u = etf["underlying"]
        mkt = nasdaq.get(t) or {}
        iss = issuer.get(t) or {}
        prev = prev_etfs.get(t) or {}

        nav      = iss.get("nav")    or mkt.get("nav")    or (prev.get("fund") or {}).get("nav")
        shares   = iss.get("shares") or mkt.get("shares") or (prev.get("fund") or {}).get("shares")
        holdings = iss.get("holdings")                     or (prev.get("fund") or {}).get("holdings")
        aum      = iss.get("aum")    or mkt.get("aum")
        if not aum and holdings and u in crypto_prices:
            aum = holdings * crypto_prices[u]
            print(f"  {t}: AUM computed = {holdings:.2f} × ${crypto_prices[u]:,.0f} = ${aum:,.0f}")
        if not aum: aum = (prev.get("fund") or {}).get("aum")

        price   = mkt.get("price")
        premium = {"usd":price-nav,"pct":(price-nav)/nav*100} if price and nav and nav>0 else None
        flow    = None
        ps      = (prev.get("fund") or {}).get("shares")
        if shares and ps and nav and shares != ps:
            d = shares - ps
            flow = {"daily_usd":d*nav,"delta_shares":d,"is_inflow":d>0,"computed_at":now_utc.isoformat()}
        if not flow and prev.get("flow"): flow = prev["flow"]

        etfs.append({"ticker":t,"name":etf["name"],"issuer":etf["issuer"],"underlying":u,"fee":etf["fee"],
            "market":{"price":price,"change":mkt.get("change"),"change_pct":mkt.get("change_pct"),"volume":mkt.get("volume")} if mkt else None,
            "fund":{"nav":nav,"nav_date":iss.get("nav_date"),"shares":shares,"aum":aum,"holdings":holdings,"premium":premium},
            "flow":flow,"onchain":None})
        totals.setdefault(u,{"aum":0.0,"flow":0.0,"count":0})
        totals[u]["aum"]   += aum or 0
        totals[u]["flow"]  += (flow or {}).get("daily_usd") or 0
        totals[u]["count"] += 1

    out = {"etfs":etfs,"totals":totals,"run_mode":RUN_MODE,"fetched_at":now_utc.isoformat()}
    print("\n☁️  Uploading...")
    r2_put_json(r2,"etf-flows.json",out,"max-age=120")
    if RUN_MODE == "full":
        r2_put_json(r2,f"etf-history/{today_str}.json",out,"max-age=86400")
    print("✅ Done")
    for u,t in totals.items():
        print(f"   {u}: AUM=${t['aum']/1e9:.2f}B  Flow=${t['flow']/1e6:.1f}M  ({t['count']} ETFs)")

if __name__ == "__main__":
    import time as _t; t0=_t.time()
    print(f"⚙️  ETF Fetcher v3 — RUN_MODE={RUN_MODE}")
    r2 = get_r2()
    run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
