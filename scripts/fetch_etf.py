"""
scripts/fetch_etf.py  v6
Fix từ log v5:
- excludeContent=true → false: lý do value=null trong mọi field
- asOfDate: lấy từ dateList[0] thay vì hardcode today (today có thể chưa có data)
- Navigate đúng: dataPointsByNameMap.sharesHeld/holdingShares + subContainersByNameMap
- ETHA: search bằng ticker qua iShares screener API
- CoinGecko đã OK, giữ nguyên
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

ISHARES_IDS = {
    "IBIT": "333011",  # confirmed ✅
    "ETHA": None,      # sẽ discover
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
    return boto3.client("s3", endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID, aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"))

def r2_get_json(r2, key):
    try:
        resp = r2.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"  [R2 GET {key}] {e}"); return None

def r2_put_json(r2, key, data, cc="max-age=120"):
    body = json.dumps(data, ensure_ascii=False, separators=(",",":")).encode("utf-8")
    r2.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=body,
                  ContentType="application/json", CacheControl=cc)

# ── Crypto prices ─────────────────────────────────────────────────
def load_crypto_prices():
    prices = {}
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd",
            headers={"User-Agent": FAKE_UA}, timeout=10)
        if r.status_code == 200:
            d = r.json()
            if "bitcoin"  in d: prices["BTC"] = float(d["bitcoin"]["usd"])
            if "ethereum" in d: prices["ETH"] = float(d["ethereum"]["usd"])
    except Exception as e:
        print(f"  [Crypto] CoinGecko error: {e}")
    print(f"  [Crypto] BTC=${prices.get('BTC')}  ETH=${prices.get('ETH')}")
    return prices

# ── Nasdaq prices ─────────────────────────────────────────────────
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
            price      = parse_num(primary.get("lastSalePrice"))
            change     = parse_num(primary.get("netChange"))
            change_pct = parse_num((primary.get("percentageChange") or "").replace("%",""))
            volume     = parse_num((primary.get("volume") or "").replace(",",""))
            results[ticker] = {"price":price,"change":change,"change_pct":change_pct,"volume":volume}
            print(f"  price=${price}")
        except Exception as e:
            print(f"\n  ✗ Nasdaq {ticker}: {e}")
        time.sleep(0.3)
    return results

# ── iShares ───────────────────────────────────────────────────────
VARNISH = ("https://www.ishares.com/varnish-api/blk-one01-product-data"
           "/product-data/api/v2/get-product-data")

def find_etha_product_id(session):
    """Tìm ETHA product ID qua iShares screener API"""
    # Cách 1: Tìm qua ticker search
    try:
        r = session.get(
            "https://www.ishares.com/us/products/etf-investments.1.n.json"
            "?search=ETHA&view=keyFacts&fc=iShares",
            headers={"Referer":"https://www.ishares.com/","User-Agent":FAKE_UA},
            timeout=12)
        print(f"  ETHA search HTTP {r.status_code}")
        if r.status_code == 200:
            txt = r.text
            print(f"  ETHA search response[:300]: {txt[:300]}")
            # Tìm productId gần "ETHA" hoặc "ethereum"
            m = re.search(r'"productPageUrl"\s*:\s*"/us/products/(\d+)/[^"]*ethereum', txt, re.I)
            if not m:
                m = re.search(r'/products/(\d+)/ishares-ethereum-trust', txt, re.I)
            if m:
                pid = m.group(1)
                print(f"  ✓ ETHA found via search: {pid}")
                return pid
    except Exception as e:
        print(f"  ETHA search error: {e}")

    # Cách 2: Probe candidate IDs (ETHA launched Jul 2024, after IBIT=333011)
    candidates = [
        "333457","333458","333490","333491","333492","333493","333494",
        "333500","333510","333520","333530","333540","333550","333560",
        "333570","333580","333590","333593","333600","333610","333620",
    ]
    print(f"  Probing {len(candidates)} ETHA candidates...")
    for pid in candidates:
        try:
            url = (f"{VARNISH}?component=holdings.all"
                   f"&portfolioId={pid}&appSubType=ISHARES&appType=PRODUCT_PAGE"
                   f"&locale=en_US&targetSite=us-ishares&userType=individual"
                   f"&excludeContent=true&includeConfig=true")
            r = session.get(url, headers={"User-Agent":FAKE_UA,"Accept":"application/json"}, timeout=6)
            if r.status_code == 200:
                name = r.json().get("fundName","")
                if name: print(f"    ID {pid}: {name}")
                if "ethereum" in name.lower() and "ishares" in name.lower():
                    print(f"  ✓ ETHA found: productId={pid}")
                    return pid
        except: pass
        time.sleep(0.2)
    return None

def fetch_ishares(session, ticker, product_id, crypto_price=None):
    """
    Fix chính: excludeContent=FALSE để lấy value thật thay vì null.
    Dùng asOfDate từ dateList[0] của response config.
    """
    hdrs = {"Referer":f"https://www.ishares.com/us/products/{product_id}/",
             "Accept":"application/json,*/*","User-Agent":FAKE_UA}

    # ── Bước 1: Lấy config (dateList) ────────────────────────────
    config_url = (f"{VARNISH}?component=holdings.all"
                  f"&portfolioId={product_id}&appSubType=ISHARES&appType=PRODUCT_PAGE"
                  f"&locale=en_US&targetSite=us-ishares&userType=individual"
                  f"&excludeContent=true&includeConfig=true")
    latest_date = None
    shares_outstanding = None
    try:
        r = session.get(config_url, headers=hdrs, timeout=15)
        print(f"  iShares config {ticker}: HTTP {r.status_code}")
        if r.status_code == 200:
            d = r.json()
            fund_name = d.get("fundName","")
            print(f"    fundName: {fund_name}")

            # Validate fund
            if ticker == "ETHA" and "ethereum" not in fund_name.lower():
                print(f"    ✗ Wrong fund: {fund_name}"); return None

            # Lấy dateList và sharesOutstanding
            comp = (d.get("componentsByNameMap") or {}).get("holdings") or {}
            cont = (comp.get("containersByNameMap") or {}).get("all") or {}
            dmap = cont.get("dataPointsByNameMap") or {}

            date_list = dmap.get("dateList",{})
            dates     = date_list.get("value") or []
            if dates:
                latest_date = str(dates[0])  # e.g. "20260624"
                print(f"    dateList[0]: {latest_date}")

            # sharesOutstanding từ downloadHeaderData (fund-level, không phải BTC count)
            hdr_data = (comp.get("properties") or {}).get("downloadHeaderData","")
            print(f"    downloadHeaderData: {hdr_data}")

    except Exception as e:
        print(f"    config error: {e}")

    # ── Bước 2: Fetch actual data với excludeContent=FALSE ────────
    data_url = (f"{VARNISH}?component=holdings.all"
                f"&portfolioId={product_id}&appSubType=ISHARES&appType=PRODUCT_PAGE"
                f"&locale=en_US&targetSite=us-ishares&userType=individual"
                f"&excludeContent=false&includeConfig=false"
                + (f"&asOfDate={latest_date}" if latest_date else ""))

    try:
        r = session.get(data_url, headers=hdrs, timeout=20)
        print(f"  iShares data {ticker}: HTTP {r.status_code} (excludeContent=false)")
        if r.status_code != 200: return None

        txt = r.text.strip()
        if not txt.startswith("{"): 
            print(f"    Non-JSON: {txt[:100]}"); return None

        print(f"    response[:3000]:\n{txt[:3000]}\n    ---END---")

        data = r.json()

        # Log top-level keys
        print(f"    top-level keys: {list(data.keys())}")

        holdings   = None
        nav_date   = latest_date
        shares_out = None

        # Traverse toàn bộ JSON — tìm mọi numeric value có khả năng là BTC/ETH count
        flat = json.dumps(data)

        # In ra tất cả numeric fields để debug
        nums = re.findall(r'"(\w+)"\s*:\s*(\d{4,}\.?\d*)', flat)
        print(f"    Numeric fields (value > 1000): {nums[:20]}")

        # Tìm holdings theo nhiều pattern
        btc_range = (1_000, 10_000_000)      # BTC: 1K-10M
        eth_range = (1_000, 1_000_000_000)    # ETH: 1K-1B

        r_range = btc_range if ticker == "IBIT" else eth_range

        for pattern in [
            r'"sharesHeld"\s*:\s*\{\s*"raw"\s*:\s*([\d\.]+)',
            r'"holdingShares"\s*:\s*\{\s*"raw"\s*:\s*([\d\.]+)',
            r'"quantity"\s*:\s*\{\s*"raw"\s*:\s*([\d\.]+)',
            r'"shares"\s*:\s*\{\s*"raw"\s*:\s*([\d\.]+)',
            r'"numberOfHoldings"\s*:\s*([\d\.]+)',
            r'"sharesHeld"\s*:\s*([\d\.]+)',
            r'"holdingShares"\s*:\s*([\d\.]+)',
        ]:
            m = re.search(pattern, flat)
            if m:
                h = parse_num(m.group(1).replace(",",""))
                if h and r_range[0] < h < r_range[1]:
                    holdings = h
                    print(f"    ✓ holdings via '{pattern[:50]}': {holdings}")
                    break

        # Tìm sharesOutstanding (số cổ phiếu ETF, không phải BTC)
        m_so = re.search(r'"sharesOutstanding"\s*:\s*\{\s*"raw"\s*:\s*([\d\.]+)', flat)
        if not m_so:
            m_so = re.search(r'"sharesOutstanding"\s*:\s*([\d\.]+)', flat)
        if m_so:
            so = parse_num(m_so.group(1))
            if so and so > 1_000_000:
                shares_out = so
                print(f"    sharesOutstanding: {shares_out}")

        # Tìm asOfDate
        m_date = re.search(r'"asOfDate"\s*:\s*"?(\d{8}|\d{4}-\d{2}-\d{2})"?', flat)
        if m_date: nav_date = m_date.group(1)

        if holdings or shares_out:
            return {"holdings":holdings, "shares":shares_out, "nav_date":nav_date}

        print(f"    ✗ No holdings found — check response[:3000] above")
        return None

    except Exception as e:
        print(f"    data error: {e}")
        return None

# ── Pipeline ──────────────────────────────────────────────────────
def run(r2):
    now_utc   = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    session   = get_session()

    prev_etfs     = {e["ticker"]:e for e in (r2_get_json(r2,"etf-flows.json") or {}).get("etfs",[])}
    crypto_prices = load_crypto_prices()

    print("\n📈 [1/3] Nasdaq prices...")
    nasdaq = fetch_nasdaq_all(session)
    print(f"  → {sum(1 for v in nasdaq.values() if v.get('price'))} tickers with price")

    issuer = {}
    if RUN_MODE == "full":
        print("\n🏦 [2/3] iShares fund data...")

        # Discover ETHA product ID
        if not ISHARES_IDS.get("ETHA"):
            print("  Finding ETHA product ID...")
            ISHARES_IDS["ETHA"] = find_etha_product_id(session)

        for etf_ticker, pid in ISHARES_IDS.items():
            if not pid:
                print(f"  {etf_ticker}: No product ID, skip")
                continue
            etf_meta = next((e for e in ETF_REGISTRY if e["ticker"]==etf_ticker), {})
            raw = fetch_ishares(session, etf_ticker, pid, crypto_prices.get(etf_meta.get("underlying","")))
            if raw:
                nav = nasdaq.get(etf_ticker,{}).get("price")
                u   = etf_meta.get("underlying","")
                aum = (raw["holdings"] * crypto_prices[u]) if raw.get("holdings") and u in crypto_prices else None
                issuer[etf_ticker] = {**raw,"nav":nav,"aum":aum}
                print(f"  ✓ {etf_ticker}: holdings={raw.get('holdings')}  shares={raw.get('shares')}  AUM=${aum:,.0f}" if aum else f"  ✓ {etf_ticker}: {raw}")
            time.sleep(1)

        print(f"\n  → issuer data: {list(issuer.keys()) or 'NONE'}")
    else:
        print("⏭️  Skip issuer (RUN_MODE=price)")

    print("\n🔧 Building output...")
    etfs=[]; totals={}
    for etf in ETF_REGISTRY:
        t   = etf["ticker"]
        u   = etf["underlying"]
        mkt = nasdaq.get(t) or {}
        iss = issuer.get(t) or {}
        prev = prev_etfs.get(t) or {}

        price    = mkt.get("price")
        nav      = iss.get("nav")    or (prev.get("fund") or {}).get("nav")
        shares   = iss.get("shares") or (prev.get("fund") or {}).get("shares")
        holdings = iss.get("holdings") or (prev.get("fund") or {}).get("holdings")
        aum      = iss.get("aum")
        if not aum and holdings and u in crypto_prices:
            aum = holdings * crypto_prices[u]
        if not aum:
            aum = (prev.get("fund") or {}).get("aum")

        premium = None
        if price and nav and nav > 0:
            premium = {"usd":price-nav,"pct":(price-nav)/nav*100}

        flow = None
        ps = (prev.get("fund") or {}).get("shares")
        if shares and ps and nav and shares != ps:
            d = shares-ps
            flow = {"daily_usd":d*nav,"delta_shares":d,"is_inflow":d>0,"computed_at":now_utc.isoformat()}
        if not flow and prev.get("flow"):
            flow = prev["flow"]

        etfs.append({
            "ticker":t,"name":etf["name"],"issuer":etf["issuer"],
            "underlying":u,"fee":etf["fee"],
            "market":{"price":price,"change":mkt.get("change"),"change_pct":mkt.get("change_pct"),"volume":mkt.get("volume")} if mkt else None,
            "fund":{"nav":nav,"nav_date":iss.get("nav_date"),"shares":shares,"aum":aum,"holdings":holdings,"premium":premium},
            "flow":flow,"onchain":None,
        })
        totals.setdefault(u,{"aum":0.0,"flow":0.0,"count":0})
        totals[u]["aum"]   += aum or 0
        totals[u]["flow"]  += (flow or {}).get("daily_usd") or 0
        totals[u]["count"] += 1

    out = {"etfs":etfs,"totals":totals,"run_mode":RUN_MODE,"fetched_at":now_utc.isoformat()}
    print("\n☁️  Uploading to R2...")
    r2_put_json(r2,"etf-flows.json",out,"max-age=120")
    if RUN_MODE=="full":
        r2_put_json(r2,f"etf-history/{today_str}.json",out,"max-age=86400")
    print("✅ Done")
    for u,t in totals.items():
        s = "+" if t["flow"]>=0 else ""
        print(f"   {u}: AUM=${t['aum']/1e9:.2f}B  Flow={s}${t['flow']/1e6:.1f}M  ({t['count']} ETFs)")

if __name__ == "__main__":
    import time as _t; t0=_t.time()
    print(f"⚙️  ETF Fetcher v6 — RUN_MODE={RUN_MODE}")
    r2=get_r2(); run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
