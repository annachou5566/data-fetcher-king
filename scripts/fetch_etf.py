"""
scripts/fetch_etf.py  v8 — FINAL WORKING
Fix từ log v7:
- dmap key là "unitsHeld" không phải "sharesHeld" → BTC count trực tiếp
- shareClass là list → src.get() crash im lặng → thêm isinstance check
- ETHA: thử range rộng hơn + search by asset class Cryptocurrency
"""

import json, os, re, time
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
    "ETHA": None,
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
    except Exception as e:
        print(f"  [R2 GET {key}] {e}"); return None

def r2_put_json(r2, key, data, cc="max-age=120"):
    body = json.dumps(data,ensure_ascii=False,separators=(",",":")).encode("utf-8")
    r2.put_object(Bucket=R2_BUCKET_NAME,Key=key,Body=body,ContentType="application/json",CacheControl=cc)

def load_crypto_prices():
    prices = {}
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd",
            headers={"User-Agent":FAKE_UA},timeout=10)
        if r.status_code == 200:
            d = r.json()
            if "bitcoin"  in d: prices["BTC"] = float(d["bitcoin"]["usd"])
            if "ethereum" in d: prices["ETH"] = float(d["ethereum"]["usd"])
    except Exception as e:
        print(f"  [Crypto] CoinGecko error: {e}")
    print(f"  [Crypto] BTC=${prices.get('BTC')}  ETH=${prices.get('ETH')}")
    return prices

def fetch_nasdaq_all(session):
    results = {}
    for ticker in ETF_TICKERS:
        try:
            r = session.get(
                f"https://api.nasdaq.com/api/quote/{ticker}/info?assetclass=etf",
                headers={"Referer":f"https://www.nasdaq.com/market-activity/funds-and-etfs/{ticker.lower()}"},
                timeout=12)
            print(f"  Nasdaq {ticker}: HTTP {r.status_code}",end="")
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

def safe_get(obj, key):
    """dict.get() safe — không crash nếu obj là list/None"""
    if isinstance(obj, dict): return obj.get(key)
    return None

def fetch_ishares(session, ticker, product_id, crypto_price=None):
    hdrs = {"Referer":f"https://www.ishares.com/us/products/{product_id}/",
             "Accept":"application/json,*/*","User-Agent":FAKE_UA}

    # Step 1: config → lấy dateList
    latest_date = None
    try:
        r = session.get(_url(product_id,True,True),headers=hdrs,timeout=15)
        print(f"  iShares config {ticker}: HTTP {r.status_code}")
        if r.status_code != 200: return None
        d         = r.json()
        fund_name = d.get("fundName","")
        print(f"    fundName: {fund_name}")
        if ticker == "ETHA" and "ethereum" not in fund_name.lower():
            print(f"    ✗ Wrong fund"); return None
        comp  = (d.get("componentsByNameMap") or {}).get("holdings",{})
        cont  = (comp.get("containersByNameMap") or {}).get("all",{})
        dmap  = cont.get("dataPointsByNameMap",{})
        dates = dmap.get("dateList",{}).get("value") or []
        if dates:
            latest_date = str(dates[0])
            print(f"    dateList[0]: {latest_date}")
    except Exception as e:
        print(f"    config error: {e}"); return None

    # Step 2: data với excludeContent=FALSE
    try:
        r = session.get(_url(product_id,False,False,latest_date),headers=hdrs,timeout=20)
        print(f"  iShares data {ticker}: HTTP {r.status_code}")
        if r.status_code != 200: return None
        txt = r.text.strip()
        if not txt.startswith("{"): return None
        d = r.json()

        comp = (d.get("componentsByNameMap") or {}).get("holdings",{})
        cont = (comp.get("containersByNameMap") or {}).get("all",{})
        dmap = cont.get("dataPointsByNameMap",{})
        print(f"    dmap keys: {list(dmap.keys())}")

        # ── AUM: marketValue[0] ───────────────────────────────────
        mv_arr = dmap.get("marketValue",{}).get("value",[])
        aum    = max((v for v in mv_arr if isinstance(v,(int,float)) and v > 0),default=None)
        print(f"    marketValue.value: {mv_arr} → AUM=${aum:,.0f}" if aum else f"    marketValue.value: {mv_arr}")

        # ── Holdings: unitsHeld[0] (từ log v7, key chính xác) ─────
        holdings = None
        # Thử theo đúng thứ tự: unitsHeld trước (confirmed từ log)
        for dp_key in ["unitsHeld","sharesHeld","quantity","numberOfShares","holdingShares","units"]:
            arr = dmap.get(dp_key,{}).get("value",[])
            if arr:
                v = arr[0] if isinstance(arr,list) else arr
                h = parse_num(v)
                print(f"    {dp_key}.value[0]: {v} → {h}")
                if h and 100 < h < 1_000_000_000:
                    holdings = h
                    print(f"    ✓ holdings via '{dp_key}': {holdings}")
                    break

        # Fallback: tính từ AUM / price
        if not holdings and aum and crypto_price and crypto_price > 0:
            holdings = aum / crypto_price
            print(f"    holdings = AUM/price = {aum:.0f}/{crypto_price:.0f} = {holdings:.2f}")

        # ── ShareClass / pageScopeData (safe access) ─────────────
        shares_out = None
        nav        = None

        # shareClass có thể là dict hoặc list — dùng safe_get
        sc = d.get("shareClass")
        sc_dict = sc if isinstance(sc,dict) else (sc[0] if isinstance(sc,list) and sc else {})
        print(f"    shareClass type: {type(sc).__name__}  keys: {list(sc_dict.keys())[:8] if sc_dict else []}")

        for src_dict in [sc_dict, d.get("pageScopeData") or {}]:
            if not isinstance(src_dict,dict): continue

            # sharesOutstanding
            if not shares_out:
                for k in ["sharesOutstanding","totalSharesOutstanding","outstandingShares"]:
                    v = src_dict.get(k)
                    if v:
                        s = parse_num(safe_get(v,"raw") or safe_get(v,"value") or v)
                        if s and s > 1_000_000:
                            shares_out = s
                            print(f"    sharesOutstanding: {shares_out}")
                            break

            # NAV
            if not nav:
                for k in ["navAmount","nav","navPerShare","netAssetValue"]:
                    v = src_dict.get(k)
                    if v:
                        n = parse_num(safe_get(v,"raw") or safe_get(v,"value") or v)
                        if n and 0.1 < n < 100_000:
                            nav = n
                            print(f"    NAV: {nav}")
                            break

        # asOfDate
        nav_date = latest_date
        ao = dmap.get("asOfDate",{}).get("value")
        if ao: nav_date = str(ao)

        if aum or holdings:
            result = {"aum":aum,"holdings":holdings,"shares":shares_out,"nav":nav,"nav_date":nav_date}
            print(f"    ✓ Extracted: {result}")
            return result

        print(f"    ✗ No data")
        return None

    except Exception as e:
        import traceback
        print(f"    ✗ data error: {e}")
        print(f"    traceback: {traceback.format_exc()[:500]}")
        return None


def find_etha_product_id(session):
    """
    v7 tried 333700-334500 → not found.
    ETHA approved May 2024, launched Jul 2024.
    Try: search iShares by asset class Cryptocurrency, or probe broader ranges.
    """
    # Approach 1: iShares search by asset class
    try:
        r = session.get(
            "https://www.ishares.com/us/products/etf-investments.1.n.json"
            "?view=keyFacts&fst=ASSETCLASSTYPE%7CCryptocurrency&fc=iShares",
            headers={"Referer":"https://www.ishares.com/","User-Agent":FAKE_UA},
            timeout=15)
        print(f"  ETHA crypto search: HTTP {r.status_code}")
        if r.status_code == 200:
            txt = r.text
            print(f"  ETHA search response[:500]: {txt[:500]}")
            # Tìm productId + ethereum trong response
            for m in re.finditer(r'"productPageUrl"\s*:\s*"/us/products/(\d+)/([^"]+)"', txt):
                pid, slug = m.group(1), m.group(2)
                if "ethereum" in slug:
                    print(f"  ✓ ETHA found via search: {pid} ({slug})")
                    return pid
    except Exception as e:
        print(f"  ETHA search error: {e}")

    # Approach 2: Probe 334500-337000 (mở rộng thêm)
    print("  Probing ETHA IDs 334500-337000 (step=50)...")
    for pid in range(334500, 337001, 50):
        try:
            r = session.get(
                _url(str(pid),True,True),
                headers={"User-Agent":FAKE_UA,"Accept":"application/json"},timeout=5)
            if r.status_code == 200:
                name = r.json().get("fundName","")
                if name: print(f"    ID {pid}: {name}")
                if "ethereum" in name.lower() and "ishares" in name.lower():
                    print(f"  ✓ ETHA found: {pid}")
                    return str(pid)
        except: pass
        time.sleep(0.15)

    return None


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

        # IBIT
        raw = fetch_ishares(session,"IBIT",ISHARES_IDS["IBIT"],crypto_prices.get("BTC"))
        if raw:
            issuer["IBIT"] = {**raw,"nav": raw.get("nav") or nasdaq.get("IBIT",{}).get("price")}
        time.sleep(1)

        # ETHA
        if not ISHARES_IDS.get("ETHA"):
            ISHARES_IDS["ETHA"] = find_etha_product_id(session)
        if ISHARES_IDS.get("ETHA"):
            raw = fetch_ishares(session,"ETHA",ISHARES_IDS["ETHA"],crypto_prices.get("ETH"))
            if raw:
                issuer["ETHA"] = {**raw,"nav": raw.get("nav") or nasdaq.get("ETHA",{}).get("price")}
        else:
            print("  ✗ ETHA: product ID not found (will use nasdaq data only)")

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
        nav      = iss.get("nav")      or (prev.get("fund") or {}).get("nav")
        shares   = iss.get("shares")   or (prev.get("fund") or {}).get("shares")
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
            dlt = shares - ps
            flow = {"daily_usd":dlt*nav,"delta_shares":dlt,"is_inflow":dlt>0,
                    "computed_at":now_utc.isoformat()}
        if not flow and prev.get("flow"):
            flow = prev["flow"]

        etfs.append({
            "ticker":t,"name":etf["name"],"issuer":etf["issuer"],
            "underlying":u,"fee":etf["fee"],
            "market":{"price":price,"change":mkt.get("change"),
                      "change_pct":mkt.get("change_pct"),"volume":mkt.get("volume")} if mkt else None,
            "fund":{"nav":nav,"nav_date":iss.get("nav_date"),"shares":shares,
                    "aum":aum,"holdings":holdings,"premium":premium},
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
    print(f"⚙️  ETF Fetcher v8 — RUN_MODE={RUN_MODE}")
    r2=get_r2(); run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
