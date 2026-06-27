"""
scripts/fetch_etf.py  v11
- IBIT: confirmed working ✅
- ETHA: productId=337614 (confirmed từ iShares website)
- Đọc ETHA_PRODUCT_ID từ env nếu muốn override
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
ETHA_PRODUCT_ID_ENV  = os.getenv("ETHA_PRODUCT_ID", "")  # set manually nếu biết

FAKE_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
           "AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36")

ISHARES_IDS = {
    "IBIT": "333011",
    "ETHA": ETHA_PRODUCT_ID_ENV or "337614",  # confirmed ✅
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
            print(f"  Nasdaq {ticker}: HTTP {r.status_code}",end="")
            if r.status_code != 200: print(); continue
            d = r.json().get("data") or {}
            p = d.get("primaryData") or {}
            price = parse_num(p.get("lastSalePrice"))
            results[ticker] = {"price":price,"change":parse_num(p.get("netChange")),
                "change_pct":parse_num((p.get("percentageChange") or "").replace("%","")),
                "volume":parse_num((p.get("volume") or "").replace(",",""))}
            print(f"  price=${price}")
        except Exception as e: print(f"\n  ✗ {ticker}: {e}")
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


def fetch_ishares(session, ticker, product_id, crypto_price=None):
    hdrs = {"Referer":f"https://www.ishares.com/us/products/{product_id}/",
             "Accept":"application/json,*/*","User-Agent":FAKE_UA}
    latest_date = None
    try:
        r = session.get(_url(product_id,True,True),headers=hdrs,timeout=15)
        print(f"  iShares config {ticker}: HTTP {r.status_code}")
        if r.status_code != 200: return None
        d = r.json()
        name = d.get("fundName","")
        print(f"    fundName: {name}")
        if ticker == "ETHA" and "ethereum" not in name.lower():
            print("    ✗ Wrong fund"); return None
        comp = (d.get("componentsByNameMap") or {}).get("holdings",{})
        cont = (comp.get("containersByNameMap") or {}).get("all",{})
        dmap = cont.get("dataPointsByNameMap",{})
        dates = dmap.get("dateList",{}).get("value") or []
        if dates: latest_date = str(dates[0]); print(f"    dateList[0]: {latest_date}")
    except Exception as e:
        print(f"    config error: {e}"); return None

    aum = None; holdings = None; nav_date = latest_date
    try:
        r = session.get(_url(product_id,False,False,as_of=latest_date),headers=hdrs,timeout=20)
        print(f"  iShares data {ticker}: HTTP {r.status_code}")
        if r.status_code == 200:
            d = r.json()
            comp = (d.get("componentsByNameMap") or {}).get("holdings",{})
            cont = (comp.get("containersByNameMap") or {}).get("all",{})
            dmap = cont.get("dataPointsByNameMap",{})
            mv = dmap.get("marketValue",{}).get("value",[])
            aum = max((v for v in mv if isinstance(v,(int,float)) and v>0),default=None)
            print(f"    AUM: ${aum:,.0f}" if aum else "    AUM: None")
            for key in ["unitsHeld","sharesHeld","quantity"]:
                arr = dmap.get(key,{}).get("value",[])
                if arr:
                    h = parse_num(arr[0] if isinstance(arr,list) else arr)
                    if h and 100 < h < 1_000_000_000:
                        holdings = h; print(f"    holdings ({key}): {holdings}"); break
            if not holdings and aum and crypto_price and crypto_price > 0:
                holdings = aum / crypto_price
                print(f"    holdings computed: {holdings:.2f}")
            ao = dmap.get("asOfDate",{}).get("value")
            if ao: nav_date = str(ao)
    except Exception as e:
        print(f"    data error: {e}")

    if aum or holdings:
        return {"aum":aum,"holdings":holdings,"shares":None,"nav":None,"nav_date":nav_date}
    return None

def run(r2):
    now_utc   = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    session   = get_session()

    cached_ids = r2_get_json(r2,"etf-ishares-ids.json") or {}
    for t,pid in cached_ids.items():
        if pid and not ISHARES_IDS.get(t):
            ISHARES_IDS[t] = pid
            print(f"  [Cache] {t}={pid}")

    prev_etfs = {e["ticker"]:e for e in (r2_get_json(r2,"etf-flows.json") or {}).get("etfs",[])}
    crypto_prices = load_crypto_prices()

    print("\n📈 [1/3] Nasdaq prices...")
    nasdaq = fetch_nasdaq_all(session)
    print(f"  → {sum(1 for v in nasdaq.values() if v.get('price'))} tickers with price")

    issuer = {}
    if RUN_MODE == "full":
        print("\n🏦 [2/3] iShares fund data...")


        for etf_ticker, pid in list(ISHARES_IDS.items()):
            if not pid: print(f"  {etf_ticker}: No productId"); continue
            etf_meta = next((e for e in ETF_REGISTRY if e["ticker"]==etf_ticker),{})
            raw = fetch_ishares(session, etf_ticker, pid, crypto_prices.get(etf_meta.get("underlying","")))
            if raw:
                nav = nasdaq.get(etf_ticker,{}).get("price")
                u   = etf_meta.get("underlying","")
                aum = raw.get("aum") or (raw["holdings"]*crypto_prices[u] if raw.get("holdings") and u in crypto_prices else None)
                issuer[etf_ticker] = {**raw,"nav":nav,"aum":aum}
                print(f"  ✓ {etf_ticker}: AUM=${(aum or 0)/1e9:.2f}B  holdings={raw.get('holdings',0):.0f}")
            time.sleep(0.5)

        print(f"\n  → issuer: {list(issuer.keys()) or 'NONE'}")
    else:
        print("⏭️  Skip issuer")

    print("\n🔧 Building output...")
    etfs=[]; totals={}
    for etf in ETF_REGISTRY:
        t=etf["ticker"]; u=etf["underlying"]
        mkt=nasdaq.get(t) or {}; iss=issuer.get(t) or {}; prev=prev_etfs.get(t) or {}
        price=mkt.get("price")
        nav=iss.get("nav") or (prev.get("fund") or {}).get("nav")
        shares=iss.get("shares") or (prev.get("fund") or {}).get("shares")
        holdings=iss.get("holdings") or (prev.get("fund") or {}).get("holdings")
        aum=iss.get("aum")
        if not aum and holdings and u in crypto_prices: aum=holdings*crypto_prices[u]
        if not aum: aum=(prev.get("fund") or {}).get("aum")
        premium={"usd":price-nav,"pct":(price-nav)/nav*100} if price and nav and nav>0 else None
        flow=None
        ps=(prev.get("fund") or {}).get("shares")
        if shares and ps and nav and shares!=ps:
            dlt=shares-ps
            flow={"daily_usd":dlt*nav,"delta_shares":dlt,"is_inflow":dlt>0,"computed_at":now_utc.isoformat()}
        if not flow and prev.get("flow"): flow=prev["flow"]
        etfs.append({"ticker":t,"name":etf["name"],"issuer":etf["issuer"],"underlying":u,"fee":etf["fee"],
            "market":{"price":price,"change":mkt.get("change"),"change_pct":mkt.get("change_pct"),"volume":mkt.get("volume")} if mkt else None,
            "fund":{"nav":nav,"nav_date":iss.get("nav_date"),"shares":shares,"aum":aum,"holdings":holdings,"premium":premium},
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
    print(f"⚙️  ETF Fetcher v11 — RUN_MODE={RUN_MODE}")
    print(f"  ETHA_PRODUCT_ID env: {ETHA_PRODUCT_ID_ENV or '(not set)'}")
    r2=get_r2(); run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
