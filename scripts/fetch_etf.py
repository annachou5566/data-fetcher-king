"""
scripts/fetch_etf.py  v7 — FINAL
Fix từ log v6:
- dataPointsByNameMap dùng array format: {"value": [val1, val2]}
  KHÔNG phải {"raw": val} như tôi regex trước đây
- marketValue.value[0] = AUM của IBIT = $45.45B ✅ (đã thấy trong log)
- sharesHeld.value[0] = BTC count (nếu có)
- shareClass field có thể có NAV/shares outstanding
- ETHA: thử range rộng hơn 333700-334300
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
    "ETHA": None,      # will discover
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

# ── iShares core ──────────────────────────────────────────────────
VARNISH = ("https://www.ishares.com/varnish-api/blk-one01-product-data"
           "/product-data/api/v2/get-product-data")

def _ishares_url(product_id, exclude_content, include_config, as_of=None):
    params = (f"component=holdings.all"
              f"&portfolioId={product_id}"
              f"&appSubType=ISHARES&appType=PRODUCT_PAGE"
              f"&locale=en_US&targetSite=us-ishares&userType=individual"
              f"&excludeContent={'true' if exclude_content else 'false'}"
              f"&includeConfig={'true' if include_config else 'false'}")
    if as_of:
        params += f"&asOfDate={as_of}"
    return f"{VARNISH}?{params}"

def _dmap_val(dmap, key, idx=0):
    """Lấy value[idx] từ dataPointsByNameMap entry"""
    entry = dmap.get(key, {})
    vals  = entry.get("value")
    if isinstance(vals, list) and len(vals) > idx:
        return vals[idx]
    if not isinstance(vals, list):
        return vals
    return None

def fetch_ishares(session, ticker, product_id, crypto_price=None):
    hdrs = {"Referer": f"https://www.ishares.com/us/products/{product_id}/",
             "Accept": "application/json,*/*", "User-Agent": FAKE_UA}

    # Step 1: config call để lấy dateList
    latest_date = None
    try:
        r = session.get(_ishares_url(product_id, True, True), headers=hdrs, timeout=15)
        print(f"  iShares config {ticker} (ID={product_id}): HTTP {r.status_code}")
        if r.status_code != 200: return None
        d = r.json()

        # Validate fund name
        fund_name = d.get("fundName","")
        print(f"    fundName: {fund_name}")
        if ticker == "ETHA" and "ethereum" not in fund_name.lower():
            print(f"    ✗ Wrong fund"); return None

        # Lấy dateList[0]
        comp = (d.get("componentsByNameMap") or {}).get("holdings",{})
        cont = (comp.get("containersByNameMap") or {}).get("all",{})
        dmap = cont.get("dataPointsByNameMap",{})
        dates = (dmap.get("dateList",{}).get("value") or [])
        if dates:
            latest_date = str(dates[0])
            print(f"    dateList[0]: {latest_date}")

    except Exception as e:
        print(f"    config error: {e}"); return None

    # Step 2: data call với excludeContent=FALSE + đúng asOfDate
    try:
        r = session.get(
            _ishares_url(product_id, False, False, latest_date),
            headers=hdrs, timeout=20)
        print(f"  iShares data {ticker}: HTTP {r.status_code}")
        if r.status_code != 200: return None

        d = r.json()

        # Navigate: componentsByNameMap.holdings.containersByNameMap.all.dataPointsByNameMap
        comp = (d.get("componentsByNameMap") or {}).get("holdings",{})
        cont = (comp.get("containersByNameMap") or {}).get("all",{})
        dmap = cont.get("dataPointsByNameMap",{})

        # Log tất cả data point keys để debug
        print(f"    dmap keys: {list(dmap.keys())}")

        # ── AUM: marketValue[0] = giá trị thị trường của BTC/ETH ──
        # Từ log v6: marketValue.value = [4.545486989197E10, 34098.75]
        #            → IBIT holds $45.45B BTC ✅
        aum_arr = dmap.get("marketValue",{}).get("value",[])
        # Lấy giá trị lớn nhất (BTC, không phải cash)
        aum = max((v for v in aum_arr if isinstance(v,(int,float)) and v > 0), default=None)
        print(f"    marketValue array: {aum_arr}")
        print(f"    → AUM: ${aum:,.0f}" if aum else "    → AUM: None")

        # ── Holdings (số BTC/ETH) ──────────────────────────────────
        holdings = None
        # Thử các data point có thể chứa BTC count
        for dp_key in ["sharesHeld","quantity","numberOfShares","holdingShares","units"]:
            arr = dmap.get(dp_key,{}).get("value",[])
            if arr:
                print(f"    {dp_key}.value: {arr}")
                # Lấy giá trị đầu tiên (holding #1 = BTC/ETH)
                v = arr[0] if isinstance(arr,list) else arr
                h = parse_num(v)
                if h and 1_000 < h < 1_000_000_000:
                    holdings = h
                    print(f"    ✓ holdings via '{dp_key}': {holdings}")
                    break

        # Nếu không có sharesHeld → tính từ AUM / crypto price
        if not holdings and aum and crypto_price and crypto_price > 0:
            holdings = aum / crypto_price
            print(f"    holdings computed: {aum} / {crypto_price} = {holdings:.2f}")

        # ── Shares outstanding (số cổ phiếu ETF) ─────────────────
        shares_out = None
        # Tìm trong shareClass field (top-level)
        share_class = d.get("shareClass") or {}
        if isinstance(share_class, dict):
            print(f"    shareClass keys: {list(share_class.keys())[:10]}")
            so_val = share_class.get("sharesOutstanding") or share_class.get("totalSharesOutstanding")
            if so_val:
                v = so_val.get("raw") or so_val.get("value") if isinstance(so_val,dict) else so_val
                s = parse_num(v)
                if s and s > 1_000_000:
                    shares_out = s
                    print(f"    sharesOutstanding (shareClass): {shares_out}")

        # Tìm trong pageScopeData
        if not shares_out:
            page_data = d.get("pageScopeData") or {}
            if isinstance(page_data, dict):
                for k in ["sharesOutstanding","totalSharesOutstanding"]:
                    v = page_data.get(k)
                    if v:
                        s = parse_num(v.get("raw") if isinstance(v,dict) else v)
                        if s and s > 1_000_000:
                            shares_out = s
                            print(f"    sharesOutstanding (pageScopeData): {shares_out}")
                            break

        # ── NAV ───────────────────────────────────────────────────
        nav = None
        for src in [share_class, d.get("pageScopeData") or {}]:
            for k in ["navAmount","nav","navPerShare"]:
                v = src.get(k)
                if v:
                    n = parse_num(v.get("raw") if isinstance(v,dict) else v)
                    if n and 0.1 < n < 100000:
                        nav = n
                        print(f"    NAV: {nav}")
                        break
            if nav: break

        # ── asOfDate ──────────────────────────────────────────────
        nav_date = latest_date
        ao = _dmap_val(dmap, "asOfDate")
        if ao: nav_date = str(ao)

        if aum or holdings:
            return {"aum":aum,"holdings":holdings,"shares":shares_out,"nav":nav,"nav_date":nav_date}

        print(f"    ✗ No data extracted")
        return None

    except Exception as e:
        print(f"    data error: {e}"); return None


def find_etha_product_id(session):
    """
    ETHA launched Jul 2024. IBIT = 333011 (Jan 2024).
    Thử range 333700-334500 (range v6 là 333457-333620, không tìm được).
    """
    print("  Probing ETHA product IDs (333700-334500)...")
    for pid in range(333700, 334501, 10):  # bước 10 để nhanh, sau refinement
        try:
            r = session.get(
                _ishares_url(str(pid), True, True),
                headers={"User-Agent":FAKE_UA,"Accept":"application/json"},
                timeout=5)
            if r.status_code == 200:
                name = r.json().get("fundName","")
                if name:
                    print(f"    ID {pid}: {name}")
                if "ethereum" in name.lower() and "ishares" in name.lower():
                    print(f"  ✓ ETHA found! productId={pid}")
                    return str(pid)
        except: pass
        time.sleep(0.15)

    # Fine-grain search nếu bước 10 miss
    print("  Fine search around hits...")
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

        # IBIT
        ibit_price = crypto_prices.get("BTC")
        raw = fetch_ishares(session, "IBIT", ISHARES_IDS["IBIT"], ibit_price)
        if raw:
            nav = raw.get("nav") or nasdaq.get("IBIT",{}).get("price")
            issuer["IBIT"] = {**raw, "nav": nav}
            print(f"  ✓ IBIT: AUM=${raw.get('aum',0)/1e9:.2f}B  holdings={raw.get('holdings',0):.0f} BTC")

        time.sleep(1)

        # ETHA — discover product ID
        if not ISHARES_IDS.get("ETHA"):
            ISHARES_IDS["ETHA"] = find_etha_product_id(session)

        if ISHARES_IDS.get("ETHA"):
            raw = fetch_ishares(session, "ETHA", ISHARES_IDS["ETHA"], crypto_prices.get("ETH"))
            if raw:
                nav = raw.get("nav") or nasdaq.get("ETHA",{}).get("price")
                issuer["ETHA"] = {**raw, "nav": nav}
                print(f"  ✓ ETHA: AUM=${raw.get('aum',0)/1e9:.2f}B  holdings={raw.get('holdings',0):.0f} ETH")
        else:
            print("  ✗ ETHA: product ID not found")

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
            d = shares - ps
            flow = {"daily_usd":d*nav,"delta_shares":d,"is_inflow":d>0,"computed_at":now_utc.isoformat()}
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
    print(f"⚙️  ETF Fetcher v7 — RUN_MODE={RUN_MODE}")
    r2=get_r2(); run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
