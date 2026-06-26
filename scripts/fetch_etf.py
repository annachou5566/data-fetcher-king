"""
scripts/fetch_etf.py  v5
Fix từ log v4:
- iShares: bỏ qua downloadHeader="true", đi thẳng vào containersByNameMap.all.data
- Crypto prices: CoinGecko thay Binance (Binance block GitHub Actions IP)
- ETHA product ID: thử hardcode + auto-discover
- Log 2000 chars response để xem đủ structure
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

# Product IDs — IBIT confirmed. ETHA sẽ auto-discover nếu hardcode sai.
ISHARES_IDS = {
    "IBIT": "333011",   # ✅ confirmed
    "ETHA": "333593",   # best guess — sẽ verify khi chạy
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

# ── Crypto prices: CoinGecko (không bị block GitHub Actions) ──────
def load_crypto_prices():
    prices = {}
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price"
            "?ids=bitcoin,ethereum&vs_currencies=usd",
            headers={"User-Agent": FAKE_UA},
            timeout=10)
        if r.status_code == 200:
            d = r.json()
            if "bitcoin"  in d: prices["BTC"] = float(d["bitcoin"]["usd"])
            if "ethereum" in d: prices["ETH"] = float(d["ethereum"]["usd"])
        print(f"  [Crypto/CoinGecko] BTC=${prices.get('BTC')}  ETH=${prices.get('ETH')}")
    except Exception as e:
        print(f"  [Crypto] CoinGecko error: {e}")

    # Fallback: Binance (có thể bị block nhưng thử)
    if not prices.get("BTC"):
        try:
            for sym, key in [("BTCUSDT","BTC"),("ETHUSDT","ETH")]:
                r = requests.get(
                    f"https://api.binance.com/api/v3/ticker/price?symbol={sym}",
                    timeout=6)
                if r.status_code == 200:
                    d = r.json()
                    if isinstance(d, dict) and "price" in d:
                        prices[key] = float(d["price"])
            print(f"  [Crypto/Binance fallback] BTC=${prices.get('BTC')}  ETH=${prices.get('ETH')}")
        except Exception as e:
            print(f"  [Crypto/Binance] {e}")

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

# ── iShares varnish-api ───────────────────────────────────────────
VARNISH = ("https://www.ishares.com/varnish-api/blk-one01-product-data"
           "/product-data/api/v2/get-product-data")

def ishares_base_params(product_id):
    return (f"appSubType=ISHARES&appType=PRODUCT_PAGE"
            f"&locale=en_US&portfolioId={product_id}"
            f"&targetSite=us-ishares&userType=individual"
            f"&excludeContent=true&includeConfig=true")

def fetch_ishares(session, ticker, product_id):
    today  = datetime.now(timezone.utc).strftime("%Y%m%d")
    params = ishares_base_params(product_id)
    hdrs   = {"Referer":f"https://www.ishares.com/us/products/{product_id}/",
               "Accept":"application/json,*/*","User-Agent":FAKE_UA}

    url = f"{VARNISH}?component=holdings.all&asOfDate={today}&{params}"
    try:
        r = session.get(url, headers=hdrs, timeout=20)
        print(f"  iShares {ticker} (ID={product_id}): HTTP {r.status_code}")
        if r.status_code != 200:
            return None

        txt = r.text.strip()
        if not txt.startswith("{"): return None

        # Log 2000 chars để thấy đủ structure
        print(f"    response[:2000]:\n{txt[:2000]}\n    ---END---")

        data = r.json()

        # Verify đúng fund
        fund_name = data.get("fundName","")
        print(f"    fundName: {fund_name}")
        if ticker == "ETHA" and "ethereum" not in fund_name.lower():
            print(f"    ✗ Wrong fund! Expected Ethereum ETF, got: {fund_name}")
            return None

        holdings   = None
        shares_out = None
        nav_date   = None

        # ── Path 1: componentsByNameMap.holdings.containersByNameMap.all.data ──
        comp_map      = data.get("componentsByNameMap") or {}
        holdings_comp = comp_map.get("holdings") or {}
        containers    = holdings_comp.get("containersByNameMap") or {}
        all_cont      = containers.get("all") or {}

        print(f"    all_cont keys: {list(all_cont.keys())}")

        # Tìm tableData trong all_cont
        for data_key in ["data","tableData","holdingsData","holdings"]:
            nested = all_cont.get(data_key)
            if nested:
                print(f"    Found '{data_key}' in all_cont, keys: {list(nested.keys()) if isinstance(nested,dict) else type(nested)}")
                tbl = nested.get("tableData") if isinstance(nested,dict) else None
                if not tbl and isinstance(nested,dict):
                    tbl = nested  # maybe nested IS tableData
                if tbl:
                    rows = tbl.get("rows") or []
                    print(f"    rows count: {len(rows)}")
                    if rows:
                        row0 = rows[0]
                        print(f"    row[0] keys: {list(row0.keys())[:12]}")
                        # Tìm holdings (số BTC/ETH)
                        for key in ["shares","sharesHeld","quantity","units","holdingShares"]:
                            val = row0.get(key)
                            if isinstance(val, dict):
                                val = val.get("raw") or val.get("value") or val.get("fmt")
                            h = parse_num(val)
                            if h and h > 100:
                                holdings = h
                                print(f"    ✓ holdings via '{key}': {holdings}")
                                break
                    # asOfDate
                    nav_date = tbl.get("asOfDate") or tbl.get("date")
                break

        # ── Path 2: fund-level sharesOutstanding từ header fields ──
        # downloadHeaderData = "inceptionDate,sharesOutstanding,..."
        # Các giá trị này nằm trong data.header hoặc top-level fields
        header_fields = (holdings_comp.get("properties",{})
                         .get("downloadHeaderData","")).split(",")
        if "sharesOutstanding" in header_fields:
            # Thử tìm trong top-level data
            for path in [
                lambda d: d.get("sharesOutstanding"),
                lambda d: d.get("header",{}).get("sharesOutstanding"),
                lambda d: d.get("fundData",{}).get("sharesOutstanding"),
            ]:
                try:
                    v = path(data)
                    if v is not None:
                        so = parse_num(v.get("raw") if isinstance(v,dict) else v)
                        if so and so > 1000:
                            shares_out = so
                            print(f"    ✓ sharesOutstanding: {shares_out}")
                            break
                except: pass

        # ── Path 3: Regex scan toàn bộ JSON ──────────────────────
        if not holdings:
            flat = json.dumps(data)
            for pattern in [
                r'"shares"\s*:\s*\{\s*"raw"\s*:\s*([\d\.]+)',
                r'"sharesHeld"\s*:\s*\{\s*"raw"\s*:\s*([\d\.]+)',
                r'"quantity"\s*:\s*\{\s*"raw"\s*:\s*([\d\.]+)',
                r'"shares"\s*:\s*"([\d,\.]+)"',
            ]:
                m = re.search(pattern, flat)
                if m:
                    h = parse_num(m.group(1).replace(",",""))
                    # BTC holdings: 100K–1M; ETH holdings: 1M–100M
                    if h and 1_000 < h < 100_000_000:
                        holdings = h
                        print(f"    ✓ holdings via regex '{pattern[:40]}': {holdings}")
                        break

        if holdings or shares_out:
            return {"holdings":holdings, "shares":shares_out, "nav_date":nav_date}

        print(f"    ✗ No holdings found in response")
        return None

    except Exception as e:
        print(f"    error: {e}")
        return None


def discover_etha_id(session):
    """Thử các product ID candidate cho ETHA"""
    candidates = ["333593","333449","333492","333521","333600","333640","333700"]
    params_base = (f"appSubType=ISHARES&appType=PRODUCT_PAGE"
                   f"&locale=en_US&targetSite=us-ishares&userType=individual"
                   f"&excludeContent=true&includeConfig=true")
    for pid in candidates:
        try:
            url = f"{VARNISH}?component=holdings.all&portfolioId={pid}&{params_base}"
            r   = session.get(url, headers={"User-Agent":FAKE_UA,"Accept":"application/json"}, timeout=8)
            if r.status_code == 200:
                name = r.json().get("fundName","")
                print(f"    ID {pid}: {name}")
                if "ethereum" in name.lower() and "ishares" in name.lower():
                    print(f"    ✓ ETHA found: productId={pid}")
                    return pid
        except: pass
        time.sleep(0.3)
    print("    ✗ ETHA product ID not found in candidates")
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
        ibit_data = fetch_ishares(session, "IBIT", ISHARES_IDS["IBIT"])
        if ibit_data:
            nav = nasdaq.get("IBIT",{}).get("price")
            issuer["IBIT"] = {**ibit_data, "nav":nav,
                "aum": ibit_data["holdings"] * crypto_prices.get("BTC",0) if ibit_data.get("holdings") else None}

        time.sleep(1)

        # ETHA — verify hardcoded ID, nếu sai thì discover
        etha_id = ISHARES_IDS.get("ETHA","333593")
        etha_data = fetch_ishares(session, "ETHA", etha_id)
        if not etha_data:
            print("  ETHA hardcoded ID failed, discovering...")
            etha_id = discover_etha_id(session)
            if etha_id:
                ISHARES_IDS["ETHA"] = etha_id
                etha_data = fetch_ishares(session, "ETHA", etha_id)
        if etha_data:
            nav = nasdaq.get("ETHA",{}).get("price")
            issuer["ETHA"] = {**etha_data, "nav":nav,
                "aum": etha_data["holdings"] * crypto_prices.get("ETH",0) if etha_data.get("holdings") else None}

        print(f"\n  → issuer data: {list(issuer.keys()) or 'NONE'}")
    else:
        print("⏭️  Skip issuer (RUN_MODE=price)")

    print("\n🔧 Building output...")
    etfs = []; totals = {}
    for etf in ETF_REGISTRY:
        t   = etf["ticker"]
        u   = etf["underlying"]
        mkt = nasdaq.get(t) or {}
        iss = issuer.get(t) or {}
        prev = prev_etfs.get(t) or {}

        price    = mkt.get("price")
        nav      = iss.get("nav") or (prev.get("fund") or {}).get("nav")
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
    if RUN_MODE == "full":
        r2_put_json(r2,f"etf-history/{today_str}.json",out,"max-age=86400")
    print("✅ Done")
    for u,t in totals.items():
        s = "+" if t["flow"] >= 0 else ""
        aum_b = t['aum']/1e9
        print(f"   {u}: AUM=${aum_b:.2f}B  Flow={s}${t['flow']/1e6:.1f}M  ({t['count']} ETFs)")

if __name__ == "__main__":
    import time as _t; t0 = _t.time()
    print(f"⚙️  ETF Fetcher v5 — RUN_MODE={RUN_MODE}")
    r2 = get_r2()
    run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
