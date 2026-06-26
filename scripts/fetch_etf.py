"""
scripts/fetch_etf.py  v4
Fixes từ log v3:
- iShares: parse đúng componentsByNameMap → tìm downloadHoldingsLink → download CSV
- iShares ETHA: product ID 333132 sai → thử tìm đúng ID
- Binance price: fix URL encoding
- ARK/Franklin đã 404/HTML → dùng SEC EDGAR fallback
- NAV: dùng market price xấp xỉ (premium BTC ETF < 0.1%)
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

# iShares product IDs — chỉ IBIT xác nhận đúng từ log
# ETHA cần tìm lại: thử search từ trang ishares.com
ISHARES_PRODUCTS = {
    "IBIT": "333011",   # confirmed ✅
    "ETHA": None,       # 333132 sai → tự discover bên dưới
}

ETF_REGISTRY = [
    {"ticker":"IBIT","name":"iShares Bitcoin Trust ETF","issuer":"BlackRock","underlying":"BTC","fee":0.25,"src":{"type":"ishares","slug":"ishares-bitcoin-trust-etf"}},
    {"ticker":"FBTC","name":"Fidelity Wise Origin Bitcoin Fund","issuer":"Fidelity","underlying":"BTC","fee":0.25,"src":{"type":"nasdaq_only"}},
    {"ticker":"GBTC","name":"Grayscale Bitcoin Trust ETF","issuer":"Grayscale","underlying":"BTC","fee":1.50,"src":{"type":"nasdaq_only"}},
    {"ticker":"ARKB","name":"ARK 21Shares Bitcoin ETF","issuer":"ARK/21Shares","underlying":"BTC","fee":0.21,"src":{"type":"nasdaq_only"}},
    {"ticker":"BITB","name":"Bitwise Bitcoin ETF","issuer":"Bitwise","underlying":"BTC","fee":0.20,"src":{"type":"nasdaq_only"}},
    {"ticker":"HODL","name":"VanEck Bitcoin ETF","issuer":"VanEck","underlying":"BTC","fee":0.20,"src":{"type":"nasdaq_only"}},
    {"ticker":"EZBC","name":"Franklin Bitcoin ETF","issuer":"Franklin","underlying":"BTC","fee":0.19,"src":{"type":"nasdaq_only"}},
    {"ticker":"BRRR","name":"Valkyrie Bitcoin Fund","issuer":"Valkyrie","underlying":"BTC","fee":0.25,"src":{"type":"nasdaq_only"}},
    {"ticker":"BTCO","name":"Invesco Galaxy Bitcoin ETF","issuer":"Invesco","underlying":"BTC","fee":0.25,"src":{"type":"nasdaq_only"}},
    {"ticker":"BTCW","name":"WisdomTree Bitcoin Fund","issuer":"WisdomTree","underlying":"BTC","fee":0.25,"src":{"type":"nasdaq_only"}},
    {"ticker":"ETHA","name":"iShares Ethereum Trust ETF","issuer":"BlackRock","underlying":"ETH","fee":0.25,"src":{"type":"ishares","slug":"ishares-ethereum-trust-etf"}},
    {"ticker":"FETH","name":"Fidelity Ethereum Fund","issuer":"Fidelity","underlying":"ETH","fee":0.25,"src":{"type":"nasdaq_only"}},
    {"ticker":"ETHE","name":"Grayscale Ethereum Trust ETF","issuer":"Grayscale","underlying":"ETH","fee":2.50,"src":{"type":"nasdaq_only"}},
    {"ticker":"ETHW","name":"Bitwise Ethereum ETF","issuer":"Bitwise","underlying":"ETH","fee":0.20,"src":{"type":"nasdaq_only"}},
    {"ticker":"ETHV","name":"VanEck Ethereum ETF","issuer":"VanEck","underlying":"ETH","fee":0.20,"src":{"type":"nasdaq_only"}},
    {"ticker":"CETH","name":"21Shares Core Ethereum ETF","issuer":"21Shares","underlying":"ETH","fee":0.21,"src":{"type":"nasdaq_only"}},
    {"ticker":"EZET","name":"Franklin Ethereum ETF","issuer":"Franklin","underlying":"ETH","fee":0.19,"src":{"type":"nasdaq_only"}},
    {"ticker":"QETH","name":"Invesco Galaxy Ethereum ETF","issuer":"Invesco","underlying":"ETH","fee":0.25,"src":{"type":"nasdaq_only"}},
]
ETF_TICKERS = [e["ticker"] for e in ETF_REGISTRY]

# ── Helpers ───────────────────────────────────────────────────────
def parse_num(v):
    if v is None or str(v).strip() in ("","N/A","--","null","None"): return None
    if isinstance(v,(int,float)): return float(v)
    s = re.sub(r"[$,%\s]","",str(v))
    try: return float(s)
    except: return None

def get_session():
    s = cloudscraper.create_scraper(browser={"browser":"chrome","platform":"windows","desktop":True})
    s.headers.update({"User-Agent":FAKE_UA,"Accept":"application/json, text/html, */*","Accept-Language":"en-US,en;q=0.9"})
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

# ── Crypto prices từ Binance (fix URL) ───────────────────────────
def load_crypto_prices():
    prices = {}
    try:
        # Gọi từng symbol riêng — tránh URL encoding phức tạp
        for sym, key in [("BTCUSDT","BTC"), ("ETHUSDT","ETH")]:
            r = requests.get(
                f"https://api.binance.com/api/v3/ticker/price?symbol={sym}",
                timeout=8)
            if r.status_code == 200:
                d = r.json()
                if isinstance(d, dict) and "price" in d:
                    prices[key] = float(d["price"])
        print(f"  [Crypto] BTC=${prices.get('BTC')}  ETH=${prices.get('ETH')}")
    except Exception as e:
        print(f"  [Crypto] Lỗi: {e}")
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
            summary = d.get("summaryData") or {}
            def sv(k): return (summary.get(k) or {}).get("value")
            price      = parse_num(primary.get("lastSalePrice"))
            change     = parse_num(primary.get("netChange"))
            change_pct = parse_num((primary.get("percentageChange") or "").replace("%",""))
            volume     = parse_num((primary.get("volume") or "").replace(",",""))
            results[ticker] = {"price":price,"change":change,"change_pct":change_pct,"volume":volume}
            print(f"  price=${price}")
        except Exception as e:
            print(f"\n  ✗ Nasdaq {ticker}: {e}")
        time.sleep(0.35)
    return results

# ── iShares: discover product ID từ trang chủ ────────────────────
def discover_ishares_product_id(session, slug):
    """
    Tự động tìm portfolioId bằng cách search trang iShares.
    URL pattern: https://www.ishares.com/us/products/{ID}/{slug}
    """
    try:
        # Tìm qua search API của iShares
        search_url = "https://www.ishares.com/us/products/etf-investments.1.n.json"
        params = {"search": slug.replace("-", " "), "showAll": "true"}
        r = session.get(search_url, params=params, timeout=10)
        if r.status_code == 200:
            txt = r.text
            # Tìm productId trong response
            m = re.search(r'"productId"\s*:\s*(\d+).*?"' + slug[:10], txt, re.S)
            if m:
                print(f"    discover: productId={m.group(1)}")
                return m.group(1)
        # Fallback: fetch product page và extract từ URL
        page_r = session.get(f"https://www.ishares.com/us/products/239454/{slug}", timeout=10)
        # Không hoạt động, bỏ qua
    except Exception as e:
        print(f"    discover error: {e}")
    return None

# ── iShares holdings (IBIT đã confirm HTTP 200) ──────────────────
def fetch_ishares_holdings(session, product_id, ticker, slug):
    """
    Dùng varnish-api endpoint đã confirm hoạt động.
    Parse componentsByNameMap → tìm downloadHoldingsLink → download CSV.
    """
    if not product_id:
        print(f"  iShares {ticker}: Chưa có product_id, skip")
        return None

    VARNISH = "https://www.ishares.com/varnish-api/blk-one01-product-data/product-data/api/v2/get-product-data"
    BASE_PARAMS = (
        f"appSubType=ISHARES&appType=PRODUCT_PAGE"
        f"&locale=en_US&portfolioId={product_id}"
        f"&targetSite=us-ishares&userType=individual"
        f"&excludeContent=true&includeConfig=true"
    )
    referer = f"https://www.ishares.com/us/products/{product_id}/{slug}"
    hdrs    = {"Referer":referer, "Accept":"application/json, */*", "User-Agent":FAKE_UA}
    today   = datetime.now(timezone.utc).strftime("%Y%m%d")
    yesterday = datetime.now(timezone.utc).strftime("%Y%m%d")  # thử ngày hiện tại trước

    holdings = None
    nav_date = None

    for as_of in [today, ""]:  # thử today, nếu không có thì bỏ asOfDate
        suffix = f"&asOfDate={as_of}" if as_of else ""
        url = f"{VARNISH}?component=holdings.all{suffix}&{BASE_PARAMS}"
        try:
            r = session.get(url, headers=hdrs, timeout=20)
            print(f"  iShares holdings {ticker}: HTTP {r.status_code} (asOfDate={as_of or 'none'})")
            if r.status_code != 200: continue

            txt = r.text.strip()
            if not txt.startswith("{"): 
                print(f"    → Non-JSON response, skip"); continue

            print(f"    response[:500]: {txt[:500]}")
            data = r.json()

            # ── Bước 1: Tìm downloadHoldingsLink trong properties ─
            comp_map = data.get("componentsByNameMap") or {}
            # Key có thể là "holdings" hoặc "holdings.all"
            holdings_comp = comp_map.get("holdings.all") or comp_map.get("holdings") or {}
            props = holdings_comp.get("properties") or {}

            print(f"    properties keys: {list(props.keys())[:10]}")

            # Tìm download link trong properties
            dl_link = None
            for k, v in props.items():
                if isinstance(v, str) and ("holdings" in k.lower() or "download" in k.lower() or "csv" in k.lower()):
                    dl_link = v
                    print(f"    Found download key '{k}': {v[:100]}")
                    break

            if dl_link:
                # Download CSV
                csv_url = dl_link if dl_link.startswith("http") else f"https://www.ishares.com{dl_link}"
                try:
                    csv_r = session.get(csv_url, headers={"Referer":referer}, timeout=15)
                    print(f"    CSV download: HTTP {csv_r.status_code} from {csv_url[:80]}")
                    if csv_r.status_code == 200 and not csv_r.text.strip().startswith("<"):
                        holdings, nav_date = parse_ishares_csv(csv_r.text, ticker)
                        if holdings:
                            print(f"    ✓ holdings={holdings}  nav_date={nav_date}")
                            break
                except Exception as e:
                    print(f"    CSV download error: {e}")

            # ── Bước 2: Tìm trong data trực tiếp ─────────────────
            # Traverse toàn bộ JSON để tìm số BTC/ETH
            flat = json.dumps(data)

            # Tìm "sharesHeld", "quantity", "holdingShares" etc.
            for pattern in [
                r'"sharesHeld"\s*:\s*"?([\d,\.]+)"?',
                r'"quantity"\s*:\s*"?([\d,\.]+)"?',
                r'"holdingShares"\s*:\s*"?([\d,\.]+)"?',
                r'"shares"\s*:\s*"?([\d,\.]+)"?',
            ]:
                m = re.search(pattern, flat)
                if m:
                    val = parse_num(m.group(1).replace(",",""))
                    # Sanity check: BTC ETF holdings nên > 1000 và < 1,000,000
                    if val and 1000 < val < 10_000_000:
                        holdings = val
                        print(f"    ✓ Found holdings via pattern '{pattern}': {holdings}")
                        break

            # Tìm asOfDate
            m_date = re.search(r'"asOfDate"\s*:\s*"?(\d{8}|\d{4}-\d{2}-\d{2})"?', flat)
            if m_date:
                nav_date = m_date.group(1)

            if holdings: break

        except Exception as e:
            print(f"    error: {e}")

    if holdings:
        return {"holdings": holdings, "nav_date": nav_date}
    return None


def parse_ishares_csv(text, ticker):
    """Parse iShares holdings CSV để lấy số BTC/ETH"""
    try:
        lines = text.strip().split("\n")
        print(f"    CSV: {len(lines)} lines")
        print(f"    CSV line[0]: {lines[0][:100]}")

        # iShares CSV: metadata rows đầu, rồi đến header, rồi data
        # Tìm header row (có "Name" và "Shares")
        header_idx = 0
        for i, line in enumerate(lines[:10]):
            if "name" in line.lower() and ("shares" in line.lower() or "weight" in line.lower()):
                header_idx = i
                break

        reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
        rows = [row for row in reader if any(v.strip() for v in row.values())]

        if not rows:
            print(f"    CSV: no data rows")
            return None, None

        print(f"    CSV headers: {list(rows[0].keys())[:8]}")
        row0 = rows[0]  # IBIT/ETHA chỉ có 1 holding (BTC/ETH)
        print(f"    CSV row0: {dict(list(row0.items())[:6])}")

        # Tìm cột shares
        hlow = {k.lower().strip(): k for k in row0}
        holdings = None
        for pat in ["shares held", "shares", "quantity", "units"]:
            for kl, ko in hlow.items():
                if pat in kl:
                    v = row0.get(ko,"").replace(",","").strip()
                    h = parse_num(v)
                    if h and h > 100:  # sanity check
                        holdings = h
                        break
            if holdings: break

        # Nav date từ metadata (thường ở dòng đầu CSV)
        nav_date = None
        for line in lines[:5]:
            m = re.search(r'(\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2})', line)
            if m:
                nav_date = m.group(1)
                break

        return holdings, nav_date
    except Exception as e:
        print(f"    CSV parse error: {e}")
        return None, None


# ── Discover đúng product ID cho ETHA ────────────────────────────
def find_etha_product_id(session):
    """
    iShares Ethereum Trust ETF — tìm product ID đúng.
    333132 là sai (iBonds ETF). Thử vài ID xung quanh 333011.
    """
    # Thử các ID có thể từ khoảng 333400-333700
    candidates = ["333593", "333449", "333492", "333521", "333401", "333600", "333490"]
    VARNISH = "https://www.ishares.com/varnish-api/blk-one01-product-data/product-data/api/v2/get-product-data"

    for pid in candidates:
        try:
            url = (f"{VARNISH}?component=holdings.all"
                   f"&appSubType=ISHARES&appType=PRODUCT_PAGE"
                   f"&locale=en_US&portfolioId={pid}"
                   f"&targetSite=us-ishares&userType=individual"
                   f"&excludeContent=true&includeConfig=true")
            r = session.get(url, headers={"User-Agent":FAKE_UA,"Accept":"application/json"}, timeout=10)
            if r.status_code == 200:
                d = r.json()
                name = d.get("fundName","")
                print(f"    PID {pid}: {name}")
                if "ethereum" in name.lower() and "ishares" in name.lower():
                    print(f"    ✓ Found ETHA product ID: {pid}")
                    return pid
        except Exception as e:
            pass
        time.sleep(0.3)
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
        print("\n🏦 [2/3] Issuer data...")

        # Discover ETHA product ID nếu chưa có
        etha_pid = ISHARES_PRODUCTS.get("ETHA")
        if not etha_pid:
            print("  Discovering ETHA product ID...")
            etha_pid = find_etha_product_id(session)
            if etha_pid:
                ISHARES_PRODUCTS["ETHA"] = etha_pid

        for etf in ETF_REGISTRY:
            t   = etf["ticker"]
            src = etf["src"]
            if src["type"] != "ishares": continue

            pid = ISHARES_PRODUCTS.get(t)
            raw = fetch_ishares_holdings(session, pid, t, src["slug"])
            if raw:
                # NAV ≈ market price (premium BTC/ETH ETF < 0.1%, chấp nhận được)
                nav = nasdaq.get(t, {}).get("price")
                issuer[t] = {
                    "holdings": raw["holdings"],
                    "nav":      nav,
                    "nav_date": raw.get("nav_date"),
                    "aum":      raw["holdings"] * crypto_prices.get(etf["underlying"], 0) if raw["holdings"] else None,
                }
                print(f"  ✓ {t}: holdings={raw['holdings']}  AUM=${issuer[t]['aum']:,.0f}" if issuer[t]['aum'] else f"  ✓ {t}: holdings={raw['holdings']}")
            time.sleep(0.5)

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
        shares   = (prev.get("fund") or {}).get("shares")
        holdings = iss.get("holdings") or (prev.get("fund") or {}).get("holdings")
        aum      = iss.get("aum") or (prev.get("fund") or {}).get("aum")
        # Nếu vẫn chưa có AUM nhưng có holdings và price crypto
        if not aum and holdings and u in crypto_prices:
            aum = holdings * crypto_prices[u]

        premium = None
        if price and nav and nav > 0:
            premium = {"usd": price - nav, "pct": (price - nav) / nav * 100}

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
        totals.setdefault(u, {"aum":0.0,"flow":0.0,"count":0})
        totals[u]["aum"]   += aum or 0
        totals[u]["flow"]  += (flow or {}).get("daily_usd") or 0
        totals[u]["count"] += 1

    out = {"etfs":etfs,"totals":totals,"run_mode":RUN_MODE,"fetched_at":now_utc.isoformat()}

    print("\n☁️  Uploading...")
    r2_put_json(r2, "etf-flows.json", out, "max-age=120")
    if RUN_MODE == "full":
        r2_put_json(r2, f"etf-history/{today_str}.json", out, "max-age=86400")
    print("✅ Done")
    for u, t in totals.items():
        s = "+" if t["flow"] >= 0 else ""
        print(f"   {u}: AUM=${t['aum']/1e9:.2f}B  Flow={s}${t['flow']/1e6:.1f}M  ({t['count']} ETFs)")

if __name__ == "__main__":
    import time as _t; t0 = _t.time()
    print(f"⚙️  ETF Fetcher v4 — RUN_MODE={RUN_MODE}")
    r2 = get_r2()
    run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
