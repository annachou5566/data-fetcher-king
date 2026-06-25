"""
scripts/fetch_etf.py  v2
ETF Crypto Data Fetcher — data-fetcher-king

Fixes v2:
- iShares: thử 3 URL format khác nhau + fallback holdings CSV
- ARK CSV: thêm debug log + handle metadata rows
- Franklin CSV: robust header detection
- Nasdaq: aggressive NAV extraction, nhiều field hơn
- AUM compute: dùng BTC/ETH price từ market-data.json trong R2
- BITB: xóa địa chỉ sai, để null cho đến khi verify được
- Log chi tiết để debug trên GitHub Actions
"""

import csv
import io
import json
import os
import re
import time
from datetime import datetime, timezone

import boto3
import cloudscraper
import requests
from botocore.config import Config

# ─────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────
RUN_MODE             = os.getenv("RUN_MODE", "full")
R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL      = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME       = os.getenv("R2_BUCKET_NAME")

FAKE_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
           "AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36")

# ─────────────────────────────────────────────────────
# REGISTRY
# ─────────────────────────────────────────────────────
ETF_REGISTRY = [
    # BTC
    {"ticker":"IBIT","name":"iShares Bitcoin Trust ETF",
     "issuer":"BlackRock","underlying":"BTC","fee":0.25,
     "src":{"type":"ishares","product_id":"333011"}},
    {"ticker":"FBTC","name":"Fidelity Wise Origin Bitcoin Fund",
     "issuer":"Fidelity","underlying":"BTC","fee":0.25,
     "src":{"type":"nasdaq_only"}},
    {"ticker":"GBTC","name":"Grayscale Bitcoin Trust ETF",
     "issuer":"Grayscale","underlying":"BTC","fee":1.50,
     "src":{"type":"grayscale","slug":"bitcoin-trust-btc"}},
    {"ticker":"ARKB","name":"ARK 21Shares Bitcoin ETF",
     "issuer":"ARK/21Shares","underlying":"BTC","fee":0.21,
     "src":{"type":"ark_csv",
            "url":"https://ark-funds.com/wp-content/uploads/funds-etf-csv/"
                  "ARK_21SHARES_BITCOIN_ETF_ARKB_HOLDINGS.csv"}},
    {"ticker":"BITB","name":"Bitwise Bitcoin ETF",
     "issuer":"Bitwise","underlying":"BTC","fee":0.20,
     "src":{"type":"nasdaq_only"}},  # custody address cần verify lại
    {"ticker":"HODL","name":"VanEck Bitcoin ETF",
     "issuer":"VanEck","underlying":"BTC","fee":0.20,
     "src":{"type":"nasdaq_only"}},
    {"ticker":"EZBC","name":"Franklin Bitcoin ETF",
     "issuer":"Franklin","underlying":"BTC","fee":0.19,
     "src":{"type":"franklin","ticker":"EZBC"}},
    {"ticker":"BRRR","name":"Valkyrie Bitcoin Fund",
     "issuer":"Valkyrie","underlying":"BTC","fee":0.25,
     "src":{"type":"nasdaq_only"}},
    {"ticker":"BTCO","name":"Invesco Galaxy Bitcoin ETF",
     "issuer":"Invesco","underlying":"BTC","fee":0.25,
     "src":{"type":"nasdaq_only"}},
    {"ticker":"BTCW","name":"WisdomTree Bitcoin Fund",
     "issuer":"WisdomTree","underlying":"BTC","fee":0.25,
     "src":{"type":"nasdaq_only"}},
    # ETH
    {"ticker":"ETHA","name":"iShares Ethereum Trust ETF",
     "issuer":"BlackRock","underlying":"ETH","fee":0.25,
     "src":{"type":"ishares","product_id":"333132"}},
    {"ticker":"FETH","name":"Fidelity Ethereum Fund",
     "issuer":"Fidelity","underlying":"ETH","fee":0.25,
     "src":{"type":"nasdaq_only"}},
    {"ticker":"ETHE","name":"Grayscale Ethereum Trust ETF",
     "issuer":"Grayscale","underlying":"ETH","fee":2.50,
     "src":{"type":"grayscale","slug":"ethereum-trust-eth"}},
    {"ticker":"ETHW","name":"Bitwise Ethereum ETF",
     "issuer":"Bitwise","underlying":"ETH","fee":0.20,
     "src":{"type":"nasdaq_only"}},
    {"ticker":"ETHV","name":"VanEck Ethereum ETF",
     "issuer":"VanEck","underlying":"ETH","fee":0.20,
     "src":{"type":"nasdaq_only"}},
    {"ticker":"CETH","name":"21Shares Core Ethereum ETF",
     "issuer":"21Shares","underlying":"ETH","fee":0.21,
     "src":{"type":"ark_csv",
            "url":"https://ark-funds.com/wp-content/uploads/funds-etf-csv/"
                  "21SHARES_CORE_ETHEREUM_ETF_CETH_HOLDINGS.csv"}},
    {"ticker":"EZET","name":"Franklin Ethereum ETF",
     "issuer":"Franklin","underlying":"ETH","fee":0.19,
     "src":{"type":"franklin","ticker":"EZET"}},
    {"ticker":"QETH","name":"Invesco Galaxy Ethereum ETF",
     "issuer":"Invesco","underlying":"ETH","fee":0.25,
     "src":{"type":"nasdaq_only"}},
]

ETF_TICKERS = [e["ticker"] for e in ETF_REGISTRY]

# ─────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────
def parse_num(v):
    if v is None or v == "" or v == "N/A" or v == "--":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = re.sub(r"[$,%\s]", "", str(v))
    s = re.sub(r"B$", "e9", s, flags=re.I)
    s = re.sub(r"M$", "e6", s, flags=re.I)
    try:
        return float(s)
    except ValueError:
        return None

def get_session():
    s = cloudscraper.create_scraper(
        browser={"browser":"chrome","platform":"windows","desktop":True}
    )
    s.headers.update({"User-Agent": FAKE_UA,
                      "Accept": "application/json, text/html, */*",
                      "Accept-Language": "en-US,en;q=0.9"})
    return s

# ─────────────────────────────────────────────────────
# R2
# ─────────────────────────────────────────────────────
def get_r2():
    return boto3.client("s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"))

def r2_get_json(r2, key):
    try:
        resp = r2.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"  [R2 GET] {key}: {e}")
        return None

def r2_put_json(r2, key, data, cache_control="max-age=120"):
    body = json.dumps(data, ensure_ascii=False, separators=(",",":")).encode("utf-8")
    r2.put_object(Bucket=R2_BUCKET_NAME, Key=key, Body=body,
                  ContentType="application/json", CacheControl=cache_control)

# ─────────────────────────────────────────────────────
# LOAD CRYPTO PRICES FROM R2 (đã có sẵn từ fetch_alpha)
# ─────────────────────────────────────────────────────
def load_crypto_prices(r2):
    """Đọc BTC/ETH price từ market-data.json đã có sẵn trong R2"""
    data = r2_get_json(r2, "market-data.json")
    prices = {}
    if not data:
        return prices
    # Tìm BTCUSDT và ETHUSDT trong data
    # market-data.json có thể là list hoặc dict — handle cả hai
    items = data if isinstance(data, list) else data.get("data", data.get("tokens", []))
    for item in (items if isinstance(items, list) else []):
        sym = item.get("symbol","").upper()
        price = parse_num(item.get("price") or item.get("lastPrice") or item.get("close"))
        if sym in ("BTCUSDT","BTC") and price:
            prices["BTC"] = price
        elif sym in ("ETHUSDT","ETH") and price:
            prices["ETH"] = price
    if prices:
        print(f"  [Crypto prices] BTC=${prices.get('BTC')}  ETH=${prices.get('ETH')}")
    else:
        print("  [Crypto prices] Không tìm được từ R2, sẽ skip AUM compute")
    return prices

# ─────────────────────────────────────────────────────
# FETCHERS
# ─────────────────────────────────────────────────────

# ── Nasdaq ────────────────────────────────────────────────────────
def fetch_nasdaq_all(session):
    results = {}
    for ticker in ETF_TICKERS:
        try:
            url = f"https://api.nasdaq.com/api/quote/{ticker}/info?assetclass=etf"
            r = session.get(url, headers={
                "Referer": f"https://www.nasdaq.com/market-activity/funds-and-etfs/{ticker.lower()}",
            }, timeout=12)

            print(f"  Nasdaq {ticker}: HTTP {r.status_code}", end="")

            if r.status_code != 200:
                print()
                continue

            d = r.json().get("data") or {}
            primary  = d.get("primaryData") or {}
            summary  = d.get("summaryData") or {}
            key_stats = d.get("keyStats") or {}

            def sv(key): return (summary.get(key) or {}).get("value")
            def kv(key): return (key_stats.get(key) or {}).get("value")

            price      = parse_num(primary.get("lastSalePrice"))
            change     = parse_num(primary.get("netChange"))
            change_pct = parse_num((primary.get("percentageChange") or "").replace("%",""))
            volume_raw = (primary.get("volume") or "").replace(",","")
            volume     = parse_num(volume_raw)

            # NAV: Bitcoin ETF thường không có NAV trên Nasdaq
            # Thử nhiều field
            nav = parse_num(
                sv("Nav") or sv("NAV") or sv("Net Asset Value") or
                sv("Previous Closing Price") or kv("NavPerShare") or kv("Nav")
            )

            # Shares outstanding — hay bị "N/A" với crypto ETF
            shares_raw = (sv("Shares Outstanding") or kv("SharesOutstanding") or "").replace(",","")
            shares = parse_num(shares_raw)

            # AUM
            aum = None
            aum_raw = sv("Total Net Assets") or kv("TotalNetAssets") or ""
            if aum_raw and aum_raw not in ("N/A","--",""):
                if "B" in aum_raw.upper():
                    aum = parse_num(re.sub(r"[^0-9\.]","",aum_raw)) * 1e9
                elif "M" in aum_raw.upper():
                    aum = parse_num(re.sub(r"[^0-9\.]","",aum_raw)) * 1e6
                else:
                    aum = parse_num(aum_raw)

            results[ticker] = {
                "price":price,"nav":nav,"shares":shares,"aum":aum,
                "change":change,"change_pct":change_pct,"volume":volume,
            }
            print(f"  price=${price}  nav={nav}  shares={shares}  aum={aum}")

        except Exception as e:
            print(f"\n  ✗ Nasdaq {ticker}: {e}")

        time.sleep(0.35)
    return results


# ── iShares (IBIT, ETHA) ──────────────────────────────────────────
def fetch_ishares(session, product_id, ticker):
    """
    Thử nhiều endpoint khác nhau của iShares.
    Endpoint 1: AJAX JSON overview
    Endpoint 2: Holdings CSV (lấy số BTC/ETH)
    """
    base = f"https://www.ishares.com/us/products/{product_id}/{ticker}"
    TS   = "1467271812596"

    # ── Thử overview JSON ─────────────────────────────
    nav, shares, aum, nav_date, holdings = None, None, None, None, None

    for url in [
        f"{base}/{TS}.ajax?tab=overview&fileType=json",
        f"https://www.ishares.com/us/literature/etf/daily-holdings/{ticker.lower()}.json",
    ]:
        try:
            r = session.get(url, headers={"Referer": base}, timeout=15)
            print(f"  iShares overview {ticker}: HTTP {r.status_code} ({url[:60]}...)")
            if r.status_code != 200:
                continue
            fd = r.json().get("fundData") or {}
            header = fd.get("fundHeader") or {}
            sci    = (header.get("shareClassInfo") or [{}])[0]
            facts  = header.get("keyFacts") or {}

            nav    = parse_num((sci.get("navAmount") or {}).get("raw") or sci.get("navAmount"))
            shares = parse_num((facts.get("sharesOutstanding") or {}).get("raw") or facts.get("sharesOutstanding"))
            aum    = parse_num((facts.get("netAssets") or {}).get("raw") or facts.get("netAssets"))
            nav_date = sci.get("navDate")
            print(f"    → nav={nav}  shares={shares}  aum={aum}")
            if nav or shares or aum:
                break
        except Exception as e:
            print(f"    → overview error: {e}")

    # ── Holdings CSV → lấy số BTC/ETH ────────────────
    for url in [
        f"{base}/{TS}.ajax?tab=holdings&fileType=csv",
        f"https://www.ishares.com/us/literature/etf/holdings/{ticker.lower()}.csv",
    ]:
        try:
            r = session.get(url, headers={"Referer": base}, timeout=15)
            print(f"  iShares holdings {ticker}: HTTP {r.status_code}")
            if r.status_code != 200:
                continue

            text  = r.text
            lines = text.strip().split("\n")
            print(f"    → CSV: {len(lines)} lines, first: {lines[0][:80]}")

            # iShares CSV: dòng 1-2 là metadata, dòng 3+ là data
            # Tìm header row (có chứa "Name" hoặc "Ticker")
            header_idx = 0
            for i, line in enumerate(lines):
                if "name" in line.lower() and ("ticker" in line.lower() or "weight" in line.lower()):
                    header_idx = i
                    break

            reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
            rows = list(reader)
            if rows:
                row0 = rows[0]
                print(f"    → First holding: {dict(list(row0.items())[:4])}")
                # Tìm cột shares
                for col in row0:
                    if "shares" in col.lower() and "held" in col.lower():
                        holdings = parse_num(row0[col])
                        break
                # Fallback: cột "Shares"
                if holdings is None:
                    for col in row0:
                        if col.lower().strip() == "shares":
                            holdings = parse_num(row0[col])
                            break
                print(f"    → holdings={holdings}")
            if holdings is not None:
                break
        except Exception as e:
            print(f"    → holdings error: {e}")

    if nav or shares or aum or holdings:
        return {"nav":nav,"shares":shares,"aum":aum,"nav_date":nav_date,"holdings":holdings}
    return None


# ── ARK / 21Shares CSV ───────────────────────────────────────────
def fetch_ark_csv(url, ticker):
    try:
        r = requests.get(url, headers={"User-Agent": FAKE_UA}, timeout=12)
        print(f"  ARK CSV {ticker}: HTTP {r.status_code}")
        if r.status_code != 200:
            return None

        text  = r.text
        lines = text.strip().split("\n")
        print(f"    → {len(lines)} lines")
        if len(lines) < 2:
            return None

        # Log vài dòng đầu để debug
        for i, line in enumerate(lines[:4]):
            print(f"    line[{i}]: {line[:100]}")

        # Tìm header row — ARK CSV dòng 0 hoặc 1 là header thực sự
        header_idx = 0
        for i, line in enumerate(lines[:5]):
            low = line.lower()
            if "date" in low and "shares" in low:
                header_idx = i
                break

        reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
        rows = [row for row in reader if any(v.strip() for v in row.values())]

        if not rows:
            print("    → No data rows found")
            return None

        print(f"    → {len(rows)} data rows, headers: {list(rows[0].keys())}")

        # ARK CSV có thể có nhiều rows (nhiều holdings)
        # Nhưng với BTC ETF, chỉ có 1 holding = BTC
        row = rows[0]
        headers_lower = {k.lower().strip(): k for k in row.keys()}

        def _get(patterns):
            for pat in patterns:
                for k_low, k_orig in headers_lower.items():
                    if pat in k_low:
                        val = row.get(k_orig, "").strip()
                        if val and val not in ("", "N/A"):
                            return val
            return None

        holdings = parse_num(_get(["shares"]))
        aum_raw  = _get(["market value"])
        aum      = parse_num((aum_raw or "").replace(",","").replace("(","").replace(")",""))
        nav_date = _get(["date"])

        print(f"    → holdings={holdings}  aum={aum}  date={nav_date}")
        return {"holdings":holdings,"aum":aum,"nav_date":nav_date}

    except Exception as e:
        print(f"  ✗ ARK CSV {ticker}: {e}")
        return None


# ── Franklin Templeton CSV ────────────────────────────────────────
def fetch_franklin(ticker):
    urls = [
        f"https://www.franklintempleton.com/content-en_US/cms/assets/fund-resources/{ticker}-holdings.csv",
        f"https://www.franklintempleton.com/content-en_US/cms/assets/fund-resources/{ticker.lower()}-holdings.csv",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers={"User-Agent": FAKE_UA}, timeout=12)
            print(f"  Franklin {ticker}: HTTP {r.status_code} ({url[-40:]})")
            if r.status_code != 200:
                continue

            lines = r.text.strip().split("\n")
            print(f"    → {len(lines)} lines, first: {lines[0][:80]}")

            # Tìm header row
            header_idx = 0
            for i, line in enumerate(lines[:10]):
                low = line.lower()
                if ("shares" in low or "quantity" in low or "security" in low):
                    header_idx = i
                    break

            reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
            rows = [row for row in reader if any(v.strip() for v in row.values())]

            if not rows:
                continue

            print(f"    → headers: {list(rows[0].keys())}")
            row = rows[0]
            headers_lower = {k.lower().strip(): k for k in row.keys()}

            def _get(patterns):
                for pat in patterns:
                    for k_low, k_orig in headers_lower.items():
                        if pat in k_low:
                            val = row.get(k_orig,"").strip()
                            if val and val not in ("","N/A"):
                                return val
                return None

            holdings = parse_num(_get(["shares","quantity","units"]))
            aum      = parse_num((_get(["market value","mkt val","value"]) or "").replace(",",""))
            print(f"    → holdings={holdings}  aum={aum}")
            return {"holdings":holdings,"aum":aum,"nav_date":datetime.now(timezone.utc).strftime("%Y-%m-%d")}

        except Exception as e:
            print(f"    → error: {e}")
    return None


# ── Grayscale ─────────────────────────────────────────────────────
def fetch_grayscale(session, slug):
    try:
        url = f"https://www.grayscale.com/funds/{slug}"
        r   = session.get(url, timeout=15)
        print(f"  Grayscale {slug}: HTTP {r.status_code}")
        if r.status_code != 200:
            return None
        html = r.text
        # Tìm JSON embedded
        m_nav    = re.search(r'"nav[_\s]?(?:per[_\s]?share)?"\s*:\s*"?([\d,\.]+)"?', html, re.I)
        m_aum    = re.search(r'"(?:aum|net[_\s]?assets?)"\s*:\s*"?([\d,\.]+)"?', html, re.I)
        m_shares = re.search(r'"shares[_\s]?outstanding"\s*:\s*"?([\d,]+)"?', html, re.I)
        result = {
            "nav":    parse_num(m_nav.group(1))    if m_nav    else None,
            "aum":    parse_num(m_aum.group(1))    if m_aum    else None,
            "shares": parse_num(m_shares.group(1)) if m_shares else None,
        }
        print(f"    → {result}")
        return result if any(result.values()) else None
    except Exception as e:
        print(f"  ✗ Grayscale {slug}: {e}")
        return None


# ─────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────
def run(r2):
    now_utc   = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y-%m-%d")
    session   = get_session()

    # Load snapshot cũ để tính flow
    prev_snapshot = r2_get_json(r2, "etf-flows.json") or {}
    prev_etfs     = {e["ticker"]: e for e in prev_snapshot.get("etfs", [])}

    # Load BTC/ETH price từ R2 để compute AUM khi có holdings
    crypto_prices = load_crypto_prices(r2)

    # ── PHASE 1: Market prices ─────────────────────────────────────
    print("\n📈 [1/3] Nasdaq ETF prices...")
    nasdaq_data = fetch_nasdaq_all(session)
    print(f"  → Got price for {sum(1 for v in nasdaq_data.values() if v.get('price'))} tickers")

    # ── PHASE 2: Issuer fund data ──────────────────────────────────
    issuer_data = {}

    if RUN_MODE == "full":
        print("\n🏦 [2/3] Issuer fund data...")
        for etf in ETF_REGISTRY:
            ticker = etf["ticker"]
            src    = etf["src"]
            data   = None

            if src["type"] == "ishares":
                data = fetch_ishares(session, src["product_id"], ticker)

            elif src["type"] == "ark_csv":
                raw = fetch_ark_csv(src["url"], ticker)
                if raw:
                    # NAV ≈ market price (premium BTC ETF thường < 0.1%)
                    nav = nasdaq_data.get(ticker, {}).get("price")
                    data = {"holdings":raw["holdings"],"aum":raw["aum"],
                            "nav":nav,"nav_date":raw["nav_date"]}

            elif src["type"] == "franklin":
                raw = fetch_franklin(src["ticker"])
                if raw:
                    nav = nasdaq_data.get(ticker, {}).get("price")
                    data = {"holdings":raw["holdings"],"aum":raw["aum"],
                            "nav":nav,"nav_date":raw["nav_date"]}

            elif src["type"] == "grayscale":
                data = fetch_grayscale(session, src["slug"])

            if data:
                issuer_data[ticker] = data

            time.sleep(0.5)

        print(f"\n  → Got issuer data for: {list(issuer_data.keys()) or 'NONE'}")
        print("\n⏭️  [3/3] On-chain skip (địa chỉ cần verify)")
    else:
        print("\n⏭️  [2/3] Skip (RUN_MODE=price)")
        print("⏭️  [3/3] Skip (RUN_MODE=price)")

    # ── BUILD OUTPUT ───────────────────────────────────────────────
    print("\n🔧 Building output...")
    etfs   = []
    totals = {}

    for etf in ETF_REGISTRY:
        ticker     = etf["ticker"]
        underlying = etf["underlying"]
        mkt        = nasdaq_data.get(ticker) or {}
        iss        = issuer_data.get(ticker) or {}
        prev       = prev_etfs.get(ticker) or {}

        # NAV: issuer > nasdaq > prev
        nav = (iss.get("nav") or mkt.get("nav") or
               (prev.get("fund") or {}).get("nav"))

        # Shares
        shares = (iss.get("shares") or mkt.get("shares") or
                  (prev.get("fund") or {}).get("shares"))

        # Holdings (số BTC/ETH)
        holdings = (iss.get("holdings") or
                    (prev.get("fund") or {}).get("holdings"))

        # AUM: issuer > compute từ holdings × crypto_price > nasdaq > prev
        aum = iss.get("aum") or mkt.get("aum")
        if not aum and holdings and underlying in crypto_prices:
            aum = holdings * crypto_prices[underlying]
            print(f"  {ticker}: AUM computed = {holdings} × ${crypto_prices[underlying]} = ${aum:,.0f}")
        if not aum:
            aum = (prev.get("fund") or {}).get("aum")

        # Premium
        price   = mkt.get("price")
        premium = None
        if price and nav and nav > 0:
            premium = {"usd": price - nav, "pct": (price - nav) / nav * 100}

        # Daily Flow = Δ shares × NAV
        flow = None
        prev_shares = (prev.get("fund") or {}).get("shares")
        if shares and prev_shares and nav and shares != prev_shares:
            delta     = shares - prev_shares
            daily_usd = delta * nav
            flow = {"daily_usd":daily_usd,"delta_shares":delta,
                    "is_inflow":daily_usd > 0,
                    "computed_at":now_utc.isoformat()}
        if not flow and prev.get("flow"):
            flow = prev["flow"]

        entry = {
            "ticker":ticker,"name":etf["name"],"issuer":etf["issuer"],
            "underlying":underlying,"fee":etf["fee"],
            "market":{
                "price":price,"change":mkt.get("change"),
                "change_pct":mkt.get("change_pct"),"volume":mkt.get("volume"),
            } if mkt else None,
            "fund":{
                "nav":nav,"nav_date":iss.get("nav_date"),
                "shares":shares,"aum":aum,"holdings":holdings,"premium":premium,
            },
            "flow":flow,
            "onchain":None,
        }
        etfs.append(entry)

        totals.setdefault(underlying, {"aum":0.0,"flow":0.0,"count":0})
        totals[underlying]["aum"]   += aum or 0
        totals[underlying]["flow"]  += (flow or {}).get("daily_usd") or 0
        totals[underlying]["count"] += 1

    output = {"etfs":etfs,"totals":totals,"run_mode":RUN_MODE,
              "fetched_at":now_utc.isoformat()}

    # ── UPLOAD ─────────────────────────────────────────────────────
    print("\n☁️  Uploading etf-flows.json to R2...")
    r2_put_json(r2, "etf-flows.json", output, "max-age=120")
    print("✅ Done")

    if RUN_MODE == "full":
        r2_put_json(r2, f"etf-history/{today_str}.json", output, "max-age=86400")

    for u, t in totals.items():
        s = "+" if t["flow"] >= 0 else ""
        print(f"   {u}: AUM=${t['aum']/1e9:.2f}B  Flow={s}${t['flow']/1e6:.1f}M  ({t['count']} ETFs)")


# ─────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    import time as _t
    t0 = _t.time()
    print(f"⚙️  ETF Fetcher v2 — RUN_MODE={RUN_MODE}")
    r2 = get_r2()
    run(r2)
    print(f"\n🏁 Done in {_t.time()-t0:.1f}s")
