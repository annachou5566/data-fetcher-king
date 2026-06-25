"""
scripts/fetch_etf.py
ETF Crypto Data Fetcher — data-fetcher-king
Chạy qua GitHub Actions cron, upload kết quả lên R2.

RUN_MODE=price  → chỉ lấy ETF market price (nhanh, chạy mỗi 30 phút trong giờ giao dịch)
RUN_MODE=full   → lấy toàn bộ (price + NAV/AUM/holdings từ issuer), chạy 1 lần/ngày
"""

import csv
import io
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta

import boto3
import cloudscraper
import requests
from botocore.config import Config

# ─────────────────────────────────────────────────────
# 1. CONFIG
# ─────────────────────────────────────────────────────
RUN_MODE             = os.getenv("RUN_MODE", "full")
R2_ACCESS_KEY_ID     = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL      = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME       = os.getenv("R2_BUCKET_NAME")
ETHERSCAN_API_KEY    = os.getenv("ETHERSCAN_API_KEY", "")   # free key, optional

FAKE_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)

# ─────────────────────────────────────────────────────
# 2. ETF REGISTRY
# ─────────────────────────────────────────────────────
ETF_REGISTRY = [
    # ── BTC ──────────────────────────────────────────
    {"ticker": "IBIT", "name": "iShares Bitcoin Trust ETF",
     "issuer": "BlackRock",   "underlying": "BTC", "fee": 0.25,
     "src": {"type": "ishares", "product_id": "333011"}},

    {"ticker": "FBTC", "name": "Fidelity Wise Origin Bitcoin Fund",
     "issuer": "Fidelity",    "underlying": "BTC", "fee": 0.25,
     "src": {"type": "nasdaq_only"}},

    {"ticker": "GBTC", "name": "Grayscale Bitcoin Trust ETF",
     "issuer": "Grayscale",   "underlying": "BTC", "fee": 1.50,
     "src": {"type": "grayscale", "slug": "bitcoin-trust-btc"}},

    {"ticker": "ARKB", "name": "ARK 21Shares Bitcoin ETF",
     "issuer": "ARK/21Shares","underlying": "BTC", "fee": 0.21,
     "src": {"type": "ark_csv",
             "url": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/"
                    "ARK_21SHARES_BITCOIN_ETF_ARKB_HOLDINGS.csv"}},

    {"ticker": "BITB", "name": "Bitwise Bitcoin ETF",
     "issuer": "Bitwise",     "underlying": "BTC", "fee": 0.20,
     # BITB là ETF DUY NHẤT public custody address chính thức
     "custody_addr": "1LQoWist8KkaUXSPKZHNvEyfrEkPHzSsCd",
     "src": {"type": "nasdaq_only"}},

    {"ticker": "HODL", "name": "VanEck Bitcoin ETF",
     "issuer": "VanEck",      "underlying": "BTC", "fee": 0.20,
     "src": {"type": "nasdaq_only"}},

    {"ticker": "EZBC", "name": "Franklin Bitcoin ETF",
     "issuer": "Franklin",    "underlying": "BTC", "fee": 0.19,
     "src": {"type": "franklin", "ticker": "EZBC"}},

    {"ticker": "BRRR", "name": "Valkyrie Bitcoin Fund",
     "issuer": "Valkyrie",    "underlying": "BTC", "fee": 0.25,
     "src": {"type": "nasdaq_only"}},

    {"ticker": "BTCO", "name": "Invesco Galaxy Bitcoin ETF",
     "issuer": "Invesco",     "underlying": "BTC", "fee": 0.25,
     "src": {"type": "nasdaq_only"}},

    {"ticker": "BTCW", "name": "WisdomTree Bitcoin Fund",
     "issuer": "WisdomTree",  "underlying": "BTC", "fee": 0.25,
     "src": {"type": "nasdaq_only"}},

    # ── ETH ──────────────────────────────────────────
    {"ticker": "ETHA", "name": "iShares Ethereum Trust ETF",
     "issuer": "BlackRock",   "underlying": "ETH", "fee": 0.25,
     "src": {"type": "ishares", "product_id": "333132"}},

    {"ticker": "FETH", "name": "Fidelity Ethereum Fund",
     "issuer": "Fidelity",    "underlying": "ETH", "fee": 0.25,
     "src": {"type": "nasdaq_only"}},

    {"ticker": "ETHE", "name": "Grayscale Ethereum Trust ETF",
     "issuer": "Grayscale",   "underlying": "ETH", "fee": 2.50,
     "src": {"type": "grayscale", "slug": "ethereum-trust-eth"}},

    {"ticker": "ETHW", "name": "Bitwise Ethereum ETF",
     "issuer": "Bitwise",     "underlying": "ETH", "fee": 0.20,
     "src": {"type": "nasdaq_only"}},

    {"ticker": "ETHV", "name": "VanEck Ethereum ETF",
     "issuer": "VanEck",      "underlying": "ETH", "fee": 0.20,
     "src": {"type": "nasdaq_only"}},

    {"ticker": "CETH", "name": "21Shares Core Ethereum ETF",
     "issuer": "21Shares",    "underlying": "ETH", "fee": 0.21,
     "src": {"type": "ark_csv",
             "url": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/"
                    "21SHARES_CORE_ETHEREUM_ETF_CETH_HOLDINGS.csv"}},

    {"ticker": "EZET", "name": "Franklin Ethereum ETF",
     "issuer": "Franklin",    "underlying": "ETH", "fee": 0.19,
     "src": {"type": "franklin", "ticker": "EZET"}},

    {"ticker": "QETH", "name": "Invesco Galaxy Ethereum ETF",
     "issuer": "Invesco",     "underlying": "ETH", "fee": 0.25,
     "src": {"type": "nasdaq_only"}},
]

ETF_TICKERS = [e["ticker"] for e in ETF_REGISTRY]

# ─────────────────────────────────────────────────────
# 3. HELPERS
# ─────────────────────────────────────────────────────
def parse_num(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = re.sub(r"[$,%\s]", "", str(v))
    # Handle B/M suffix
    s = re.sub(r"B$", "e9", s, flags=re.I)
    s = re.sub(r"M$", "e6", s, flags=re.I)
    try:
        return float(s)
    except ValueError:
        return None

def get_session():
    """cloudscraper session — bypass Cloudflare trên iShares, Grayscale"""
    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    s.headers.update({
        "User-Agent": FAKE_UA,
        "Accept":     "application/json, text/html, */*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s

# ─────────────────────────────────────────────────────
# 4. R2
# ─────────────────────────────────────────────────────
def get_r2():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )

def r2_get_json(r2, key):
    try:
        resp = r2.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception:
        return None

def r2_put_json(r2, key, data, cache_control="max-age=120"):
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    r2.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=key,
        Body=body,
        ContentType="application/json",
        CacheControl=cache_control,
    )

# ─────────────────────────────────────────────────────
# 5. FETCHERS
# ─────────────────────────────────────────────────────

# ── 5a. Nasdaq API — ETF price, NAV, shares ───────────────────────
def fetch_nasdaq_all(session):
    """
    Lấy price + NAV + shares cho tất cả ETF từ Nasdaq.
    Return: dict { ticker: {price, nav, shares, aum, change, change_pct, volume} }
    """
    results = {}
    for ticker in ETF_TICKERS:
        try:
            url = f"https://api.nasdaq.com/api/quote/{ticker}/info?assetclass=etf"
            r = session.get(url, headers={
                "Referer": f"https://www.nasdaq.com/market-activity/funds-and-etfs/{ticker.lower()}",
            }, timeout=12)
            if r.status_code != 200:
                print(f"  Nasdaq {ticker}: HTTP {r.status_code}")
                continue

            d = r.json().get("data") or {}
            primary = d.get("primaryData") or {}
            summary = d.get("summaryData") or {}
            key_stats = d.get("keyStats") or {}

            def _sum(key):
                return (summary.get(key) or {}).get("value")

            def _ks(key):
                return (key_stats.get(key) or {}).get("value")

            price      = parse_num(primary.get("lastSalePrice"))
            change     = parse_num(primary.get("netChange"))
            change_pct = parse_num((primary.get("percentageChange") or "").replace("%", ""))
            volume     = parse_num((primary.get("volume") or "").replace(",", ""))

            # NAV: thử nhiều field khác nhau tùy ETF
            nav = parse_num(_sum("Nav") or _sum("Previous Closing Price") or _ks("NavPerShare"))

            # Shares outstanding
            shares_raw = (_sum("Shares Outstanding") or _ks("SharesOutstanding") or "").replace(",", "")
            shares = parse_num(shares_raw)

            # AUM
            aum_raw = _sum("Total Net Assets") or ""
            aum = None
            if aum_raw:
                if "B" in aum_raw.upper():
                    aum = parse_num(re.sub(r"[^0-9\.]", "", aum_raw)) * 1e9
                elif "M" in aum_raw.upper():
                    aum = parse_num(re.sub(r"[^0-9\.]", "", aum_raw)) * 1e6
                else:
                    aum = parse_num(aum_raw)

            results[ticker] = {
                "price": price, "nav": nav, "shares": shares,
                "aum": aum, "change": change, "change_pct": change_pct, "volume": volume,
            }
            print(f"  ✓ Nasdaq {ticker}: ${price}  NAV=${nav}")

        except Exception as e:
            print(f"  ✗ Nasdaq {ticker}: {e}")

        time.sleep(0.4)  # tránh rate-limit

    return results


# ── 5b. iShares JSON endpoint ─────────────────────────────────────
def fetch_ishares(session, product_id, ticker):
    """iShares expose JSON overview + holdings — không cần auth"""
    try:
        base = f"https://www.ishares.com/us/products/{product_id}/{ticker}"
        ajax = f"{base}/1467271812596.ajax"

        # Overview: NAV, shares, AUM
        r = session.get(f"{ajax}?tab=overview&fileType=json",
                        headers={"Referer": f"{base}"}, timeout=15)
        if r.status_code != 200:
            return None

        fd = r.json().get("fundData") or {}
        header = fd.get("fundHeader") or {}
        sci    = (header.get("shareClassInfo") or [{}])[0]
        facts  = header.get("keyFacts") or {}

        nav     = parse_num((sci.get("navAmount") or {}).get("raw") or sci.get("navAmount"))
        shares  = parse_num((facts.get("sharesOutstanding") or {}).get("raw") or facts.get("sharesOutstanding"))
        aum     = parse_num((facts.get("netAssets") or {}).get("raw") or facts.get("netAssets"))
        nav_date = sci.get("navDate")

        # Holdings: lấy số BTC/ETH (holding #1)
        holdings = None
        try:
            rh = session.get(f"{ajax}?tab=holdings&fileType=json",
                             headers={"Referer": f"{base}"}, timeout=12)
            if rh.status_code == 200:
                rows = rh.json().get("tableData", {}).get("rows") or []
                if rows:
                    h0 = rows[0]
                    holdings = parse_num((h0.get("shares") or {}).get("raw") or h0.get("shares"))
        except Exception:
            pass

        print(f"  ✓ iShares {ticker}: NAV={nav}  shares={shares}  holdings={holdings}")
        return {"nav": nav, "shares": shares, "aum": aum, "nav_date": nav_date, "holdings": holdings}

    except Exception as e:
        print(f"  ✗ iShares {ticker}: {e}")
        return None


# ── 5c. ARK / 21Shares CSV ───────────────────────────────────────
def fetch_ark_csv(url, ticker):
    try:
        r = requests.get(url, headers={"User-Agent": FAKE_UA}, timeout=10)
        if r.status_code != 200:
            return None

        reader = csv.DictReader(io.StringIO(r.text))
        rows   = [row for row in reader]
        if not rows:
            return None

        # ARK CSV: company=BITCOIN, shares=số BTC, market value($)=AUM
        row = rows[0]
        headers_lower = {k.lower().strip(): k for k in row.keys()}

        def _get(patterns):
            for pat in patterns:
                for k_low, k_orig in headers_lower.items():
                    if pat in k_low:
                        return row.get(k_orig)
            return None

        holdings = parse_num(_get(["shares", "quantity"]))
        aum      = parse_num((_get(["market value", "market_value"]) or "").replace(",", ""))
        nav_date = _get(["date"])

        print(f"  ✓ ARK CSV {ticker}: holdings={holdings}  aum={aum}")
        return {"holdings": holdings, "aum": aum, "nav_date": nav_date}

    except Exception as e:
        print(f"  ✗ ARK CSV {ticker}: {e}")
        return None


# ── 5d. Franklin Templeton CSV ───────────────────────────────────
def fetch_franklin(ticker):
    try:
        url = (f"https://www.franklintempleton.com/content-en_US/cms/assets"
               f"/fund-resources/{ticker}-holdings.csv")
        r = requests.get(url, headers={"User-Agent": FAKE_UA}, timeout=12)
        if r.status_code != 200:
            return None

        # Bỏ qua metadata rows ở đầu, tìm header thực sự
        lines = r.text.strip().split("\n")
        start = 0
        for i, line in enumerate(lines):
            low = line.lower()
            if "shares" in low or "quantity" in low or "security" in low:
                start = i
                break

        reader = csv.DictReader(io.StringIO("\n".join(lines[start:])))
        rows   = [row for row in reader]
        if not rows:
            return None

        row = rows[0]
        headers_lower = {k.lower().strip(): k for k in row.keys()}

        def _get(patterns):
            for pat in patterns:
                for k_low, k_orig in headers_lower.items():
                    if pat in k_low:
                        return row.get(k_orig)
            return None

        holdings = parse_num(_get(["shares", "quantity"]))
        aum      = parse_num((_get(["market value", "value"]) or "").replace(",", ""))

        print(f"  ✓ Franklin {ticker}: holdings={holdings}  aum={aum}")
        return {
            "holdings": holdings,
            "aum":      aum,
            "nav_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        }
    except Exception as e:
        print(f"  ✗ Franklin {ticker}: {e}")
        return None


# ── 5e. Grayscale ────────────────────────────────────────────────
def fetch_grayscale(session, slug):
    try:
        url = f"https://www.grayscale.com/funds/{slug}"
        r   = session.get(url, timeout=15)
        if r.status_code != 200:
            return None

        html = r.text
        # Tìm JSON embedded trong trang
        m_nav    = re.search(r'"nav[_\s]?(?:per[_\s]?share)?"\s*:\s*"?([\d,\.]+)"?', html, re.I)
        m_aum    = re.search(r'"(?:aum|net[_\s]?assets)"\s*:\s*"?([\d,\.]+)"?', html, re.I)
        m_shares = re.search(r'"shares[_\s]?outstanding"\s*:\s*"?([\d,]+)"?', html, re.I)

        result = {
            "nav":    parse_num(m_nav.group(1))    if m_nav    else None,
            "aum":    parse_num(m_aum.group(1))    if m_aum    else None,
            "shares": parse_num(m_shares.group(1)) if m_shares else None,
        }
        print(f"  ✓ Grayscale {slug}: nav={result['nav']}  aum={result['aum']}")
        return result
    except Exception as e:
        print(f"  ✗ Grayscale {slug}: {e}")
        return None


# ── 5f. Blockstream — on-chain verify BITB (địa chỉ public) ──────
def fetch_onchain_btc(address):
    try:
        r = requests.get(
            f"https://blockstream.info/api/address/{address}",
            timeout=10
        )
        if r.status_code != 200:
            return None
        d = r.json()
        funded  = d["chain_stats"]["funded_txo_sum"]
        spent   = d["chain_stats"]["spent_txo_sum"]
        balance = (funded - spent) / 1e8  # satoshi → BTC
        print(f"  ✓ On-chain BITB: {balance:.2f} BTC")
        return {"balance": balance, "checked_at": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        print(f"  ✗ Blockstream: {e}")
        return None


# ─────────────────────────────────────────────────────
# 6. PIPELINE
# ─────────────────────────────────────────────────────
def run(r2):
    now_utc    = datetime.now(timezone.utc)
    today_str  = now_utc.strftime("%Y-%m-%d")
    session    = get_session()

    # Load snapshot cũ để tính flow (Δ shares × NAV)
    prev_snapshot = r2_get_json(r2, "etf-flows.json") or {}
    prev_etfs     = {e["ticker"]: e for e in prev_snapshot.get("etfs", [])}

    # ── PHASE 1: Market prices (chạy mọi lần) ─────────────────────
    print("\n📈 [1/3] Fetching Nasdaq ETF prices...")
    nasdaq_data = fetch_nasdaq_all(session)

    # ── PHASE 2: Issuer fund data (chỉ khi RUN_MODE=full) ─────────
    issuer_data = {}
    onchain     = {}

    if RUN_MODE == "full":
        print("\n🏦 [2/3] Fetching issuer fund data...")
        for etf in ETF_REGISTRY:
            ticker = etf["ticker"]
            src    = etf["src"]
            data   = None

            if src["type"] == "ishares":
                data = fetch_ishares(session, src["product_id"], ticker)

            elif src["type"] == "ark_csv":
                raw = fetch_ark_csv(src["url"], ticker)
                if raw:
                    # AUM từ CSV là tổng market value của holdings
                    # NAV = market price từ Nasdaq (best fallback)
                    nav = nasdaq_data.get(ticker, {}).get("nav") or \
                          nasdaq_data.get(ticker, {}).get("price")
                    data = {
                        "holdings": raw["holdings"],
                        "aum":      raw["aum"],
                        "nav":      nav,
                        "nav_date": raw["nav_date"],
                    }

            elif src["type"] == "franklin":
                raw = fetch_franklin(src["ticker"])
                if raw:
                    nav = nasdaq_data.get(ticker, {}).get("nav") or \
                          nasdaq_data.get(ticker, {}).get("price")
                    data = {"holdings": raw["holdings"], "aum": raw["aum"],
                            "nav": nav, "nav_date": raw["nav_date"]}

            elif src["type"] == "grayscale":
                data = fetch_grayscale(session, src["slug"])

            # nasdaq_only: dùng data từ Nasdaq phase 1

            if data:
                issuer_data[ticker] = data

            time.sleep(0.5)

        # On-chain verify BITB
        print("\n⛓️  [3/3] On-chain verify BITB...")
        bitb = next((e for e in ETF_REGISTRY if e["ticker"] == "BITB"), None)
        if bitb and bitb.get("custody_addr"):
            oc = fetch_onchain_btc(bitb["custody_addr"])
            if oc:
                onchain["BITB"] = oc
    else:
        print("\n⏭️  [2/3] Skipping issuer data (RUN_MODE=price)")
        print("⏭️  [3/3] Skipping on-chain (RUN_MODE=price)")

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

        # Shares: issuer > nasdaq > prev
        shares = (iss.get("shares") or mkt.get("shares") or
                  (prev.get("fund") or {}).get("shares"))

        # AUM: issuer > nasdaq > compute
        aum = (iss.get("aum") or mkt.get("aum") or
               (shares * nav if shares and nav else None))

        # Holdings (BTC/ETH count)
        holdings = iss.get("holdings")

        # Premium / Discount
        price   = mkt.get("price")
        premium = None
        if price and nav:
            premium = {"usd": price - nav, "pct": (price - nav) / nav * 100}

        # Daily Flow = Δ shares × NAV
        flow = None
        prev_shares = (prev.get("fund") or {}).get("shares")
        if shares and prev_shares and nav and shares != prev_shares:
            delta    = shares - prev_shares
            daily_usd = delta * nav
            flow = {
                "daily_usd":   daily_usd,
                "delta_shares": delta,
                "is_inflow":   daily_usd > 0,
                "computed_at": now_utc.isoformat(),
            }

        # Preserve flow từ hôm qua nếu không tính được hôm nay
        if not flow and prev.get("flow"):
            flow = prev["flow"]

        entry = {
            "ticker":     ticker,
            "name":       etf["name"],
            "issuer":     etf["issuer"],
            "underlying": underlying,
            "fee":        etf["fee"],
            "market": {
                "price":      price,
                "change":     mkt.get("change"),
                "change_pct": mkt.get("change_pct"),
                "volume":     mkt.get("volume"),
            } if mkt else None,
            "fund": {
                "nav":      nav,
                "nav_date": iss.get("nav_date"),
                "shares":   shares,
                "aum":      aum,
                "holdings": holdings,
                "premium":  premium,
            },
            "flow":    flow,
            "onchain": onchain.get(ticker),
        }
        etfs.append(entry)

        # Totals per underlying
        if underlying not in totals:
            totals[underlying] = {"aum": 0.0, "flow": 0.0, "count": 0}
        totals[underlying]["aum"]   += aum or 0
        totals[underlying]["flow"]  += (flow or {}).get("daily_usd") or 0
        totals[underlying]["count"] += 1

    output = {
        "etfs":       etfs,
        "totals":     totals,
        "run_mode":   RUN_MODE,
        "fetched_at": now_utc.isoformat(),
    }

    # ── UPLOAD R2 ──────────────────────────────────────────────────
    print("\n☁️  Uploading to R2...")
    r2_put_json(r2, "etf-flows.json", output, cache_control="max-age=120")
    print("✅ etf-flows.json uploaded")

    # Daily snapshot cho historical data
    if RUN_MODE == "full":
        r2_put_json(r2, f"etf-history/{today_str}.json", output,
                    cache_control="max-age=86400")
        print(f"✅ etf-history/{today_str}.json uploaded")

    # Tổng kết
    for u, t in totals.items():
        flow_sign = "+" if t["flow"] >= 0 else ""
        print(f"   {u}: AUM=${t['aum']/1e9:.1f}B  Flow={flow_sign}${t['flow']/1e6:.0f}M  ({t['count']} ETFs)")


# ─────────────────────────────────────────────────────
# 7. MAIN
# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    import time as _time
    start = _time.time()
    print(f"⚙️  ETF Fetcher — RUN_MODE={RUN_MODE}")

    r2 = get_r2()
    if not r2:
        print("❌ R2 client failed — check secrets")
        exit(1)

    run(r2)
    print(f"\n🏁 Done in {_time.time() - start:.1f}s")
