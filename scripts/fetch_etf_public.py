#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_etf_public.py

Probe-only ETF holdings fetcher.
- Does NOT modify your existing fetch_etf.py
- Does NOT upload anything
- Does NOT write R2 by default
- Prints structured logs so you can copy the result back to ChatGPT

What it tries:
1) BlackRock / iShares product API for IBIT, ETHA
2) ARK public CSV for ARKB
3) Generic issuer website probing:
   - candidate holdings URLs
   - CSV/JSON/HTML discovery
   - link extraction for .csv/.json holdings files
4) If a previous holdings snapshot exists in R2 (read-only, optional), it can compute flow = delta holdings * price

Environment:
  RUN_MODE=probe|full          (default: probe)
  READ_R2=1|0                  (default: 0)
  R2_ACCESS_KEY_ID             optional
  R2_SECRET_ACCESS_KEY         optional
  R2_ENDPOINT_URL              optional
  R2_BUCKET_NAME               optional
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
import cloudscraper

try:
    import boto3
    from botocore.config import Config
    HAS_BOTO3 = True
except Exception:
    HAS_BOTO3 = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except Exception:
    HAS_BS4 = False


RUN_MODE = os.getenv("RUN_MODE", "probe").strip().lower()
READ_R2 = os.getenv("READ_R2", "0").strip() == "1"

R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

FAKE_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

PRICE_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "HYP": "hyperliquid",
}

# Keep this aligned with your current coverage target.
ETF_REGISTRY = [
    {"ticker":"IBIT","issuer":"BlackRock","underlying":"BTC","src":"ishares","product_id":"333011"},
    {"ticker":"ETHA","issuer":"BlackRock","underlying":"ETH","src":"ishares","product_id":"337614"},

    {"ticker":"ARKB","issuer":"ARK/21Shares","underlying":"BTC","src":"ark_csv","ark_fund_name":"21SHARES_BITCOIN_ETF"},

    {"ticker":"FBTC","issuer":"Fidelity","underlying":"BTC","src":"generic"},
    {"ticker":"FETH","issuer":"Fidelity","underlying":"ETH","src":"generic"},
    {"ticker":"FSOL","issuer":"Fidelity","underlying":"SOL","src":"generic"},

    {"ticker":"BITB","issuer":"Bitwise","underlying":"BTC","src":"generic"},
    {"ticker":"ETHW","issuer":"Bitwise","underlying":"ETH","src":"generic"},
    {"ticker":"BSOL","issuer":"Bitwise","underlying":"SOL","src":"generic"},
    {"ticker":"BHYP","issuer":"Bitwise","underlying":"HYP","src":"generic"},

    {"ticker":"GBTC","issuer":"Grayscale","underlying":"BTC","src":"generic"},
    {"ticker":"BTC","issuer":"Grayscale","underlying":"BTC","src":"generic"},
    {"ticker":"ETHE","issuer":"Grayscale","underlying":"ETH","src":"generic"},
    {"ticker":"GSOL","issuer":"Grayscale","underlying":"SOL","src":"generic"},
    {"ticker":"HYPG","issuer":"Grayscale","underlying":"HYP","src":"generic"},

    {"ticker":"HODL","issuer":"VanEck","underlying":"BTC","src":"generic"},
    {"ticker":"ETHV","issuer":"VanEck","underlying":"ETH","src":"generic"},
    {"ticker":"VSOL","issuer":"VanEck","underlying":"SOL","src":"generic"},

    {"ticker":"EZBC","issuer":"Franklin","underlying":"BTC","src":"generic"},
    {"ticker":"EZET","issuer":"Franklin","underlying":"ETH","src":"generic"},
    {"ticker":"SOEZ","issuer":"Franklin","underlying":"SOL","src":"generic"},

    {"ticker":"BTCO","issuer":"Invesco","underlying":"BTC","src":"generic"},
    {"ticker":"QETH","issuer":"Invesco","underlying":"ETH","src":"generic"},

    {"ticker":"BTCW","issuer":"WisdomTree","underlying":"BTC","src":"generic"},
    {"ticker":"BRRR","issuer":"Valkyrie","underlying":"BTC","src":"generic"},

    # 21Shares / others
    {"ticker":"CETH","issuer":"21Shares","underlying":"ETH","src":"generic"},
    {"ticker":"TSOL","issuer":"21Shares","underlying":"SOL","src":"generic"},
    {"ticker":"THYP","issuer":"21Shares","underlying":"HYP","src":"generic"},
]

STATIC_BTC_HOLDINGS = {
    "FBTC": 204870.57,
    "GBTC": 203601.41,
    "ARKB": 157218.40,
    "BITB": 141486.62,
    "HODL": 22924.98,
    "EZBC": 17942.63,
    "BTCW": 15745.38,
    "BTCO": 14510.33,
    "BRRR": 6939.32,
}

HOLDINGS_HISTORY_KEY = "etf-holdings-history.json"

KNOWN_ISSUER_DOMAINS = {
    "BlackRock": ["ishares.com"],
    "ARK/21Shares": ["ark-funds.com", "21shares.com", "assets.ark-funds.com"],
    "Fidelity": ["fidelity.com", "digital.fidelity.com"],
    "Bitwise": ["bitwiseinvestments.com", "bitwiseinvestments.com"],
    "Grayscale": ["grayscale.com"],
    "VanEck": ["vaneck.com"],
    "Franklin": ["franklintempleton.com"],
    "Invesco": ["invesco.com"],
    "WisdomTree": ["wisdomtree.com"],
    "Valkyrie": ["valkyriefunds.com", "valkyrieinvest.com"],
    "21Shares": ["21shares.com"],
}

ISSUER_HEADER_ORDER = [
    "BlackRock", "ARK/21Shares", "Fidelity", "Bitwise", "Grayscale",
    "VanEck", "Franklin", "Invesco", "WisdomTree", "Valkyrie", "21Shares",
]


def parse_num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s in ("", "N/A", "--", "None", "null", "-"):
        return None
    s = re.sub(r"[$,%\s]", "", s)
    try:
        return float(s)
    except Exception:
        return None


def get_session():
    s = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "desktop": True})
    s.headers.update({
        "User-Agent": FAKE_UA,
        "Accept": "application/json,text/plain,text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def get_r2():
    if not HAS_BOTO3:
        return None
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT_URL, R2_BUCKET_NAME]):
        return None
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


def load_prices():
    prices = {}
    try:
        ids = ",".join(PRICE_IDS.values())
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
        r = requests.get(url, headers={"User-Agent": FAKE_UA}, timeout=15)
        if r.status_code == 200:
            d = r.json()
            for sym, cid in PRICE_IDS.items():
                if cid in d and "usd" in d[cid]:
                    prices[sym] = float(d[cid]["usd"])
    except Exception as e:
        print(f"[PRICE] {e}")
    print("[PRICE]", " ".join(f"{k}=${v}" for k, v in prices.items()) or "no data")
    return prices


def fetch_json(session, url, headers=None, timeout=20):
    try:
        r = session.get(url, headers=headers or {}, timeout=timeout)
        if r.status_code != 200:
            return None, r
        try:
            return r.json(), r
        except Exception:
            return None, r
    except Exception as e:
        return {"_error": str(e)}, None


def fetch_html(session, url, headers=None, timeout=20):
    try:
        r = session.get(url, headers=headers or {}, timeout=timeout)
        if r.status_code != 200:
            return None, r
        return r.text, r
    except Exception as e:
        return f"__ERROR__:{e}", None


def extract_links(html: str, base_url: str) -> List[str]:
    out = []
    if not html:
        return out

    if HAS_BS4:
        try:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a.get("href")
                if not href:
                    continue
                if href.startswith("//"):
                    href = "https:" + href
                elif href.startswith("/"):
                    from urllib.parse import urljoin
                    href = urljoin(base_url, href)
                out.append(href)
        except Exception:
            pass
    else:
        # very light fallback
        for m in re.finditer(r'href=["\']([^"\']+)["\']', html, re.I):
            href = m.group(1)
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                from urllib.parse import urljoin
                href = urljoin(base_url, href)
            out.append(href)
    # unique preserve order
    seen = set()
    uniq = []
    for u in out:
        if u not in seen:
            uniq.append(u)
            seen.add(u)
    return uniq


def extract_potential_data_urls(html: str, base_url: str) -> List[str]:
    urls = []
    if not html:
        return urls

    # raw CSV/JSON link hints
    for m in re.finditer(r'(https?://[^\s"\']+\.(?:csv|json))', html, re.I):
        urls.append(m.group(1))

    # relative URLs ending csv/json
    for m in re.finditer(r'(["\'])([^"\']+\.(?:csv|json))\1', html, re.I):
        u = m.group(2)
        if u.startswith("/"):
            from urllib.parse import urljoin
            u = urljoin(base_url, u)
        urls.append(u)

    # filenames with holdings
    for m in re.finditer(r'(https?://[^\s"\']+holdings[^\s"\']*)', html, re.I):
        urls.append(m.group(1))

    # unique preserve order
    seen = set()
    uniq = []
    for u in urls:
        if u not in seen:
            uniq.append(u)
            seen.add(u)
    return uniq


def parse_csv_holdings(text: str) -> Tuple[Optional[float], Optional[str], Optional[float]]:
    """
    Try to parse holdings + nav date + aum from CSV-like content.
    Returns: (holdings, nav_date, aum)
    """
    if not text:
        return None, None, None
    try:
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            return None, None, None

        for row in rows:
            row_l = {str(k).lower(): v for k, v in row.items() if k is not None}
            name = str(row_l.get("company") or row_l.get("fund") or row_l.get("issuer") or "").lower()
            if "cash" in name or "total" in name:
                continue

            qty = None
            for key in ("shares", "shares held", "shares_held", "quantity", "units held", "unitsheld"):
                if key in row_l and row_l[key]:
                    qty = parse_num(row_l[key])
                    if qty and qty > 0:
                        break

            aum = None
            for key in ("market value", "marketvalue", "aum", "value", "mv"):
                if key in row_l and row_l[key]:
                    aum = parse_num(row_l[key])
                    if aum and aum > 0:
                        break

            nav_date = row_l.get("date") or row_l.get("asofdate") or row_l.get("as of date")
            if qty is not None:
                return qty, nav_date, aum
    except Exception:
        return None, None, None
    return None, None, None


def try_fetch_any_csv_or_json(session, url: str) -> Dict[str, Any]:
    """
    Tries URL directly and returns normalized data if it looks like holdings.
    """
    try:
        r = session.get(url, headers={"User-Agent": FAKE_UA}, timeout=20, allow_redirects=True)
        ctype = (r.headers.get("content-type") or "").lower()
        text = r.text if hasattr(r, "text") else ""
        result = {
            "url": url,
            "final_url": r.url,
            "status_code": r.status_code,
            "content_type": ctype,
        }
        if r.status_code != 200:
            result["ok"] = False
            return result

        if "json" in ctype or text.lstrip().startswith("{") or text.lstrip().startswith("["):
            try:
                data = r.json()
                result["ok"] = True
                result["kind"] = "json"
                result["data_preview"] = str(data)[:400]
                # We don't assume schema. Just preserve.
                return result
            except Exception:
                pass

        if "csv" in ctype or ".csv" in url.lower() or "holdings" in url.lower():
            qty, nav_date, aum = parse_csv_holdings(text)
            result["ok"] = qty is not None or aum is not None
            result["kind"] = "csv"
            result["holdings"] = qty
            result["nav_date"] = nav_date
            result["aum"] = aum
            return result

        # HTML fallback
        result["ok"] = True
        result["kind"] = "html"
        result["has_holdings_word"] = "holdings" in text.lower()
        result["has_aum_word"] = "aum" in text.lower()
        result["has_csv_word"] = ".csv" in text.lower()
        result["links"] = extract_links(text, r.url)[:20]
        result["data_urls"] = extract_potential_data_urls(text, r.url)[:20]
        return result
    except Exception as e:
        return {"url": url, "ok": False, "error": str(e)}


def compute_flow(today_holdings, prev_holdings, price):
    if today_holdings is None or prev_holdings is None or price is None:
        return None
    if prev_holdings <= 0:
        return None
    return (today_holdings - prev_holdings) * price


@dataclass
class ETFResult:
    ticker: str
    issuer: str
    underlying: str
    source: str
    status: str
    holdings: Optional[float] = None
    aum: Optional[float] = None
    nav_date: Optional[str] = None
    price: Optional[float] = None
    flow: Optional[Dict[str, Any]] = None
    notes: List[str] = field(default_factory=list)
    probes: List[Dict[str, Any]] = field(default_factory=list)


class BaseAdapter:
    name: str = "Base"

    def supports(self, etf: Dict[str, Any]) -> bool:
        return False

    def fetch(self, session, etf: Dict[str, Any], prices: Dict[str, float]) -> ETFResult:
        raise NotImplementedError


class BlackRockAdapter(BaseAdapter):
    name = "BlackRock"

    def supports(self, etf):
        return etf.get("src") == "ishares"

    def _url(self, pid, excl, incl, as_of=None):
        base = "https://www.ishares.com/varnish-api/blk-one01-product-data/product-data/api/v2/get-product-data"
        p = (
            f"component=holdings.all&portfolioId={pid}"
            f"&appSubType=ISHARES&appType=PRODUCT_PAGE"
            f"&locale=en_US&targetSite=us-ishares&userType=individual"
            f"&excludeContent={'true' if excl else 'false'}"
            f"&includeConfig={'true' if incl else 'false'}"
        )
        if as_of:
            p += f"&asOfDate={as_of}"
        return f"{base}?{p}"

    def fetch(self, session, etf, prices):
        t = etf["ticker"]
        pid = etf["product_id"]
        underlying = etf["underlying"]
        price = prices.get(t)
        result = ETFResult(ticker=t, issuer=etf["issuer"], underlying=underlying, source="ishares", status="starting", price=price)

        hdrs = {
            "Referer": f"https://www.ishares.com/us/products/{pid}/",
            "Accept": "application/json,*/*",
            "User-Agent": FAKE_UA,
        }

        try:
            r = session.get(self._url(pid, True, True), headers=hdrs, timeout=20)
            if r.status_code != 200:
                result.status = f"config_http_{r.status_code}"
                result.notes.append(f"config_http_{r.status_code}")
                return result
            d = r.json()
            comp = (d.get("componentsByNameMap") or {}).get("holdings", {})
            cont = (comp.get("containersByNameMap") or {}).get("all", {})
            dmap = cont.get("dataPointsByNameMap", {})
            dates = (((dmap.get("dateList") or {}).get("value")) or [])
            latest_date = str(dates[0]) if dates else None
        except Exception as e:
            result.status = "config_error"
            result.notes.append(str(e))
            return result

        try:
            r = session.get(self._url(pid, False, False, as_of=latest_date), headers=hdrs, timeout=25)
            if r.status_code != 200:
                result.status = f"data_http_{r.status_code}"
                result.notes.append(f"data_http_{r.status_code}")
                return result
            d = r.json()
            comp = (d.get("componentsByNameMap") or {}).get("holdings", {})
            cont = (comp.get("containersByNameMap") or {}).get("all", {})
            dmap = cont.get("dataPointsByNameMap", {})
            mv = ((dmap.get("marketValue") or {}).get("value")) or []
            aum = max((v for v in mv if isinstance(v, (int, float)) and v > 0), default=None)

            holdings = None
            for key in ("unitsHeld", "sharesHeld", "quantity"):
                arr = ((dmap.get(key) or {}).get("value")) or []
                if arr:
                    h = parse_num(arr[0] if isinstance(arr, list) else arr)
                    if h and 1 < h < 1_000_000_000:
                        holdings = h
                        break

            ao = ((dmap.get("asOfDate") or {}).get("value"))
            result.status = "ok"
            result.holdings = holdings
            result.aum = aum
            result.nav_date = str(ao) if ao else latest_date
            return result
        except Exception as e:
            result.status = "data_error"
            result.notes.append(str(e))
            return result


class ArkCsvAdapter(BaseAdapter):
    name = "ARK CSV"

    def supports(self, etf):
        return etf.get("src") == "ark_csv"

    def fetch(self, session, etf, prices):
        t = etf["ticker"]
        result = ETFResult(ticker=t, issuer=etf["issuer"], underlying=etf["underlying"], source="ark_csv", status="starting", price=prices.get(t))
        fund_name = etf["ark_fund_name"]
        url = f"https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_{fund_name}_ETF_{t}_HOLDINGS.csv"
        try:
            r = session.get(url, headers={"User-Agent": FAKE_UA}, timeout=20)
            if r.status_code != 200:
                result.status = f"csv_http_{r.status_code}"
                result.notes.append(url)
                return result
            text = r.content.decode("utf-8-sig", errors="ignore")
            qty, nav_date, aum = parse_csv_holdings(text)
            result.status = "ok" if (qty is not None or aum is not None) else "csv_parse_failed"
            result.holdings = qty
            result.nav_date = nav_date
            result.aum = aum
            result.notes.append(url)
            return result
        except Exception as e:
            result.status = "csv_error"
            result.notes.append(str(e))
            return result


class GenericIssuerAdapter(BaseAdapter):
    name = "Generic issuer probe"

    def __init__(self):
        self.domain_map = KNOWN_ISSUER_DOMAINS

    def supports(self, etf):
        return etf.get("src") == "generic"

    def candidate_urls(self, etf):
        issuer = etf["issuer"]
        ticker = etf["ticker"].lower()

        domains = self.domain_map.get(issuer, [])
        if not domains:
            # Try a few common patterns, but keep it bounded.
            domains = [re.sub(r"[^a-z0-9]+", "", issuer.lower()) + ".com"]

        paths = [
            f"/{ticker}",
            f"/products/{ticker}",
            f"/product/{ticker}",
            f"/funds/{ticker}",
            f"/etfs/{ticker}",
            f"/etf/{ticker}",
            f"/holdings/{ticker}",
            f"/portfolio/{ticker}",
            f"/{ticker}/holdings",
            f"/products/{ticker}/holdings",
            f"/etfs/{ticker}/holdings",
            f"/funds/{ticker}/holdings",
        ]

        urls = []
        for domain in domains:
            for scheme in ("https://www.", "https://"):
                base = f"{scheme}{domain}" if scheme == "https://" else f"{scheme}{domain}"
                # base may duplicate www but it is okay for probing
                for p in paths:
                    urls.append(base + p)
        # unique preserve order
        seen = set()
        uniq = []
        for u in urls:
            if u not in seen:
                uniq.append(u)
                seen.add(u)
        return uniq[:30]

    def discover_from_html(self, session, html_url, html_text):
        found = []
        links = extract_links(html_text, html_url)
        for link in links:
            if any(k in link.lower() for k in (".csv", ".json", "holdings", "portfolio", "data")):
                found.append(link)
        for u in extract_potential_data_urls(html_text, html_url):
            found.append(u)
        # unique preserve order
        seen = set()
        uniq = []
        for u in found:
            if u not in seen:
                uniq.append(u)
                seen.add(u)
        return uniq[:25]

    def fetch(self, session, etf, prices):
        t = etf["ticker"]
        issuer = etf["issuer"]
        result = ETFResult(
            ticker=t,
            issuer=issuer,
            underlying=etf["underlying"],
            source="generic",
            status="probing",
            price=prices.get(t),
        )

        for url in self.candidate_urls(etf):
            probe = try_fetch_any_csv_or_json(session, url)
            result.probes.append(probe)
            if not probe.get("ok"):
                continue

            kind = probe.get("kind")
            if kind == "csv":
                if probe.get("holdings") is not None:
                    result.status = "ok_csv"
                    result.holdings = probe.get("holdings")
                    result.aum = probe.get("aum")
                    result.nav_date = probe.get("nav_date")
                    result.notes.append(url)
                    return result
            elif kind == "json":
                # generic JSON found; keep as probe evidence
                result.status = "json_found"
                result.notes.append(url)
                # continue probing in case we find direct holdings CSV later
            elif kind == "html":
                # inspect HTML for additional links
                html_text, _ = fetch_html(session, probe.get("final_url") or url)
                if html_text and not str(html_text).startswith("__ERROR__"):
                    discovered = self.discover_from_html(session, probe.get("final_url") or url, html_text)
                    for durl in discovered:
                        probe2 = try_fetch_any_csv_or_json(session, durl)
                        result.probes.append(probe2)
                        if probe2.get("kind") == "csv" and probe2.get("holdings") is not None:
                            result.status = "ok_discovered_csv"
                            result.holdings = probe2.get("holdings")
                            result.aum = probe2.get("aum")
                            result.nav_date = probe2.get("nav_date")
                            result.notes.append(durl)
                            return result
                        if probe2.get("kind") == "json":
                            result.status = "json_discovered"
                else:
                    continue

        # If no direct holding source found, keep evidence.
        if any((p.get("status_code") == 200 for p in result.probes if isinstance(p, dict))):
            if result.status == "probing":
                result.status = "needs_manual_review"
        else:
            result.status = "no_public_endpoint_found"
        return result


def load_holdings_history(r2):
    if not r2:
        return {}
    data = r2_get_json(r2, HOLDINGS_HISTORY_KEY)
    return data if isinstance(data, dict) else {}


def main():
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    print("=" * 80)
    print(f"ETF public probe — RUN_MODE={RUN_MODE}  READ_R2={int(READ_R2)}")
    print("=" * 80)

    session = get_session()
    prices = load_prices()

    r2 = get_r2() if READ_R2 else None
    holdings_history = load_holdings_history(r2) if r2 else {}

    adapters = [BlackRockAdapter(), ArkCsvAdapter(), GenericIssuerAdapter()]

    grouped: Dict[str, List[ETFResult]] = {}
    for issuer in ISSUER_HEADER_ORDER:
        grouped[issuer] = []
    others: Dict[str, List[ETFResult]] = {}

    total_ok = 0
    total_fail = 0
    total_partial = 0

    for etf in ETF_REGISTRY:
        issuer = etf["issuer"]
        adapter = next((a for a in adapters if a.supports(etf)), None)
        if adapter is None:
            result = ETFResult(
                ticker=etf["ticker"],
                issuer=issuer,
                underlying=etf["underlying"],
                source="none",
                status="no_adapter",
                price=prices.get(etf["ticker"]),
            )
        else:
            result = adapter.fetch(session, etf, prices)

        # Compute flow if possible and previous holdings exists
        prev_holdings = (holdings_history.get(result.ticker) or {}).get("holdings")
        if result.holdings is not None and result.price is not None:
            coin_price = prices.get(result.underlying)
            flow_val = compute_flow(result.holdings, prev_holdings, coin_price)
            if flow_val is not None:
                result.flow = {
                    "daily_usd": flow_val,
                    "is_inflow": flow_val > 0,
                    "source": "self_computed",
                    "date": today,
                }

        if result.status.startswith("ok") or result.status in ("json_found", "json_discovered"):
            if result.holdings is not None:
                total_ok += 1
            else:
                total_partial += 1
        elif result.status in ("needs_manual_review", "probing", "no_public_endpoint_found", "no_adapter"):
            total_fail += 1
        else:
            total_partial += 1

        if issuer in grouped:
            grouped[issuer].append(result)
        else:
            others.setdefault(issuer, []).append(result)

    # Print grouped logs
    def print_group(title: str, items: List[ETFResult]):
        print("\n" + "=" * 80)
        print(title.upper())
        print("=" * 80)
        for r in items:
            status = r.status
            holdings = f"{r.holdings:.4f}" if isinstance(r.holdings, (int, float)) else "NA"
            aum = f"${r.aum/1e9:.2f}B" if isinstance(r.aum, (int, float)) else "NA"
            flow = "NA"
            if r.flow and isinstance(r.flow.get("daily_usd"), (int, float)):
                flow = f'{"+" if r.flow["daily_usd"] >= 0 else ""}${r.flow["daily_usd"]/1e6:.2f}M'
            price = f"${r.price:.4f}" if isinstance(r.price, (int, float)) else "NA"
            print(f"{r.ticker:<6} {status:<22} price={price:<12} holdings={holdings:<14} aum={aum:<10} flow={flow}")
            if r.notes:
                print(f"       notes: {', '.join(r.notes[:3])}")
            # keep probe summary short so the log is copyable
            for p in r.probes[:2]:
                if isinstance(p, dict):
                    label = p.get("kind") or "probe"
                    url = p.get("final_url") or p.get("url") or ""
                    code = p.get("status_code")
                    if code:
                        print(f"       probe: {label} {code} {url}")
                    else:
                        print(f"       probe: {label} {url}")

    for issuer in ISSUER_HEADER_ORDER:
        print_group(issuer, grouped.get(issuer, []))

    if others:
        for issuer, items in others.items():
            print_group(issuer, items)

    # summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Success : {total_ok}")
    print(f"Partial : {total_partial}")
    print(f"Failed  : {total_fail}")

    fail_list = [r.ticker for grp in list(grouped.values()) + list(others.values()) for r in grp if not r.status.startswith("ok") and r.status not in ("json_found", "json_discovered")]
    if fail_list:
        print("\nFailed tickers:")
        print(", ".join(fail_list))

    # Totals by underlying, based only on known holdings
    totals: Dict[str, Dict[str, float]] = {}
    for grp in list(grouped.values()) + list(others.values()):
        for r in grp:
            if r.underlying not in totals:
                totals[r.underlying] = {"aum": 0.0, "flow": 0.0, "count": 0}
            totals[r.underlying]["count"] += 1
            totals[r.underlying]["aum"] += r.aum or 0.0
            totals[r.underlying]["flow"] += (r.flow or {}).get("daily_usd") or 0.0

    print("\nUnderlying totals (known only):")
    for u, t in totals.items():
        s = "+" if t["flow"] >= 0 else ""
        print(f"{u}: AUM=${t['aum']/1e9:.2f}B  Flow={s}${t['flow']/1e6:.2f}M  count={t['count']}")

    print("\nDone.")
    print("If you want to improve coverage, paste me the FAIL list and a few probe lines.")
    print("=" * 80)


if __name__ == "__main__":
    main()
