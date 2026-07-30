"""
Test crawl toàn bộ 17 quỹ Grayscale qua r.jina.ai.

Đã sửa so với bản trước:
1. Strip markdown (*, _, #, |) TRƯỚC khi regex — bug khiến GAVA/GXRP có data
   nhưng tất cả field ra None vì "**TOTAL APT IN TRUST**" không khớp pattern.
2. Tăng MAX_RETRIES 3→6 và backoff dài hơn — log cho thấy nhiều ticker cần
   tới attempt 2-3 mới qua được Kasada challenge (không ổn định theo request).
3. Thêm header x-engine: browser — khuyến nghị chính thức của Jina cho site
   có bot-challenge mạnh.
4. Lưu lại blocked_snippet (300 ký tự đầu của response bị chặn) vào JSON để
   kiểm tra chính xác trang chặn nói gì, thay vì chỉ log ra console.
5. Tăng thời gian nghỉ giữa các ticker 1.5s → 6-9s ngẫu nhiên, giảm áp lực
   lên proxy pool của Jina.
"""

import json
import os
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
OUTPUT_FILE = ROOT / "grayscale_all.json"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 6

GRAYSCALE_FUNDS = [
    {"ticker": "GAVA", "url": "https://etfs.grayscale.com/gava", "kind": "spot_single (Aptos)"},
    {"ticker": "BTC",  "url": "https://etfs.grayscale.com/btc",  "kind": "spot_single (Bitcoin Mini Trust)"},
    {"ticker": "GBTC", "url": "https://etfs.grayscale.com/gbtc", "kind": "spot_single (Bitcoin Trust)"},
    {"ticker": "GLNK", "url": "https://etfs.grayscale.com/glnk", "kind": "spot_single (Chainlink)"},
    {"ticker": "GDOG", "url": "https://etfs.grayscale.com/gdog", "kind": "spot_single (Dogecoin)"},
    {"ticker": "ETHE", "url": "https://etfs.grayscale.com/ethe", "kind": "spot_single (Ethereum Trust)"},
    {"ticker": "ETH",  "url": "https://etfs.grayscale.com/eth",  "kind": "spot_single (Ethereum Mini Trust)"},
    {"ticker": "HYPG", "url": "https://etfs.grayscale.com/hypg", "kind": "spot_single (Hyperliquid)"},
    {"ticker": "GSOL", "url": "https://etfs.grayscale.com/gsol", "kind": "spot_single (Solana)"},
    {"ticker": "GSUI", "url": "https://etfs.grayscale.com/gsui", "kind": "spot_single_staking (Sui)"},
    {"ticker": "GXRP", "url": "https://etfs.grayscale.com/gxrp", "kind": "spot_single (XRP)"},
    {"ticker": "GDLC", "url": "https://etfs.grayscale.com/gdlc", "kind": "multi_asset (rổ 5 coin)"},
    {"ticker": "BCOR", "url": "https://etfs.grayscale.com/bcor", "kind": "equity (Bitcoin Adopters — cổ phiếu cty)"},
    {"ticker": "BTCC", "url": "https://etfs.grayscale.com/btcc", "kind": "derivatives (Bitcoin Covered Call)"},
    {"ticker": "MNRS", "url": "https://etfs.grayscale.com/mnrs", "kind": "equity (Bitcoin Miners — cổ phiếu cty)"},
    {"ticker": "BPI",  "url": "https://etfs.grayscale.com/bpi",  "kind": "derivatives (Bitcoin Premium Income)"},
    {"ticker": "ETCO", "url": "https://etfs.grayscale.com/etco", "kind": "derivatives (Ethereum Covered Call)"},
]


def log(tag, msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] [{tag}] {msg}", flush=True)


def backoff_sleep(attempt):
    delay = min(3 ** attempt, 25) + random.uniform(0, 2)
    log("RETRY", f"sleeping {delay:.2f}s (attempt {attempt})")
    time.sleep(delay)


def fetch_jina(url, ticker):
    """Fetch qua r.jina.ai. Trả về (success, text, status, error, blocked_snippet)."""
    jina_url = f"https://r.jina.ai/{url}"
    headers = {
        "Accept": "text/plain",
        "x-no-cache": "true",
        "x-engine": "browser",
        "x-timeout": "30",
    }
    jina_key = os.getenv("JINA_API_KEY")
    if jina_key:
        headers["Authorization"] = f"Bearer {jina_key}"

    last_blocked_snippet = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log(ticker, f"jina attempt {attempt} -> {jina_url}")
            resp = requests.get(jina_url, headers=headers, timeout=REQUEST_TIMEOUT)
            status = resp.status_code
            text = resp.text or ""
            log(ticker, f"status={status} len={len(text)}")

            if status == 200 and text.strip():
                looks_blocked = (
                    re.search(r"verify your browser|security checkpoint", text, re.IGNORECASE)
                    or len(text) < 500
                )
                if looks_blocked:
                    last_blocked_snippet = text[:300]
                    log(ticker, f"WARNING: nghi checkpoint (len={len(text)}) — snippet: {text[:150]!r}")
                    if attempt < MAX_RETRIES:
                        backoff_sleep(attempt)
                    continue
                return True, text, status, None, None
            else:
                if attempt < MAX_RETRIES:
                    backoff_sleep(attempt)
        except Exception as e:
            log(ticker, f"exception: {e}")
            if attempt < MAX_RETRIES:
                backoff_sleep(attempt)

    return False, None, None, "all jina attempts failed", last_blocked_snippet


def parse_metrics(text, ticker):
    """Trích số liệu cụ thể. Strip markdown TRƯỚC khi regex (bug đã sửa)."""
    if not text:
        return {}

    clean = text.replace("\xa0", " ")
    clean = re.sub(r"[*_#|]", " ", clean)
    clean = re.sub(r"[ \t]+", " ", clean)

    patterns = {
        "total_in_trust": r"TOTAL\s+([A-Z]+)\s+IN\s+TRUST\s*\n?\s*\$?([\d,]{1,15}\.?\d*)",
        "aum_non_gaap": r"ASSETS UNDER MANAGEMENT \(NON-GAAP\)\s*\n?\s*\$?([\d,]{1,15}\.?\d*)",
        "gaap_aum": r"GAAP AUM\s*\n?\s*\$?([\d,]{1,15}\.?\d*)",
        "nav_per_share": r"NET ASSET VALUE \(NAV\) PER SHARE\s*\n?\s*\$?([\d,]{1,15}\.\d+)",
        "market_price": r"(?<!1D CHANGE \()MARKET PRICE\s*\n?\s*\$?([\d,]{1,15}\.\d+)",
        "shares_outstanding": r"SHARES OUTSTANDING\s*\n?\s*([\d,]{1,15}\.?\d*)",
        "sponsors_fee": r"SPONSOR'?S FEE\s*\n?\s*([\d.]+)%",
        "as_of_date": r"As of (\d{1,2}/\d{1,2}/\d{4})",
    }

    result = {}
    coin_symbol = None
    for key, pat in patterns.items():
        m = re.search(pat, clean, re.IGNORECASE)
        if not m:
            result[key] = None
            continue
        if key == "total_in_trust":
            coin_symbol = m.group(1)
            result[key] = float(m.group(2).replace(",", ""))
        elif key == "as_of_date":
            result[key] = m.group(1)
        else:
            result[key] = float(m.group(1).replace(",", ""))

    result["coin_symbol_detected"] = coin_symbol

    title_match = re.search(r"^Title:\s*(.+)$", text, re.MULTILINE)
    result["title"] = title_match.group(1).strip() if title_match else None

    return result


def run_all():
    started_at = datetime.now(timezone.utc).isoformat()
    results = {}

    for fund in GRAYSCALE_FUNDS:
        ticker, url, kind = fund["ticker"], fund["url"], fund["kind"]
        log("=====", f"--- {ticker} ({kind}) ---")

        success, text, status, error, blocked_snippet = fetch_jina(url, ticker)

        entry = {
            "ticker": ticker,
            "url": url,
            "kind": kind,
            "success": success,
            "status_code": status,
            "error_message": error,
            "blocked_snippet": blocked_snippet,
            "raw_markdown_length": len(text) if text else 0,
        }

        if success:
            metrics = parse_metrics(text, ticker)
            entry["metrics"] = metrics
            entry["raw_markdown"] = text

            log(ticker, f"title            = {metrics.get('title')}")
            log(ticker, f"coin_detected    = {metrics.get('coin_symbol_detected')}")
            log(ticker, f"total_in_trust   = {metrics.get('total_in_trust')}")
            log(ticker, f"aum_non_gaap     = {metrics.get('aum_non_gaap')}")
            log(ticker, f"gaap_aum         = {metrics.get('gaap_aum')}")
            log(ticker, f"nav_per_share    = {metrics.get('nav_per_share')}")
            log(ticker, f"market_price     = {metrics.get('market_price')}")
            log(ticker, f"shares_outst.    = {metrics.get('shares_outstanding')}")
            log(ticker, f"sponsors_fee(%)  = {metrics.get('sponsors_fee')}")
            log(ticker, f"as_of_date       = {metrics.get('as_of_date')}")

            has_holdings = metrics.get("total_in_trust") is not None
            log(ticker, f"=> {'✅ CÓ holdings, dùng được cho Flow' if has_holdings else '⚠️  KHÔNG có holdings (bình thường nếu là covered-call/miners/equity/multi-asset)'}")
        else:
            entry["metrics"] = None
            entry["raw_markdown"] = None
            log(ticker, f"❌ FAILED: {error}")
            if blocked_snippet:
                log(ticker, f"   blocked_snippet: {blocked_snippet!r}")

        results[ticker] = entry
        time.sleep(random.uniform(6, 9))

    return results, started_at


def main():
    results, started_at = run_all()

    success_count = sum(1 for r in results.values() if r["success"])
    holdings_count = sum(
        1 for r in results.values()
        if r["success"] and r.get("metrics", {}).get("total_in_trust") is not None
    )

    final_data = {
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_funds_tested": len(GRAYSCALE_FUNDS),
        "success_count": success_count,
        "funds_with_holdings_data": holdings_count,
        "results": results,
    }

    OUTPUT_FILE.write_text(
        json.dumps(final_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log("OUTPUT", f"written to {OUTPUT_FILE} ({success_count}/{len(GRAYSCALE_FUNDS)} thành công, {holdings_count} có holdings)")

    print("\n" + "=" * 70)
    print("TÓM TẮT KẾT QUẢ:")
    print("=" * 70)
    for ticker, entry in results.items():
        status_icon = "✅" if entry["success"] else "❌"
        holdings_icon = ""
        if entry["success"]:
            has_h = entry.get("metrics", {}).get("total_in_trust") is not None
            holdings_icon = " [có holdings]" if has_h else " [không có holdings]"
        print(f"  {status_icon} {ticker:6s} ({entry['kind']}){holdings_icon}")
    print("=" * 70)


if __name__ == "__main__":
    main()
