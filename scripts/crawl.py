"""
Crawl toàn bộ 17 quỹ Grayscale qua r.jina.ai với API key.

Thay đổi so với bản free-tier:
1. Bắt buộc JINA_API_KEY (route qua pool ổn định hơn free-tier ẩn danh,
   free-tier trước đó dính 429 dai dẳng do rate-limit theo IP dùng chung).
2. Phát hiện đúng bản chất lỗi: "Vercel Security Checkpoint" + "429 Too Many
   Requests" là Grayscale tự rate-limit ở tầng gốc, KHÔNG phải bot-challenge
   — nên retry dồn dập không giúp ích, giảm hẳn số lần retry, tăng nghỉ giữa
   các ticker để tránh cộng dồn request vào cùng cửa sổ rate-limit.
3. Timeout tổng thể được tính toán để không bị GitHub Actions cancel giữa
   chừng (17 ticker x thời gian tối đa mỗi ticker phải nằm dưới timeout job).
"""

import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
OUTPUT_FILE = ROOT / "grayscale_all.json"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3          # giảm từ 6 — lỗi 429 dai dẳng, retry nhiều không giúp
DELAY_BETWEEN_TICKERS = (8, 12)  # giây, ngẫu nhiên

JINA_API_KEY = os.getenv("JINA_API_KEY")

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
    delay = min(4 ** attempt, 20) + random.uniform(0, 2)
    log("RETRY", f"sleeping {delay:.2f}s (attempt {attempt})")
    time.sleep(delay)


def is_rate_limited_response(text):
    """Nhận diện đúng bản chất: trang lỗi 429 gốc của Vercel/Grayscale,
    không phải bot-challenge thật."""
    if not text:
        return False
    return bool(re.search(r"Vercel Security Checkpoint|Too Many Requests", text, re.IGNORECASE)) or len(text) < 500


def fetch_jina(url, ticker):
    """Fetch qua r.jina.ai với API key. Trả về (success, text, status, error, blocked_snippet)."""
    jina_url = f"https://r.jina.ai/{url}"
    headers = {
        "Accept": "text/plain",
        "x-no-cache": "true",
        "x-engine": "browser",
        "x-timeout": "30",
        "Authorization": f"Bearer {JINA_API_KEY}",
    }

    last_blocked_snippet = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log(ticker, f"jina attempt {attempt} -> {jina_url}")
            resp = requests.get(jina_url, headers=headers, timeout=REQUEST_TIMEOUT)
            status = resp.status_code
            text = resp.text or ""
            log(ticker, f"status={status} len={len(text)}")

            # Header rate-limit của chính Jina (khác với 429 do Grayscale trả về
            # trong BODY) — nếu Jina tự rate-limit request của mình thì retry
            # có ý nghĩa hơn nhiều so với việc Grayscale rate-limit ở nguồn.
            remaining = resp.headers.get("x-ratelimit-remaining")
            if remaining is not None:
                log(ticker, f"jina rate-limit remaining={remaining}")

            if status == 200 and text.strip():
                if is_rate_limited_response(text):
                    last_blocked_snippet = text[:300]
                    log(ticker, f"WARNING: Grayscale trả 429 (qua Jina) — snippet: {text[:150]!r}")
                    if attempt < MAX_RETRIES:
                        backoff_sleep(attempt)
                    continue
                return True, text, status, None, None
            else:
                last_blocked_snippet = text[:300] if text else None
                if attempt < MAX_RETRIES:
                    backoff_sleep(attempt)
        except Exception as e:
            log(ticker, f"exception: {e}")
            if attempt < MAX_RETRIES:
                backoff_sleep(attempt)

    return False, None, None, "all jina attempts failed (Grayscale rate-limit hoặc lỗi khác)", last_blocked_snippet


def parse_metrics(text, ticker):
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

    for i, fund in enumerate(GRAYSCALE_FUNDS):
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

        # Ghi JSON từng phần sau MỖI ticker — nếu job bị cancel/timeout giữa
        # chừng (như lần trước), vẫn có dữ liệu partial thay vì mất trắng.
        _write_partial(results, started_at)

        if i < len(GRAYSCALE_FUNDS) - 1:
            time.sleep(random.uniform(*DELAY_BETWEEN_TICKERS))

    return results, started_at


def _write_partial(results, started_at):
    success_count = sum(1 for r in results.values() if r["success"])
    holdings_count = sum(
        1 for r in results.values()
        if r["success"] and r.get("metrics", {}).get("total_in_trust") is not None
    )
    data = {
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_funds_tested": len(GRAYSCALE_FUNDS),
        "funds_processed_so_far": len(results),
        "success_count": success_count,
        "funds_with_holdings_data": holdings_count,
        "results": results,
    }
    OUTPUT_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    if not JINA_API_KEY:
        log("FATAL", "Thiếu JINA_API_KEY — thêm secret JINA_API_KEY trong GitHub repo settings")
        # vẫn ghi 1 file JSON rỗng để artifact upload không bị lỗi hoàn toàn
        OUTPUT_FILE.write_text(
            json.dumps({"error": "JINA_API_KEY not set", "results": {}}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        sys.exit(1)

    log("INIT", f"JINA_API_KEY loaded (length={len(JINA_API_KEY)})")

    results, started_at = run_all()
    _write_partial(results, started_at)

    success_count = sum(1 for r in results.values() if r["success"])
    holdings_count = sum(
        1 for r in results.values()
        if r["success"] and r.get("metrics", {}).get("total_in_trust") is not None
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
