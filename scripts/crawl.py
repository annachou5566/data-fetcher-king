"""
Multi-tier scraper with automatic fallback chain:
  1) curl_cffi (TLS/browser impersonation)
  2) Playwright (headless real browser)
  3) cloudscraper
  4) Crawl4AI (last resort)

Always writes a JSON output file, even on total failure.
"""

import asyncio
import json
import os
import random
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

URL = "https://etfs.grayscale.com/btc"
ROOT = Path(__file__).resolve().parent
OUTPUT_FILE = ROOT / "grayscale_btc.json"

REQUEST_TIMEOUT = 30  # seconds, per attempt
MAX_RETRIES_PER_METHOD = 3

REALISTIC_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

DEFAULT_HEADERS = {
    "User-Agent": REALISTIC_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


def log(tag, msg):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] [{tag}] {msg}", flush=True)


def backoff_sleep(attempt):
    base = min(2 ** attempt, 10)
    jitter = random.uniform(0, 1.5)
    delay = base + jitter
    log("RETRY", f"sleeping {delay:.2f}s before retry (attempt {attempt})")
    time.sleep(delay)


def extract_text_and_title(html):
    """Best-effort text/title extraction using BeautifulSoup, never raises."""
    title = None
    extracted_text = None
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html or "", "lxml")
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        extracted_text = soup.get_text(separator="\n", strip=True)
    except Exception as e:
        log("PARSE", f"bs4 extraction failed: {e}")
    return title, extracted_text


def make_result(method, success, status_code=None, html=None, final_url=None,
                 error_message=None, markdown=None, debug=None):
    title, extracted_text = extract_text_and_title(html) if html else (None, None)
    return {
        "method_used": method,
        "success": success,
        "status_code": status_code,
        "final_url": final_url,
        "title": title,
        "html": html,
        "html_length": len(html) if html else 0,
        "extracted_text": extracted_text,
        "cleaned_html": extracted_text,  # alias for compatibility
        "markdown": markdown,
        "markdown_length": len(markdown) if markdown else 0,
        "error_message": error_message,
        "debug": debug or {},
    }

# ---------------------------------------------------------------------------
# Tier 0: r.jina.ai Reader — fetch qua hạ tầng của Jina, né chặn IP GitHub Actions
# Free, không cần signup, không cần API key (rate limit 20 req/phút không key)
# ---------------------------------------------------------------------------
def fetch_jina_reader():
    method = "jina_reader"
    try:
        import requests
    except ImportError as e:
        log(method, f"not installed: {e}")
        return make_result(method, False, error_message=f"import failed: {e}")

    jina_url = f"https://r.jina.ai/{URL}"
    jina_headers = {
        "Accept": "text/plain",
        "x-no-cache": "true",  # bỏ qua cache nếu response cũ từng bị lỗi/chặn
    }
    # Nếu sau này bạn có JINA_API_KEY (vẫn free, chỉ tăng rate limit), tự động dùng
    jina_key = os.getenv("JINA_API_KEY")
    if jina_key:
        jina_headers["Authorization"] = f"Bearer {jina_key}"

    for attempt in range(1, MAX_RETRIES_PER_METHOD + 1):
        try:
            log(method, f"attempt {attempt} -> {jina_url}")
            resp = requests.get(jina_url, headers=jina_headers, timeout=REQUEST_TIMEOUT)
            status = resp.status_code
            text = resp.text or ""
            log(method, f"status={status} len={len(text)}")

            if status == 200 and text.strip():
                # Jina trả về format: "Title: ...\nURL Source: ...\nMarkdown Content:\n..."
                title = None
                for line in text.splitlines()[:5]:
                    if line.lower().startswith("title:"):
                        title = line.split(":", 1)[1].strip()
                        break

                return {
                    "method_used": method,
                    "success": True,
                    "status_code": status,
                    "final_url": jina_url,
                    "title": title,
                    "html": None,
                    "html_length": 0,
                    "extracted_text": text,
                    "cleaned_html": text,
                    "markdown": text,
                    "markdown_length": len(text),
                    "error_message": None,
                    "debug": {"attempt": attempt, "source": "r.jina.ai"},
                }
            else:
                log(method, f"non-200 or empty body (status={status})")
                if attempt < MAX_RETRIES_PER_METHOD:
                    backoff_sleep(attempt)
        except Exception as e:
            log(method, f"exception: {e}")
            if attempt < MAX_RETRIES_PER_METHOD:
                backoff_sleep(attempt)

    return make_result(method, False, error_message="all jina_reader attempts failed")

# ---------------------------------------------------------------------------
# Tier 1: curl_cffi (TLS / HTTP2 / browser fingerprint impersonation)
# ---------------------------------------------------------------------------
def fetch_curl_cffi():
    method = "curl_cffi"
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError as e:
        log(method, f"not installed: {e}")
        return make_result(method, False, error_message=f"import failed: {e}")

    impersonations = ["chrome124", "chrome120", "chrome110", "safari17_0"]

    for attempt in range(1, MAX_RETRIES_PER_METHOD + 1):
        imp = impersonations[(attempt - 1) % len(impersonations)]
        try:
            log(method, f"attempt {attempt} using impersonate={imp}")
            resp = cffi_requests.get(
                URL,
                impersonate=imp,
                timeout=REQUEST_TIMEOUT,
                headers=DEFAULT_HEADERS,
                allow_redirects=True,
            )
            status = resp.status_code
            log(method, f"status={status} len={len(resp.text or '')}")

            if status == 200 and resp.text:
                return make_result(
                    method, True, status_code=status, html=resp.text,
                    final_url=str(resp.url),
                    debug={"impersonate": imp, "attempt": attempt},
                )
            else:
                log(method, f"non-200 or empty body (status={status})")
                if attempt < MAX_RETRIES_PER_METHOD:
                    backoff_sleep(attempt)
        except Exception as e:
            log(method, f"exception: {e}")
            if attempt < MAX_RETRIES_PER_METHOD:
                backoff_sleep(attempt)

    return make_result(method, False, error_message="all curl_cffi attempts failed")


# ---------------------------------------------------------------------------
# Tier 2: Playwright headless (real browser, executes JS)
# ---------------------------------------------------------------------------
async def fetch_playwright():
    method = "playwright"
    try:
        from playwright.async_api import async_playwright
    except ImportError as e:
        log(method, f"not installed: {e}")
        return make_result(method, False, error_message=f"import failed: {e}")

    for attempt in range(1, MAX_RETRIES_PER_METHOD + 1):
        try:
            log(method, f"attempt {attempt}")
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                    ],
                )
                context = await browser.new_context(
                    user_agent=REALISTIC_UA,
                    viewport={"width": 1366, "height": 768},
                    locale="en-US",
                    extra_http_headers={
                        k: v for k, v in DEFAULT_HEADERS.items()
                        if k != "User-Agent"
                    },
                )

                # Best-effort stealth patch, optional dependency
                try:
                    from playwright_stealth import stealth_async
                    page = await context.new_page()
                    await stealth_async(page)
                except ImportError:
                    page = await context.new_page()

                response = await page.goto(
                    URL, timeout=REQUEST_TIMEOUT * 1000, wait_until="domcontentloaded"
                )
                # Give the SPA a moment to render
                try:
                    await page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass

                html = await page.content()
                status = response.status if response else None
                final_url = page.url

                await context.close()
                await browser.close()

                log(method, f"status={status} len={len(html or '')}")

                if status and status < 400 and html:
                    return make_result(
                        method, True, status_code=status, html=html,
                        final_url=final_url, debug={"attempt": attempt},
                    )
                else:
                    log(method, f"bad status={status}")
                    if attempt < MAX_RETRIES_PER_METHOD:
                        backoff_sleep(attempt)
        except Exception as e:
            log(method, f"exception: {e}")
            if attempt < MAX_RETRIES_PER_METHOD:
                backoff_sleep(attempt)

    return make_result(method, False, error_message="all playwright attempts failed")


# ---------------------------------------------------------------------------
# Tier 3: cloudscraper
# ---------------------------------------------------------------------------
def fetch_cloudscraper():
    method = "cloudscraper"
    try:
        import cloudscraper
    except ImportError as e:
        log(method, f"not installed: {e}")
        return make_result(method, False, error_message=f"import failed: {e}")

    for attempt in range(1, MAX_RETRIES_PER_METHOD + 1):
        try:
            log(method, f"attempt {attempt}")
            scraper = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
            resp = scraper.get(URL, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
            log(method, f"status={resp.status_code} len={len(resp.text or '')}")

            if resp.status_code == 200 and resp.text:
                return make_result(
                    method, True, status_code=resp.status_code, html=resp.text,
                    final_url=resp.url, debug={"attempt": attempt},
                )
            else:
                if attempt < MAX_RETRIES_PER_METHOD:
                    backoff_sleep(attempt)
        except Exception as e:
            log(method, f"exception: {e}")
            if attempt < MAX_RETRIES_PER_METHOD:
                backoff_sleep(attempt)

    return make_result(method, False, error_message="all cloudscraper attempts failed")


# ---------------------------------------------------------------------------
# Tier 4: Crawl4AI (last resort — known to 429 on this site already)
# ---------------------------------------------------------------------------
async def fetch_crawl4ai():
    method = "crawl4ai"
    try:
        from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
    except ImportError as e:
        log(method, f"not installed: {e}")
        return make_result(method, False, error_message=f"import failed: {e}")

    for attempt in range(1, MAX_RETRIES_PER_METHOD + 1):
        try:
            log(method, f"attempt {attempt}")
            browser_config = BrowserConfig(headless=True, verbose=False, user_agent=REALISTIC_UA)
            run_config = CrawlerRunConfig(
                page_timeout=int(REQUEST_TIMEOUT * 1000),
                cache_mode=CacheMode.BYPASS,
                wait_for="css:body",
                word_count_threshold=1,
            )
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=URL, config=run_config)

            success = getattr(result, "success", False)
            status_code = getattr(result, "status_code", None)
            html = getattr(result, "html", None)
            markdown = getattr(result, "markdown", None)
            error_message = getattr(result, "error_message", None)

            log(method, f"success={success} status={status_code}")

            if success:
                return make_result(
                    method, True, status_code=status_code, html=html,
                    markdown=markdown if isinstance(markdown, str) else str(markdown) if markdown else None,
                    debug={"attempt": attempt},
                )
            else:
                log(method, f"failed: {error_message}")
                if attempt < MAX_RETRIES_PER_METHOD:
                    backoff_sleep(attempt)
        except Exception as e:
            log(method, f"exception: {e}")
            if attempt < MAX_RETRIES_PER_METHOD:
                backoff_sleep(attempt)

    return make_result(method, False, error_message="all crawl4ai attempts failed")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
async def run_all_tiers():
    attempts_log = []

    # Tier 0 — thử trước tiên, né chặn IP GitHub Actions
    r = fetch_jina_reader()
    attempts_log.append({"method": r["method_used"], "success": r["success"], "error": r["error_message"]})
    if r["success"]:
        return r, attempts_log

    # Tier 1
    r = fetch_curl_cffi()
    attempts_log.append({"method": r["method_used"], "success": r["success"], "error": r["error_message"]})
    if r["success"]:
        return r, attempts_log

    # Tier 2
    r = await fetch_playwright()
    attempts_log.append({"method": r["method_used"], "success": r["success"], "error": r["error_message"]})
    if r["success"]:
        return r, attempts_log

    # Tier 3
    r = fetch_cloudscraper()
    attempts_log.append({"method": r["method_used"], "success": r["success"], "error": r["error_message"]})
    if r["success"]:
        return r, attempts_log

    # Tier 4 (last resort)
    r = await fetch_crawl4ai()
    attempts_log.append({"method": r["method_used"], "success": r["success"], "error": r["error_message"]})
    return r, attempts_log


async def main():
    started_at = datetime.now(timezone.utc).isoformat()
    result, attempts_log = None, []

    try:
        result, attempts_log = await run_all_tiers()
    except Exception as e:
        log("FATAL", f"unexpected top-level exception: {e}")
        result = make_result(
            "none", False,
            error_message=f"unexpected top-level exception: {e}\n{traceback.format_exc()}",
        )

    final_data = {
        "url": URL,
        "workspace": os.getenv("GITHUB_WORKSPACE"),
        "script_path": str(Path(__file__).resolve()),
        "output_path": str(OUTPUT_FILE.resolve()),
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "attempts_summary": attempts_log,
        **result,
    }

    # Always write output, no matter what
    try:
        OUTPUT_FILE.write_text(
            json.dumps(final_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log("OUTPUT", f"written to {OUTPUT_FILE}")
    except Exception as e:
        # last-ditch effort: write a minimal error file so artifact upload never has nothing
        log("OUTPUT", f"failed to write full JSON: {e}")
        OUTPUT_FILE.write_text(
            json.dumps({"success": False, "error_message": f"failed to serialize result: {e}"}),
            encoding="utf-8",
        )

    if not final_data.get("success"):
        # Non-zero exit so the job is visibly marked failed,
        # but the artifact/JSON has already been written above.
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
