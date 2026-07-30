import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

URL = "https://etfs.grayscale.com/btc"
OUTPUT_FILE = Path("grayscale_btc.json")


async def main():
    browser_config = BrowserConfig(
        headless=True,
        verbose=True,
    )

    run_config = CrawlerRunConfig(
        page_timeout=60000,
        remove_overlay_elements=True,
        process_iframes=True,
        cache_mode=CacheMode.BYPASS,
        wait_for="js:document.readyState === 'complete'",
        word_count_threshold=1,
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        result = await crawler.arun(
            url=URL,
            config=run_config,
        )

    data = {
        "url": URL,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "success": getattr(result, "success", None),
        "status_code": getattr(result, "status_code", None),
        "error_message": getattr(result, "error_message", None),
        "title": getattr(result, "title", None),
        "html_length": len(getattr(result, "html", "") or ""),
        "cleaned_html_length": len(getattr(result, "cleaned_html", "") or ""),
        "markdown_length": len(getattr(result, "markdown", "") or ""),
        "html": getattr(result, "html", None),
        "cleaned_html": getattr(result, "cleaned_html", None),
        "markdown": getattr(result, "markdown", None),
        "links": getattr(result, "links", None),
        "media": getattr(result, "media", None),
        "metadata": getattr(result, "metadata", None),
    }

    OUTPUT_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    asyncio.run(main())
