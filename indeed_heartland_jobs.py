"""
Indeed job scraper for Heartland Payroll experience (Playwright advanced version).

Key enhancements implemented:
1. **Rotating residential proxies** – optional `--proxy_file` (one `http://ip:port` per line). A random proxy is used for every request.
2. **Randomised viewport & User‑Agent** – brand‑new browser context per page with random screen size and UA string.
3. **Human‑like scrolling & jitter** – mouse wheel events and random wait times after the page loads.
4. **SQLite persistence** – results are appended to `heartland_jobs.db` (table `jobs`) in addition to the CSV.

Usage example:
    python indeed_heartland_playwright_enhanced.py \
        --query_text "experience with Heartland Payroll" \
        --pages 5 \
        --proxy_file proxies.txt

Dependencies:
    pip install playwright pandas beautifulsoup4
    playwright install chromium
"""

import argparse
import asyncio
import csv
import logging
import random
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

RESULTS_PER_PAGE = 10

# A small pool of modern desktop UAs. Extend as needed.
DEFAULT_USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_0) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Safari/605.1.15"
    ),
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/124.0",
]


def build_search_url(query: str, page: int, country: str) -> str:
    """Compose Indeed search URL."""
    base = "https://www.indeed.com/jobs"
    if country.lower() != "us":
        base = f"https://{country}.indeed.com/jobs"
    start = page * RESULTS_PER_PAGE
    return f"{base}?q={query.replace(' ', '+')}&start={start}"


def random_viewport() -> Dict[str, int]:
    """Return a random viewport dict suitable for Playwright context."""
    return {
        "width": random.randint(1280, 1920),
        "height": random.randint(720, 1080),
    }


def choose_proxy(proxies: List[str]) -> Optional[Dict[str, str]]:
    """Return a random proxy config or None if no proxies provided."""
    if not proxies:
        return None
    proxy_str = random.choice(proxies)
    return {"server": proxy_str}


async def open_context(browser: Browser, proxies: List[str]) -> BrowserContext:
    """Create a fresh browser context with random UA, viewport & proxy."""
    ua = random.choice(DEFAULT_USER_AGENTS)
    viewport = random_viewport()
    proxy_cfg = choose_proxy(proxies)
    context = await browser.new_context(
        user_agent=ua,
        viewport=viewport,
        proxy=proxy_cfg,
    )
    return context


async def human_scroll(page: Page) -> None:
    """Scroll the page a little to mimic human behaviour."""
    for _ in range(random.randint(2, 4)):
        await page.mouse.wheel(0, random.randint(400, 800))
        await page.wait_for_timeout(random.randint(400, 900))


async def fetch_page_html(context: BrowserContext, url: str) -> str:
    """Navigate & return page HTML (with human scrolling)."""
    page = await context.new_page()
    await page.goto(url, timeout=45_000)
    await page.wait_for_selector("a.tapItem[data-jk]", timeout=20_000)
    await human_scroll(page)
    html = await page.content()
    await page.close()
    return html


def parse_jobs(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[Dict[str, str]] = []
    for card in soup.select("a.tapItem[data-jk]"):
        title_elem = card.find("h2", class_="jobTitle")
        company_elem = card.find("span", class_="companyName")
        location_elem = card.find("div", class_="companyLocation")
        if not (title_elem and company_elem):
            continue
        jobs.append(
            {
                "title": title_elem.get_text(strip=True),
                "company": company_elem.get_text(strip=True),
                "location": location_elem.get_text(strip=True) if location_elem else "",
                "url": f"https://www.indeed.com/viewjob?jk={card['data-jk']}",
            }
        )
    return jobs


def load_proxies(path: Optional[str]) -> List[str]:
    if not path:
        return []
    p = Path(path)
    if not p.is_file():
        logging.warning("Proxy file %s not found → proxies disabled", path)
        return []
    return [line.strip() for line in p.read_text().splitlines() if line.strip()]


def save_to_sqlite(df: pd.DataFrame, db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        df.to_sql("jobs", conn, if_exists="append", index=False)


async def scrape(query: str, country: str, pages: int, proxies: List[str]) -> pd.DataFrame:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        all_rows: List[Dict[str, str]] = []
        seen_urls = set()

        for pnum in range(pages):
            context = await open_context(browser, proxies)
            url = build_search_url(query, pnum, country)
            logging.info("Visiting %s", url)
            html = await fetch_page_html(context, url)
            await context.close()

            rows = parse_jobs(html)
            for r in rows:
                if r["url"] not in seen_urls:
                    seen_urls.add(r["url"])
                    all_rows.append(r)
            logging.info("Page %d → %d new ads", pnum + 1, len(rows))

        await browser.close()
    return pd.DataFrame(all_rows)


def main(argv: List[str]) -> None:
    parser = argparse.ArgumentParser(description="Enhanced Playwright Indeed scraper")
    parser.add_argument("--query_text", default="experience with Heartland Payroll")
    parser.add_argument("--country", default="us")
    parser.add_argument("--pages", type=int, default=3)
    parser.add_argument("--proxy_file", help="File with proxies (http://ip:port per line)")
    parser.add_argument("--csv", default="heartland_jobs.csv", help="CSV output filename")
    parser.add_argument("--db", default="heartland_jobs.db", help="SQLite DB filename")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    proxies = load_proxies(args.proxy_file)
    df = asyncio.run(scrape(args.query_text, args.country, args.pages, proxies))
    if df.empty:
        logging.warning("No jobs found.")
        return

    # Persist
    df.to_csv(args.csv, index=False, quoting=csv.QUOTE_NONNUMERIC)
    save_to_sqlite(df, args.db)

    print(
        f"Scraped {len(df)} unique ads from {df['company'].nunique()} companies → "
        f"{args.csv} & {args.db}"
    )


if __name__ == "__main__":
    main(sys.argv[1:])
