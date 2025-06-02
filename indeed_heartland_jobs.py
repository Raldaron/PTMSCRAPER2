#!/usr/bin/env python
"""Indeed job scraper – Playwright + Oxylabs proxies (Pylance-clean)."""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import random
import sqlite3
import string
import sys
from pathlib import Path
from typing import List, Optional, TypedDict, cast

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

# --------------------------------------------------------------------------- #
# ----------------------------  type helpers  --------------------------------#
class ViewportSize(TypedDict):
    width: int
    height: int


class ProxySettings(TypedDict, total=False):
    server: str
    username: Optional[str]
    password: Optional[str]
    bypass: Optional[str]


# --------------------------------------------------------------------------- #
# -----------------------------  constants  ----------------------------------#
INDEED_BASE = "https://www.indeed.com/jobs"
RESULTS_PER_PAGE = 10

UA_POOL: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)…Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4)…Chrome/123.0 Safari/537.36",
]

# --------------------------------------------------------------------------- #
# ------------------------  proxy-helper section  ----------------------------#
def _rand_sid(n: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def build_oxy_proxy(
    user: str,
    pwd: str,
    oxy_type: str = "resi",
    geo: str | None = None,
) -> str:
    if oxy_type == "resi":
        parts = [f"customer-{user}", f"sess-{_rand_sid()}"]
        if geo:
            cc, *_ = geo.split(",", 1)
            parts.append(f"cc-{cc.lower()}")
        username = "-".join(parts)
        return f"http://{username}:{pwd}@pr.oxylabs.io:7777"
    return f"http://{user}:{pwd}@dc.pr.oxylabs.io:10000"


def load_proxy_list(path: Path) -> list[str]:
    if not path.is_file():
        logging.warning("Proxy file %s not found – skipping.", path)
        return []
    with path.open(encoding="utf-8") as f:
        proxies = [ln.strip() for ln in f if ln.strip()]
    random.shuffle(proxies)
    return proxies


async def live_proxies(candidates: list[str]) -> list[str]:
    if not candidates:
        return []
    ok: list[str] = []
    timeout = aiohttp.ClientTimeout(total=6)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        for pxy in candidates:
            try:
                async with sess.head("https://www.indeed.com", proxy=pxy) as r:
                    if r.status < 400:
                        ok.append(pxy)
            except Exception:
                continue
    logging.info("Proxy check: %d / %d live", len(ok), len(candidates))
    return ok


# --------------------------------------------------------------------------- #
# --------------------------  Playwright helpers  ---------------------------#
def rand_viewport() -> ViewportSize:
    return {"width": random.randint(1280, 1920), "height": random.randint(720, 1080)}


def pick_proxy(proxies: list[str]) -> ProxySettings | None:
    return {"server": random.choice(proxies)} if proxies else None


async def open_browser() -> Browser:
    pw = await async_playwright().start()
    return await pw.chromium.launch(headless=True)


async def new_context(browser: Browser, proxies: list[str]) -> BrowserContext:
    ctx = await browser.new_context(
        user_agent=random.choice(UA_POOL),
        viewport=cast(ViewportSize, rand_viewport()),
        proxy=cast(ProxySettings, pick_proxy(proxies)),
    )
    return ctx


async def human_scroll(page: Page) -> None:
    await page.mouse.wheel(0, 600)
    await page.wait_for_timeout(random.randint(400, 800))


async def fetch_html(
    ctx: BrowserContext,
    url: str,
    proxies: list[str],
    timeout_ms: int = 60_000,
    retries: int = 3,
) -> str:
    for attempt in range(1, retries + 1):
        page = await ctx.new_page()
        try:
            await page.goto(url, timeout=timeout_ms, wait_until="load")
            await page.wait_for_selector("a.tapItem[data-jk]", timeout=20_000)
            await human_scroll(page)
            html = await page.content()
            await page.close()
            return html
        except Exception as exc:
            logging.warning("Timeout (%s) attempt %d/%d at %s",
                            type(exc).__name__, attempt, retries, url)
            await page.close()
            # new context = new proxy / UA
            browser_ref = ctx.browser
            if browser_ref is None:
                raise RuntimeError("Lost connection to browser")
            ctx = await new_context(cast(Browser, browser_ref), proxies)
    raise TimeoutError(f"Failed to fetch {url} after {retries} retries")


# --------------------------------------------------------------------------- #
# ------------------------------  scraping  ----------------------------------#
def search_url(query: str, page: int, country: str) -> str:
    base = INDEED_BASE if country.lower() == "us" else f"https://{country}.indeed.com/jobs"
    return f"{base}?q={query.replace(' ', '+')}&start={page * RESULTS_PER_PAGE}"


def parse_jobs(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict[str, str]] = []
    for card in soup.select("a.tapItem[data-jk]"):
        title = card.find("h2", class_="jobTitle")
        company = card.find("span", class_="companyName")
        loc = card.find("div", class_="companyLocation")
        if not (title and company):
            continue
        out.append(
            dict(
                title=title.get_text(strip=True),
                company=company.get_text(strip=True),
                location=loc.get_text(strip=True) if loc else "",
                url=f"https://www.indeed.com/viewjob?jk={card['data-jk']}",
            )
        )
    return out


async def scrape(
    query: str,
    country: str,
    pages: int,
    proxies: list[str],
    nav_timeout: int,
) -> pd.DataFrame:
    browser = await open_browser()
    ctx = await new_context(browser, proxies)
    seen: set[str] = set()
    rows: list[dict[str, str]] = []
    try:
        for p in range(pages):
            url = search_url(query, p, country)
            logging.info("Visiting %s", url)
            html = await fetch_html(ctx, url, proxies, nav_timeout)
            for job in parse_jobs(html):
                if job["url"] not in seen:
                    seen.add(job["url"])
                    rows.append(job)
            logging.info("Page %d done – total rows %d", p + 1, len(rows))
    finally:
        await browser.close()
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# ------------------------------  storage  -----------------------------------#
def save_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    logging.info("CSV → %s", path)


def save_sqlite(df: pd.DataFrame, db: Path) -> None:
    import sqlite3

    with sqlite3.connect(db) as conn:
        df.to_sql("jobs", conn, if_exists="append", index=False)
    logging.info("SQLite → %s (+%d rows)", db, len(df))


# --------------------------------------------------------------------------- #
# --------------------------------  CLI  ------------------------------------#
def arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("Indeed scraper (Playwright + Oxylabs)")
    p.add_argument("--query_text", default="experience with Heartland Payroll")
    p.add_argument("--country", default="us")
    p.add_argument("--pages", type=int, default=3)
    p.add_argument("--oxy_type", choices=["resi", "dc"], default="resi")
    p.add_argument("--oxy_user", default=os.getenv("OXY_USER"))
    p.add_argument("--oxy_pass", default=os.getenv("OXY_PASS"))
    p.add_argument("--proxy_file", type=Path, default=Path("proxies.txt"))
    p.add_argument("--nav_timeout", type=int, default=60_000)
    p.add_argument("--csv_out", type=Path, default=Path("heartland_jobs.csv"))
    p.add_argument("--db_out", type=Path, default=Path("heartland_jobs.db"))
    return p


async def main_async(args: argparse.Namespace) -> None:
    proxies: list[str] = []
    if args.oxy_user and args.oxy_pass:
        proxies.append(build_oxy_proxy(args.oxy_user, args.oxy_pass, args.oxy_type))
    proxies.extend(load_proxy_list(args.proxy_file))
    proxies = await live_proxies(proxies)

    df = await scrape(
        args.query_text, args.country, args.pages, proxies, args.nav_timeout
    )
    if df.empty:
        logging.warning("No jobs found.")
        return

    df.drop_duplicates(subset=["url"], inplace=True)
    save_csv(df, args.csv_out)
    save_sqlite(df, args.db_out)
    print(f"✓ Scraped {len(df)} ads from {df['company'].nunique()} companies")


def main(argv: list[str]) -> None:
    args = arg_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main(sys.argv[1:])
