#!/usr/bin/env python
"""
Indeed job scraper – powered by Oxylabs Web Scraper API (Realtime).
No headless browser, no proxies: each page is fetched via one HTTPS POST.

Example:
    python indeed_heartland_oxylabs.py --pages 5
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sqlite3
import sys
from pathlib import Path
from time import sleep
from typing import Dict, List

import pandas as pd
import requests, os, pprint
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth
import time
from requests.exceptions import ReadTimeout, ConnectionError

# --------------------------------------------------------------------------- #
# ----------------------------  credentials  -------------------------------- #
API_USER = "rstyshklfrd_7uSI4"
API_PASS = "Wv+dHF8zgtM7XVv"

ENDPOINT = "https://realtime.oxylabs.io/v1/queries"
INDEED_BASE = "https://www.indeed.com/jobs"
RESULTS_PER_PAGE = 10
HEADERS = {"Content-Type": "application/json"}

# --------------------------------------------------------------------------- #
# ----------------------------- config tweak -------------------------------- #
DEFAULT_TIMEOUT = 90          # seconds to wait for ONE Oxylabs reply
MAX_RETRIES     = 4           # how many times we retry the same page


# --------------------------------------------------------------------------- #
# ---------------------------  helper functions  ---------------------------- #
os.environ["NO_PROXY"] = os.environ.get("NO_PROXY", "") + ",realtime.oxylabs.io"
pprint.pprint(requests.utils.get_environ_proxies("https://realtime.oxylabs.io"))
def build_indeed_url(query: str, page: int, country: str) -> str:
    base = INDEED_BASE if country.lower() == "us" else f"https://{country}.indeed.com/jobs"
    start = page * RESULTS_PER_PAGE
    return f"{base}?q={query.replace(' ', '+')}&start={start}"

def fetch_page_html(url: str, timeout_s: int, retries: int = 4) -> str:
    payload = {"source": "universal", "url": url}
    auth = HTTPBasicAuth(API_USER, API_PASS)
    no_proxy = {"https": ""}        # <-- fixed

    for attempt in range(1, retries + 1):
        try:
            r = requests.post(
                ENDPOINT,
                json=payload,
                auth=auth,
                timeout=(5, timeout_s),
                proxies=no_proxy        # <-- fixed
            )
            # … keep the rest unchanged …
        except Exception as e:
            if attempt == retries:
                logging.error("Failed to fetch page after %d attempts: %s", retries, e)
                return ""
            sleep(2)
    return ""


def parse_jobs(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[Dict[str, str]] = []

    for card in soup.select("a.tapItem[data-jk]"):
        title = card.find("h2", class_="jobTitle")
        company = card.find("span", class_="companyName")
        location = card.find("div", class_="companyLocation")
        if not (title and company):
            continue
        jobs.append(
            {
                "title": title.get_text(strip=True),
                "company": company.get_text(strip=True),
                "location": location.get_text(strip=True) if location else "",
                "url": f"https://www.indeed.com/viewjob?jk={card['data-jk']}",
            }
        )
    return jobs


def save_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    logging.info("Wrote CSV → %s", path)


def save_sqlite(df: pd.DataFrame, db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        df.to_sql("jobs", conn, if_exists="append", index=False)
    logging.info("Wrote %d rows to SQLite → %s", len(df), db_path)


# --------------------------------------------------------------------------- #
# ---------------------------------  CLI  -----------------------------------#
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("Indeed scraper using Oxylabs Web Scraper API")
    p.add_argument("--query_text", default="experience with Heartland Payroll")
    p.add_argument("--country", default="us")
    p.add_argument("--pages", type=int, default=3)

    # ⬇️  new line
    p.add_argument(
        "--req_timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="seconds to wait for one Oxylabs response (default 90)",
    )

    p.add_argument("--csv_out", type=Path, default=Path("heartland_jobs.csv"))
    p.add_argument("--db_out", type=Path, default=Path("heartland_jobs.db"))
    p.add_argument("--sleep", type=float, default=0.5)
    return p



def main(argv: List[str]) -> None:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    seen_urls: set[str] = set()
    all_rows: List[Dict[str, str]] = []

    for page in range(args.pages):
        url = build_indeed_url(args.query_text, page, args.country)
        logging.info("Fetching %s", url)
        html = fetch_page_html(url, args.req_timeout)
        rows = parse_jobs(html)
        for r in rows:
            if r["url"] not in seen_urls:
                seen_urls.add(r["url"])
                all_rows.append(r)
        logging.info("Page %d → %d ads (cumulative %d)",
                     page + 1, len(rows), len(all_rows))
        sleep(args.sleep)

    if not all_rows:
        logging.warning("No jobs found.")
        return

    df = pd.DataFrame(all_rows)
    save_csv(df, args.csv_out)
    save_sqlite(df, args.db_out)
    print(f"✓ Scraped {len(df)} unique ads from {df['company'].nunique()} companies")


if __name__ == "__main__":
    main(sys.argv[1:])
