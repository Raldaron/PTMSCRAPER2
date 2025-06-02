#!/usr/bin/env python
"""
Indeed job scraper – powered by Oxylabs Web Scraper API (Realtime).
No headless browser, no proxies: each page is fetched via one HTTPS POST.

Example:
    python indeed_heartland_jobs.py --pages 5
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import pprint
import sqlite3
import sys
import time
from pathlib import Path
from time import sleep
from typing import Dict, List

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, ReadTimeout

import pandas as pd

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Error: BeautifulSoup4 is required but not installed.")
    print("")
    print("To fix this, create a virtual environment and install dependencies:")
    print("  python -m venv .venv")
    print("  .venv\\Scripts\\activate  (Windows)")
    print("  pip install beautifulsoup4 pandas requests")
    print("")
    print("Or install globally:")
    print("  pip install beautifulsoup4 pandas requests")
    sys.exit(1)

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
DEFAULT_TIMEOUT = 90  # seconds to wait for ONE Oxylabs reply
MAX_RETRIES = 4  # how many times we retry the same page


# --------------------------------------------------------------------------- #
# ---------------------------  helper functions  ---------------------------- #
os.environ["NO_PROXY"] = (
    os.environ.get("NO_PROXY", "") + ",realtime.oxylabs.io"
)
pprint.pprint(
    requests.utils.get_environ_proxies("https://realtime.oxylabs.io")
)


def build_indeed_url(query: str, page: int, country: str) -> str:
    base = INDEED_BASE if country.lower() == "us" else f"https://{country}.indeed.com/jobs"
    start = page * RESULTS_PER_PAGE
    
    # URL encode the query properly
    import urllib.parse
    encoded_query = urllib.parse.quote_plus(query)
    
    url = f"{base}?q={encoded_query}&start={start}"
    logging.info(f"Built URL: {url}")
    return url


def fetch_page_html(url: str, timeout_s: int, retries: int = MAX_RETRIES) -> str:
    payload = {
        "source": "universal",
        "url": url,
        "render": "html"  # Ensure we get rendered HTML
    }
    auth = HTTPBasicAuth(API_USER, API_PASS)
    
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Attempting to fetch: {url}")
            resp = requests.post(
                ENDPOINT,
                json=payload,
                auth=auth,
                headers=HEADERS,
                timeout=(10, timeout_s),  # Increased connection timeout
            )
            
            # Log the response status and content for debugging
            logging.info(f"Response status: {resp.status_code}")
            
            if resp.status_code == 401:
                logging.error("Authentication failed - check your API credentials")
                return ""
            elif resp.status_code == 429:
                logging.warning("Rate limited - waiting longer...")
                sleep(10)
                continue
                
            resp.raise_for_status()
            data = resp.json()
            
            # Log the response structure for debugging
            logging.debug(f"Response keys: {data.keys()}")
            
            content = data.get("results", [{}])[0].get("content", "")
            if not content:
                logging.warning("Empty content received")
            return content
            
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error {e.response.status_code}: {e}")
            if e.response.status_code == 401:
                logging.error("Check your Oxylabs credentials")
                return ""
        except Exception as exc:
            logging.warning(f"Attempt {attempt}/{retries} failed for {url}: {exc}")
            
        sleep(min(2 ** attempt, 10))  # Exponential backoff

    logging.error(f"Failed to fetch page after {retries} attempts: {url}")
    return ""

def parse_jobs(html: str) -> List[Dict[str, str]]:
    if not html:
        logging.warning("No HTML content to parse")
        return []
        
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[Dict[str, str]] = []
    
    # Log a sample of the HTML for debugging
    logging.debug(f"HTML sample: {html[:500]}...")
    
    # Try multiple selector patterns
    selectors = [
        "a.tapItem[data-jk]",
        "[data-jk]",
        ".jobsearch-SerpJobCard",
        ".slider_container .slider_item"
    ]
    
    cards = []
    for selector in selectors:
        cards = soup.select(selector)
        if cards:
            logging.info(f"Found {len(cards)} job cards using selector: {selector}")
            break
    
    if not cards:
        logging.warning("No job cards found with any selector")
        # Save HTML for debugging
        with open("debug_output.html", "w", encoding="utf-8") as f:
            f.write(html)
        return []

    for card in cards:
        try:
            # Try multiple ways to extract job data
            title_elem = (card.find("h2", class_="jobTitle") or 
                         card.find("a", {"data-jk": True}) or
                         card.find("h2"))
            
            company_elem = (card.find("span", class_="companyName") or
                           card.find(".companyName") or
                           card.find("span", string=lambda x: bool(x and len(x.strip()) > 0)))
            
            location_elem = card.find("div", class_="companyLocation")
            
            if title_elem and company_elem:
                job_id = card.get("data-jk", "")
                jobs.append({
                    "title": title_elem.get_text(strip=True),
                    "company": company_elem.get_text(strip=True),
                    "location": location_elem.get_text(strip=True) if location_elem else "",
                    "url": f"https://www.indeed.com/viewjob?jk={job_id}" if job_id else "",
                })
        except Exception as e:
            logging.warning(f"Error parsing job card: {e}")
            continue
    
    logging.info(f"Successfully parsed {len(jobs)} jobs")
    return jobs

def save_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    logging.info("Wrote CSV → %s", path)


def save_sqlite(df: pd.DataFrame, db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        df.to_sql("jobs", conn, if_exists="append", index=False)
    logging.info("Wrote %d rows to SQLite → %s", len(df), db_path)

def test_connection() -> bool:
    """Test the Oxylabs API connection with a simple request"""
    test_url = "https://httpbin.org/get"
    payload = {"source": "universal", "url": test_url}
    auth = HTTPBasicAuth(API_USER, API_PASS)
    
    try:
        resp = requests.post(ENDPOINT, json=payload, auth=auth, headers=HEADERS, timeout=30)
        logging.info(f"Test response status: {resp.status_code}")
        if resp.status_code == 200:
            logging.info("API connection successful")
            return True
        else:
            logging.error(f"API test failed: {resp.text}")
            return False
    except Exception as e:
        logging.error(f"API test error: {e}")
        return False
    
    # ... rest of your code


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
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s: %(message)s"
    )

    # Test API connection first
    if not test_connection():
        logging.error("API connection test failed. Check your credentials and network.")
        return

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
        logging.info(
            "Page %d → %d ads (cumulative %d)",
            page + 1,
            len(rows),
            len(all_rows),
        )
        sleep(args.sleep)

    if not all_rows:
        logging.warning("No jobs found.")
        return

    df = pd.DataFrame(all_rows)
    save_csv(df, args.csv_out)
    save_sqlite(df, args.db_out)
    print(
        f"✓ Scraped {len(df)} unique ads from {df['company'].nunique()} companies"
    )


if __name__ == "__main__":
    main(sys.argv[1:])
