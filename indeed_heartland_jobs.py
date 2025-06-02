"""Indeed job scraper for Heartland Payroll experience.

This script queries Indeed for job ads mentioning Heartland Payroll.
Results are saved to ``heartland_jobs.csv``.
"""

import argparse
import logging
import random
import sys
import time
from typing import Dict, List
from urllib.parse import quote_plus, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Constant desktop User-Agent string for all requests
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/117.0 Safari/537.36"
)


def build_search_url(query: str, page: int, country: str) -> str:
    """Construct an Indeed search URL for the given query and page."""
    base = "https://www.indeed.com/jobs"
    if country.lower() != "us":
        # Use country subdomain if not US
        base = f"https://{country}.indeed.com/jobs"
    query_param = quote_plus(query)
    start = page * 10
    return f"{base}?q={query_param}&start={start}"


def fetch_page(url: str, session: requests.Session) -> str:
    """Retrieve HTML content from ``url`` with retry and CAPTCHA detection."""
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = session.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logging.warning("Request failed: %s. Retrying in 5s...", exc)
        time.sleep(5)
        try:
            resp = session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as exc2:
            logging.error("Second attempt failed: %s", exc2)
            return ""
    # Skip pages that show captcha/unusual traffic notice
    if "our systems have detected unusual traffic" in resp.text.lower():
        logging.warning("CAPTCHA detected at %s", url)
        return ""
    return resp.text


def parse_jobs(html: str) -> List[Dict[str, str]]:
    """Parse individual job cards from search results HTML."""
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[Dict[str, str]] = []
    for card in soup.select("a.tapItem[data-jk]"):
        title_elem = card.find("h2", class_="jobTitle")
        company_elem = card.find("span", class_="companyName")
        location_elem = card.find("div", class_="companyLocation")
        if not title_elem or not company_elem or not card.get("href"):
            continue
        job = {
            "title": title_elem.get_text(strip=True),
            "company": company_elem.get_text(strip=True),
            "location": location_elem.get_text(strip=True) if location_elem else "",
            "url": urljoin("https://www.indeed.com", card["href"]),
        }
        jobs.append(job)
    return jobs


def scrape_jobs(query: str, country: str, pages: int) -> pd.DataFrame:
    """Scrape multiple result pages and return all unique job ads."""
    session = requests.Session()
    all_jobs: List[Dict[str, str]] = []  # collected job rows
    seen_urls = set()  # track URLs to avoid duplicates

    for page in range(pages):
        # Build and fetch each paginated result set
        url = build_search_url(query, page, country)
        logging.info("Fetching %s", url)
        html = fetch_page(url, session)
        if not html:
            continue
        jobs = parse_jobs(html)
        # Deduplicate by job URL
        for job in jobs:
            if job["url"] not in seen_urls:
                seen_urls.add(job["url"])
                all_jobs.append(job)
        logging.info("Page %d: %d ads", page + 1, len(jobs))
        time.sleep(random.uniform(1, 3))

    return pd.DataFrame(all_jobs)


def main(argv: List[str]) -> None:
    """Entry point for the command line interface."""
    parser = argparse.ArgumentParser(
        description="Scrape Indeed for job ads mentioning Heartland Payroll"
    )
    parser.add_argument("--query_text", default="experience with Heartland Payroll")
    parser.add_argument("--country", default="us")
    parser.add_argument("--pages", type=int, default=3)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

    df = scrape_jobs(args.query_text, args.country, args.pages)
    if df.empty:
        logging.info("No jobs found.")
        return

    df.to_csv("heartland_jobs.csv", index=False)
    summary = (
        f"Scraped {len(df)} unique ads from "
        f"{df['company'].nunique()} companies → heartland_jobs.csv"
    )
    print(summary)


if __name__ == "__main__":
    main(sys.argv[1:])
