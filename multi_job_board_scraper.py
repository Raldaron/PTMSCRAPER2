import argparse
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import datetime as dt
import logging
from typing import List, Dict


def polite_sleep() -> None:
    """Pause between requests."""
    time.sleep(random.uniform(0.5, 1.5))


def request_with_retry(url: str, params: Dict = None, headers: Dict = None) -> requests.Response:
    """HTTP GET with retries and error handling."""
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            if resp.status_code == 403 or 'captcha' in resp.text.lower():
                raise RuntimeError('Blocked by CAPTCHA or 403')
            if resp.status_code >= 500:
                polite_sleep()
                continue
            resp.raise_for_status()
            return resp
        except Exception as exc:
            logging.warning("Attempt %s failed for %s: %s", attempt + 1, url, exc)
            polite_sleep()
    raise RuntimeError(f"Failed to fetch {url}")


def fetch_upwork(keyword: str, max_posts: int) -> List[Dict[str, str]]:
    """Fetch postings from Upwork."""
    posts, offset = [], 0
    url = "https://www.upwork.com/ab/find-work/api/1.0/jobs"
    while len(posts) < max_posts:
        params = {"q": keyword, "paging": f"{offset};50"}
        try:
            resp = request_with_retry(url, params=params)
        except RuntimeError:
            logging.error("Upwork blocked")
            return []
        data = resp.json().get("searchResults", [])
        if not data:
            break
        for job in data:
            posts.append({
                "board": "Upwork",
                "post_id": job.get("ciphertext") or job.get("id"),
                "title": job.get("title"),
                "company": job.get("client", {}).get("company_name"),
                "location": job.get("client", {}).get("country"),
                "url": f"https://www.upwork.com/jobs/{job.get('ciphertext')}",
                "date_posted": job.get("created_on"),
            })
            if len(posts) >= max_posts:
                break
        offset += 50
        polite_sleep()
    return posts


def fetch_freelancer(keyword: str, max_posts: int) -> List[Dict[str, str]]:
    """Fetch postings from Freelancer."""
    posts, offset, limit = [], 0, 50
    url = "https://www.freelancer.com/api/projects/0.1/projects/active/"
    while len(posts) < max_posts:
        params = {"query": keyword, "offset": offset, "limit": limit}
        try:
            resp = request_with_retry(url, params=params)
        except RuntimeError:
            logging.error("Freelancer blocked")
            return []
        data = resp.json().get("result", {}).get("projects", [])
        if not data:
            break
        for proj in data:
            posts.append({
                "board": "Freelancer",
                "post_id": proj.get("id"),
                "title": proj.get("title"),
                "company": str(proj.get("owner_id")),
                "location": proj.get("location", {}).get("country"),
                "url": f"https://www.freelancer.com/projects/{proj.get('seo_url')}",
                "date_posted": proj.get("submitdate"),
            })
            if len(posts) >= max_posts:
                break
        offset += limit
        polite_sleep()
    return posts


def fetch_dice(keyword: str, max_posts: int) -> List[Dict[str, str]]:
    """Scrape postings from Dice."""
    posts, page = [], 1
    url = "https://www.dice.com/jobs"
    while len(posts) < max_posts:
        params = {"q": keyword, "page": page}
        try:
            resp = request_with_retry(url, params=params)
        except RuntimeError:
            logging.error("Dice blocked")
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("dhi-job-card")
        if not cards:
            break
        for card in cards:
            post_id = card.get("data-jobid")
            posts.append({
                "board": "Dice",
                "post_id": post_id,
                "title": (card.select_one("a.card-title-link") or {}).get_text(strip=True),
                "company": (card.select_one(".card-company") or {}).get_text(strip=True),
                "location": (card.select_one(".card-location") or {}).get_text(strip=True),
                "url": f"https://www.dice.com/job-detail/{post_id}",
                "date_posted": (card.select_one("relative-time") or {}).get("datetime"),
            })
            if len(posts) >= max_posts:
                break
        page += 1
        polite_sleep()
    return posts


def fetch_hcareers(keyword: str, max_posts: int) -> List[Dict[str, str]]:
    """Scrape postings from HCareers."""
    posts, page = [], 1
    url = "https://www.hcareers.com/search-jobs"
    while len(posts) < max_posts:
        params = {"q": keyword, "page": page}
        try:
            resp = request_with_retry(url, params=params)
        except RuntimeError:
            logging.error("HCareers blocked")
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select(".job-card")
        if not cards:
            break
        for card in cards:
            link = card.select_one("a")
            posts.append({
                "board": "HCareers",
                "post_id": card.get("data-job-id"),
                "title": (card.select_one(".job-title") or {}).get_text(strip=True),
                "company": (card.select_one(".job-employer") or {}).get_text(strip=True),
                "location": (card.select_one(".job-location") or {}).get_text(strip=True),
                "url": f"https://www.hcareers.com{link['href']}" if link else None,
                "date_posted": (card.select_one(".job-date") or {}).get_text(strip=True),
            })
            if len(posts) >= max_posts:
                break
        page += 1
        polite_sleep()
    return posts


def fetch_usajobs(keyword: str, max_posts: int) -> List[Dict[str, str]]:
    """Fetch postings from USAJobs API."""
    posts, page = [], 1
    url = "https://data.usajobs.gov/api/search"
    headers = {"User-Agent": "job-scraper"}
    while len(posts) < max_posts:
        params = {"Keyword": keyword, "Page": page}
        try:
            resp = request_with_retry(url, params=params, headers=headers)
        except RuntimeError:
            logging.error("USAJobs blocked")
            return []
        data = resp.json().get("SearchResult", {}).get("SearchResultItems", [])
        if not data:
            break
        for item in data:
            job = item.get("MatchedObjectDescriptor", {})
            posts.append({
                "board": "USAJobs",
                "post_id": job.get("PositionID"),
                "title": job.get("PositionTitle"),
                "company": job.get("OrganizationName"),
                "location": job.get("PositionLocationDisplay"),
                "url": job.get("PositionURI"),
                "date_posted": job.get("PublicationStartDate"),
            })
            if len(posts) >= max_posts:
                break
        page += 1
        polite_sleep()
    return posts


def normalize_jobs(records: List[Dict[str, str]]) -> pd.DataFrame:
    """Return deduplicated DataFrame."""
    df = pd.DataFrame(records)
    df["_key"] = (df["title"].fillna("").str.lower() + df["company"].fillna("").str.lower())
    df = df.drop_duplicates("_key").drop(columns="_key")
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape multiple job boards")
    parser.add_argument("--keyword", default="Heartland Payroll")
    parser.add_argument("--max_posts", type=int, default=200)
    parser.add_argument("--out_csv", default="heartland_multiboard_jobs.csv")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    all_posts = []
    for func in [fetch_upwork, fetch_freelancer, fetch_dice, fetch_hcareers, fetch_usajobs]:
        try:
            all_posts.extend(func(args.keyword, args.max_posts))
        except RuntimeError:
            continue

    total = len(all_posts)
    df = normalize_jobs(all_posts)
    df.to_csv(args.out_csv, index=False)
    logging.info(
        "Scraped %s total posts across 5 boards -> %s unique rows", total, len(df)
    )


if __name__ == "__main__":
    main()
