#!/usr/bin/env python3
"""Collect real-time chatter referencing Heartland Payroll from multiple sources."""

import argparse
import os
import logging
import time
from datetime import datetime, timedelta
import json
import re
from typing import List, Dict

import requests
import praw
from bs4 import BeautifulSoup


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since_hours",
        type=int,
        default=24,
        help="How far back to pull posts.",
    )
    parser.add_argument(
        "--twitter_bearer",
        default=os.getenv("TW_BEARER"),
        help="Twitter API bearer token.",
    )
    parser.add_argument(
        "--reddit_client",
        default=os.getenv("REDDIT_CLIENT_ID"),
        help="Reddit API client ID.",
    )
    parser.add_argument(
        "--reddit_secret",
        default=os.getenv("REDDIT_SECRET"),
        help="Reddit API client secret.",
    )
    parser.add_argument(
        "--out_jsonl",
        default="heartland_mentions.jsonl",
        help="Path to output JSONL file.",
    )
    return parser.parse_args()


QUERY = '"Heartland Payroll" ("my company" OR "at work" OR "employer") -is:retweet'
SUBREDDITS = ["payroll", "accounting", "sysadmin", "humanresources"]
RATE_LIMIT_DELAY_SPICEWORKS = 0.5
TWITTER_FIELDS = {
    "tweet.fields": "id,text,created_at,author_id",
    "expansions": "author_id",
    "user.fields": "username",
}


def extract_possible_company(text: str) -> str:
    pattern = r"(?:at|for)\s+([A-Z][\w& ]{2,40})"
    m = re.search(pattern, text)
    return m.group(1).strip() if m else ""


def wait_on_rate_limit(resp: requests.Response):
    if resp.status_code == 429:
        reset = resp.headers.get("x-rate-limit-reset")
        if reset:
            now = time.time()
            wait = max(0, int(reset) - now)
            logging.warning("Rate limited. Sleeping %s seconds", wait)
            time.sleep(wait)


def fetch_twitter(bearer: str, since: datetime) -> List[Dict]:
    headers = {"Authorization": f"Bearer {bearer}"}
    params = TWITTER_FIELDS.copy()
    params["query"] = QUERY
    params["max_results"] = 100
    params["start_time"] = since.isoformat("T") + "Z"

    url = "https://api.twitter.com/2/tweets/search/recent"
    records = []
    next_token = None

    while True:
        if next_token:
            params["next_token"] = next_token
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code == 429:
            wait_on_rate_limit(resp)
            continue
        resp.raise_for_status()
        data = resp.json()
        users = {u["id"]: u["username"] for u in data.get("includes", {}).get("users", [])}
        for t in data.get("data", []):
            username = users.get(t.get("author_id"), "")
            text = t.get("text", "")
            record = {
                "source": "twitter",
                "tweet_id": t.get("id"),
                "username": username,
                "created_at": t.get("created_at"),
                "text": text,
            }
            company = extract_possible_company(text)
            if company:
                record["guessed_company"] = company
            records.append(record)
        next_token = data.get("meta", {}).get("next_token")
        if not next_token:
            break
        time.sleep(1)  # respect 1 req/s
    return records


def fetch_reddit(client_id: str, client_secret: str, since: datetime) -> List[Dict]:
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent="heartland-listener/0.1",
    )
    records = []
    since_epoch = int(since.timestamp())
    for sub_name in SUBREDDITS:
        subreddit = reddit.subreddit(sub_name)
        # submissions
        for submission in subreddit.search("\"Heartland Payroll\"", sort="new"):
            if submission.created_utc < since_epoch:
                break
            text = submission.title + "\n" + submission.selftext
            record = {
                "source": "reddit",
                "subreddit": sub_name,
                "type": "submission",
                "id": submission.id,
                "created_utc": submission.created_utc,
                "text": text,
            }
            company = extract_possible_company(text)
            if company:
                record["guessed_company"] = company
            records.append(record)
        # comments
        for comment in subreddit.comments(limit=None):
            if comment.created_utc < since_epoch:
                break
            if "Heartland Payroll" in comment.body:
                record = {
                    "source": "reddit",
                    "subreddit": sub_name,
                    "type": "comment",
                    "id": comment.id,
                    "created_utc": comment.created_utc,
                    "text": comment.body,
                }
                company = extract_possible_company(comment.body)
                if company:
                    record["guessed_company"] = company
                records.append(record)
    return records


def fetch_spiceworks() -> List[Dict]:
    url = "https://community.spiceworks.com/search?q=heartland%20payroll"
    resp = requests.get(url)
    time.sleep(RATE_LIMIT_DELAY_SPICEWORKS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    records = []
    for item in soup.select(".search-item"):
        link = item.find("a")
        if not link:
            continue
        title = link.get_text(strip=True)
        href = link.get("href")
        text = title
        record = {
            "source": "spiceworks",
            "title": title,
            "link": href,
        }
        company = extract_possible_company(text)
        if company:
            record["guessed_company"] = company
        records.append(record)
    return records


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

    since = datetime.utcnow() - timedelta(hours=args.since_hours)

    all_records: List[Dict] = []
    if args.twitter_bearer:
        logging.info("Fetching Twitter posts")
        twitter_records = fetch_twitter(args.twitter_bearer, since)
        all_records.extend(twitter_records)
    else:
        twitter_records = []
        logging.warning("No Twitter bearer token provided; skipping Twitter fetch")

    if args.reddit_client and args.reddit_secret:
        logging.info("Fetching Reddit posts")
        reddit_records = fetch_reddit(args.reddit_client, args.reddit_secret, since)
        all_records.extend(reddit_records)
    else:
        reddit_records = []
        logging.warning("No Reddit credentials provided; skipping Reddit fetch")

    logging.info("Fetching Spiceworks posts")
    spice_records = fetch_spiceworks()
    all_records.extend(spice_records)

    guessed_count = sum(1 for r in all_records if "guessed_company" in r)

    with open(args.out_jsonl, "w", encoding="utf-8") as f:
        for rec in all_records:
            json.dump(rec, f)
            f.write("\n")

    logging.info(
        "%d Twitter, %d Reddit, %d Spiceworks posts → %d with guessed company names",
        len(twitter_records),
        len(reddit_records),
        len(spice_records),
        guessed_count,
    )


if __name__ == "__main__":
    main()
