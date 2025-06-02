import argparse
import csv
import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text

SEC_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
SEC_USER_AGENT = "Company Name contact@example.com"
SEC_SLEEP = 0.2
SEC_LIMIT_PER_MIN = 10

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def download_pdf(url: str, out_dir: Path) -> Optional[Path]:
    """Download PDF if size < 8MB."""
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        size = int(r.headers.get("Content-Length", 0))
        if size and size > 8 * 1024 * 1024:
            logging.info("Skipping %s, file too large", url)
            return None
        filename = out_dir / Path(url).name
        with open(filename, "wb") as f:
            f.write(r.content)
        return filename
    except Exception as e:
        logging.warning("Failed to download %s: %s", url, e)
        return None


def extract_pdf_snippets(path: Path, keyword: str = "Heartland Payroll") -> List[str]:
    """Extract text snippets around keyword."""
    try:
        text = extract_text(str(path))
    except Exception as e:
        logging.warning("Failed to parse %s: %s", path, e)
        return []
    pattern = re.compile(r".{0,40}%s.{0,40}" % re.escape(keyword), re.IGNORECASE)
    return pattern.findall(text)


@dataclass
class FilingHit:
    source: str
    entity: str
    date: str
    url: str
    snippet: str


class EdgarSearcher:
    FILING_TYPES = {"10-K", "10-Q", "S-1", "DEF 14A"}

    def __init__(self, keyword: str, limit: int = 200):
        self.keyword = keyword
        self.limit = limit
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": SEC_USER_AGENT})
        self.hits: List[FilingHit] = []

    def search(self) -> List[FilingHit]:
        start = 0
        while len(self.hits) < self.limit:
            payload = {
                "keys": self.keyword,
                "category": "fulltext",
                "start": start,
                "count": 100,
            }
            try:
                r = self.session.post(SEC_SEARCH_URL, json=payload, timeout=30)
                r.raise_for_status()
            except Exception as e:
                logging.warning("SEC request failed: %s", e)
                break
            data = r.json()
            items = data.get("hits", {}).get("hits", [])
            if not items:
                break
            for item in items:
                source = item.get("_source", {})
                form_type = source.get("formType")
                if form_type not in self.FILING_TYPES:
                    continue
                cik = source.get("cik")
                company = source.get("display_names", ["Unknown"])[0]
                date = source.get("filedAt", "")[:10]
                url = source.get("linkToFilingDetails")
                snippet = "..."  # API returns snippet? not always; placeholder
                self.hits.append(FilingHit("edgar", company, date, url, snippet))
                if len(self.hits) >= self.limit:
                    break
            start += len(items)
            time.sleep(SEC_SLEEP)
            if start >= data.get("total", 0):
                break
        return self.hits


class StateDolSearcher:
    PDF_RE = re.compile(r"href=[\"'](.*?\.pdf)[\"']", re.IGNORECASE)

    def __init__(self, states: List[str], keyword: str, out_dir: Path):
        self.states = states
        self.keyword = keyword
        self.out_dir = out_dir
        self.session = requests.Session()
        self.hits: List[FilingHit] = []

    def search_state(self, state: str):
        domain = f"https://{state.lower()}.gov"
        try:
            r = self.session.get(domain, timeout=30)
            r.raise_for_status()
        except Exception as e:
            logging.warning("Failed to fetch %s: %s", domain, e)
            return
        pdf_urls = self.PDF_RE.findall(r.text)
        for pdf_url in pdf_urls:
            if not pdf_url.startswith("http"):
                pdf_url = domain.rstrip("/") + "/" + pdf_url.lstrip("/")
            pdf_path = download_pdf(pdf_url, self.out_dir)
            if not pdf_path:
                continue
            snippets = extract_pdf_snippets(pdf_path, self.keyword)
            for snip in snippets:
                self.hits.append(
                    FilingHit("state_dol", state, "", pdf_url, snip.strip())
                )

    def search(self) -> List[FilingHit]:
        for state in self.states:
            self.search_state(state)
        return self.hits


class RfpSearcher:
    PORTALS = {
        "govspend": "https://www.govspend.com/search?q={query}",
        "bidnet": "https://www.bidnet.com/search?keyword={query}",
    }

    def __init__(self, portals: List[str], keyword: str):
        self.portals = portals
        self.keyword = keyword
        self.session = requests.Session()
        self.hits: List[FilingHit] = []

    def search_portal(self, portal: str):
        if portal not in self.PORTALS:
            logging.info("Unknown portal %s", portal)
            return
        url = self.PORTALS[portal].format(query=self.keyword.replace(" ", "+"))
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            logging.warning("Failed to fetch %s: %s", url, e)
            return
        soup = BeautifulSoup(r.text, "html.parser")
        for link in soup.find_all("a", href=True):
            text = link.get_text(" ", strip=True)
            if self.keyword.lower() in text.lower():
                href = link["href"]
                if not href.startswith("http"):
                    href = url.split("/")[0] + "//" + url.split("/")[2] + href
                self.hits.append(
                    FilingHit(portal, portal, "", href, text)
                )

    def search(self) -> List[FilingHit]:
        for portal in self.portals:
            self.search_portal(portal)
        return self.hits


def save_hits_to_csv(hits: List[FilingHit], path: Path):
    rows = [hit.__dict__ for hit in hits]
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)


def parse_args():
    parser = argparse.ArgumentParser(description="Public Filings and RFP Search")
    parser.add_argument("--edgar_limit", type=int, default=200)
    parser.add_argument("--state_list", type=str, default="")
    parser.add_argument("--rfp_portals", type=str, default="")
    parser.add_argument("--out_dir", type=str, default="./filings")
    return parser.parse_args()


def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    keyword = "Heartland Payroll"

    edgar_searcher = EdgarSearcher(keyword, args.edgar_limit)
    edgar_hits = edgar_searcher.search()
    save_hits_to_csv(edgar_hits, out_dir / "edgar_hits.csv")

    states = [s.strip() for s in args.state_list.split(",") if s.strip()]
    if "ALL" in states:
        states = [
            "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
        ]
    state_searcher = StateDolSearcher(states, keyword, out_dir)
    state_hits = state_searcher.search()
    save_hits_to_csv(state_hits, out_dir / "state_dol_hits.csv")

    portals = [p.strip() for p in args.rfp_portals.split(",") if p.strip()]
    rfp_searcher = RfpSearcher(portals, keyword)
    rfp_hits = rfp_searcher.search()
    save_hits_to_csv(rfp_hits, out_dir / "rfp_hits.csv")


if __name__ == "__main__":
    main()
