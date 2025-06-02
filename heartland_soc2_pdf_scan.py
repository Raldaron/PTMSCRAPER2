import argparse
import csv
import hashlib
import json
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from pdfminer.high_level import extract_pages
from pdfminer.pdfparser import PDFSyntaxError


def search_links(query: str, limit: int) -> List[str]:
    """Search Google via SerpAPI or fallback to FREE_GOOGLE_CSE env var."""
    serp_key = os.getenv("SERPAPI_KEY")
    if serp_key:
        params = {
            "engine": "google",
            "q": query,
            "num": limit,
            "api_key": serp_key,
        }
        try:
            resp = requests.get("https://serpapi.com/search.json", params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            links = [r.get("link") for r in data.get("organic_results", []) if r.get("link")]
            return links[:limit]
        except requests.RequestException as exc:
            logging.error("SerpAPI request failed: %s", exc)
            return []
    free_cse = os.getenv("FREE_GOOGLE_CSE")
    if free_cse:
        try:
            data = json.loads(free_cse)
            links = [i.get("link") for i in data.get("items", []) if i.get("link")]
            return links[:limit]
        except json.JSONDecodeError as exc:
            logging.error("Failed to parse FREE_GOOGLE_CSE JSON: %s", exc)
            return []
    logging.error("SERPAPI_KEY not set. Please export SERPAPI_KEY environment variable.")
    return []


def download_pdf(url: str, output_dir: Path) -> Optional[Path]:
    """Download a PDF and return its path or None on failure."""
    try:
        r = requests.get(url, stream=True, timeout=20)
        if r.status_code >= 400:
            logging.error("Failed to download %s: HTTP %s", url, r.status_code)
            return None
        size_header = int(r.headers.get("Content-Length", 0))
        if size_header and size_header > 10 * 1024 * 1024:
            logging.warning("Skipping %s: file size exceeds 10MB", url)
            return None
        tmp_path = output_dir / "tmp_download"
        total = 0
        hasher = hashlib.sha256()
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                total += len(chunk)
                if total > 10 * 1024 * 1024:
                    logging.warning("Skipping %s: downloaded size exceeds 10MB", url)
                    f.close()
                    tmp_path.unlink(missing_ok=True)
                    return None
                hasher.update(chunk)
                f.write(chunk)
        final_path = output_dir / f"{hasher.hexdigest()}.pdf"
        tmp_path.rename(final_path)
        return final_path
    except requests.RequestException as exc:
        logging.error("Error downloading %s: %s", url, exc)
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return None


def scan_pdf_for_term(pdf_path: Path, term: str) -> List[Tuple[int, str]]:
    """Return list of (page_num, snippet) where term appears."""
    hits = []
    pattern = re.compile(term, re.IGNORECASE)
    try:
        for page_num, page in enumerate(extract_pages(str(pdf_path)), start=1):
            text = "".join(
                element.get_text() for element in page if hasattr(element, "get_text")
            )
            for match in pattern.finditer(text):
                start = max(match.start() - 60, 0)
                end = match.end() + 60
                snippet = text[start:end].replace("\n", " ")
                hits.append((page_num, snippet))
        return hits
    except PDFSyntaxError as exc:
        logging.error("PDFSyntaxError in %s: %s", pdf_path, exc)
        return []
    except Exception as exc:
        logging.error("Failed to parse %s: %s", pdf_path, exc)
        return []


def append_results(csv_path: Path, filename: str, url: str, hits: List[Tuple[int, str]]) -> None:
    """Append search hits to CSV."""
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for page_num, snippet in hits:
            writer.writerow([filename, url, page_num, snippet])


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan PDFs for 'Heartland Payroll'.")
    parser.add_argument("--query", default='"Heartland Payroll" pdf', help="Google query string")
    parser.add_argument("--limit", type=int, default=50, help="Max Google results")
    parser.add_argument("--output_dir", default="./pdfs", help="Directory for downloaded PDFs")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = Path("heartland_pdf_hits.csv")
    if not csv_path.exists():
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["pdf_filename", "url", "page_num", "snippet"])

    urls = search_links(args.query, args.limit)
    pdf_urls = [u for u in urls if u and u.lower().endswith(".pdf")]

    scanned = 0
    matched = 0
    for url in pdf_urls:
        time.sleep(random.uniform(1, 2))
        pdf_path = download_pdf(url, output_dir)
        if not pdf_path:
            continue
        scanned += 1
        hits = scan_pdf_for_term(pdf_path, "Heartland Payroll")
        if hits:
            matched += 1
            append_results(csv_path, pdf_path.name, url, hits)

    print(f"Scanned {scanned} PDFs -> {matched} contained references; see {csv_path.name}")


if __name__ == "__main__":
    main()
