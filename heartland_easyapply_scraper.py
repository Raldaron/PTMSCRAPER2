#!/usr/bin/env python3
"""
heartland_easyapply_scraper.py
────────────────────────────────────────────────────────────────────────────
Discovers companies that use **Heartland Payroll’s EasyApply ATS** and
exports a lead list as CSV.  Features:

1. **Multiple Google-dorks at once** – scrape every ©-year variant you want.
2. **SerpAPI backend** – 100 organic results per call, no CAPTCHA hassle.
3. **Credit throttle** – stop automatically when you hit your own ceiling.
4. **Async page visits** – pull job pages in parallel to extract the company.
5. **Push leads to Slack/CRM** – optional JSON webhook right after CSV write.

Dependencies  ▸  `pip install -U requests aiohttp beautifulsoup4 tqdm python-dateutil`
Python ≥ 3.9 is recommended.

© 2025 – MIT license.  Use ethically and respect privacy laws.  Enjoy!
"""

#############################################################################
# ═════════════════════════════  C O N F I G  ══════════════════════════════ #
#############################################################################

import os, re, csv, json, time, urllib.parse, asyncio, requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict

import aiohttp
from bs4 import BeautifulSoup
from dateutil import tz
from tqdm import tqdm

# ── Google-dorks (add / remove at will) ────────────────────────────────────
QUERIES: List[str] = [
    'site:easyapply.co "© 2025 Heartland Payroll"',
    'site:easyapply.co "© 2024 Heartland Payroll"',
    'site:easyapply.co "© 2023 Heartland Payroll"',
]

# ── SerpAPI & credit management ───────────────────────────────────────────
SERPAPI_KEY: str = (
    os.getenv("SERPAPI_KEY")  # preferred – keep secrets out of code
    or "f2fac39a728d35704c76771e3d430662c313a80aec987837e551cbc23f99eb18"  # ← user-supplied
)
MAX_PAGES_PER_QUERY: int = 10      # each page = 100 organic results = 1 credit
MAX_TOTAL_CREDITS: int   = 25      # hard ceiling for the whole run
SLEEP_BETWEEN_PAGES: int = 2       # seconds – evens out request rate

# ── Async fetch tuning ────────────────────────────────────────────────────
CONCURRENT_FETCHES: int = 25       # ↑ to go faster, ↓ if memory-constrained
FETCH_TIMEOUT: int = 25            # seconds per EasyApply page

# ── Output ────────────────────────────────────────────────────────────────
OUTFILE: Path = Path("heartland_easyapply_leads.csv")

# ── Optional webhook push (Slack / HubSpot / Airtable …) ──────────────────
WEBHOOK_URL: str | None = os.getenv("LEADS_WEBHOOK_URL")
WEBHOOK_BATCH_SIZE: int = 100      # Slack limit ≈ 4 MB – tune per API

#############################################################################
# ══════════════════════  S E R P A P I   S E A R C H  ═════════════════════ #
#############################################################################


def serpapi_page(query: str, page: int) -> List[str]:
    """
    Return EasyApply links from one SerpAPI results page (100 organic rows).
    """
    resp = requests.get(
        "https://serpapi.com/search",
        params={
            "engine": "google",
            "q": query,
            "num": 100,
            "start": (page - 1) * 100,
            "api_key": SERPAPI_KEY,
        },
        timeout=25,
    )
    resp.raise_for_status()
    payload = resp.json()
    links: List[str] = []
    for r in payload.get("organic_results", []):
        link = r.get("link", "")
        if link.startswith("https://easyapply.co/"):
            links.append(link.split("?")[0])  # drop tracking params
    return links


def harvest_all_easyapply_links() -> List[str]:
    """
    Loop through QUERIES & pages until MAX_TOTAL_CREDITS is hit.
    Returns a *sorted*, de-duplicated list of EasyApply URLs.
    """
    all_links: set[str] = set()
    credits_used = 0

    for q in QUERIES:
        for page in range(1, MAX_PAGES_PER_QUERY + 1):
            if credits_used >= MAX_TOTAL_CREDITS:
                print(f"• Credit cap hit ({credits_used}) – stopping search\n")
                return sorted(all_links)

            links = serpapi_page(q, page)
            credits_used += 1

            if not links:
                break  # out of results for this query

            all_links.update(links)
            print(
                f"  ↳ {len(links):3} links from page {page} "
                f"| total {len(all_links)} "
                f"| credits {credits_used}"
            )
            time.sleep(SLEEP_BETWEEN_PAGES)

    return sorted(all_links)


# Alias for legacy call-site
get_serp_results = harvest_all_easyapply_links

#############################################################################
# ═════════════════════  A S Y N C   J O B   S C R A P E  ═════════════════ #
#############################################################################


async def fetch_html(session: aiohttp.ClientSession, url: str):
    try:
        timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status == 200:
                return url, await resp.text()
    except Exception:
        pass
    return url, None


COMPANY_PATTERNS = [
    re.compile(
        r'"hiringOrganization"\s*:\s*{\s*"@type"\s*:\s*"Organization"\s*,\s*"name"\s*:\s*"([^"]+)"'
    ),
    re.compile(r"(?i)(.+?)\s+\|\s+Apply\s+Now"),  # <title>Foo | Apply Now</title>
    re.compile(r"(?i)Apply\s+for\s+.+?\s+at\s+(.+)$"),
]


def guess_company(html: str | None) -> str | None:
    if not html:
        return None
    # simple regex passes (fast)
    snippet = html[:20000]
    for pat in COMPANY_PATTERNS:
        m = pat.search(snippet)
        if m:
            return m.group(1).strip()

    # fallback: <h1> tag
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    if h1:
        return h1.text.strip()
    return None


async def gather_company_info(urls: List[str]):
    connector = aiohttp.TCPConnector(limit=CONCURRENT_FETCHES)
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": "Mozilla/5.0"},
    ) as session:
        tasks = [asyncio.create_task(fetch_html(session, u)) for u in urls]
        for coro in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Fetching job pages",
            ncols=80,
        ):
            yield await coro


#############################################################################
# ════════════════════════  W E B H O O K   P U S H  ══════════════════════ #
#############################################################################


def push_leads(leads: List[Dict]):
    """
    POST leads to WEBHOOK_URL in JSON batches.
    No-op if WEBHOOK_URL is not set.
    """
    if not WEBHOOK_URL:
        print("• WEBHOOK_URL not set – skipping webhook push")
        return

    def batched(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i : i + size]

    headers = {"Content-Type": "application/json"}

    for batch in batched(leads, WEBHOOK_BATCH_SIZE):
        try:
            resp = requests.post(
                WEBHOOK_URL,
                data=json.dumps(batch),
                headers=headers,
                timeout=20,
            )
            resp.raise_for_status()
            print(f"  ✅ pushed {len(batch)} leads → {WEBHOOK_URL}")
        except requests.HTTPError as e:
            text = resp.text[:200].replace("\n", " ")
            print(f"  ❌ push failed ({e}) – response: {text}")


#############################################################################
# ════════════════════════════  M A I N  ═══════════════════════════════════ #
#############################################################################


def main() -> None:
    print("\n🔎  Harvesting EasyApply links …\n")
    urls = get_serp_results()
    print(f"\n→ Found {len(urls)} unique job pages\n")

    if not urls:
        print("No results – exiting.")
        return

    leads: List[Dict] = []
    est = tz.gettz("America/New_York")
    first_seen = datetime.now(tz=est).isoformat(timespec="seconds")

    loop = asyncio.get_event_loop()

    async def run():
        async for url, html in gather_company_info(urls):
            company = guess_company(html)
            if company:
                leads.append(
                    {
                        "company_name": company,
                        "easyapply_url": url,
                        "first_seen_at": first_seen,
                        "source_dork": "multiple",  # all queries merged
                    }
                )

    loop.run_until_complete(run())

    if not leads:
        print("😕  No companies recognised – nothing to write.")
        return

    # De-duplicate by company + URL (rare dupes if multiple dorks captured same job)
    seen = set()
    deduped = []
    for lead in leads:
        key = (lead["company_name"].lower(), lead["easyapply_url"])
        if key not in seen:
            deduped.append(lead)
            seen.add(key)

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTFILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=deduped[0].keys())
        writer.writeheader()
        writer.writerows(deduped)

    print(f"✅  Wrote {len(deduped)} leads → {OUTFILE.resolve()}\n")

    # Optional webhook
    push_leads(deduped)


if __name__ == "__main__":
    main()
