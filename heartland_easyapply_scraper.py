#!/usr/bin/env python3
"""
heartland_easyapply_scraper.py
────────────────────────────────────────────────────────────────────────────
Discovers companies that use **Heartland Payroll’s EasyApply ATS** and
exports a lead list as CSV.

Now harvests URLs from **both** sources:

1. **Google / SerpAPI dorks** (credits-based, high precision)  
2. **EasyApply XML sitemaps** (zero credits, huge recall)

Features
────────
• Credit throttle & retry logic for SerpAPI  
• Early-exit on empty result pages  
• Async page fetch to extract company names  
• Webhook stub (disabled) retained for later use  

Requires → `pip install -U requests aiohttp beautifulsoup4 tqdm python-dateutil`
Python ≥ 3.9 recommended.
"""

#############################################################################
# ═════════════════════════════  I M P O R T S  ════════════════════════════ #
#############################################################################

import os, re, csv, json, time, asyncio, requests, xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from typing import List, Dict

import aiohttp
from bs4 import BeautifulSoup
from dateutil import tz
from tqdm import tqdm
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, ReadTimeout
from typing import cast          # add with the other imports
import re, itertools


#############################################################################
# ═════════════════════════════  C O N F I G  ══════════════════════════════ #
#############################################################################

# ── SerpAPI & credit guard ────────────────────────────────────────────────
SERPAPI_KEY: str = os.getenv("SERPAPI_KEY", "DEMO_KEY_REPLACE_ME")
MAX_PAGES_PER_QUERY: int = int(os.getenv("MAX_PAGES_PER_QUERY", "5"))
MAX_TOTAL_CREDITS: int   = int(os.getenv("MAX_TOTAL_CREDITS",  "94"))
SLEEP_BETWEEN_PAGES: int = 2        # pause between SerpAPI calls (sec)

# ── EasyApply sitemap scrape ──────────────────────────────────────────────
# Limit how many daily sitemap files to fetch (None = all ≅ last ~60 days)
SITEMAP_DAYS: int | None = int(os.getenv("SITEMAP_DAYS", "10"))

# ── Async fetch tuning ────────────────────────────────────────────────────
CONCURRENT_FETCHES = 25
FETCH_TIMEOUT = 25

# ── Output ────────────────────────────────────────────────────────────────
OUTFILE: Path = Path("heartland_easyapply_leads.csv")

# ── Optional webhook (disabled) ───────────────────────────────────────────
WEBHOOK_URL: str | None = None
WEBHOOK_BATCH_SIZE = 100

#############################################################################
# ═════════════════════  S E R P A P I   S E T U P  ═══════════════════════ #
#############################################################################

RETRIES = Retry(
    total=3,
    backoff_factor=1.5,
    status_forcelist=(502, 503, 504),
    allowed_methods=("GET",),
)
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=RETRIES))

# ── Google-dorks ----------------------------------------------------------#
YEAR_DORKS = [
    f'site:easyapply.co "© {y} Heartland Payroll"' for y in range(2025, 2017, -1)
]
GENERIC_DORKS = [
    'site:easyapply.co/job "Heartland Payroll"',
    'site:easyapply.co "Powered by Heartland"',
    'inurl:easyapply.co/company "Heartland"',
    '"hiringOrganization" "Heartland Payroll"',
]
_q = os.getenv("QUERIES")
QUERIES: List[str] = (
    [s.strip() for s in _q.split("|") if s.strip()] if _q else YEAR_DORKS + GENERIC_DORKS
)

#############################################################################
# ═══════════════════  S I T E M A P   H A R V E S T  ═════════════════════ #
#############################################################################

def _extract_locs(text: str) -> list[str]:
    """Grab every <loc>…</loc> value via regex (tolerates sloppy XML)."""
    return re.findall(r"<loc>(.*?)</loc>", text, re.I | re.S)

def harvest_sitemap_links(days: int | None = None) -> List[str]:
    """
    Robustly gather job / company URLs from EasyApply sitemaps even when
    Cloudflare blocks us.  Fallback strategy:
      ① robots.txt "Sitemap:" lines
      ② /sitemap.xml  and /sitemap_index.xml
      ③ If both blocked → synthetic /sitemap_YYYY-MM-DD.xml list
    """
    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0.0.0 Safari/537.36"}
    ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    urls: set[str] = set()

    def grab(url: str) -> str | None:
        """GET with UA spoof; return text or None."""
        try:
            r = session.get(url, headers=hdrs, timeout=20)
            if r.status_code == 200 and "<html" not in r.text[:200].lower():
                return r.text
            # Cloudflare/html fallback via textise dot iitty
            proxy = f"https://r.jina.ai/http://{url.lstrip('https://').lstrip('http://')}"
            r2 = session.get(proxy, timeout=20)
            return r2.text if r2.status_code == 200 else None
        except Exception:
            return None

    # ① robots.txt
    robots = grab("https://easyapply.co/robots.txt")
    index_urls = []
    if robots:
        index_urls.extend(
            line.split(":", 1)[1].strip()
            for line in robots.splitlines()
            if line.lower().startswith("sitemap:")
        )

    # ② common fall-backs
    index_urls += [
        "https://easyapply.co/sitemap.xml",
        "https://easyapply.co/sitemap_index.xml",
    ]

    # de-dupe while preserving order
    seen = set(); index_urls = [u for u in index_urls if not (u in seen or seen.add(u))]

    daily_maps: list[str] = []
    for idx in index_urls:
        xml = grab(idx)
        if not xml:
            continue
        try:
            root = ET.fromstring(xml)
            daily_maps.extend(
                loc.text for loc in root.iter(f"{ns}loc") if loc.text
            )
        except ET.ParseError:
            daily_maps.extend(re.findall(r"<loc>(.*?)</loc>", xml, re.I | re.S))

    # ③ fabricate daily sitemaps if Cloudflare hid everything
    if not daily_maps:
        print("⚠️  No sitemap index reachable – fabricating daily list")
        from datetime import date, timedelta
        today = date.today()
        rng = range(days or 30)      # default 30 days back
        daily_maps = [
            f"https://easyapply.co/sitemap_{(today - timedelta(x)).isoformat()}.xml"
            for x in rng
        ]

    if days:
        daily_maps = daily_maps[:days]

    # gather URLs from each daily map
    for sm in daily_maps:
        xml = grab(sm)
        if not xml:
            continue
        try:
            root = ET.fromstring(xml)
            locs = [loc.text for loc in root.iter(f"{ns}loc") if loc.text]
        except ET.ParseError:
            locs = re.findall(r"<loc>(.*?)</loc>", xml, re.I | re.S)
        urls.update(locs)

    easyapply = [u for u in urls if "/job/" in u or "/company/" in u]
    print(f"🗺️  Sitemap harvest: {len(easyapply):,} EasyApply URLs (days={days})")
    return easyapply




#############################################################################
# ═══════════════════  S E R P A P I   H A R V E S T  ═════════════════════ #
#############################################################################

def serpapi_page(query: str, page: int) -> List[str]:
    """Fetch ONE SerpAPI page and return EasyApply links (up to 100)."""
    try:
        resp = session.get(
            "https://serpapi.com/search",
            params={
                "engine": "google",
                "q": query,
                "num": 100,
                "start": (page - 1) * 100,
                "api_key": SERPAPI_KEY,
            },
            timeout=35,
        )
        resp.raise_for_status()
    except ReadTimeout:
        print(f"  ⚠️  Timeout on “{query}” page {page}; skipping")
        return []
    except RequestException as e:
        msg = getattr(e.response, "text", "")[:120].replace("\n", " ")
        print(f"  ⚠️  SerpAPI error on “{query}” page {page}: {msg}")
        return []

    payload = resp.json()
    return [
        r["link"].split("?")[0]
        for r in payload.get("organic_results", [])
        if r.get("link", "").startswith("https://easyapply.co/")
    ]


def harvest_serpapi_links() -> List[str]:
    """Run every dork until credit cap or empty page; return de-duplicated URLs."""
    if SERPAPI_KEY == "DEMO_KEY_REPLACE_ME":
        print("⚠️  SERPAPI_KEY not set – skipping SerpAPI mode.\n")
        return []

    links_seen: set[str] = set()
    credits_used = 0

    for q in QUERIES:
        print(f"🔍 Query: {q}")
        for page in range(1, MAX_PAGES_PER_QUERY + 1):
            if credits_used >= MAX_TOTAL_CREDITS:
                print(f"• Credit cap hit ({credits_used}) – stop Google dorks.\n")
                return sorted(links_seen)

            links = serpapi_page(q, page)
            if not links:
                break  # first empty page → stop this query

            links_seen.update(links)
            credits_used += 1
            print(
                f"  ↳ {len(links):3} links from page {page} "
                f"| total {len(links_seen)} "
                f"| credits {credits_used}"
            )
            time.sleep(SLEEP_BETWEEN_PAGES)

    return sorted(links_seen)

#############################################################################
# ═══════════════════  A S Y N C   J O B   S C R A P E  ═══════════════════ #
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
    re.compile(r"(?i)(.+?)\s+\|\s+Apply\s+Now"),
    re.compile(r"(?i)Apply\s+for\s+.+?\s+at\s+(.+)$"),
]

def guess_company(html: str | None) -> str | None:
    if not html:
        return None
    snippet = html[:20000]
    for pat in COMPANY_PATTERNS:
        if (m := pat.search(snippet)):
            return m.group(1).strip()
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    return h1.text.strip() if h1 else None

async def gather_company_info(urls: List[str]):
    connector = aiohttp.TCPConnector(limit=CONCURRENT_FETCHES)
    async with aiohttp.ClientSession(
        connector=connector, headers={"User-Agent": "Mozilla/5.0"}
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
# ════════════════════════  W E B H O O K   S T U B  ══════════════════════ #
#############################################################################

def push_leads(_: List[Dict]):
    """No-op unless WEBHOOK_URL is set."""
    if not WEBHOOK_URL:
        return
    # webhook logic unchanged …

#############################################################################
# ════════════════════════════  M A I N  ═══════════════════════════════════ #
#############################################################################

async def main_async() -> None:
    print("\n🔎  Collecting EasyApply URLs …\n")

    urls_from_sitemaps = harvest_sitemap_links(SITEMAP_DAYS)
    urls_from_serpapi  = harvest_serpapi_links()

    urls = sorted(set(urls_from_sitemaps) | set(urls_from_serpapi))
    print(f"\n→ Combined list: {len(urls):,} unique job / company pages\n")

    if not urls:
        print("No URLs harvested – exiting.")
        return

    leads: List[Dict] = []
    first_seen = datetime.now(tz.gettz("America/New_York")).isoformat(timespec="seconds")

    async for url, html in gather_company_info(urls):
        company = guess_company(html)
        if company and "Heartland" in (html or ""):   # sanity check—Heartland-hosted
            leads.append(
                {
                    "company_name": company,
                    "easyapply_url": url,
                    "first_seen_at": first_seen,
                }
            )

    if not leads:
        print("😕  No Heartland companies recognised – nothing to write.")
        return

    # De-duplicate by (company, url)
    seen, deduped = set(), []
    for L in leads:
        key = (L["company_name"].lower(), L["easyapply_url"])
        if key not in seen:
            deduped.append(L)
            seen.add(key)

    OUTFILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTFILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=deduped[0].keys())
        writer.writeheader()
        writer.writerows(deduped)

    print(f"✅  Wrote {len(deduped):,} leads → {OUTFILE.resolve()}\n")
    push_leads(deduped)

if __name__ == "__main__":
    asyncio.run(main_async())
