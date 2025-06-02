# AGENTS.md  
Guidance for AI agents (and human contributors) working on the **Indeed Heartland Scraper** project.

---

## 1 – What this repo does
* Scrapes Indeed job-listings that mention **Heartland Payroll**.  
* Fetches pages through **Oxylabs Web Scraper API Realtime** (no headless browser).  
* Parses HTML with **BeautifulSoup**, exports to **CSV** and **SQLite**.

The primary entry-point is `indeed_heartland_jobs.py`.

---

## 2 – Repo map (top-level)
| Path | Purpose |
|------|---------|
| `indeed_heartland_jobs.py` | CLI script – builds URLs, calls Oxylabs, parses jobs, writes CSV/DB |
| `requirements.txt`         | Locked runtime deps (`pandas`, `beautifulsoup4`, `requests`) |
| `heartland_jobs.csv`       | Output sample (ignored by Git) |
| `heartland_jobs.db`        | SQLite store (ignored by Git) |
| `README.md`                | End-user instructions |
| `AGENTS.md`                | **(this file)** – rules & tips for AI agents |

*(Any new helper modules should live in `src/` to keep the root tidy.)*

---

## 3 – Quick setup (what to run)
```bash
python -m venv .venv            # create isolated env
.venv\Scripts\Activate.ps1      # or source .venv/bin/activate
pip install -r requirements.txt

## 4 – Running the scraper
python indeed_heartland_jobs.py \
       --pages 5 \
       --query_text "experience with Heartland Payroll" \
       --country us \
       --req_timeout 120

Environment credentials (already hard-coded for dev, remove before prod!):
API_USER = "rstyshklfrd_7uSI4"
API_PASS = "Wv+dHF8zgtM7XVv"
In production move them to OXY_USER / OXY_PASS env-vars.

## 5 – Standard dev tasks for Codex
✅ Add a CLI flag
Update build_parser() in indeed_heartland_jobs.py.

Reflect in README.md help table.

Include a default that won’t break existing CI calls.

✅ Refactor parsing logic
Keep the public function signature of parse_jobs(html:str) → list[dict].

Write unit tests in tests/test_parse_jobs.py using saved HTML fixtures.

✅ Bump deps
Pin new version in requirements.txt.

Run pip install -r requirements.txt && pytest locally to ensure green.

## 6 – Coding conventions
Area	Rule
Formatter	black (run black .)
Imports	Group stdlib / third-party / local; use isort preset black
Typing	Use PEP-604 unions (`str
Logging	logging.info/warning/error; never use bare print in lib code
Docstring	Google style, sentence-case, imperative mood
Tests	pytest; prefer fixtures over ad-hoc files

## 7 – How to test quickly
pytest -q                    # run all unit tests
python indeed_heartland_jobs.py --pages 1 --req_timeout 40
A healthy run prints something like
✓ Scraped 10 unique ads from 7 companies.

## 8 – Continuous integration notes
CI runs pip install -r requirements.txt, pytest, and checks black --check ..
Any command that exits non-zero will fail the pipeline.

## 9 – Gotchas / FAQ
ReadTimeout from Oxylabs → raise --req_timeout or check corporate proxy bypass.

Indeed may change CSS classes – update the selectors in parse_jobs().

Don’t commit credentials; CI injects OXY_USER/PASS secrets.

End of AGENTS.md