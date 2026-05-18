#!/usr/bin/env python3
"""
HomeCareersOnline.com — Job Data Ingestion Pipeline
====================================================
Pulls remote/home-based job listings from multiple free APIs,
normalizes them into a common schema, deduplicates, stores in
SQLite, and exports clean JSON for any frontend to consume.

Data Sources:
  1. RemoteOK       — Free JSON API, no key needed
  2. Adzuna         — Free developer API (key required, free tier)
  3. Remotive       — Free JSON API, no key needed
  4. Arbeitnow      — Free JSON API, no key needed
  5. USAJobs        — Free gov API (key required, instant approval)

Usage:
  python job_pipeline.py ingest        # Pull from all sources
  python job_pipeline.py ingest --source remoteok   # Single source
  python job_pipeline.py export        # Export JSON for frontend
  python job_pipeline.py stats         # Show DB statistics
  python job_pipeline.py purge --days 30  # Remove stale listings
"""

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --- Optional imports (installed via requirements.txt) ---
try:
    import requests
except ImportError:
    print("Run: pip install requests")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "jobs.db"
EXPORT_DIR = BASE_DIR / "export"
CONFIG_PATH = BASE_DIR / "config.json"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("pipeline")

# Rate-limit: seconds between API calls to same source
REQUEST_DELAY = 1.5

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            id              TEXT PRIMARY KEY,   -- SHA-256 of (source + external_id)
            source          TEXT NOT NULL,       -- e.g. 'remoteok', 'adzuna'
            external_id     TEXT NOT NULL,       -- ID from the source API
            title           TEXT NOT NULL,
            company         TEXT,
            description     TEXT,
            category        TEXT,
            tags            TEXT,               -- JSON array of tags/keywords
            job_type        TEXT,               -- full-time, part-time, contract
            location        TEXT,               -- "Remote", "US Remote", etc.
            salary_min      REAL,
            salary_max      REAL,
            salary_currency TEXT,
            url             TEXT NOT NULL,       -- Canonical apply/detail URL
            affiliate_url   TEXT,               -- Monetized click-through URL
            company_logo    TEXT,
            posted_at       TEXT,               -- ISO 8601
            ingested_at     TEXT NOT NULL,       -- When we first pulled it
            last_seen_at    TEXT NOT NULL,       -- Last time API returned it
            expired         INTEGER DEFAULT 0,
            UNIQUE(source, external_id)
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source);
        CREATE INDEX IF NOT EXISTS idx_jobs_posted ON jobs(posted_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_category ON jobs(category);
        CREATE INDEX IF NOT EXISTS idx_jobs_expired ON jobs(expired);

        CREATE TABLE IF NOT EXISTS ingest_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source      TEXT NOT NULL,
            started_at  TEXT NOT NULL,
            finished_at TEXT,
            new_jobs    INTEGER DEFAULT 0,
            updated     INTEGER DEFAULT 0,
            errors      INTEGER DEFAULT 0,
            status      TEXT DEFAULT 'running'
        );
    """)
    conn.commit()


def generate_job_id(source: str, external_id: str) -> str:
    raw = f"{source}::{external_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def upsert_job(conn: sqlite3.Connection, job: dict) -> str:
    """Insert or update a job. Returns 'new', 'updated', or 'unchanged'."""
    now = datetime.now(timezone.utc).isoformat()
    job_id = generate_job_id(job["source"], job["external_id"])

    existing = conn.execute("SELECT id, last_seen_at FROM jobs WHERE id = ?", (job_id,)).fetchone()

    if existing is None:
        conn.execute("""
            INSERT INTO jobs (id, source, external_id, title, company, description,
                              category, tags, job_type, location, salary_min, salary_max,
                              salary_currency, url, affiliate_url, company_logo,
                              posted_at, ingested_at, last_seen_at, expired)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        """, (
            job_id, job["source"], job["external_id"], job["title"],
            job.get("company"), job.get("description"), job.get("category"),
            json.dumps(job.get("tags", [])), job.get("job_type"),
            job.get("location", "Remote"), job.get("salary_min"),
            job.get("salary_max"), job.get("salary_currency"),
            job["url"], job.get("affiliate_url"), job.get("company_logo"),
            job.get("posted_at", now), now, now
        ))
        return "new"
    else:
        conn.execute("""
            UPDATE jobs SET last_seen_at = ?, expired = 0,
                            title = ?, company = ?, description = ?,
                            salary_min = ?, salary_max = ?, salary_currency = ?
            WHERE id = ?
        """, (now, job["title"], job.get("company"), job.get("description"),
              job.get("salary_min"), job.get("salary_max"),
              job.get("salary_currency"), job_id))
        return "updated"


# ---------------------------------------------------------------------------
# Source: RemoteOK (FREE, no key)
# ---------------------------------------------------------------------------
def fetch_remoteok() -> list[dict]:
    """RemoteOK free JSON API — remote jobs only."""
    log.info("Fetching RemoteOK...")
    url = "https://remoteok.com/api"
    headers = {"User-Agent": "HomeCareersOnline/1.0 (job-board-aggregator)"}

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # First item is a legal notice, skip it
    jobs = []
    for item in data[1:]:
        posted = item.get("date", "")
        tags = item.get("tags", [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]

        jobs.append({
            "source": "remoteok",
            "external_id": str(item.get("id", "")),
            "title": item.get("position", "Untitled"),
            "company": item.get("company", ""),
            "description": item.get("description", ""),
            "category": _guess_category(item.get("position", ""), tags),
            "tags": tags,
            "job_type": "full-time",
            "location": item.get("location", "Remote"),
            "salary_min": _parse_salary(item.get("salary_min")),
            "salary_max": _parse_salary(item.get("salary_max")),
            "salary_currency": "USD" if item.get("salary_min") else None,
            "url": item.get("url", f"https://remoteok.com/l/{item.get('id','')}"),
            "company_logo": item.get("company_logo", ""),
            "posted_at": posted,
        })

    log.info(f"  RemoteOK returned {len(jobs)} listings")
    return jobs


# ---------------------------------------------------------------------------
# Source: Remotive (FREE, no key)
# ---------------------------------------------------------------------------
def fetch_remotive() -> list[dict]:
    """Remotive free JSON API."""
    log.info("Fetching Remotive...")
    url = "https://remotive.com/api/remote-jobs"
    headers = {"User-Agent": "HomeCareersOnline/1.0"}

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    jobs = []
    for item in data.get("jobs", []):
        tags = item.get("tags", [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]

        jobs.append({
            "source": "remotive",
            "external_id": str(item.get("id", "")),
            "title": item.get("title", "Untitled"),
            "company": item.get("company_name", ""),
            "description": item.get("description", ""),
            "category": item.get("category", ""),
            "tags": tags,
            "job_type": item.get("job_type", "").lower().replace("_", "-"),
            "location": item.get("candidate_required_location", "Worldwide"),
            "salary_min": None,
            "salary_max": None,
            "salary_currency": None,
            "url": item.get("url", ""),
            "company_logo": item.get("company_logo_url", ""),
            "posted_at": item.get("publication_date", ""),
        })

    log.info(f"  Remotive returned {len(jobs)} listings")
    return jobs


# ---------------------------------------------------------------------------
# Source: Arbeitnow (FREE, no key)
# ---------------------------------------------------------------------------
def fetch_arbeitnow() -> list[dict]:
    """Arbeitnow free JSON API — remote-friendly jobs."""
    log.info("Fetching Arbeitnow...")
    url = "https://www.arbeitnow.com/api/job-board-api"
    headers = {"User-Agent": "HomeCareersOnline/1.0"}

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    jobs = []
    for item in data.get("data", []):
        if not item.get("remote", False):
            continue  # Only remote jobs for our board

        tags = item.get("tags", [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]

        jobs.append({
            "source": "arbeitnow",
            "external_id": str(item.get("slug", item.get("title", ""))),
            "title": item.get("title", "Untitled"),
            "company": item.get("company_name", ""),
            "description": item.get("description", ""),
            "category": _guess_category(item.get("title", ""), tags),
            "tags": tags,
            "job_type": "full-time",
            "location": item.get("location", "Remote"),
            "salary_min": None,
            "salary_max": None,
            "salary_currency": None,
            "url": item.get("url", ""),
            "company_logo": "",
            "posted_at": _epoch_to_iso(item.get("created_at")),
        })

    log.info(f"  Arbeitnow returned {len(jobs)} listings (remote only)")
    return jobs


# ---------------------------------------------------------------------------
# Source: Adzuna (FREE tier — requires API key)
# ---------------------------------------------------------------------------
def fetch_adzuna(config: dict) -> list[dict]:
    """Adzuna API — paginates through multiple pages and search terms per country."""
    app_id = config.get("adzuna_app_id", "")
    app_key = config.get("adzuna_app_key", "")
    if not app_id or not app_key:
        log.warning("  Adzuna: No API keys in config.json — skipping")
        return []

    log.info("Fetching Adzuna...")
    countries = config.get("adzuna_countries", ["us", "gb", "ca", "au"])
    max_pages = config.get("adzuna_max_pages", 5)  # Pages per country per query (50 results each)

    # Multiple search queries to broaden coverage across job types
    search_queries = config.get("adzuna_queries", [
        "remote",
        "work from home",
        "hybrid",
        "customer service",
        "data entry",
        "healthcare remote",
        "marketing",
        "software developer",
        "accounting",
        "project manager",
    ])

    all_jobs = []
    seen_ids = set()  # Deduplicate across queries

    for country in countries:
        country_count = 0

        for query in search_queries:
            for page in range(1, max_pages + 1):
                url = (
                    f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
                    f"?app_id={app_id}&app_key={app_key}"
                    f"&what={query.replace(' ', '+')}"
                    f"&results_per_page=50"
                    f"&sort_by=date"
                )
                try:
                    resp = requests.get(url, timeout=30)
                    resp.raise_for_status()
                    data = resp.json()
                    results = data.get("results", [])

                    if not results:
                        break  # No more pages for this query

                    for item in results:
                        ext_id = str(item.get("id", ""))
                        if ext_id in seen_ids:
                            continue
                        seen_ids.add(ext_id)

                        salary_min = item.get("salary_min")
                        salary_max = item.get("salary_max")

                        all_jobs.append({
                            "source": "adzuna",
                            "external_id": ext_id,
                            "title": item.get("title", "Untitled"),
                            "company": item.get("company", {}).get("display_name", ""),
                            "description": item.get("description", ""),
                            "category": item.get("category", {}).get("label", ""),
                            "tags": [item.get("category", {}).get("tag", "")],
                            "job_type": item.get("contract_time", ""),
                            "location": item.get("location", {}).get("display_name", country.upper()),
                            "salary_min": salary_min,
                            "salary_max": salary_max,
                            "salary_currency": item.get("salary_currency", ""),
                            "url": item.get("redirect_url", ""),
                            "affiliate_url": item.get("redirect_url", ""),
                            "posted_at": item.get("created", ""),
                        })
                        country_count += 1

                    # If fewer than 50 results, no more pages
                    if len(results) < 50:
                        break

                    time.sleep(REQUEST_DELAY)

                except Exception as e:
                    log.error(f"  Adzuna [{country.upper()}] q='{query}' p{page} failed: {e}")
                    break  # Move to next query on error

            time.sleep(REQUEST_DELAY)

        log.info(f"  Adzuna [{country.upper()}]: {country_count} unique listings")

    log.info(f"  Adzuna total: {len(all_jobs)} unique listings across {len(countries)} countries")
    return all_jobs


# ---------------------------------------------------------------------------
# Source: USAJobs (FREE — requires API key from developer.usajobs.gov)
# ---------------------------------------------------------------------------
def fetch_usajobs(config: dict) -> list[dict]:
    """USAJobs API — free, requires Authorization-Key + User-Agent email."""
    api_key = config.get("usajobs_api_key", "")
    email = config.get("usajobs_email", "")
    if not api_key or not email:
        log.warning("  USAJobs: No API key in config.json — skipping")
        return []

    log.info("Fetching USAJobs (telework/remote)...")
    url = (
        "https://data.usajobs.gov/api/search"
        "?RemoteIndicator=True"
        "&ResultsPerPage=100"
        "&SortField=DatePosted"
        "&SortDirection=Desc"
    )
    headers = {
        "Authorization-Key": api_key,
        "User-Agent": email,
        "Host": "data.usajobs.gov",
    }

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    jobs = []
    results = data.get("SearchResult", {}).get("SearchResultItems", [])
    for item in results:
        m = item.get("MatchedObjectDescriptor", {})
        pos = m.get("PositionLocation", [{}])
        loc = pos[0].get("LocationName", "Remote") if pos else "Remote"

        salary_min = None
        salary_max = None
        remuneration = m.get("PositionRemuneration", [{}])
        if remuneration:
            salary_min = _parse_salary(remuneration[0].get("MinimumRange"))
            salary_max = _parse_salary(remuneration[0].get("MaximumRange"))

        jobs.append({
            "source": "usajobs",
            "external_id": m.get("PositionID", ""),
            "title": m.get("PositionTitle", "Untitled"),
            "company": m.get("OrganizationName", "U.S. Government"),
            "description": m.get("UserArea", {}).get("Details", {}).get("MajorDuties", [""])[0] if m.get("UserArea") else "",
            "category": m.get("JobCategory", [{}])[0].get("Name", "") if m.get("JobCategory") else "",
            "tags": ["government", "federal", "usajobs"],
            "job_type": m.get("PositionSchedule", [{}])[0].get("Name", "").lower() if m.get("PositionSchedule") else "",
            "location": loc,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "salary_currency": "USD",
            "url": m.get("PositionURI", ""),
            "posted_at": m.get("PublicationStartDate", ""),
        })

    log.info(f"  USAJobs returned {len(jobs)} remote listings")
    return jobs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
CATEGORY_KEYWORDS = {
    "Technology & IT": ["software", "developer", "devops", "sre", "backend", "frontend", "fullstack",
                        "platform", "cloud", "systems admin", "network engineer", "it support", "it manager",
                        "cybersecurity", "information security", "site reliability", "infrastructure",
                        "blockchain", "solutions architect", "enterprise architect", "cloud architect",
                        "test automation", "qa engineer", "mobile developer"],
    "AI & Machine Learning": ["machine learning", " ai ", "artificial intelligence", "prompt engineer",
                              "ai trainer", "nlp", "natural language", "computer vision", "deep learning",
                              "ai engineer", "ml engineer", "data scientist"],
    "Data & Analytics": ["data analyst", "data engineer", "database admin", "business intelligence",
                         "analytics", "data entry", "research analyst", "market research",
                         "intelligence analyst", "policy analyst", "statistician"],
    "Design & Creative": ["designer", "ux", "ui ", "graphic design", "creative director", "illustrat",
                          "video editor", "motion graphics", "photographer", "web design", "art director",
                          "visual design", "brand design"],
    "Marketing": ["marketing", "seo", "social media", "growth", "brand manager", "public relations",
                  "email marketing", "content marketing", "digital marketing", "communications"],
    "Sales & Business Dev": ["sales", "account executive", "account manager", "business develop",
                             "inside sales", "sales engineer", "sales manager", "revenue",
                             "real estate"],
    "Customer Support": ["customer service", "customer support", "call center", "technical support",
                         "help desk", "client success", "service desk", "customer experience"],
    "Product & Project Management": ["product manager", "product owner", "program manager",
                                     "project manager", "scrum master", "agile coach", "business analyst"],
    "Finance & Accounting": ["accounting", "bookkeeper", "accounts payable", "accounts receivable",
                             "financial analyst", "tax preparer", "payroll", "auditor", "controller",
                             "cfo", "finance", "budget", "treasury"],
    "Banking & Insurance": ["banking", "loan officer", "mortgage", "financial advisor", "investment",
                            "wealth management", "insurance", "claims adjuster", "underwriter",
                            "actuary", "insurance agent"],
    "HR & Recruiting": ["recruiter", "human resources", "talent acquisition", "hr generalist",
                        "compensation", "benefits", "people operations", "training specialist",
                        "people ops"],
    "Writing & Content": ["writer", "editor", "copywriter", "technical writer", "journalist",
                          "grant writer", "ux writer", "content creator", "blogger", "proofreader"],
    "Healthcare": ["healthcare", "medical billing", "medical coding", "nurse", "telehealth",
                   "health informatics", "clinical research", "pharmacy", "mental health",
                   "therapist", "counselor", "dietitian", "radiolog", "laboratory",
                   "occupational therap", "physical therap", "speech patholog", "veterinar",
                   "physician", "dental", "optometr", "medical assistant"],
    "Education & Training": ["teacher", "tutor", "instructional design", "curriculum",
                             "esl", "academic advisor", "training coordinator", "professor",
                             "education", "learning", "teaching"],
    "Legal": ["legal", "paralegal", "compliance", "contract specialist", "legal assistant",
              "attorney", "lawyer", "litigation", "regulatory"],
    "Administrative & Operations": ["administrative", "executive assistant", "virtual assistant",
                                     "office manager", "receptionist", "operations coordinator",
                                     "operations manager", "coordinator", "clerk"],
    "Supply Chain & Logistics": ["supply chain", "logistics", "procurement", "warehouse",
                                 "inventory", "shipping", "distribution", "freight",
                                 "transportation", "dispatcher", "fleet manager", "cdl driver"],
    "Engineering (Non-Software)": ["mechanical engineer", "electrical engineer", "civil engineer",
                                   "chemical engineer", "industrial engineer", "structural engineer",
                                   "environmental engineer", "biomedical engineer"],
    "Construction & Trades": ["construction", "estimator", "safety manager", "building inspector",
                              "foreman", "superintendent", "plumber", "electrician", "hvac"],
    "Retail & E-Commerce": ["retail", "e-commerce", "merchandiser", "buyer", "store manager",
                            "loss prevention", "visual merchandis", "ecommerce"],
    "Manufacturing & Quality": ["manufacturing", "quality assurance", "production manager",
                                "process engineer", "quality control", "lean", "six sigma"],
    "Nonprofit & Social Services": ["nonprofit", "fundrais", "program coordinator", "community outreach",
                                    "social worker", "case manager", "advocacy", "grant"],
    "Hospitality & Travel": ["hospitality", "hotel", "event planner", "travel agent",
                             "restaurant manager", "tourism", "catering", "concierge"],
    "Translation & Languages": ["translator", "interpreter", "localization", "bilingual",
                                "multilingual", "translation"],
    "Government & Public Sector": ["government", "federal", "municipal", "public sector",
                                   "civil service", "policy", "usajobs"],
}

def _guess_category(title: str, tags: list) -> str:
    combined = (title + " " + " ".join(tags)).lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw in combined:
                return category
    return "Other"


def _parse_salary(val) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def _epoch_to_iso(ts) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return ""


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------
def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}


SOURCE_MAP = {
    "remoteok": lambda cfg: fetch_remoteok(),
    "remotive": lambda cfg: fetch_remotive(),
    "arbeitnow": lambda cfg: fetch_arbeitnow(),
    "adzuna": fetch_adzuna,
    "usajobs": fetch_usajobs,
}


def run_ingest(sources: list[str] | None = None):
    config = load_config()
    conn = get_db()

    if sources is None:
        sources = list(SOURCE_MAP.keys())

    total_new = 0
    total_updated = 0

    for source_name in sources:
        if source_name not in SOURCE_MAP:
            log.warning(f"Unknown source: {source_name}")
            continue

        log_id = conn.execute(
            "INSERT INTO ingest_log (source, started_at) VALUES (?, ?)",
            (source_name, datetime.now(timezone.utc).isoformat())
        ).lastrowid
        conn.commit()

        new_count = 0
        update_count = 0
        error_count = 0

        try:
            fetcher = SOURCE_MAP[source_name]
            jobs = fetcher(config)

            for job in jobs:
                try:
                    result = upsert_job(conn, job)
                    if result == "new":
                        new_count += 1
                    elif result == "updated":
                        update_count += 1
                except Exception as e:
                    log.error(f"  Error upserting job: {e}")
                    error_count += 1

            conn.commit()
            status = "success"

        except Exception as e:
            log.error(f"Source {source_name} failed: {e}")
            error_count += 1
            status = "error"

        conn.execute("""
            UPDATE ingest_log
            SET finished_at = ?, new_jobs = ?, updated = ?, errors = ?, status = ?
            WHERE id = ?
        """, (datetime.now(timezone.utc).isoformat(), new_count, update_count, error_count, status, log_id))
        conn.commit()

        total_new += new_count
        total_updated += update_count
        log.info(f"  {source_name}: +{new_count} new, ~{update_count} updated, {error_count} errors")

        time.sleep(REQUEST_DELAY)

    log.info(f"\nIngestion complete: {total_new} new jobs, {total_updated} updated")
    conn.close()


def run_export():
    """Export active jobs as JSON files for frontend consumption."""
    conn = get_db()
    EXPORT_DIR.mkdir(exist_ok=True)

    # --- Full export ---
    rows = conn.execute("""
        SELECT * FROM jobs
        WHERE expired = 0
        ORDER BY posted_at DESC
    """).fetchall()

    jobs_list = []
    for row in rows:
        job = dict(row)
        job["tags"] = json.loads(job["tags"]) if job["tags"] else []
        # Strip full HTML description for the listing feed (keep it short)
        if job.get("description"):
            import re
            clean = re.sub(r'<[^>]+>', '', job["description"])
            job["description_preview"] = clean[:300] + "..." if len(clean) > 300 else clean
        jobs_list.append(job)

    # All jobs
    with open(EXPORT_DIR / "jobs_all.json", "w") as f:
        json.dump({"generated_at": datetime.now(timezone.utc).isoformat(),
                    "total": len(jobs_list), "jobs": jobs_list}, f, indent=2)

    # By category
    categories = {}
    for job in jobs_list:
        cat = job.get("category") or "Other"
        categories.setdefault(cat, []).append(job)

    for cat, cat_jobs in categories.items():
        safe_name = cat.lower().replace(" ", "-").replace("/", "-")
        with open(EXPORT_DIR / f"jobs_{safe_name}.json", "w") as f:
            json.dump({"category": cat, "total": len(cat_jobs), "jobs": cat_jobs}, f, indent=2)

    # Category index
    cat_index = [{"name": cat, "count": len(cat_jobs),
                   "file": f"jobs_{cat.lower().replace(' ', '-').replace('/', '-')}.json"}
                  for cat, cat_jobs in sorted(categories.items(), key=lambda x: -len(x[1]))]
    with open(EXPORT_DIR / "categories.json", "w") as f:
        json.dump({"categories": cat_index}, f, indent=2)

    # Source stats
    source_stats = conn.execute("""
        SELECT source, COUNT(*) as count,
               MAX(last_seen_at) as last_seen
        FROM jobs WHERE expired = 0
        GROUP BY source
    """).fetchall()
    with open(EXPORT_DIR / "sources.json", "w") as f:
        json.dump({"sources": [dict(r) for r in source_stats]}, f, indent=2)

    log.info(f"Exported {len(jobs_list)} jobs to {EXPORT_DIR}/")
    log.info(f"  Categories: {len(categories)}")
    log.info(f"  Files: jobs_all.json + {len(categories)} category files + categories.json + sources.json")
    conn.close()


def run_stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM jobs WHERE expired = 0").fetchone()[0]
    expired = conn.execute("SELECT COUNT(*) FROM jobs WHERE expired = 1").fetchone()[0]

    print(f"\n{'='*50}")
    print(f"  HomeCareersOnline — Job Database Stats")
    print(f"{'='*50}")
    print(f"  Active listings:  {total:,}")
    print(f"  Expired listings: {expired:,}")

    print(f"\n  By Source:")
    for row in conn.execute("SELECT source, COUNT(*) as c FROM jobs WHERE expired=0 GROUP BY source ORDER BY c DESC"):
        print(f"    {row['source']:15s} {row['c']:>6,}")

    print(f"\n  By Category:")
    for row in conn.execute("SELECT category, COUNT(*) as c FROM jobs WHERE expired=0 GROUP BY category ORDER BY c DESC LIMIT 15"):
        print(f"    {(row['category'] or 'Uncategorized'):20s} {row['c']:>6,}")

    print(f"\n  Recent Ingestions:")
    for row in conn.execute("SELECT * FROM ingest_log ORDER BY id DESC LIMIT 10"):
        print(f"    {row['source']:15s} | {row['status']:7s} | +{row['new_jobs']} new | {row['started_at'][:19]}")

    print()
    conn.close()


def run_purge(days: int = 30):
    """Mark jobs not seen in `days` as expired."""
    conn = get_db()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    cur = conn.execute("UPDATE jobs SET expired = 1 WHERE last_seen_at < ? AND expired = 0", (cutoff,))
    count = cur.rowcount
    conn.commit()
    log.info(f"Marked {count} jobs as expired (not seen in {days} days)")
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="HomeCareersOnline Job Pipeline")
    sub = parser.add_subparsers(dest="command")

    p_ingest = sub.add_parser("ingest", help="Pull jobs from API sources")
    p_ingest.add_argument("--source", "-s", help="Single source to fetch")

    p_export = sub.add_parser("export", help="Export JSON for frontend")

    p_stats = sub.add_parser("stats", help="Show database statistics")

    p_purge = sub.add_parser("purge", help="Expire stale listings")
    p_purge.add_argument("--days", "-d", type=int, default=30, help="Days threshold")

    args = parser.parse_args()

    if args.command == "ingest":
        sources = [args.source] if args.source else None
        run_ingest(sources)
    elif args.command == "export":
        run_export()
    elif args.command == "stats":
        run_stats()
    elif args.command == "purge":
        run_purge(args.days)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
