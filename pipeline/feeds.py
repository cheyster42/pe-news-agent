"""
pipeline/feeds.py

Ingests articles from RSS feeds for all firms in the DB.
- Deduplicates by URL (INSERT ... ON CONFLICT DO NOTHING)
- Scrapes full body text for articles missing raw_text
- Skips NDA firms entirely
- Safe to run multiple times (idempotent)
"""

import os
import logging
import requests
import feedparser
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
REQUEST_TIMEOUT = 10  # seconds
MAX_BODY_CHARS = 20_000  # truncate very long articles before storing


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def fetch_firms(cur):
    """Return all non-NDA firms that have an rss_url."""
    cur.execute("""
        SELECT id, name, slug, rss_url, website_url
        FROM firms
        WHERE is_nda = FALSE AND rss_url IS NOT NULL
    """)
    return cur.fetchall()


def insert_article(cur, firm_id, url, title, raw_text, published_at):
    """Insert article, silently skip if URL already exists."""
    cur.execute("""
        INSERT INTO articles (firm_id, url, title, raw_text, published_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING
    """, (firm_id, url, title, raw_text, published_at))
    return cur.rowcount  # 1 = inserted, 0 = duplicate


# ── Scraping ──────────────────────────────────────────────────────────────────

def scrape_body(url):
    """
    Fetch a page and extract readable body text.
    Returns None on any failure — ingestion continues without body.
    """
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers={
            "User-Agent": "Mozilla/5.0 (compatible; VRPDigestBot/1.0)"
        })
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove boilerplate tags
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        # Prefer <article> or <main> if present, else fall back to <body>
        container = soup.find("article") or soup.find("main") or soup.body
        if not container:
            return None

        text = container.get_text(separator="\n", strip=True)
        return text[:MAX_BODY_CHARS]

    except Exception as e:
        log.warning("Scrape failed for %s: %s", url, e)
        return None


# ── RSS parsing ───────────────────────────────────────────────────────────────

def parse_published(entry):
    """Return a UTC datetime from a feedparser entry, or None."""
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    if t:
        return datetime(*t[:6], tzinfo=timezone.utc)
    return None


def ingest_feed(cur, firm_id, firm_name, rss_url):
    """Parse one RSS feed and insert new articles. Returns (inserted, skipped)."""
    log.info("Fetching feed: %s (%s)", firm_name, rss_url)
    try:
        feed = feedparser.parse(rss_url)
    except Exception as e:
        log.error("Failed to parse feed for %s: %s", firm_name, e)
        return 0, 0

    inserted = skipped = 0
    for entry in feed.entries:
        url = entry.get("link", "").strip()
        if not url:
            continue

        title = entry.get("title", "").strip() or None

        # Use RSS summary as body if available, otherwise scrape
        raw_text = entry.get("summary") or entry.get("content", [{}])[0].get("value")
        if not raw_text:
            raw_text = scrape_body(url)
        elif len(raw_text) > MAX_BODY_CHARS:
            raw_text = raw_text[:MAX_BODY_CHARS]

        published_at = parse_published(entry)

        rows = insert_article(cur, firm_id, url, title, raw_text, published_at)
        if rows:
            inserted += 1
            log.info("  + inserted: %s", title or url)
        else:
            skipped += 1

    return inserted, skipped


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    conn = get_conn()
    try:
        with conn:
            cur = conn.cursor()
            firms = fetch_firms(cur)
            log.info("Found %d firms with RSS feeds", len(firms))

            total_inserted = total_skipped = 0
            for firm_id, name, slug, rss_url, _ in firms:
                ins, skip = ingest_feed(cur, firm_id, name, rss_url)
                total_inserted += ins
                total_skipped += skip

            log.info("Done. Inserted: %d | Duplicates skipped: %d",
                     total_inserted, total_skipped)
    finally:
        conn.close()


if __name__ == "__main__":
    run()