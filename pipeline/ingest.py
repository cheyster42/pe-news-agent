"""
pipeline/ingest.py

Two steps:
  1. Ingest articles from RSS feeds for all firms in the DB.
  2. Run Claude analysis on new articles — classifies industry and sentiment.

Modes (ANTHROPIC_VERSION env var):
  hbs      → prints prompt, waits for manual paste (no API cost)
  personal → calls Anthropic API automatically

Idempotent: safe to run multiple times.
  - Deduplicates articles by URL
  - Only analyzes articles where industry IS NULL
  - Skips NDA firms entirely
"""

import os
import json
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

DATABASE_URL      = os.environ["DATABASE_URL"]
ANTHROPIC_VERSION = os.environ.get("ANTHROPIC_VERSION", "hbs")  # 'hbs' | 'personal'
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
MODEL             = "claude-sonnet-4-20250514"
REQUEST_TIMEOUT   = 10
MAX_BODY_CHARS    = 20_000
ANALYSIS_BATCH    = 20

VALID_INDUSTRIES = frozenset([
    "solar", "storage", "hydrogen", "nuclear", "grid",
    "carbon-capture", "ev", "oil-gas", "other",
])

SYSTEM_PROMPT = """\
You are an analyst at a venture capital firm focused on energy transition and climate tech.
You will receive the title and body text of a news article or press release.

Respond with ONLY a valid JSON object — no preamble, no markdown, no explanation.

Schema:
{
  "industry": "<one of: solar, storage, hydrogen, nuclear, grid, carbon-capture, ev, oil-gas, other>",
  "sentiment": "<one of: positive, negative, neutral>"
}

Industry guide:
  solar         = solar power generation, panels, inverters
  storage       = battery energy storage, BESS, grid-scale storage
  hydrogen      = green/blue hydrogen, fuel cells, electrolysers
  nuclear       = nuclear fission/fusion, SMRs
  grid          = transmission, distribution, grid infrastructure
  carbon-capture = CCS, DAC, CCUS, carbon removal
  ev            = electric vehicles, EV charging, mobility
  oil-gas       = oil, gas, LNG, fossil fuel sector
  other         = any sector not listed above

Sentiment guide:
  positive = favorable for energy transition, investment opportunity, or sector growth
  negative = setback, risk, adverse regulation, or negative market signal
  neutral  = informational, mixed signals, or not clearly positive or negative
"""

USER_PROMPT_TEMPLATE = """\
Title: {title}

Body:
{body}
"""


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


def fetch_unanalyzed(cur, batch_size):
    """Return articles missing Claude analysis (industry IS NULL)."""
    cur.execute("""
        SELECT a.id, a.title, a.raw_text
        FROM articles a
        JOIN firms f ON f.id = a.firm_id
        WHERE a.industry IS NULL
          AND a.raw_text IS NOT NULL
          AND f.is_nda = FALSE
        ORDER BY a.ingested_at DESC
        LIMIT %s
    """, (batch_size,))
    return cur.fetchall()


def save_analysis(cur, article_id, industry, sentiment):
    cur.execute("""
        UPDATE articles
        SET industry  = %s,
            sentiment = %s
        WHERE id = %s
    """, (industry, sentiment, article_id))


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

        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

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


# ── Prompt / response helpers ─────────────────────────────────────────────────

def build_user_prompt(title, raw_text):
    body = (raw_text or "")[:4000].strip()
    return USER_PROMPT_TEMPLATE.format(
        title=title or "(no title)",
        body=body or "(no body text)",
    )


def parse_response(text):
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    parsed    = json.loads(text)
    industry  = str(parsed["industry"]).strip().lower()
    sentiment = str(parsed["sentiment"]).strip().lower()
    if industry not in VALID_INDUSTRIES:
        industry = "other"
    if sentiment not in ("positive", "negative", "neutral"):
        raise ValueError(f"unexpected sentiment: {sentiment}")
    return industry, sentiment


# ── Claude: personal API path ─────────────────────────────────────────────────

def call_claude_api(title, raw_text):
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=100,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": build_user_prompt(title, raw_text)}],
    )
    return parse_response(resp.content[0].text)


# ── Claude: HBS interactive path ──────────────────────────────────────────────

def call_claude_hbs(title, raw_text):
    """
    Prints the prompt and waits for you to paste Claude's response.
    Commands:
      skip → skip this article, move to next
      done → stop cleanly, keep all progress so far
      <JSON> + blank line → save and continue
    """
    print("\n" + "=" * 60)
    print("PASTE THIS INTO CLAUDE (claude.ai or Claude Code):")
    print("  'skip' to skip  |  'done' to stop")
    print("=" * 60)
    print(f"System: {SYSTEM_PROMPT}\n")
    print(build_user_prompt(title, raw_text))
    print("=" * 60)
    print("Paste JSON response (or 'skip' / 'done'):")
    lines = []
    while True:
        line = input()
        if line.strip().lower() == "skip":
            raise ValueError("skipped by user")
        if line.strip().lower() == "done":
            raise StopIteration("done by user")
        if line == "" and lines:
            break
        lines.append(line)
    return parse_response("\n".join(lines))


# ── Analysis step ─────────────────────────────────────────────────────────────

def run_analysis(conn):
    """Classify industry and sentiment for any unanalyzed articles."""
    if ANTHROPIC_VERSION == "personal":
        if not ANTHROPIC_API_KEY:
            raise RuntimeError("ANTHROPIC_API_KEY required when ANTHROPIC_VERSION=personal")
        log.info("Analysis mode: personal API key (automated)")
    else:
        log.info("Analysis mode: HBS Claude Code (interactive — paste responses manually)")

    with conn:
        cur = conn.cursor()
        articles = fetch_unanalyzed(cur, ANALYSIS_BATCH)
        log.info("Found %d articles to analyze", len(articles))

        succeeded = failed = 0
        for article_id, title, raw_text in articles:
            log.info("Analyzing: %s", (title or str(article_id))[:80])
            try:
                if ANTHROPIC_VERSION == "personal":
                    industry, sentiment = call_claude_api(title, raw_text)
                else:
                    industry, sentiment = call_claude_hbs(title, raw_text)
                save_analysis(cur, article_id, industry, sentiment)
                log.info("  → industry=%s sentiment=%s", industry, sentiment)
                succeeded += 1
            except StopIteration:
                log.info("Stopped by user. Succeeded so far: %d", succeeded)
                return
            except Exception as e:
                log.warning("  ✗ failed (%s) — skipping", e)
                failed += 1

        log.info("Analysis done. Succeeded: %d | Failed: %d", succeeded, failed)


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    conn = get_conn()
    try:
        # Step 1 — ingest RSS feeds
        with conn:
            cur = conn.cursor()
            firms = fetch_firms(cur)
            log.info("Found %d firms with RSS feeds", len(firms))

            total_inserted = total_skipped = 0
            for firm_id, name, slug, rss_url, _ in firms:
                ins, skip = ingest_feed(cur, firm_id, name, rss_url)
                total_inserted += ins
                total_skipped += skip

            log.info("Ingest done. Inserted: %d | Duplicates skipped: %d",
                     total_inserted, total_skipped)

        # Step 2 — Claude analysis (industry + sentiment)
        run_analysis(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    run()
