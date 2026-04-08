"""
pipeline/ingest.py

Two-pass analysis pipeline:

  Pass 1  (every new article, RSS summary / title):
    → Claude classifies: industry, deal_type, geography, company_stage
    → rule-based preliminary relevance_score (1–5) written to DB

  Pass 2  (only articles where relevance_score >= 3, full body text):
    → Claude refines: relevance_score, sentiment_color, summary
    → scrapes full body if raw_text < 200 chars

Modes (ANTHROPIC_VERSION env var):
  hbs      → prints Pass 1 / Pass 2 prompts with clear labels, waits for paste
  personal → calls Anthropic API automatically

Flags:
  --backfill  → single-pass API backfill of all articles where industry IS NULL
                always uses ANTHROPIC_API_KEY (ignores ANTHROPIC_VERSION)
                classifies all fields in one Claude call per article
                commits every 20 articles; safe to re-run

Idempotent:
  - Deduplicates articles by URL
  - Pass 1 only runs where industry IS NULL
  - Pass 2 only runs where relevance_score >= 3 AND summary IS NULL
  - Skips NDA firms and registration-gated sources (access_tier != 'public')

Required DB columns (run once if not already present):
  ALTER TABLE articles ADD COLUMN IF NOT EXISTS deal_type TEXT;
  ALTER TABLE articles ADD COLUMN IF NOT EXISTS geography TEXT;
  ALTER TABLE articles ADD COLUMN IF NOT EXISTS company_stage TEXT;
  ALTER TABLE articles ADD COLUMN IF NOT EXISTS relevance_score INTEGER;
  ALTER TABLE articles ADD COLUMN IF NOT EXISTS sentiment_color TEXT;
  ALTER TABLE articles ADD COLUMN IF NOT EXISTS summary TEXT;
"""

import argparse
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
PASS2_SCRAPE_MIN  = 200  # scrape full body if raw_text shorter than this

VALID_INDUSTRIES = frozenset([
    "solar", "storage", "hydrogen", "nuclear", "grid",
    "carbon-capture", "geothermal", "oil-gas", "ev", "other",
])
VALID_DEAL_TYPES = frozenset([
    "funding", "m&a", "project_finance", "ipo_exit",
    "partnership", "policy", "market_intelligence", "none",
])
VALID_GEOGRAPHIES = frozenset([
    "north_america", "europe", "asia_pacific",
    "latam", "middle_east_africa", "global",
])
VALID_COMPANY_STAGES = frozenset([
    "early_stage", "growth", "infrastructure",
    "public_company", "policy_body", "other",
])


# ── Pass 1 prompts ────────────────────────────────────────────────────────────

PASS1_SYSTEM = """\
You are an analyst at a venture capital firm focused on energy transition and climate tech.
You will receive the title and RSS summary of a news article or press release.

Respond with ONLY a valid JSON object — no preamble, no markdown, no explanation.

Schema:
{
  "industry": "<one of: solar, storage, hydrogen, nuclear, grid, carbon-capture, geothermal, oil-gas, ev, other>",
  "deal_type": "<one of: funding, m&a, project_finance, ipo_exit, partnership, policy, market_intelligence, none>",
  "geography": "<one of: north_america, europe, asia_pacific, latam, middle_east_africa, global>",
  "company_stage": "<one of: early_stage, growth, infrastructure, public_company, policy_body, other>"
}

Industry guide:
  solar          = solar power generation, panels, inverters, trackers
  storage        = battery energy storage, BESS, grid-scale storage
  hydrogen       = green/blue hydrogen, fuel cells, electrolysers
  nuclear        = fission, fusion, SMRs
  grid           = transmission, distribution, grid infrastructure
  carbon-capture = CCS, DAC, CCUS, carbon removal
  geothermal     = geothermal power or heat
  oil-gas        = oil, gas, LNG, fossil fuel sector
  ev             = electric vehicles, EV charging, mobility
  other          = any sector not listed above

deal_type guide:
  funding           = venture/private equity investment, seed, Series A/B/C, debt raise
  m&a               = merger, acquisition, takeover, consolidation
  project_finance   = project financing, construction financing, tax equity, PPA
  ipo_exit          = IPO, SPAC, secondary sale, fund exit
  partnership       = JV, offtake agreement, strategic partnership, MOU
  policy            = regulation, legislation, government mandate, subsidy, tariff
  market_intelligence = market analysis, research report, pricing, forecast, trend
  none              = no deal or policy signal (company update, opinion, general news)

company_stage guide:
  early_stage   = pre-revenue, seed, Series A
  growth        = scaling revenue, Series B+, pre-profitability
  infrastructure = asset-heavy, operating projects, utilities
  public_company = listed on exchange
  policy_body   = government agency, regulatory body, NGO
  other         = does not fit above
"""

PASS1_USER_TEMPLATE = """\
Title: {title}

Summary:
{summary}
"""


# ── Pass 2 prompts ────────────────────────────────────────────────────────────

PASS2_SYSTEM = """\
You are an analyst at a venture capital firm focused on energy transition and climate tech.
You will receive the title and full body text of a news article or press release.

Respond with ONLY a valid JSON object — no preamble, no markdown, no explanation.

Schema:
{
  "relevance_score": <integer 1–5>,
  "sentiment_color": "<single evocative phrase capturing the article's signal for a PE professional>",
  "summary": "<2 sentences, max 60 words, focused on investment signal>"
}

relevance_score guide:
  5 = highly actionable (imminent closing, signed term sheet, major M&A confirmed)
  4 = strong signal (announced funding, project finance deal, significant M&A)
  3 = moderate signal (strategic partnership, notable policy shift, market intelligence)
  2 = weak signal (company update, minor industry news, directional but not actionable)
  1 = noise (opinion piece, no transaction or investment signal)

sentiment_color examples (adapt freely — be specific and concrete):
  "strongly positive — $300M infrastructure deal closing in Q3"
  "cautionary — regulatory headwinds slowing permitting timelines"
  "neutral — market sizing report, no transaction signal"
  "mixed — large TAM but execution risk flagged by management"

summary: 2 sentences max 60 words total. Lead with the investment signal, then the context.
"""

PASS2_USER_TEMPLATE = """\
Title: {title}

Body:
{body}
"""


# ── Backfill prompt (single call: all fields at once) ────────────────────────

BACKFILL_SYSTEM = """\
You are an analyst at a venture capital firm focused on energy transition and climate tech.
You will receive the title and full text of a news article or press release.

Respond with ONLY a valid JSON object — no preamble, no markdown, no explanation.

Schema:
{
  "industry": "<one of: solar, storage, hydrogen, nuclear, grid, carbon-capture, geothermal, oil-gas, ev, other>",
  "deal_type": "<one of: funding, m&a, project_finance, ipo_exit, partnership, policy, market_intelligence, none>",
  "geography": "<one of: north_america, europe, asia_pacific, latam, middle_east_africa, global>",
  "company_stage": "<one of: early_stage, growth, infrastructure, public_company, policy_body, other>",
  "relevance_score": <integer 1–5>,
  "sentiment_color": "<single evocative phrase capturing the article's signal for a PE professional>",
  "summary": "<2 sentences, max 60 words, focused on investment signal>"
}

Industry: solar=solar PV/generation, storage=BESS/battery, hydrogen=H2/fuel cells,
  nuclear=fission/fusion/SMR, grid=transmission/distribution, carbon-capture=CCS/DAC,
  geothermal=geothermal power/heat, oil-gas=fossil fuels, ev=EV/charging, other=anything else

deal_type: funding=VC/PE/debt raise, m&a=merger/acquisition, project_finance=project/construction finance/PPA,
  ipo_exit=IPO/SPAC/exit, partnership=JV/offtake/MOU, policy=regulation/mandate/subsidy,
  market_intelligence=report/forecast/analysis, none=no transaction or policy signal

company_stage: early_stage=seed/Series A, growth=Series B+/scaling, infrastructure=asset-heavy/utilities,
  public_company=listed, policy_body=government/NGO/regulator, other=unclear

relevance_score:
  5 = highly actionable (imminent close, signed term sheet, major confirmed M&A)
  4 = strong signal (announced funding, project finance, significant M&A)
  3 = moderate signal (partnership, notable policy, market intelligence)
  2 = weak signal (company update, directional but not actionable)
  1 = noise (opinion, no investment signal)

sentiment_color: one evocative phrase, e.g.:
  "strongly positive — $300M infrastructure deal closing in Q3"
  "cautionary — regulatory headwinds slowing permitting"
  "neutral — market sizing report, no transaction signal"

summary: 2 sentences max 60 words. Lead with the investment signal, then context.
"""

BACKFILL_USER_TEMPLATE = """\
Title: {title}

Body:
{body}
"""


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def fetch_firms(cur):
    """Return non-NDA, public-access firms that have an RSS feed."""
    cur.execute("""
        SELECT id, name, slug, rss_url, website_url
        FROM firms
        WHERE is_nda = FALSE
          AND rss_url IS NOT NULL
          AND access_tier = 'public'
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


def fetch_pass1_articles(cur, batch_size, backfill=False):
    """Articles not yet classified (industry IS NULL).
    backfill=True removes the LIMIT so all historical articles are processed."""
    query = """
        SELECT a.id, a.title, a.raw_text
        FROM articles a
        JOIN firms f ON f.id = a.firm_id
        WHERE a.industry IS NULL
          AND f.is_nda = FALSE
          AND f.access_tier = 'public'
        ORDER BY a.ingested_at DESC
    """
    if backfill:
        cur.execute(query)
    else:
        cur.execute(query + " LIMIT %s", (batch_size,))
    return cur.fetchall()


def fetch_pass2_articles(cur, batch_size, backfill=False):
    """High-relevance articles not yet deep-analyzed (summary IS NULL).
    backfill=True removes the LIMIT so all qualifying articles are processed."""
    query = """
        SELECT a.id, a.title, a.raw_text, a.url
        FROM articles a
        JOIN firms f ON f.id = a.firm_id
        WHERE a.relevance_score >= 3
          AND a.summary IS NULL
          AND f.is_nda = FALSE
          AND f.access_tier = 'public'
        ORDER BY a.relevance_score DESC, a.ingested_at DESC
    """
    if backfill:
        cur.execute(query)
    else:
        cur.execute(query + " LIMIT %s", (batch_size,))
    return cur.fetchall()


def save_pass1(cur, article_id, industry, deal_type, geography, company_stage, relevance_score):
    cur.execute("""
        UPDATE articles
        SET industry        = %s,
            deal_type       = %s,
            geography       = %s,
            company_stage   = %s,
            relevance_score = %s
        WHERE id = %s
    """, (industry, deal_type, geography, company_stage, relevance_score, article_id))


def save_pass2(cur, article_id, relevance_score, sentiment_color, summary):
    cur.execute("""
        UPDATE articles
        SET relevance_score = %s,
            sentiment_color = %s,
            summary         = %s
        WHERE id = %s
    """, (relevance_score, sentiment_color, summary, article_id))


def fetch_backfill_articles(cur):
    """All articles where industry IS NULL, ordered oldest-first for stable batching."""
    cur.execute("""
        SELECT a.id, a.title, a.raw_text, a.url
        FROM articles a
        JOIN firms f ON f.id = a.firm_id
        WHERE a.industry IS NULL
          AND f.is_nda = FALSE
          AND f.access_tier = 'public'
        ORDER BY a.ingested_at ASC
    """)
    return cur.fetchall()


def save_backfill(cur, article_id, industry, deal_type, geography, company_stage,
                  relevance_score, sentiment_color, summary):
    cur.execute("""
        UPDATE articles
        SET industry        = %s,
            deal_type       = %s,
            geography       = %s,
            company_stage   = %s,
            relevance_score = %s,
            sentiment_color = %s,
            summary         = %s
        WHERE id = %s
    """, (industry, deal_type, geography, company_stage,
          relevance_score, sentiment_color, summary, article_id))


# ── Relevance scoring (Pass 1 rule logic) ────────────────────────────────────

HIGH_VALUE_DEALS = frozenset(["funding", "m&a", "project_finance", "ipo_exit"])

def derive_relevance_score(industry, deal_type):
    if deal_type in HIGH_VALUE_DEALS and industry != "other":
        return 4
    if deal_type in ("partnership", "policy"):
        return 3
    if deal_type == "market_intelligence":
        return 3
    if deal_type == "none" and industry != "other":
        return 2
    return 1


# ── Scraping ──────────────────────────────────────────────────────────────────

def scrape_body(url):
    """
    Fetch a page and extract readable body text.
    Returns None on any failure — caller continues gracefully.
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


# ── Pass 1: prompt / parse / call ────────────────────────────────────────────

def build_pass1_prompt(title, raw_text):
    summary = (raw_text or "").strip()[:2000] or "(no summary)"
    return PASS1_USER_TEMPLATE.format(
        title=title or "(no title)",
        summary=summary,
    )


def parse_pass1_response(text):
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    parsed = json.loads(text)

    industry      = str(parsed["industry"]).strip().lower()
    deal_type     = str(parsed["deal_type"]).strip().lower()
    geography     = str(parsed["geography"]).strip().lower()
    company_stage = str(parsed["company_stage"]).strip().lower()

    if industry not in VALID_INDUSTRIES:
        industry = "other"
    if deal_type not in VALID_DEAL_TYPES:
        deal_type = "none"
    if geography not in VALID_GEOGRAPHIES:
        geography = "global"
    if company_stage not in VALID_COMPANY_STAGES:
        company_stage = "other"

    return industry, deal_type, geography, company_stage


def call_pass1_api(title, raw_text):
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=150,
        system=PASS1_SYSTEM,
        messages=[{"role": "user", "content": build_pass1_prompt(title, raw_text)}],
    )
    return parse_pass1_response(resp.content[0].text)


def call_pass1_hbs(title, raw_text, index, total):
    print("\n" + "=" * 70)
    print(f"PASS 1 ANALYSIS — Article {index} of {total}")
    print("  'skip' to skip this article  |  'done' to stop cleanly")
    print("=" * 70)
    print("── SYSTEM PROMPT ──────────────────────────────────────────────────")
    print(PASS1_SYSTEM)
    print("── USER PROMPT ────────────────────────────────────────────────────")
    print(build_pass1_prompt(title, raw_text))
    print("=" * 70)
    print("Paste JSON response (end with a blank line):")

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
    return parse_pass1_response("\n".join(lines))


# ── Pass 2: prompt / parse / call ────────────────────────────────────────────

def get_pass2_body(raw_text, url):
    """Return full body text, scraping if raw_text is too short."""
    if raw_text and len(raw_text.strip()) >= PASS2_SCRAPE_MIN:
        return raw_text.strip()[:MAX_BODY_CHARS]
    log.info("  raw_text too short (%d chars) — scraping %s",
             len(raw_text or ""), url)
    scraped = scrape_body(url)
    return (scraped or raw_text or "").strip()[:MAX_BODY_CHARS]


def build_pass2_prompt(title, body):
    body = (body or "").strip() or "(no body text)"
    return PASS2_USER_TEMPLATE.format(
        title=title or "(no title)",
        body=body,
    )


def parse_pass2_response(text):
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    parsed = json.loads(text)

    relevance_score = int(parsed["relevance_score"])
    if not 1 <= relevance_score <= 5:
        raise ValueError(f"relevance_score out of range: {relevance_score}")
    sentiment_color = str(parsed["sentiment_color"]).strip()
    summary         = str(parsed["summary"]).strip()

    return relevance_score, sentiment_color, summary


def call_pass2_api(title, body):
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=300,
        system=PASS2_SYSTEM,
        messages=[{"role": "user", "content": build_pass2_prompt(title, body)}],
    )
    return parse_pass2_response(resp.content[0].text)


def call_pass2_hbs(title, body, index, total):
    print("\n" + "=" * 70)
    print(f"PASS 2 ANALYSIS — Article {index} of {total}")
    print("  'skip' to skip this article  |  'done' to stop cleanly")
    print("=" * 70)
    print("── SYSTEM PROMPT ──────────────────────────────────────────────────")
    print(PASS2_SYSTEM)
    print("── USER PROMPT ────────────────────────────────────────────────────")
    print(build_pass2_prompt(title, body))
    print("=" * 70)
    print("Paste JSON response (end with a blank line):")

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
    return parse_pass2_response("\n".join(lines))


# ── Backfill: prompt / parse / call ──────────────────────────────────────────

def build_backfill_prompt(title, raw_text):
    body = (raw_text or "").strip()[:MAX_BODY_CHARS] or "(no body text)"
    return BACKFILL_USER_TEMPLATE.format(
        title=title or "(no title)",
        body=body,
    )


def parse_backfill_response(text):
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    parsed = json.loads(text)

    industry      = str(parsed["industry"]).strip().lower()
    deal_type     = str(parsed["deal_type"]).strip().lower()
    geography     = str(parsed["geography"]).strip().lower()
    company_stage = str(parsed["company_stage"]).strip().lower()

    if industry not in VALID_INDUSTRIES:
        industry = "other"
    if deal_type not in VALID_DEAL_TYPES:
        deal_type = "none"
    if geography not in VALID_GEOGRAPHIES:
        geography = "global"
    if company_stage not in VALID_COMPANY_STAGES:
        company_stage = "other"

    relevance_score = int(parsed["relevance_score"])
    if not 1 <= relevance_score <= 5:
        raise ValueError(f"relevance_score out of range: {relevance_score}")
    sentiment_color = str(parsed["sentiment_color"]).strip()
    summary         = str(parsed["summary"]).strip()

    return industry, deal_type, geography, company_stage, relevance_score, sentiment_color, summary


def call_backfill_api(title, raw_text):
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=400,
        system=BACKFILL_SYSTEM,
        messages=[{"role": "user", "content": build_backfill_prompt(title, raw_text)}],
    )
    return parse_backfill_response(resp.content[0].text)


# ── Analysis orchestration ────────────────────────────────────────────────────

def run_pass1(conn, backfill=False):
    """Classify and score all unanalyzed articles."""
    if ANTHROPIC_VERSION == "personal":
        if not ANTHROPIC_API_KEY:
            raise RuntimeError("ANTHROPIC_API_KEY required when ANTHROPIC_VERSION=personal")
        log.info("Pass 1 mode: personal API key (automated)")
    else:
        log.info("Pass 1 mode: HBS interactive (paste responses manually)")

    with conn:
        cur = conn.cursor()
        articles = fetch_pass1_articles(cur, ANALYSIS_BATCH, backfill=backfill)
        total = len(articles)
        log.info("Pass 1: %d articles to classify", total)

        succeeded = failed = 0
        for i, (article_id, title, raw_text) in enumerate(articles, 1):
            log.info("Pass 1 [%d/%d]: %s", i, total, (title or str(article_id))[:80])
            try:
                if ANTHROPIC_VERSION == "personal":
                    industry, deal_type, geography, company_stage = call_pass1_api(title, raw_text)
                else:
                    industry, deal_type, geography, company_stage = call_pass1_hbs(title, raw_text, i, total)

                relevance_score = derive_relevance_score(industry, deal_type)
                save_pass1(cur, article_id, industry, deal_type, geography, company_stage, relevance_score)
                log.info("  → industry=%s deal_type=%s geography=%s stage=%s score=%d",
                         industry, deal_type, geography, company_stage, relevance_score)
                succeeded += 1

            except StopIteration:
                log.info("Pass 1 stopped by user. Succeeded: %d", succeeded)
                return
            except Exception as e:
                log.warning("  ✗ Pass 1 failed (%s) — skipping", e)
                failed += 1

        log.info("Pass 1 done. Succeeded: %d | Failed: %d", succeeded, failed)


def run_pass2(conn, backfill=False):
    """Deep-analyze high-relevance articles (score >= 3)."""
    if ANTHROPIC_VERSION == "personal":
        log.info("Pass 2 mode: personal API key (automated)")
    else:
        log.info("Pass 2 mode: HBS interactive (paste responses manually)")

    with conn:
        cur = conn.cursor()
        articles = fetch_pass2_articles(cur, ANALYSIS_BATCH, backfill=backfill)
        total = len(articles)
        log.info("Pass 2: %d articles to deep-analyze", total)

        succeeded = failed = 0
        for i, (article_id, title, raw_text, url) in enumerate(articles, 1):
            log.info("Pass 2 [%d/%d]: %s", i, total, (title or str(article_id))[:80])
            try:
                body = get_pass2_body(raw_text, url)

                if ANTHROPIC_VERSION == "personal":
                    relevance_score, sentiment_color, summary = call_pass2_api(title, body)
                else:
                    relevance_score, sentiment_color, summary = call_pass2_hbs(title, body, i, total)

                save_pass2(cur, article_id, relevance_score, sentiment_color, summary)
                log.info("  → score=%d | %s", relevance_score, sentiment_color[:60])
                succeeded += 1

            except StopIteration:
                log.info("Pass 2 stopped by user. Succeeded: %d", succeeded)
                return
            except Exception as e:
                log.warning("  ✗ Pass 2 failed (%s) — skipping", e)
                failed += 1

        log.info("Pass 2 done. Succeeded: %d | Failed: %d", succeeded, failed)


def run_backfill(conn):
    """
    Single-pass API backfill: classifies all fields in one Claude call per article.
    Always uses ANTHROPIC_API_KEY regardless of ANTHROPIC_VERSION.
    Commits every ANALYSIS_BATCH articles so progress is preserved on interruption.
    """
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY is required for --backfill mode")

    with conn:
        cur = conn.cursor()
        articles = fetch_backfill_articles(cur)

    total = len(articles)
    log.info("Backfill: %d articles to process", total)
    if total == 0:
        log.info("Nothing to backfill.")
        return

    succeeded = failed = 0
    for batch_start in range(0, total, ANALYSIS_BATCH):
        batch = articles[batch_start:batch_start + ANALYSIS_BATCH]
        with conn:
            cur = conn.cursor()
            for article_id, title, raw_text, url in batch:
                try:
                    result = call_backfill_api(title, raw_text)
                    save_backfill(cur, article_id, *result)
                    succeeded += 1
                except Exception as e:
                    log.warning("  ✗ id=%s failed (%s) — skipping", article_id, e)
                    failed += 1
        processed = min(batch_start + ANALYSIS_BATCH, total)
        log.info("Processed %d/%d (succeeded=%d failed=%d)", processed, total, succeeded, failed)

    log.info("Backfill done. Succeeded: %d | Failed: %d", succeeded, failed)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(backfill=False):
    conn = get_conn()
    try:
        if backfill:
            log.info("*** BACKFILL MODE — single-pass API classification of all unanalyzed articles ***")
            run_backfill(conn)
            return

        # Normal mode: ingest → Pass 1 → Pass 2
        with conn:
            cur = conn.cursor()
            firms = fetch_firms(cur)
            log.info("Found %d public firms with RSS feeds", len(firms))

            total_inserted = total_skipped = 0
            for firm_id, name, slug, rss_url, _ in firms:
                ins, skip = ingest_feed(cur, firm_id, name, rss_url)
                total_inserted += ins
                total_skipped += skip

            log.info("Ingest done. Inserted: %d | Duplicates skipped: %d",
                     total_inserted, total_skipped)

        run_pass1(conn)
        run_pass2(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest and analyze energy news articles.")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Run Pass 1 + Pass 2 on all unanalyzed articles (no batch limit, skips RSS ingest).",
    )
    args = parser.parse_args()
    run(backfill=args.backfill)
