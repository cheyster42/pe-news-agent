"""
pipeline/summarize.py

Summarizes unsummarized articles using the Anthropic Claude API.
- ANTHROPIC_VERSION=hbs    → interactive mode, paste responses manually (no API cost)
- ANTHROPIC_VERSION=personal → automated mode, uses your own Anthropic API key

Safe to re-run (only touches unsummarized rows).
Skips NDA firm articles entirely.
"""

import os
import json
import logging
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL      = os.environ["DATABASE_URL"]
ANTHROPIC_VERSION = os.environ.get("ANTHROPIC_VERSION", "hbs")  # 'hbs' | 'personal'
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
MODEL             = "claude-sonnet-4-20250514"
BATCH_SIZE        = 20
MAX_TOKENS        = 300

SYSTEM_PROMPT = """\
You are an analyst at a venture capital firm focused on energy transition and climate tech.
You will receive the title and body text of a news article or press release.

Respond with ONLY a valid JSON object — no preamble, no markdown, no explanation.

Schema:
{
  "summary": "<2 sentences, max 60 words. Focus on the investment/market signal.>",
  "relevance_score": <integer 1-5>,
  "tags": ["<tag1>", "<tag2>", ...]
}

Relevance score guide:
  5 = Direct signal: funding round, M&A, major partnership, policy that moves capital
  4 = Strong context: technology breakthrough, regulatory development, market sizing
  3 = Useful background: industry trend, opinion piece from credible source
  2 = Weak signal: general news, tangentially related
  1 = Not relevant: off-topic, duplicate angle, low-quality source

Tags: 2-5 short lowercase tags from this set where applicable:
  funding, m&a, policy, technology, grid, storage, solar, wind, hydrogen,
  nuclear, geothermal, ev, carbon-capture, permitting, utilities, oil-gas,
  climate-tech, series-a, series-b, growth-equity, ipo
Add 1-2 custom tags if none of the above fit.
"""

USER_PROMPT_TEMPLATE = """\
Title: {title}

Body:
{body}
"""


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def fetch_unsummarized(cur, batch_size):
    cur.execute("""
        SELECT a.id, a.title, a.raw_text
        FROM articles a
        JOIN firms f ON f.id = a.firm_id
        WHERE a.summarized_at IS NULL
          AND a.raw_text IS NOT NULL
          AND f.is_nda = FALSE
        ORDER BY a.ingested_at DESC
        LIMIT %s
    """, (batch_size,))
    return cur.fetchall()


def save_summary(cur, article_id, summary, relevance_score, tags):
    cur.execute("""
        UPDATE articles
        SET summary         = %s,
            relevance_score = %s,
            tags            = %s,
            summarized_at   = %s
        WHERE id = %s
    """, (summary, relevance_score, tags, datetime.now(timezone.utc), article_id))


def mark_failed(cur, article_id):
    cur.execute("""
        UPDATE articles
        SET summarized_at = %s,
            summary       = '[summarization failed]'
        WHERE id = %s
    """, (datetime.now(timezone.utc), article_id))


# ── Prompt builder ────────────────────────────────────────────────────────────

def build_user_prompt(title, raw_text):
    body = (raw_text or "")[:4000].strip()
    return USER_PROMPT_TEMPLATE.format(
        title=title or "(no title)",
        body=body or "(no body text)",
    )


# ── Response parser ───────────────────────────────────────────────────────────

def parse_response(text):
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    parsed          = json.loads(text)
    summary         = str(parsed["summary"]).strip()
    relevance_score = int(parsed["relevance_score"])
    tags            = [str(t).lower().strip() for t in parsed.get("tags", [])]
    if not (1 <= relevance_score <= 5):
        raise ValueError(f"relevance_score out of range: {relevance_score}")
    return summary, relevance_score, tags


# ── Claude: personal API path ─────────────────────────────────────────────────

def call_claude_api(title, raw_text):
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": build_user_prompt(title, raw_text)}],
    )
    return parse_response(resp.content[0].text)


# ── Claude: HBS interactive path ──────────────────────────────────────────────

def call_claude_hbs(title, raw_text):
    """
    Prints the prompt and waits for you to paste Claude's response.
    Commands:
      skip → mark failed, move to next article
      done → stop cleanly, keep all progress so far
      <JSON> + blank line → save and continue
    """
    print("\n" + "="*60)
    print("PASTE THIS INTO CLAUDE (claude.ai or Claude Code):")
    print("  'skip' to skip  |  'done' to stop")
    print("="*60)
    print(f"System: {SYSTEM_PROMPT}\n")
    print(build_user_prompt(title, raw_text))
    print("="*60)
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


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    if ANTHROPIC_VERSION == "personal":
        if not ANTHROPIC_API_KEY:
            raise RuntimeError("ANTHROPIC_API_KEY required when ANTHROPIC_VERSION=personal")
        log.info("Mode: personal API key (automated)")
    else:
        log.info("Mode: HBS Claude Code (interactive — paste responses manually)")

    conn = get_conn()
    try:
        with conn:
            cur = conn.cursor()
            articles = fetch_unsummarized(cur, BATCH_SIZE)
            log.info("Found %d articles to summarize", len(articles))

            succeeded = failed = 0
            for article_id, title, raw_text in articles:
                log.info("Summarizing: %s", (title or str(article_id))[:80])
                try:
                    if ANTHROPIC_VERSION == "personal":
                        summary, score, tags = call_claude_api(title, raw_text)
                    else:
                        summary, score, tags = call_claude_hbs(title, raw_text)
                    save_summary(cur, article_id, summary, score, tags)
                    log.info("  → score=%d tags=%s", score, tags)
                    succeeded += 1
                except StopIteration:
                    log.info("Stopped by user. Succeeded so far: %d", succeeded)
                    return
                except Exception as e:
                    log.warning("  ✗ failed (%s) — marking and continuing", e)
                    mark_failed(cur, article_id)
                    failed += 1

            log.info("Done. Succeeded: %d | Failed: %d", succeeded, failed)
    finally:
        conn.close()


if __name__ == "__main__":
    run()