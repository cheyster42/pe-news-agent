"""
pipeline/digest.py

Reads scored articles from the DB, filters by keywords, and sends the daily
digest email via Resend.

Scoring now happens in pipeline/ingest.py (Claude analysis).
This file contains no scoring logic.

Idempotent by design:
  - Running digest twice sends the same email with the same content
  - Running with zero qualifying articles sends an empty digest (no crash)

Usage:
  python pipeline/digest.py                  # digest for today
  python pipeline/digest.py 2026-04-06       # digest for a specific date
"""

import os
import sys
import logging
import resend
import psycopg2
from datetime import datetime, timezone, date
from dotenv import load_dotenv

# Allow `from config.keywords import ...` when invoked as pipeline/digest.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.keywords import KEYWORDS

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL      = os.environ["DATABASE_URL"]
RESEND_API_KEY    = os.environ["RESEND_API_KEY"]
DIGEST_RECIPIENTS = [e.strip() for e in os.environ["DIGEST_RECIPIENTS"].split(",")]
FROM_EMAIL        = os.environ.get("FROM_EMAIL", "onboarding@resend.dev")
MIN_RELEVANCE     = int(os.environ.get("MIN_RELEVANCE_SCORE", "3"))
MAX_ARTICLES      = int(os.environ.get("MAX_DIGEST_ARTICLES", "15"))

resend.api_key = RESEND_API_KEY

SCORE_LABELS = {5: "Must-read", 4: "High signal", 3: "Useful context"}

SENTIMENT_COLORS = {
    "positive": "#27ae60",
    "negative": "#e74c3c",
    "neutral":  "#7f8c8d",
}


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def fetch_articles_for_date(cur, target_date):
    """
    Fetch articles ingested on target_date whose title matches at least one
    keyword. Excludes NDA firms. Ordered by relevance_score DESC.
    """
    kw_clauses = " OR ".join(["a.title ILIKE %s"] * len(KEYWORDS))
    kw_params  = [f"%{kw}%" for kw in KEYWORDS]

    cur.execute(f"""
        SELECT
            a.id,
            a.title,
            a.url,
            a.summary,
            a.relevance_score,
            a.tags,
            a.published_at,
            f.name      AS firm_name,
            f.type      AS firm_type,
            a.industry,
            a.sentiment
        FROM articles a
        JOIN firms f ON f.id = a.firm_id
        WHERE f.is_nda = FALSE
          AND a.ingested_at::date = %s
          AND ({kw_clauses})
        ORDER BY a.relevance_score DESC NULLS LAST,
                 a.published_at   DESC NULLS LAST
        LIMIT %s
    """, [target_date] + kw_params + [MAX_ARTICLES])
    return cur.fetchall()


def upsert_digest(cur, digest_date, article_ids):
    """Create or replace digest row and its article links."""
    cur.execute("""
        INSERT INTO digests (digest_date)
        VALUES (%s)
        ON CONFLICT (digest_date) DO UPDATE SET created_at = NOW()
        RETURNING id
    """, (digest_date,))
    digest_id = cur.fetchone()[0]

    cur.execute("DELETE FROM digest_articles WHERE digest_id = %s", (digest_id,))
    for aid in article_ids:
        cur.execute("""
            INSERT INTO digest_articles (digest_id, article_id)
            VALUES (%s, %s) ON CONFLICT DO NOTHING
        """, (digest_id, aid))

    return digest_id


def mark_sent(cur, digest_id):
    cur.execute("""
        UPDATE digests SET email_sent = TRUE, sent_at = %s WHERE id = %s
    """, (datetime.now(timezone.utc), digest_id))


# ── Email rendering ───────────────────────────────────────────────────────────

def render_html(digest_date, articles):
    date_str = digest_date.strftime("%A, %B %d, %Y")
    count    = len(articles)

    sections = {"vc": [], "portfolio": [], "energy_co": [], "media": []}
    section_labels = {
        "vc":        "VC & Investment",
        "portfolio": "Portfolio Companies",
        "energy_co": "Energy Sector",
        "media":     "Industry News",
    }
    for row in articles:
        firm_type = row[8]
        sections.setdefault(firm_type, []).append(row)

    section_html = ""
    for key, label in section_labels.items():
        rows = sections.get(key, [])
        if not rows:
            continue
        cards = ""
        for aid, title, url, summary, score, tags, pub_at, firm_name, firm_type, industry, sentiment in rows:
            score_label = SCORE_LABELS.get(score, f"Score {score}") if score else ""
            tag_pills = " ".join(
                f'<span style="background:#e8f4fd;color:#1a5276;padding:2px 8px;'
                f'border-radius:12px;font-size:11px;margin-right:4px">{t}</span>'
                for t in (tags or [])
            )
            pub_str = pub_at.strftime("%d %b") if pub_at else ""

            industry_badge = ""
            if industry:
                industry_badge = (
                    f'<span style="background:#eaf4fb;color:#1a5276;padding:2px 7px;'
                    f'border-radius:10px;font-size:10px;margin-right:4px">{industry}</span>'
                )

            sentiment_badge = ""
            if sentiment:
                color = SENTIMENT_COLORS.get(sentiment, "#7f8c8d")
                sentiment_badge = (
                    f'<span style="background:{color};color:#fff;padding:2px 7px;'
                    f'border-radius:10px;font-size:10px;margin-right:4px">{sentiment}</span>'
                )

            meta_parts = [firm_name]
            if pub_str:
                meta_parts.append(pub_str)
            if score_label:
                meta_parts.append(score_label)

            cards += f"""
            <div style="border-left:3px solid #2e86c1;padding:10px 14px;
                        margin-bottom:16px;background:#fafafa">
              <div style="font-size:11px;color:#888;margin-bottom:4px">
                {' · '.join(meta_parts)}
              </div>
              <a href="{url}" style="font-size:15px;font-weight:600;color:#1a252f;
                                     text-decoration:none;line-height:1.4">
                {title or url}
              </a>
              <p style="font-size:13px;color:#444;margin:6px 0 8px;line-height:1.6">
                {summary or ''}
              </p>
              <div style="margin-bottom:4px">{industry_badge}{sentiment_badge}</div>
              <div>{tag_pills}</div>
            </div>"""
        section_html += f"""
        <h3 style="font-size:13px;font-weight:600;text-transform:uppercase;
                   letter-spacing:.08em;color:#2e86c1;border-bottom:1px solid #e0e0e0;
                   padding-bottom:6px;margin:28px 0 12px">{label}</h3>
        {cards}"""

    return f"""<!DOCTYPE html>
    <html>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
                 max-width:640px;margin:0 auto;padding:24px;color:#1a252f">
      <div style="background:#1a252f;padding:20px 24px;border-radius:8px;margin-bottom:28px">
        <div style="font-size:11px;color:#aab7b8;text-transform:uppercase;
                    letter-spacing:.1em;margin-bottom:4px">VRP Intelligence Digest</div>
        <div style="font-size:22px;font-weight:700;color:#fff">{date_str}</div>
        <div style="font-size:13px;color:#aab7b8;margin-top:6px">
          {count} article{"s" if count != 1 else ""} · keyword-filtered
        </div>
      </div>
      {section_html or '<p style="color:#888">No articles matched the keyword filter today.</p>'}
      <div style="margin-top:36px;padding-top:16px;border-top:1px solid #e0e0e0;
                  font-size:11px;color:#aab7b8">
        VRP Intelligence Digest · auto-generated
      </div>
    </body>
    </html>"""


def render_text(digest_date, articles):
    date_str = digest_date.strftime("%A, %B %d, %Y")
    lines = [f"VRP Intelligence Digest — {date_str}", "=" * 50, ""]
    for aid, title, url, summary, score, tags, pub_at, firm_name, firm_type, industry, sentiment in articles:
        score_str    = f"[{score}/5]" if score else "[unscored]"
        industry_str = f" | {industry}" if industry else ""
        sentiment_str = f" | {sentiment}" if sentiment else ""
        lines += [
            f"{score_str} {firm_name}{industry_str}{sentiment_str}",
            title or url,
            summary or "",
            url,
            "",
        ]
    return "\n".join(lines)


def send_email(digest_date, articles):
    subject = (
        f"VRP Digest {digest_date.strftime('%d %b')} "
        f"· {len(articles)} article{'s' if len(articles) != 1 else ''}"
    )
    params = {
        "from":    FROM_EMAIL,
        "to":      DIGEST_RECIPIENTS,
        "subject": subject,
        "html":    render_html(digest_date, articles),
        "text":    render_text(digest_date, articles),
    }
    response = resend.Emails.send(params)
    log.info("Email sent. Resend id: %s", response.get("id"))


# ── Main ──────────────────────────────────────────────────────────────────────

def run(digest_date=None):
    if digest_date is None:
        digest_date = datetime.now(timezone.utc).date()
    log.info("Building digest for %s", digest_date)

    conn = get_conn()
    try:
        with conn:
            cur = conn.cursor()

            # Step 1 — fetch keyword-matching articles for this date
            log.info("Fetching keyword-filtered articles...")
            articles = fetch_articles_for_date(cur, digest_date)
            log.info("Found %d qualifying articles", len(articles))

            # Step 2 — upsert digest record
            digest_id = upsert_digest(cur, digest_date, [r[0] for r in articles])

            # Step 3 — send email
            log.info("Sending email to %s...", DIGEST_RECIPIENTS)
            send_email(digest_date, articles)
            mark_sent(cur, digest_id)
            log.info("Digest complete for %s", digest_date)
    finally:
        conn.close()


if __name__ == "__main__":
    # Optional: pass a date arg e.g. python pipeline/digest.py 2026-04-06
    target = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else None
    run(target)
