"""
api/main.py

FastAPI app — serves article and digest data from Neon.

Run locally:
  uvicorn api.main:app --reload

Endpoints:
  GET /health
  GET /articles          — filterable list, sorted by relevance_score DESC
  GET /articles/{id}     — single article by UUID
  GET /digests           — digest run history
"""

import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

app = FastAPI(title="PE News Agent API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── DB ────────────────────────────────────────────────────────────────────────

@contextmanager
def get_cursor():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            yield conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    finally:
        conn.close()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/articles")
def list_articles(
    industry:      Optional[str] = Query(None),
    deal_type:     Optional[str] = Query(None),
    geography:     Optional[str] = Query(None),
    company_stage: Optional[str] = Query(None),
    min_score:     Optional[int] = Query(None, ge=1, le=5),
    limit:         int           = Query(20, ge=1, le=200),
):
    filters = []
    params  = []

    if industry:
        filters.append("a.industry = %s")
        params.append(industry)
    if deal_type:
        filters.append("a.deal_type = %s")
        params.append(deal_type)
    if geography:
        filters.append("a.geography = %s")
        params.append(geography)
    if company_stage:
        filters.append("a.company_stage = %s")
        params.append(company_stage)
    if min_score is not None:
        filters.append("a.relevance_score >= %s")
        params.append(min_score)

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    params.append(limit)

    sql = f"""
        SELECT
            a.id,
            a.url,
            a.title,
            a.published_at,
            a.ingested_at,
            a.industry,
            a.deal_type,
            a.geography,
            a.company_stage,
            a.relevance_score,
            a.sentiment_color,
            a.summary,
            f.name  AS firm_name,
            f.slug  AS firm_slug,
            f.type  AS firm_type
        FROM articles a
        JOIN firms f ON f.id = a.firm_id
        {where}
        ORDER BY a.relevance_score DESC NULLS LAST, a.ingested_at DESC
        LIMIT %s
    """

    with get_cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return [dict(r) for r in rows]


@app.get("/articles/{article_id}")
def get_article(article_id: str):
    sql = """
        SELECT
            a.id,
            a.url,
            a.title,
            a.raw_text,
            a.published_at,
            a.ingested_at,
            a.industry,
            a.deal_type,
            a.geography,
            a.company_stage,
            a.relevance_score,
            a.sentiment_color,
            a.summary,
            f.name       AS firm_name,
            f.slug       AS firm_slug,
            f.type       AS firm_type,
            f.website_url AS firm_website
        FROM articles a
        JOIN firms f ON f.id = a.firm_id
        WHERE a.id = %s
    """
    with get_cursor() as cur:
        cur.execute(sql, (article_id,))
        row = cur.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Article not found")

    return dict(row)


@app.get("/digests")
def list_digests():
    sql = """
        SELECT
            d.id,
            d.created_at,
            d.period_start,
            d.period_end,
            COUNT(da.article_id) AS article_count
        FROM digests d
        LEFT JOIN digest_articles da ON da.digest_id = d.id
        GROUP BY d.id
        ORDER BY d.created_at DESC
        LIMIT 50
    """
    with get_cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    return [dict(r) for r in rows]
