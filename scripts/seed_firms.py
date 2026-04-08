"""
scripts/seed_firms.py

One-time seed of the firms table. Safe to re-run — uses INSERT ... ON CONFLICT DO NOTHING.

BEFORE RUNNING:
  - Add/remove firms in the FIRMS list below to match your actual coverage universe
  - Mark is_nda=True for any ZIP/VRP portfolio companies under NDA
  - Confirm with VRP which portfolio companies should be excluded from shared output
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]

# ── Firm definitions ──────────────────────────────────────────────────────────
# Fields: name, slug, type, rss_url, website_url, is_nda, access_tier
#
# type options:
#   'portfolio'   — VRP/ZIP portfolio company (confirm NDA status before sharing)
#   'vc'          — Climate/cleantech VC to track for deal flow signals
#   'energy_co'   — Utility, oil & gas, or energy operator
#   'media'       — Industry analyst or news source
#
# rss_url: primary feed. None = scrape-only (feeds.py will skip unless you add scraper)
# is_nda: True = excluded from all ingestion and shared output

FIRMS = [

    # ── Climate / Cleantech VCs ───────────────────────────────────────────────
    # Track for portfolio announcements, fund news, investment theses
    {
        "name": "Breakthrough Energy Ventures",
        "slug": "breakthrough-energy",
        "type": "vc",
        "rss_url": "https://www.breakthroughenergy.org/feed",
        "website_url": "https://www.breakthroughenergy.org/news",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Energy Impact Partners",
        "slug": "energy-impact-partners",
        "type": "vc",
        "rss_url": "https://www.energyimpactpartners.com/feed",
        "website_url": "https://www.energyimpactpartners.com/news",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Energize Capital",
        "slug": "energize-capital",
        "type": "vc",
        "rss_url": "https://www.energizecap.com/rss",
        "website_url": "https://www.energizecap.com/news",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Congruent Ventures",
        "slug": "congruent-ventures",
        "type": "vc",
        "rss_url": None,  # publishes via Substack — add URL if they activate one
        "website_url": "https://www.congruentvc.com",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Lowercarbon Capital",
        "slug": "lowercarbon-capital",
        "type": "vc",
        "rss_url": "https://medium.com/feed/lowercarbon-capital",
        "website_url": "https://lowercarboncapital.com",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Prelude Ventures",
        "slug": "prelude-ventures",
        "type": "vc",
        "rss_url": None,
        "website_url": "https://www.preludeventures.com",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Galvanize Climate Solutions",
        "slug": "galvanize-climate",
        "type": "vc",
        "rss_url": None,
        "website_url": "https://galvanizeclimate.com",
        "is_nda": False,
        "access_tier": "public",
    },

    # ── Energy Sector Firms ───────────────────────────────────────────────────
    # Utilities, oil & gas, industrials — track for M&A, policy, capex signals
    {
        "name": "NextEra Energy",
        "slug": "nextera-energy",
        "type": "energy_co",
        "rss_url": "https://www.nexteraenergy.com/news/rss.xml",
        "website_url": "https://www.nexteraenergy.com/news",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Constellation Energy",
        "slug": "constellation-energy",
        "type": "energy_co",
        "rss_url": None,
        "website_url": "https://www.constellationenergy.com/newsroom",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "ExxonMobil",
        "slug": "exxonmobil",
        "type": "energy_co",
        "rss_url": "https://corporate.exxonmobil.com/rss/news",
        "website_url": "https://corporate.exxonmobil.com/news",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "BP",
        "slug": "bp",
        "type": "energy_co",
        "rss_url": "https://www.bp.com/en/global/corporate/news-and-insights/press-releases.rss.xml",
        "website_url": "https://www.bp.com/en/global/corporate/news-and-insights",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Equinor",
        "slug": "equinor",
        "type": "energy_co",
        "rss_url": "https://www.equinor.com/news.rss",
        "website_url": "https://www.equinor.com/news",
        "is_nda": False,
        "access_tier": "public",
    },

    # ── Industry Analysts / News Sources ─────────────────────────────────────
    # High-signal sources for deal flow, policy, and market intelligence
    {
        "name": "Canary Media",
        "slug": "canary-media",
        "type": "media",
        "rss_url": "https://www.canarymedia.com/rss.xml",
        "website_url": "https://www.canarymedia.com",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "CleanTechnica",
        "slug": "cleantechnica",
        "type": "media",
        "rss_url": "https://cleantechnica.com/feed",
        "website_url": "https://cleantechnica.com",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Renewable Energy World",
        "slug": "renewable-energy-world",
        "type": "media",
        "rss_url": "https://www.renewableenergyworld.com/feed",
        "website_url": "https://www.renewableenergyworld.com",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "MIT Energy Initiative",
        "slug": "mit-energy-initiative",
        "type": "media",
        "rss_url": "https://energy.mit.edu/news/feed",
        "website_url": "https://energy.mit.edu/news",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Energy Storage News",
        "slug": "energy-storage-news",
        "type": "media",
        "rss_url": "https://www.energy-storage.news/feed",
        "website_url": "https://www.energy-storage.news",
        "is_nda": False,
        "access_tier": "public",
    },
    
    # ── Additional high-signal sources (from mentor recommendations) ──────────
    {
        "name": "CTVC by Sightline Climate",
        "slug": "ctvc-sightline",
        "type": "media",
        "rss_url": "https://www.ctvc.co/feed",
        "website_url": "https://www.ctvc.co",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Axios Energy & Climate",
        "slug": "axios-energy",
        "type": "media",
        "rss_url": "https://api.axios.com/feed/energy-climate",
        "website_url": "https://www.axios.com/energy-climate",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Stratechery",
        "slug": "stratechery",
        "type": "media",
        "rss_url": "https://stratechery.com/feed",
        "website_url": "https://stratechery.com",
        "is_nda": False,
        "access_tier": "registration",
    },
    {
        "name": "StrictlyVC",
        "slug": "strictlyvc",
        "type": "media",
        "rss_url": "https://strictlyvc.com/feed",
        "website_url": "https://strictlyvc.com",
        "is_nda": False,
        "access_tier": "registration",
    },
    {
        "name": "Sourcery (Molly O'Shea)",
        "slug": "sourcery-molly-oshea",
        "type": "media",
        "rss_url": "https://mollyoshea.substack.com/feed",
        "website_url": "https://mollyoshea.substack.com",
        "is_nda": False,
        "access_tier": "registration",
    },

    # ── Tier 1 Public Sources ─────────────────────────────────────────────────
    {
        "name": "PV Tech",
        "slug": "pv-tech",
        "type": "media",
        "rss_url": "https://www.pv-tech.org/feed",
        "website_url": "https://www.pv-tech.org",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "reNEWS",
        "slug": "renews",
        "type": "media",
        "rss_url": "https://renews.biz/feed",
        "website_url": "https://renews.biz",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Carbon Brief",
        "slug": "carbon-brief",
        "type": "media",
        "rss_url": "https://www.carbonbrief.org/feed",
        "website_url": "https://www.carbonbrief.org",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "RMI",
        "slug": "rmi",
        "type": "media",
        "rss_url": "https://rmi.org/feed",
        "website_url": "https://rmi.org",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Volts",
        "slug": "volts",
        "type": "media",
        "rss_url": "https://www.volts.wtf/feed",
        "website_url": "https://www.volts.wtf",
        "is_nda": False,
        "access_tier": "public",
    },
    {
        "name": "Heatmap News",
        "slug": "heatmap-news",
        "type": "media",
        "rss_url": "https://heatmap.news/feed",
        "website_url": "https://heatmap.news",
        "is_nda": False,
        "access_tier": "public",
    },

    # ── VRP / ZIP Portfolio Companies ────────────────────────────────────────
    # IMPORTANT: Confirm NDA status with VRP before setting is_nda=False.
    # These are placeholder examples — replace with your actual portfolio list.
    # Set is_nda=True for any company VRP says should be excluded from shared output.
    {
        "name": "Portfolio Company A",       # replace with real name
        "slug": "portfolio-co-a",
        "type": "portfolio",
        "rss_url": None,
        "website_url": "https://example.com",
        "is_nda": True,                      # default NDA=True until confirmed otherwise
        "access_tier": "public",
    },
    {
        "name": "Portfolio Company B",       # replace with real name
        "slug": "portfolio-co-b",
        "type": "portfolio",
        "rss_url": None,
        "website_url": "https://example.com",
        "is_nda": True,
        "access_tier": "public",
    },
]


# ── DB insert ─────────────────────────────────────────────────────────────────

def run():
    conn = psycopg2.connect(DATABASE_URL)
    inserted = skipped = 0
    try:
        with conn:
            cur = conn.cursor()
            for f in FIRMS:
                cur.execute("""
                    INSERT INTO firms (name, slug, type, rss_url, website_url, is_nda, access_tier)
                    VALUES (%(name)s, %(slug)s, %(type)s, %(rss_url)s, %(website_url)s, %(is_nda)s, %(access_tier)s)
                    ON CONFLICT (slug) DO UPDATE SET access_tier = EXCLUDED.access_tier
                """, f)
                if cur.rowcount:
                    inserted += 1
                    print(f"  + {f['name']}")
                else:
                    skipped += 1
                    print(f"  ~ skipped (exists): {f['name']}")
        print(f"\nDone. Inserted: {inserted} | Already existed: {skipped}")
    finally:
        conn.close()


if __name__ == "__main__":
    run()