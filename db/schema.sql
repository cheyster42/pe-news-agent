-- Add industry classification and sentiment columns to articles.
-- IF NOT EXISTS makes these safe to run multiple times.

ALTER TABLE articles ADD COLUMN IF NOT EXISTS industry TEXT;
ALTER TABLE articles ADD COLUMN IF NOT EXISTS sentiment TEXT;
ALTER TABLE firms ADD COLUMN IF NOT EXISTS access_tier TEXT NOT NULL DEFAULT 'public';
