-- 001: articles 테이블 — 크롤링된 기사 저장
CREATE TABLE IF NOT EXISTS articles (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    score INTEGER DEFAULT 0,
    comments INTEGER DEFAULT 0,
    summary TEXT,
    body TEXT,
    tags TEXT[] DEFAULT '{}',
    raw_json JSONB,
    crawled_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source);
CREATE INDEX IF NOT EXISTS idx_articles_crawled_at ON articles(crawled_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_score ON articles(score DESC);
