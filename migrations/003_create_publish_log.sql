-- 003: publish_log 테이블 — 발행 이력 (published.json 대체)
CREATE TABLE IF NOT EXISTS publish_log (
    id SERIAL PRIMARY KEY,
    article_id TEXT NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    platform TEXT NOT NULL,
    published_at TIMESTAMPTZ DEFAULT NOW(),
    message_id TEXT,
    UNIQUE(article_id, platform)
);

CREATE INDEX IF NOT EXISTS idx_publish_log_platform ON publish_log(platform);
CREATE INDEX IF NOT EXISTS idx_publish_log_published_at ON publish_log(published_at DESC);
