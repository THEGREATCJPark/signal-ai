-- Signal DB schema — portable SQLite / Postgres
-- Uses JSON column for source-specific metadata so all sources fit one table.

CREATE TABLE IF NOT EXISTS posts (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  source          TEXT NOT NULL,              -- 'hackernews' | 'reddit' | 'arxiv' | 'discord' | ...
  source_id       TEXT NOT NULL,              -- source-side unique id
  source_url      TEXT,
  author          TEXT,
  content         TEXT NOT NULL,              -- raw, full text preserved
  timestamp       TEXT NOT NULL,              -- ISO8601 with tz
  parent_id       TEXT,                       -- for replies/threads
  metadata        TEXT NOT NULL DEFAULT '{}', -- JSON string
  fetched_at      TEXT NOT NULL,              -- ISO8601 with tz
  -- Derived columns (can be backfilled later):
  embedding       BLOB,                       -- for vector search (later)
  score           REAL,                       -- normalized engagement score
  UNIQUE (source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_posts_timestamp ON posts (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_posts_source ON posts (source);
CREATE INDEX IF NOT EXISTS idx_posts_fetched ON posts (fetched_at DESC);

-- FTS5 full-text search (SQLite only; Postgres would use tsvector)
CREATE VIRTUAL TABLE IF NOT EXISTS posts_fts USING fts5(
  content,
  content_rowid='id',
  content='posts',
  tokenize='unicode61'
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS posts_ai AFTER INSERT ON posts BEGIN
  INSERT INTO posts_fts(rowid, content) VALUES (new.id, new.content);
END;

CREATE TRIGGER IF NOT EXISTS posts_ad AFTER DELETE ON posts BEGIN
  INSERT INTO posts_fts(posts_fts, rowid, content) VALUES ('delete', old.id, old.content);
END;

CREATE TRIGGER IF NOT EXISTS posts_au AFTER UPDATE ON posts BEGIN
  INSERT INTO posts_fts(posts_fts, rowid, content) VALUES ('delete', old.id, old.content);
  INSERT INTO posts_fts(rowid, content) VALUES (new.id, new.content);
END;
