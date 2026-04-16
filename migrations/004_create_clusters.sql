-- 004: article_clusters — 유사 기사 그룹핑 (시맨틱 중복 제거)
CREATE TABLE IF NOT EXISTS article_clusters (
    id SERIAL PRIMARY KEY,
    cluster_name TEXT,
    representative_id TEXT REFERENCES articles(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS article_cluster_members (
    cluster_id INTEGER NOT NULL REFERENCES article_clusters(id) ON DELETE CASCADE,
    article_id TEXT NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    similarity FLOAT,
    PRIMARY KEY (cluster_id, article_id)
);
