-- 002: pgvector 확장 활성화 + article_embeddings 테이블
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS article_embeddings (
    article_id TEXT PRIMARY KEY REFERENCES articles(id) ON DELETE CASCADE,
    embedding VECTOR(384),
    model_name TEXT DEFAULT 'multilingual-e5-small',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- IVFFlat 인덱스: 데이터 100건 이상일 때 효과적
-- 초기에는 데이터가 적으므로 인덱스 생성은 backfill 후 수동 실행 권장
-- CREATE INDEX idx_embeddings_vector ON article_embeddings
--     USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
