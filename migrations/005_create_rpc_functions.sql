-- 005: pgvector 벡터 검색 RPC 함수

-- 코사인 유사도로 유사 기사 검색
CREATE OR REPLACE FUNCTION match_articles(
    query_embedding VECTOR(384),
    match_count INT DEFAULT 10,
    match_threshold FLOAT DEFAULT 0.0
)
RETURNS TABLE (
    article_id TEXT,
    title TEXT,
    source TEXT,
    url TEXT,
    summary TEXT,
    body TEXT,
    score INT,
    crawled_at TIMESTAMPTZ,
    similarity FLOAT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        a.id AS article_id,
        a.title,
        a.source,
        a.url,
        a.summary,
        a.body,
        a.score,
        a.crawled_at,
        (1 - (e.embedding <=> query_embedding))::FLOAT AS similarity
    FROM article_embeddings e
    JOIN articles a ON a.id = e.article_id
    WHERE (1 - (e.embedding <=> query_embedding)) > match_threshold
    ORDER BY e.embedding <=> query_embedding
    LIMIT match_count;
END;
$$;

-- 임베딩이 없는 기사 조회
CREATE OR REPLACE FUNCTION get_unembedded_articles(lim INT DEFAULT 100)
RETURNS TABLE (
    id TEXT,
    title TEXT,
    summary TEXT,
    body TEXT,
    source TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT a.id, a.title, a.summary, a.body, a.source
    FROM articles a
    LEFT JOIN article_embeddings e ON a.id = e.article_id
    WHERE e.article_id IS NULL
    ORDER BY a.crawled_at DESC
    LIMIT lim;
END;
$$;
