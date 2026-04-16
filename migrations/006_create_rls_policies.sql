-- 006: RLS (Row Level Security) 정책 — anon 키로 접근 허용
-- Supabase는 기본적으로 RLS가 활성화되어 있으므로 정책 추가 필요

-- articles
ALTER TABLE articles ENABLE ROW LEVEL SECURITY;
CREATE POLICY "articles_all" ON articles FOR ALL USING (true) WITH CHECK (true);

-- article_embeddings
ALTER TABLE article_embeddings ENABLE ROW LEVEL SECURITY;
CREATE POLICY "embeddings_all" ON article_embeddings FOR ALL USING (true) WITH CHECK (true);

-- publish_log
ALTER TABLE publish_log ENABLE ROW LEVEL SECURITY;
CREATE POLICY "publish_log_all" ON publish_log FOR ALL USING (true) WITH CHECK (true);

-- article_clusters
ALTER TABLE article_clusters ENABLE ROW LEVEL SECURITY;
CREATE POLICY "clusters_all" ON article_clusters FOR ALL USING (true) WITH CHECK (true);

-- article_cluster_members
ALTER TABLE article_cluster_members ENABLE ROW LEVEL SECURITY;
CREATE POLICY "cluster_members_all" ON article_cluster_members FOR ALL USING (true) WITH CHECK (true);
