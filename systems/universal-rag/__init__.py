"""
Universal RAG system.

Complete pipeline:
  ingest(path) → chunk → embed → index → store
  query(q) → expand → hybrid_retrieve(BM25 + embedding + graph) → RRF fuse → read(LLM) → verify → answer

No domain-specific code. Works on any text corpus.
"""
