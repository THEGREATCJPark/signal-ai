"""
Embedding module. Supports:
  - local: sentence-transformers (multilingual, offline)
  - api: Gemini proxy (via OpenAI-compat endpoint)

Default: local with 'paraphrase-multilingual-MiniLM-L12-v2' (384 dims,
good Korean support, ~50MB model).
"""
from __future__ import annotations

import os
from typing import Protocol


class Embedder(Protocol):
    dim: int
    def encode(self, texts: list[str]) -> list[list[float]]: ...


class LocalEmbedder:
    """sentence-transformers based local embedder."""

    def __init__(self, model_name: str = "paraphrase-multilingual-MiniLM-L12-v2"):
        from sentence_transformers import SentenceTransformer
        self.model = SentenceTransformer(model_name)
        self.dim = self.model.get_sentence_embedding_dimension()

    def encode(self, texts: list[str], batch_size: int = 64) -> list[list[float]]:
        vecs = self.model.encode(texts, batch_size=batch_size, show_progress_bar=False)
        return [v.tolist() for v in vecs]


class GeminiProxyEmbedder:
    """Use the Gemini OpenAI-compat proxy for embeddings (if supported).
    Falls back to local if proxy doesn't support embeddings."""

    def __init__(self, base_url: str = "http://127.0.0.1:8321/v1", dim: int = 384):
        self.base_url = base_url
        self.dim = dim
        # Use local as actual backend since Gemini web doesn't expose embedding API
        self._local = LocalEmbedder()
        self.dim = self._local.dim

    def encode(self, texts: list[str]) -> list[list[float]]:
        return self._local.encode(texts)
