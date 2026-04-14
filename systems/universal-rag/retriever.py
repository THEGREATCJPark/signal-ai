"""
Hybrid retriever: BM25 + embedding cosine + RRF fusion.

This is the core of the RAG pipeline. Given a query:
1. Tokenize + expand → BM25 search over SQLite store
2. Embed query → cosine similarity over embedding vectors
3. RRF fuse both rankings
4. Return top-K chunk_ids with scores and raw text
"""
from __future__ import annotations

import math
import re
import struct
from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .store import Store
    from .embedder import Embedder


# Tokenization (same as build_index.py for compatibility)
ASCII_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{1,}")
DIGIT_RE = re.compile(r"\d{2,}")
KOREAN_CHAR_RE = re.compile(r"[\uac00-\ud7a3]+")


def tokenize(text: str) -> list[str]:
    tokens = []
    tokens.extend(t.lower() for t in ASCII_WORD_RE.findall(text))
    tokens.extend(DIGIT_RE.findall(text))
    for run in KOREAN_CHAR_RE.findall(text):
        for n in (2, 3):
            if len(run) < n:
                continue
            for i in range(len(run) - n + 1):
                tokens.append(run[i: i + n])
    return tokens


BASE_SYNONYMS = {
    "usb": ["USB", "usb", "유에스비", "외부저장", "외장하드", "저장장치"],
    "로그인": ["로그인", "login", "접속", "인증", "세션"],
    "기술분석": ["기술분석", "디지털기술분석", "디지털증거", "복구", "추출"],
    "유출": ["유출", "반출", "복사", "다운로드"],
    "영업비밀": ["영업비밀", "기업비밀", "기술자료"],
}


def expand_query(q: str) -> list[str]:
    variants = {q}
    for tok in q.split():
        if len(tok) >= 2:
            variants.add(tok)
    ql = q.lower()
    for key, members in BASE_SYNONYMS.items():
        if key in q or key.lower() in ql or any(m.lower() in ql for m in members):
            for m in members:
                variants.add(m)
    return sorted(variants)


def weighted_tokens(variants: list[str]) -> list[tuple[str, float]]:
    out: dict[str, float] = defaultdict(float)
    for v in variants:
        toks = tokenize(v)
        if not toks:
            continue
        w = 1.0 / math.sqrt(len(toks))
        for t in toks:
            out[t] += w
    return list(out.items())


def cosine_sim(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def rrf_fuse(rankings: list[list[tuple[int, float]]], k: int = 10) -> list[tuple[int, float]]:
    scores: dict[int, float] = defaultdict(float)
    for ranking in rankings:
        for rank, (cid, _) in enumerate(ranking):
            scores[cid] += 1.0 / (k + rank + 1)
    return sorted(scores.items(), key=lambda kv: kv[1], reverse=True)


class HybridRetriever:
    def __init__(self, store: "Store", embedder: "Embedder"):
        self.store = store
        self.embedder = embedder
        # Cache all embeddings in memory for fast cosine search
        self._vec_cache: dict[int, list[float]] = {}

    def load_vectors(self):
        """Load all embeddings into memory for fast search."""
        cur = self.store.con.execute("SELECT chunk_id, vector FROM embeddings")
        for chunk_id, blob in cur:
            self._vec_cache[chunk_id] = list(struct.unpack(f"{len(blob)//4}f", blob))

    def _get_course_chunk_ids(self, course_id: str) -> set[int]:
        """Get chunk_ids belonging to a specific course."""
        cur = self.store.con.execute(
            "SELECT c.chunk_id FROM chunks c JOIN documents d ON c.doc_id=d.doc_id "
            "WHERE json_extract(d.meta, '$.course_id')=?",
            (course_id,),
        )
        return {r[0] for r in cur.fetchall()}

    def search(
        self,
        query: str,
        top_k: int = 10,
        bm25_weight: float = 1.0,
        embed_weight: float = 1.0,
        course_filter: str | None = None,
    ) -> list[dict]:
        """Hybrid BM25 + embedding search with RRF fusion."""
        variants = expand_query(query)
        wterms = weighted_tokens(variants)

        # Optional course filter applied at retrieval level
        allowed_chunks = None
        if course_filter:
            allowed_chunks = self._get_course_chunk_ids(course_filter)

        # BM25 ranking (filter inline)
        bm25_all = self.store.bm25_search(wterms, top_k=top_k * 20)
        if allowed_chunks is not None:
            bm25_ranked = [(cid, s) for cid, s in bm25_all if cid in allowed_chunks][:top_k * 5]
        else:
            bm25_ranked = bm25_all[:top_k * 5]

        # Embedding ranking (filter inline)
        if self._vec_cache:
            q_vec = self.embedder.encode([query])[0]
            embed_scores = []
            for cid, vec in self._vec_cache.items():
                if allowed_chunks is not None and cid not in allowed_chunks:
                    continue
                sim = cosine_sim(q_vec, vec)
                embed_scores.append((cid, sim))
            embed_ranked = sorted(embed_scores, key=lambda x: x[1], reverse=True)[: top_k * 5]
        else:
            embed_ranked = []

        # RRF fusion
        rankings = [bm25_ranked]
        if embed_ranked:
            rankings.append(embed_ranked)
        fused = rrf_fuse(rankings, k=10)

        # Build result with chunk text and doc metadata
        results = []
        seen_texts = set()  # dedup near-identical chunks
        for chunk_id, score in fused[:top_k * 3]:
            chunk = self.store.get_chunk(chunk_id)
            if not chunk:
                continue
            # Dedup by text prefix
            prefix = chunk["text"][:100]
            if prefix in seen_texts:
                continue
            seen_texts.add(prefix)
            doc = self.store.get_doc(chunk["doc_id"])
            results.append({
                "chunk_id": chunk_id,
                "score": round(score, 4),
                "text": chunk["text"],
                "char_start": chunk["char_start"],
                "char_end": chunk["char_end"],
                "doc_id": chunk["doc_id"],
                "doc_path": doc["path"] if doc else "?",
                "doc_meta": doc["meta"] if doc else {},
            })
            if len(results) >= top_k:
                break
        return results
