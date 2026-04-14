"""
Persistent SQLite store for documents, chunks, embeddings, and BM25 postings.

Tables:
  documents    — one row per source file (path, sha256, metadata JSON)
  chunks       — one row per text chunk (doc_id, chunk_idx, text, char_start, char_end)
  embeddings   — one row per chunk (chunk_id, vector BLOB)
  bm25_terms   — (term TEXT, chunk_id INT, tf INT)
  bm25_df      — (term TEXT, df INT)
  bm25_meta    — (key TEXT, value TEXT) — total_chunks, avg_dl
  citations    — (from_doc_id, to_case_no, direction)
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import struct
from pathlib import Path


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _pack_vec(vec: list[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)


def _unpack_vec(blob: bytes) -> list[float]:
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


class Store:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.con = sqlite3.connect(self.db_path)
        self.con.execute("PRAGMA journal_mode=WAL")
        self._create_tables()

    def _create_tables(self):
        self.con.executescript("""
            CREATE TABLE IF NOT EXISTS documents (
                doc_id INTEGER PRIMARY KEY,
                path TEXT UNIQUE,
                sha256 TEXT,
                meta TEXT DEFAULT '{}'
            );
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id INTEGER PRIMARY KEY,
                doc_id INTEGER REFERENCES documents(doc_id),
                chunk_idx INTEGER,
                text TEXT,
                char_start INTEGER,
                char_end INTEGER,
                tok_count INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS embeddings (
                chunk_id INTEGER PRIMARY KEY REFERENCES chunks(chunk_id),
                vector BLOB
            );
            CREATE TABLE IF NOT EXISTS bm25_postings (
                term TEXT,
                chunk_id INTEGER,
                tf INTEGER,
                PRIMARY KEY (term, chunk_id)
            );
            CREATE TABLE IF NOT EXISTS bm25_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS citations (
                from_doc_id INTEGER,
                to_ref TEXT,
                direction TEXT DEFAULT 'outbound'
            );
            CREATE INDEX IF NOT EXISTS idx_chunks_doc ON chunks(doc_id);
            CREATE INDEX IF NOT EXISTS idx_bm25_term ON bm25_postings(term);
            CREATE INDEX IF NOT EXISTS idx_cit_from ON citations(from_doc_id);
            CREATE INDEX IF NOT EXISTS idx_cit_to ON citations(to_ref);
        """)
        self.con.commit()

    # ---- document ops ----

    def upsert_doc(self, path: str, sha256: str, meta: dict) -> int:
        cur = self.con.execute(
            "INSERT INTO documents (path, sha256, meta) VALUES (?, ?, ?) "
            "ON CONFLICT(path) DO UPDATE SET sha256=excluded.sha256, meta=excluded.meta "
            "RETURNING doc_id",
            (path, sha256, json.dumps(meta, ensure_ascii=False)),
        )
        doc_id = cur.fetchone()[0]
        self.con.commit()
        return doc_id

    def get_doc(self, doc_id: int) -> dict | None:
        cur = self.con.execute(
            "SELECT doc_id, path, sha256, meta FROM documents WHERE doc_id=?", (doc_id,)
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"doc_id": row[0], "path": row[1], "sha256": row[2], "meta": json.loads(row[3])}

    def doc_count(self) -> int:
        return self.con.execute("SELECT COUNT(*) FROM documents").fetchone()[0]

    # ---- chunk ops ----

    def insert_chunks(self, doc_id: int, chunks: list[dict]):
        """chunks = [{'text': str, 'char_start': int, 'char_end': int, 'tok_count': int}]"""
        # Delete existing chunks for this doc
        self.con.execute("DELETE FROM chunks WHERE doc_id=?", (doc_id,))
        for i, c in enumerate(chunks):
            self.con.execute(
                "INSERT INTO chunks (doc_id, chunk_idx, text, char_start, char_end, tok_count) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (doc_id, i, c["text"], c["char_start"], c["char_end"], c.get("tok_count", 0)),
            )
        self.con.commit()

    def get_chunks(self, doc_id: int) -> list[dict]:
        cur = self.con.execute(
            "SELECT chunk_id, chunk_idx, text, char_start, char_end, tok_count "
            "FROM chunks WHERE doc_id=? ORDER BY chunk_idx",
            (doc_id,),
        )
        return [
            {"chunk_id": r[0], "chunk_idx": r[1], "text": r[2],
             "char_start": r[3], "char_end": r[4], "tok_count": r[5]}
            for r in cur.fetchall()
        ]

    def get_chunk(self, chunk_id: int) -> dict | None:
        cur = self.con.execute(
            "SELECT chunk_id, doc_id, chunk_idx, text, char_start, char_end FROM chunks WHERE chunk_id=?",
            (chunk_id,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {"chunk_id": r[0], "doc_id": r[1], "chunk_idx": r[2],
                "text": r[3], "char_start": r[4], "char_end": r[5]}

    def chunk_count(self) -> int:
        return self.con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]

    def all_chunk_ids(self) -> list[int]:
        return [r[0] for r in self.con.execute("SELECT chunk_id FROM chunks ORDER BY chunk_id")]

    # ---- embedding ops ----

    def upsert_embedding(self, chunk_id: int, vec: list[float]):
        self.con.execute(
            "INSERT INTO embeddings (chunk_id, vector) VALUES (?, ?) "
            "ON CONFLICT(chunk_id) DO UPDATE SET vector=excluded.vector",
            (chunk_id, _pack_vec(vec)),
        )

    def upsert_embeddings_batch(self, pairs: list[tuple[int, list[float]]]):
        for cid, vec in pairs:
            self.upsert_embedding(cid, vec)
        self.con.commit()

    def get_embedding(self, chunk_id: int) -> list[float] | None:
        cur = self.con.execute("SELECT vector FROM embeddings WHERE chunk_id=?", (chunk_id,))
        row = cur.fetchone()
        return _unpack_vec(row[0]) if row else None

    def embedding_count(self) -> int:
        return self.con.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]

    # ---- BM25 ops ----

    def rebuild_bm25(self, tokenize_fn):
        """Rebuild BM25 index from all chunks. tokenize_fn(text) -> list[str]."""
        self.con.execute("DELETE FROM bm25_postings")
        self.con.execute("DELETE FROM bm25_meta")
        from collections import Counter

        total_toks = 0
        chunk_count = 0
        df: dict[str, int] = Counter()
        cur = self.con.execute("SELECT chunk_id, text FROM chunks")
        batch = []
        for chunk_id, text in cur:
            tokens = tokenize_fn(text)
            tf = Counter(tokens)
            total_toks += len(tokens)
            chunk_count += 1
            for term in tf:
                df[term] += 1
            for term, count in tf.items():
                batch.append((term, chunk_id, count))
            if len(batch) > 50000:
                self.con.executemany(
                    "INSERT INTO bm25_postings (term, chunk_id, tf) VALUES (?, ?, ?)", batch
                )
                batch = []
        if batch:
            self.con.executemany(
                "INSERT INTO bm25_postings (term, chunk_id, tf) VALUES (?, ?, ?)", batch
            )
        avg_dl = total_toks / max(1, chunk_count)
        self.con.execute(
            "INSERT OR REPLACE INTO bm25_meta (key, value) VALUES ('total_chunks', ?)",
            (str(chunk_count),),
        )
        self.con.execute(
            "INSERT OR REPLACE INTO bm25_meta (key, value) VALUES ('avg_dl', ?)",
            (str(avg_dl),),
        )
        # Store df
        for term, d in df.items():
            self.con.execute(
                "INSERT OR REPLACE INTO bm25_meta (key, value) VALUES (?, ?)",
                (f"df:{term}", str(d)),
            )
        self.con.commit()

    def bm25_search(self, query_terms: list[tuple[str, float]], top_k: int = 20,
                    k1: float = 1.5, b: float = 0.75) -> list[tuple[int, float]]:
        """Return [(chunk_id, score)] sorted desc."""
        import math

        N = int(self.con.execute(
            "SELECT value FROM bm25_meta WHERE key='total_chunks'"
        ).fetchone()[0])
        avg_dl = float(self.con.execute(
            "SELECT value FROM bm25_meta WHERE key='avg_dl'"
        ).fetchone()[0])

        scores: dict[int, float] = {}
        for term, weight in query_terms:
            df_row = self.con.execute(
                "SELECT value FROM bm25_meta WHERE key=?", (f"df:{term}",)
            ).fetchone()
            if not df_row:
                continue
            df = int(df_row[0])
            if df / max(N, 1) > 0.5:
                continue
            idf = math.log(1 + (N - df + 0.5) / (df + 0.5))
            postings = self.con.execute(
                "SELECT chunk_id, tf FROM bm25_postings WHERE term=?", (term,)
            ).fetchall()
            for chunk_id, tf in postings:
                # Get chunk token count
                dl_row = self.con.execute(
                    "SELECT tok_count FROM chunks WHERE chunk_id=?", (chunk_id,)
                ).fetchone()
                dl = dl_row[0] if dl_row else avg_dl
                denom = tf + k1 * (1 - b + b * dl / avg_dl)
                scores[chunk_id] = scores.get(chunk_id, 0.0) + weight * idf * (tf * (k1 + 1)) / denom
        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        return ranked[:top_k]

    # ---- citation ops ----

    def insert_citations(self, doc_id: int, refs: list[str]):
        self.con.execute("DELETE FROM citations WHERE from_doc_id=?", (doc_id,))
        for ref in refs:
            self.con.execute(
                "INSERT INTO citations (from_doc_id, to_ref) VALUES (?, ?)",
                (doc_id, ref),
            )
        self.con.commit()

    def close(self):
        self.con.close()
