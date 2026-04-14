from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
import re
from typing import Any
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from .config import (
    DEFAULT_KEYWORD_COUNT,
    DEFAULT_PER_KEYWORD_LIMIT,
    DEFAULT_SELECT_MODEL,
    DEFAULT_TOP_K,
    RECORD_DB_PATH,
    WORKSPACE_SCRIPTS,
)
from .models import aggregate_selected_rows

if str(WORKSPACE_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_SCRIPTS))

import memory_evidence_rag as rag  # type: ignore
import record_search_index as search_index  # type: ignore


SAFE_TOKEN_PATTERN = re.compile(r"[0-9A-Za-z가-힣]+")
DISCIPLINE_FOCUS_TERMS = (
    "쟁점",
    "쟁점처분",
    "쟁점처분취소",
    "쟁점처분무효",
    "쟁점처분무효확인",
    "해임",
    "파면",
    "견책",
    "감봉",
    "정직",
    "강등",
    "강임",
    "벌점",
    "공권정지",
)
ANTI_NOISE_TERMS = (
    "재심",
    "재심청구",
    "재심대상자료",
    "양도담보",
    "분양대금",
)


def sanitize_search_query(query: str) -> str:
    tokens = SAFE_TOKEN_PATTERN.findall(query or "")
    return " ".join(tokens).strip()


def _is_meaningful_label(value: str) -> bool:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return False
    if text.isdigit():
        return False
    if len(text) <= 2 and text.isalnum():
        return False
    return True


def _query_focus_terms(user_task: str) -> set[str]:
    normalized = sanitize_search_query(user_task)
    focus_terms: set[str] = set()
    for term in DISCIPLINE_FOCUS_TERMS:
        if term in normalized:
            focus_terms.add(term)
    return focus_terms


def _row_penalty(row: dict[str, Any], *, user_task: str) -> float:
    parts = [
        str(row.get("title") or ""),
        str(row.get("record_title") or ""),
        str(row.get("record_type") or ""),
        str(row.get("source_org") or ""),
        str(row.get("record_number") or ""),
    ]
    label_text = " ".join(" ".join(parts).split())
    penalty = 0.0
    if not _is_meaningful_label(str(row.get("record_title") or "")) and not _is_meaningful_label(str(row.get("title") or "")):
        penalty += 75.0
    if str(row.get("source_dataset") or "") in {"02_public_record_set", "03_domain_record_set"} and not _is_meaningful_label(str(row.get("record_title") or "")):
        penalty += 40.0

    focus_terms = _query_focus_terms(user_task)
    if focus_terms:
        if not any(term in label_text for term in focus_terms):
            penalty += 160.0
        else:
            penalty -= 12.0

    normalized_task = sanitize_search_query(user_task)
    if "재심" not in normalized_task and any(term in label_text for term in ANTI_NOISE_TERMS):
        penalty += 220.0
    return penalty


def _rerank_ranked_rows(ranked: list[dict[str, Any]], *, user_task: str, limit: int) -> list[dict[str, Any]]:
    rescored: list[dict[str, Any]] = []
    for row in ranked:
        adjusted = dict(row)
        adjusted_score = float(row.get("best_score") or 0.0) + _row_penalty(row, user_task=user_task)
        adjusted["adjusted_score"] = adjusted_score
        rescored.append(adjusted)
    rescored.sort(
        key=lambda item: (
            float(item.get("adjusted_score") or 0.0),
            -int(item.get("keyword_hit_count") or 0),
            str(item.get("record_date") or ""),
            str(item.get("canonical_id") or ""),
        )
    )
    return rescored[:limit]


def build_search_queries(
    user_task: str,
    *,
    keyword_count: int = DEFAULT_KEYWORD_COUNT,
    select_model: str = DEFAULT_SELECT_MODEL,
    keyword_generator=rag.generate_search_keywords,
    timeout_seconds: int = 20,
) -> list[str]:
    base_query = sanitize_search_query(user_task)
    fallback_queries: list[str] = []
    if base_query:
        fallback_queries.append(base_query)
        fallback_queries.extend(token for token in base_query.split(" ") if token and token not in fallback_queries)

    keywords: list[str] = []
    executor = ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        future = executor.submit(keyword_generator, user_task, model=select_model, keyword_count=keyword_count)
        keywords = list(future.result(timeout=timeout_seconds) or [])
    except (TimeoutError, RuntimeError, ValueError):
        if future is not None:
            future.cancel()
        keywords = []
    except Exception:
        if future is not None:
            future.cancel()
        keywords = []
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    queries: list[str] = []
    for query in [base_query] + [sanitize_search_query(keyword) for keyword in keywords]:
        if query and query not in queries:
            queries.append(query)
    for query in fallback_queries:
        if query and query not in queries:
            queries.append(query)
    return queries


def _fetch_record_rows(conn: sqlite3.Connection, canonical_ids: list[str]) -> list[dict[str, Any]]:
    if not canonical_ids:
        return []
    placeholders = ",".join("?" for _ in canonical_ids)
    rows = conn.execute(
        f"""
        select canonical_id, source_dataset, source_path, title, record_number, source_org, record_date, record_title, record_type, full_text, text_hash
        from records
        where canonical_id in ({placeholders})
        """,
        canonical_ids,
    ).fetchall()
    by_id = {
        row[0]: {
            "canonical_id": row[0],
            "source_dataset": row[1],
            "source_path": row[2],
            "title": row[3],
            "record_number": row[4],
            "source_org": row[5],
            "record_date": row[6],
            "record_title": row[7],
            "record_type": row[8],
            "full_text": row[9],
            "text_hash": row[10],
        }
        for row in rows
    }
    return [by_id[item] for item in canonical_ids if item in by_id]


def canonical_row_to_selected_record(row: dict[str, Any]) -> dict[str, Any]:
    source_path = str(row.get("source_path") or "")
    full_text = str(row.get("full_text") or "")
    document_title = str(row.get("title") or row.get("record_title") or row.get("record_number") or row.get("canonical_id") or "")
    suffix = Path(source_path).suffix.lstrip(".") or "txt"
    return {
        "file_id": str(row.get("canonical_id") or ""),
        "relative_path": source_path,
        "absolute_path": source_path,
        "document_title": document_title,
        "doc_type": suffix,
        "source_group": "structured_record",
        "token_count": max(1, len(full_text) // 4),
        "anchor_text": " ".join(full_text.split())[:500],
        "extracted_text": full_text,
        "candidate_boundaries": [],
        "is_direct_evidence": False,
        "is_format_sample": False,
        "content_hash": str(row.get("text_hash") or ""),
        "duplicate_paths": [],
        "record_number": str(row.get("record_number") or ""),
        "source_org": str(row.get("source_org") or ""),
        "record_date": str(row.get("record_date") or ""),
        "record_title": str(row.get("record_title") or ""),
    }


def select_top_records(
    user_task: str,
    *,
    db_path: Path = RECORD_DB_PATH,
    keyword_count: int = DEFAULT_KEYWORD_COUNT,
    per_keyword_limit: int = DEFAULT_PER_KEYWORD_LIMIT,
    top_k: int = DEFAULT_TOP_K,
    select_model: str = DEFAULT_SELECT_MODEL,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    queries = build_search_queries(
        user_task,
        keyword_count=keyword_count,
        select_model=select_model,
    )
    conn = sqlite3.connect(db_path)
    try:
        keyword_hits: dict[str, list[dict[str, Any]]] = {}
        query_limit = max(per_keyword_limit, top_k * 2)
        for query in queries:
            keyword_hits[query] = search_index.search_records(conn, query, limit=query_limit)
        candidate_limit = max(top_k, top_k * 3)
        ranked = aggregate_selected_rows(keyword_hits, limit=candidate_limit)
        candidate_count = len(ranked)
        ranked = _rerank_ranked_rows(ranked, user_task=user_task, limit=candidate_limit)
        canonical_ids = [str(row.get("canonical_id") or "") for row in ranked if str(row.get("canonical_id") or "").strip()]
        full_rows = _fetch_record_rows(conn, canonical_ids)
    finally:
        conn.close()
    selected_records = [canonical_row_to_selected_record(row) for row in full_rows[:top_k]]
    selection_meta = {
        "selection_source": str(db_path),
        "selected_count": len(selected_records),
        "candidate_count": candidate_count,
        "keywords": queries[1:],
        "queries": queries,
        "top_k": top_k,
    }
    return selected_records, selection_meta
