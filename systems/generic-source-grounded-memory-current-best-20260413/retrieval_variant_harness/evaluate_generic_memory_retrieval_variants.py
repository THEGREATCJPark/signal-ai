#!/usr/bin/env python3
"""Compare generic memory retrieval variants on domain/course contracts.

The harness keeps raw source text intact and builds projection indexes over it.
It is intentionally stdlib-only so the retrieval architecture can be tested
without provider availability affecting the result.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


TOKEN_RE = re.compile(r"[A-Za-z0-9_]+|[가-힣]+")
EXACT_ID_PREFIXES = ("vis_", "src_", "obs_", "prob_", "lmeta_", "gmeta_")
STOP_TOKENS = {
    "것",
    "그",
    "이",
    "저",
    "수",
    "등",
    "및",
    "그리고",
    "하지만",
    "합니다",
    "됩니다",
    "회로이론",
    "강의영상",
    "녹음본",
    "후보",
}
SLIM_PAYLOAD_KEYS = {
    "text",
    "raw_text",
    "body",
    "excerpt",
    "supporting_text",
    "full_text",
    "snippets",
    "evidence_spans",
    "aspect_verifications",
}
DEICTIC_REFERENCE_ANCHORS = [
    "아까",
    "앞서",
    "지난",
    "저번",
    "이전",
    "위",
    "아래",
    "그 자료",
    "이 자료",
    "해당 자료",
    "본 자료",
    "본 영상",
    "본 표",
    "자료를 보고",
    "보고 말",
    "다시 보면",
    "가리킵",
    "참조",
    "연결",
]
STRONG_RELATION_EVIDENCE = {"explicit_source_id", "explicit_file_ref", "explicit_title_ref"}
BROAD_DEICTIC_ANCHORS = {"위", "아래", "참조", "지난", "이전", "앞서", "연결"}
QUERY_ALIASES = {
    "카톡": ["메신저"],
    "접속": ["로그인"],
    "장치": ["장치", "장치"],
    "장치": ["장치"],
    "암호": ["비밀번호"],
    "순서대로": ["순차", "순차 대입"],
    "음성": ["녹음본", "audio"],
    "원본 강의": ["강의영상"],
    "강의와": ["강의영상"],
    "이어짐": ["연결", "보조"],
}


def tokenize(text: str) -> list[str]:
    return [match.group(0).lower() for match in TOKEN_RE.finditer(text or "")]


def exact_query_ids_from_tokens(tokens: list[str]) -> list[str]:
    return unique_preserve_order(
        [
            token
            for token in tokens
            if any(token.startswith(prefix) for prefix in EXACT_ID_PREFIXES)
        ]
    )


def doc_matches_exact_id(doc: dict[str, Any], exact_id: str) -> bool:
    target = exact_id.lower()
    for value in [doc.get("doc_id", ""), doc.get("source_id", ""), doc.get("raw_ref", ""), doc.get("text", "")]:
        if target in str(value).lower():
            return True
    metadata = doc.get("metadata", {})
    if isinstance(metadata, dict):
        for key in ["visual_atom_id", "source_id", "observation_id", "problem_id", "meta_id", "target_id"]:
            if target in str(metadata.get(key, "")).lower():
                return True
    return False


def exact_id_match_count(doc: dict[str, Any], exact_ids: list[str]) -> int:
    return sum(1 for exact_id in exact_ids if doc_matches_exact_id(doc, exact_id))


def expand_query_text(query: str) -> str:
    additions: list[str] = []
    for alias, expansions in QUERY_ALIASES.items():
        if alias in query:
            additions.extend(expansions)
    return " ".join([query, *unique_preserve_order(additions)])


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def make_doc(
    doc_id: str,
    domain: str,
    kind: str,
    text: str,
    source_id: str | None = None,
    raw_ref: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "doc_id": doc_id,
        "domain": domain,
        "kind": kind,
        "text": text or "",
        "source_id": source_id or "",
        "raw_ref": raw_ref or "",
        "metadata": metadata or {},
    }


def split_raw_chunks(text: str, max_chars: int = 1800, overlap_chars: int = 180) -> list[str]:
    """Split oversized raw sources while keeping the original pack untouched."""
    if len(text or "") <= max_chars:
        return [text or ""]
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chars)
        if end < len(text):
            boundary = max(text.rfind("\n", start, end), text.rfind(".", start, end), text.rfind("다.", start, end))
            if boundary > start + max_chars // 2:
                end = boundary + 1
        chunks.append(text[start:end].strip())
        if end >= len(text):
            break
        start = max(0, end - overlap_chars)
    return [chunk for chunk in chunks if chunk]


def normalize_pack(pack: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    sources = pack.get("sources", [])
    source_by_id = {source.get("source_id"): source for source in sources}

    summary_docs: list[dict[str, Any]] = []
    raw_docs: list[dict[str, Any]] = []
    compressed_docs: list[dict[str, Any]] = []
    hierarchy_docs: list[dict[str, Any]] = []
    relation_docs: list[dict[str, Any]] = []
    atomic_docs: list[dict[str, Any]] = []

    for source in sources:
        source_id = source.get("source_id") or source.get("id") or f"source_{len(raw_docs)}"
        domain = source.get("domain", "generic")
        raw_ref = source.get("raw_ref") or source.get("original_path") or source.get("path") or ""
        title = source.get("title", "")
        summary = source.get("summary", "")
        raw_text = source.get("raw_text") or source.get("text") or ""
        metadata = {key: value for key, value in source.items() if key not in {"summary", "raw_text", "text"}}

        summary_docs.append(
            make_doc(
                f"summary::{source_id}",
                domain,
                "summary",
                "\n".join(part for part in [title, summary] if part),
                source_id,
                raw_ref,
                metadata,
            )
        )
        raw_chunks = split_raw_chunks(raw_text)
        for chunk_index, chunk_text in enumerate(raw_chunks):
            chunk_suffix = "" if len(raw_chunks) == 1 else f"::chunk_{chunk_index:04d}"
            raw_docs.append(
                make_doc(
                    f"raw::{source_id}{chunk_suffix}",
                    domain,
                    "raw_chunk" if len(raw_chunks) > 1 else "raw",
                    "\n".join(part for part in [title, chunk_text] if part),
                    source_id,
                    raw_ref,
                    {**metadata, "chunk_index": chunk_index, "chunk_count": len(raw_chunks)},
                )
            )
        compressed_docs.append(
            make_doc(
                f"compressed::{source_id}",
                domain,
                "compressed_projection",
                build_lossy_projection_text(title, summary, raw_text, source_id, raw_ref),
                source_id,
                raw_ref,
                metadata,
            )
        )
        hierarchy_docs.append(
            make_doc(
                f"hierarchy::{source_id}",
                domain,
                "hierarchical_anchor",
                "\n".join(
                    part
                    for part in [
                        f"anchor source_id={source_id} raw_ref={raw_ref}",
                        title,
                        summary,
                        extract_lead_and_key_sentences(raw_text),
                    ]
                    if part
                ),
                source_id,
                raw_ref,
                metadata,
            )
        )
        atomic_docs.append(
            make_doc(
                f"atomic_source::{source_id}",
                domain,
                "atomic_source_provenance",
                "\n".join(
                    part
                    for part in [
                        "source provenance",
                        f"source_id={source_id}",
                        f"raw_ref={raw_ref}",
                        title,
                        summary,
                    ]
                    if part
                ),
                source_id,
                raw_ref,
                metadata,
            )
        )

    for relation in pack.get("relations", []) + pack.get("recording_relations", []):
        source_id = relation.get("source_id") or relation.get("recording_source_id") or ""
        target_id = relation.get("target_id") or relation.get("candidate_source_id") or ""
        source = source_by_id.get(source_id, {})
        target = source_by_id.get(target_id, {})
        relation_id = relation.get("relation_id") or f"relation_{len(relation_docs)}"
        relation_text = " ".join(
            str(part)
            for part in [
                relation.get("kind", ""),
                relation.get("text", ""),
                relation.get("use_policy", ""),
                source.get("title", ""),
                target.get("title", ""),
                source.get("raw_ref") or source.get("original_path") or "",
                target.get("raw_ref") or target.get("original_path") or "",
            ]
            if part
        )
        relation_docs.append(
            make_doc(
                f"relation::{relation_id}",
                source.get("domain") or target.get("domain") or "generic",
                "relation",
                relation_text,
                source_id,
                source.get("raw_ref") or source.get("original_path") or "",
                {"source_id": source_id, "target_id": target_id, **relation},
            )
        )

    atomic_docs.extend(build_generic_memory_atoms(sources))
    relation_docs.extend(derive_generic_relation_docs(sources))
    atomic_docs.extend(build_repetition_count_docs(sources))

    for problem in pack.get("problems", []):
        source_id = problem.get("source_id") or ""
        source = source_by_id.get(source_id, {})
        problem_id = problem.get("problem_id") or f"problem_{len(atomic_docs)}"
        local_meta = problem.get("local_meta") or []
        if isinstance(local_meta, str):
            local_meta = [local_meta]
        text = "\n".join(
            str(part)
            for part in [
                source.get("title", ""),
                problem.get("question_text", ""),
                problem.get("official_explanation", ""),
                "\n".join(str(item) for item in local_meta),
            ]
            if part
        )
        atomic_docs.append(
            make_doc(
                f"atomic_problem::{problem_id}",
                source.get("domain", "course"),
                "atomic_problem",
                text,
                source_id,
                source.get("raw_ref") or source.get("original_path") or "",
                {"problem_id": problem_id, **problem},
            )
        )

    for observation in pack.get("observations", []):
        source_id = observation.get("source_id") or ""
        source = source_by_id.get(source_id, {})
        observation_id = observation.get("observation_id") or f"observation_{len(raw_docs)}"
        text = "\n".join(
            str(part)
            for part in [
                source.get("title", ""),
                observation.get("type", ""),
                observation.get("derived_kind", ""),
                observation.get("path", ""),
                observation.get("text", ""),
            ]
            if part
        )
        raw_docs.append(
            make_doc(
                f"raw_observation::{observation_id}",
                source.get("domain", pack.get("course", {}).get("name", "course")),
                "raw_observation",
                text,
                source_id,
                observation.get("path") or source.get("raw_ref") or source.get("original_path") or "",
                {"observation_id": observation_id, **observation},
            )
        )
        hierarchy_docs.append(
            make_doc(
                f"hierarchy_observation::{observation_id}",
                source.get("domain", "course"),
                "hierarchical_anchor",
                "\n".join(
                    part
                    for part in [
                        f"anchor source_id={source_id} observation_id={observation_id}",
                        source.get("title", ""),
                        extract_lead_and_key_sentences(observation.get("text", "")),
                    ]
                    if part
                ),
                source_id,
                observation.get("path") or source.get("raw_ref") or source.get("original_path") or "",
                {"observation_id": observation_id, **observation},
            )
        )

    for meta_key, kind in [("global_meta", "atomic_global_meta"), ("local_meta", "atomic_local_meta")]:
        for meta in pack.get(meta_key, []):
            source_id = meta.get("source_id") or ""
            source = source_by_id.get(source_id, {})
            meta_id = meta.get("meta_id") or f"{meta_key}_{len(atomic_docs)}"
            text = "\n".join(
                str(part)
                for part in [
                    source.get("title", ""),
                    meta_key,
                    meta.get("problem_id", ""),
                    meta.get("text", ""),
                ]
                if part
            )
            atomic_docs.append(
                make_doc(
                    f"{kind}::{meta_id}",
                    source.get("domain", "course"),
                    kind,
                    text,
                    source_id,
                    source.get("raw_ref") or source.get("original_path") or "",
                    {"meta_id": meta_id, **meta},
                )
            )

    for visual in pack.get("visual_atoms", []):
        source_id = visual.get("source_id") or ""
        source = source_by_id.get(source_id, {})
        visual_id = visual.get("visual_atom_id") or f"visual_{len(atomic_docs)}"
        text = "\n".join(
            str(part)
            for part in [
                f"visual_atom_id={visual_id}",
                source.get("title", ""),
                visual.get("kind", ""),
                visual.get("text_hint", ""),
                visual.get("image_path", ""),
                visual.get("confidence", ""),
            ]
            if part
        )
        atomic_docs.append(
            make_doc(
                f"atomic_visual::{visual_id}",
                source.get("domain", "course"),
                "atomic_visual",
                text,
                source_id,
                visual.get("image_path") or source.get("raw_ref") or source.get("original_path") or "",
                {"visual_atom_id": visual_id, **visual},
            )
        )

    return {
        "summary_only": summary_docs,
        "raw_leaf": raw_docs + atomic_docs,
        "compressed_projection": compressed_docs,
        "hierarchical_anchor": hierarchy_docs,
        "graph_relation": relation_docs,
        "atomic_kag": atomic_docs + relation_docs,
        "coverage_patch": raw_docs + atomic_docs + relation_docs + compressed_docs + hierarchy_docs,
        "ultimate_rrf": raw_docs + atomic_docs + relation_docs + compressed_docs + hierarchy_docs + summary_docs,
    }


def derive_generic_relation_docs(sources: list[dict[str, Any]], max_targets_per_source: int = 3) -> list[dict[str, Any]]:
    """Infer generic cross-source reference edges from metadata and raw text.

    The relation engine is deliberately domain-neutral: it looks for deictic
    language, explicit file/source references, temporal proximity, shared title
    anchors, path-family proximity, and shared lexical anchors. Media-specific
    labels are kept only as compatibility/evidence labels on top of the generic
    `generic_candidate_refers_to` edge.
    """
    docs: list[dict[str, Any]] = []
    for source in sources:
        source_id = source.get("source_id") or source.get("id") or ""
        if not source_id:
            continue
        source_text = relation_source_text(source)
        source_content_text = relation_source_content_text(source)
        if not has_relation_candidate_signal(source, source_content_text, sources):
            continue

        candidates: list[tuple[float, dict[str, Any], list[str]]] = []
        for target in sources:
            target_id = target.get("source_id") or target.get("id") or ""
            if not target_id or target_id == source_id:
                continue
            if not should_score_relation_candidate(source, target, source_text):
                continue
            score, evidence = score_generic_relation_candidate(source, target, source_text)
            if score >= 5.0:
                candidates.append((score, target, evidence))

        for score, target, evidence in sorted(candidates, key=lambda item: (-item[0], source_key(item[1])))[:max_targets_per_source]:
            target_id = target.get("source_id") or target.get("id") or ""
            relation_id = f"generic_refers_to::{source_id}::{target_id}"
            evidence_labels = ["generic_candidate_refers_to", *evidence]
            if is_audio_like(source) and is_video_like(target) and has_temporal_proximity(source, target):
                evidence_labels.append("inferred_same_week_audio_video")
            text = " ".join(
                str(part)
                for part in [
                    *unique_preserve_order(evidence_labels),
                    f"score={score:.2f}",
                    source.get("title", ""),
                    target.get("title", ""),
                    source_id,
                    target_id,
                    source.get("raw_ref") or source.get("original_path") or "",
                    target.get("raw_ref") or target.get("original_path") or "",
                    extract_relation_excerpt(source_text),
                    f"target_raw_excerpt={extract_relation_excerpt(relation_source_content_text(target))}",
                    "generic relation graph inferred that source refers to target by metadata/text evidence",
                ]
                if part
            )
            docs.append(
                make_doc(
                    f"relation::{relation_id}",
                    source.get("domain") or target.get("domain") or "generic",
                    "relation",
                    text,
                    source_id,
                    source.get("raw_ref") or source.get("original_path") or "",
                    {
                        "relation_id": relation_id,
                        "kind": "generic_candidate_refers_to",
                        "source_id": source_id,
                        "target_id": target_id,
                        "source_title": source.get("title", ""),
                        "target_title": target.get("title", ""),
                        "target_raw_ref": target.get("raw_ref") or target.get("original_path") or "",
                        "score": score,
                        "evidence": unique_preserve_order(evidence_labels),
                    },
                )
            )
    docs.extend(build_transitive_relation_docs(docs, sources))
    return docs


def infer_same_week_recording_relations(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Backward-compatible wrapper for older callers."""
    return [
        doc
        for doc in derive_generic_relation_docs(sources)
        if "inferred_same_week_audio_video" in doc.get("text", "")
    ]


def build_transitive_relation_docs(direct_docs: list[dict[str, Any]], sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    source_by_id = {source.get("source_id") or source.get("id"): source for source in sources}
    direct_edges = [
        doc
        for doc in direct_docs
        if doc.get("metadata", {}).get("kind") == "generic_candidate_refers_to"
    ]
    outgoing: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for edge in direct_edges:
        outgoing[str(edge.get("metadata", {}).get("source_id") or "")].append(edge)

    transitive_docs: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for first_edge in direct_edges:
        first_meta = first_edge.get("metadata", {})
        if not has_strong_relation_edge(first_meta):
            continue
        source_id = str(first_meta.get("source_id") or "")
        via_id = str(first_meta.get("target_id") or "")
        if not source_id or not via_id:
            continue
        for second_edge in outgoing.get(via_id, []):
            second_meta = second_edge.get("metadata", {})
            if not has_strong_relation_edge(second_meta):
                continue
            target_id = str(second_meta.get("target_id") or "")
            if not target_id or target_id in {source_id, via_id}:
                continue
            edge_key = (source_id, via_id, target_id)
            if edge_key in seen:
                continue
            seen.add(edge_key)
            relation_id = f"generic_multi_hop::{source_id}::{via_id}::{target_id}"
            source = source_by_id.get(source_id, {})
            via = source_by_id.get(via_id, {})
            target = source_by_id.get(target_id, {})
            text = " ".join(
                str(part)
                for part in [
                    "generic_multi_hop_refers_to",
                    f"source_id={source_id}",
                    f"via_id={via_id}",
                    f"target_id={target_id}",
                    source.get("title", ""),
                    via.get("title", ""),
                    target.get("title", ""),
                    first_edge.get("text", ""),
                    second_edge.get("text", ""),
                    f"target_raw_excerpt={extract_relation_excerpt(relation_source_content_text(target))}",
                    "generic relation graph inferred a two-hop source via target chain",
                ]
                if part
            )
            transitive_docs.append(
                make_doc(
                    f"relation::{relation_id}",
                    source.get("domain") or via.get("domain") or target.get("domain") or "generic",
                    "relation",
                    text,
                    source_id,
                    source.get("raw_ref") or source.get("original_path") or "",
                    {
                        "relation_id": relation_id,
                        "kind": "generic_multi_hop_refers_to",
                        "source_id": source_id,
                        "via_id": via_id,
                        "target_id": target_id,
                        "target_raw_ref": target.get("raw_ref") or target.get("original_path") or "",
                        "evidence": [
                            first_meta.get("relation_id"),
                            second_meta.get("relation_id"),
                        ],
                    },
                )
            )
    return transitive_docs


def has_strong_relation_edge(metadata: dict[str, Any]) -> bool:
    return bool(STRONG_RELATION_EVIDENCE & set(metadata.get("evidence", [])))


def source_key(source: dict[str, Any]) -> str:
    return str(source.get("source_id") or source.get("id") or source.get("title") or "")


def relation_source_text(source: dict[str, Any]) -> str:
    return "\n".join(
        str(source.get(key, ""))
        for key in ["source_id", "title", "summary", "raw_text", "text", "raw_ref", "original_path", "path"]
        if source.get(key)
    )


def relation_source_content_text(source: dict[str, Any]) -> str:
    return "\n".join(
        str(source.get(key, ""))
        for key in ["summary", "raw_text", "text"]
        if source.get(key)
    )


def has_reference_signal(source_content_text: str) -> bool:
    if deictic_reference_anchors_in_text(source_content_text):
        return True
    return bool(re.search(r"(파일\s*\d+|file\s*\d+)", source_content_text))


def contains_identifier_reference(text: str, identifier: str) -> bool:
    if not text or not identifier:
        return False
    if identifier not in text:
        return False
    return bool(re.search(rf"(?<![A-Za-z0-9_]){re.escape(identifier)}(?![A-Za-z0-9_])", text))


def deictic_reference_anchors_in_text(text: str) -> list[str]:
    anchors: list[str] = []
    for anchor in DEICTIC_REFERENCE_ANCHORS:
        if anchor in BROAD_DEICTIC_ANCHORS:
            continue
        if anchor in text:
            anchors.append(anchor)
    reference_nouns = r"(자료|표|파일|문서|영상|그림|이미지|회로|내용|사례|강의)"
    for broad_anchor in ["위", "아래", "이전"]:
        if re.search(rf"{broad_anchor}\s+{reference_nouns}", text):
            anchors.append(broad_anchor)
    if re.search(rf"앞서\s*(본|말한|언급한|다룬|확인한)?\s*{reference_nouns}", text):
        anchors.append("앞서")
    if re.search(rf"지난\s*(시간|강의|자료|영상|문서|파일|표|번|주차)", text):
        anchors.append("지난")
    if re.search(rf"({reference_nouns}|source_id|raw_ref).{{0,20}}참[고조]|참[고조].{{0,20}}({reference_nouns}|source_id|raw_ref)", text):
        anchors.append("참조")
    if re.search(rf"({reference_nouns}|source_id|raw_ref).{{0,20}}연결|연결.{{0,20}}({reference_nouns}|source_id|raw_ref)", text):
        anchors.append("연결")
    return unique_preserve_order(anchors)


def has_relation_candidate_signal(source: dict[str, Any], source_content_text: str, sources: list[dict[str, Any]]) -> bool:
    if has_reference_signal(source_content_text):
        return True
    source_id = source.get("source_id") or source.get("id") or ""
    source_title = str(source.get("title") or "")
    source_ref = str(source.get("raw_ref") or source.get("original_path") or source.get("path") or "")
    source_ref_name = Path(source_ref).name if source_ref else ""
    for target in sources:
        target_id = str(target.get("source_id") or target.get("id") or "")
        if not target_id or target_id == source_id:
            continue
        target_title = str(target.get("title") or "")
        target_ref = str(target.get("raw_ref") or target.get("original_path") or target.get("path") or "")
        target_ref_name = Path(target_ref).name if target_ref else ""
        if contains_identifier_reference(source_content_text, target_id):
            return True
        if target_title and target_title != source_title and target_title in source_content_text:
            return True
        if target_ref_name and target_ref_name != source_ref_name and target_ref_name in source_content_text:
            return True
    return False


def score_generic_relation_candidate(
    source: dict[str, Any],
    target: dict[str, Any],
    source_text: str,
) -> tuple[float, list[str]]:
    score = 0.0
    evidence: list[str] = []
    target_text = relation_source_text(target)
    target_id = str(target.get("source_id") or target.get("id") or "")
    target_ref = str(target.get("raw_ref") or target.get("original_path") or target.get("path") or "")
    target_title = str(target.get("title") or "")

    if deictic_reference_anchors_in_text(source_text):
        score += 2.0
        evidence.append("deictic_reference")
    if contains_identifier_reference(source_text, target_id):
        score += 10.0
        evidence.append("explicit_source_id")
    if target_ref and (target_ref in source_text or Path(target_ref).name in source_text):
        score += 8.0
        evidence.append("explicit_file_ref")
    if target_title and target_title in source_text:
        score += 8.0
        evidence.append("explicit_title_ref")
    if has_temporal_proximity(source, target):
        score += 3.0
        evidence.append("temporal_proximity")
    if has_path_family_proximity(source, target):
        score += 2.5
        evidence.append("path_family_proximity")

    target_title_terms = relation_anchor_terms(target_title, min_len=1, max_terms=8)
    matched_title_terms = [term for term in target_title_terms if term in source_text]
    if matched_title_terms:
        score += min(4.0, len(matched_title_terms) * 1.25)
        evidence.append("title_anchor_overlap")

    source_terms = set(relation_anchor_terms(source_text, max_terms=80))
    target_terms = set(relation_anchor_terms(target_text, max_terms=80))
    shared_terms = sorted(source_terms & target_terms)
    if shared_terms:
        score += min(4.0, len(shared_terms) * 0.5)
        evidence.append("lexical_anchor_overlap")

    if is_modality_reference_compatible(source_text, target):
        score += 2.0
        evidence.append("modality_reference_compatible")

    return score, unique_preserve_order(evidence)


def should_score_relation_candidate(source: dict[str, Any], target: dict[str, Any], source_text: str) -> bool:
    source_title = str(source.get("title") or "")
    source_ref = str(source.get("raw_ref") or source.get("original_path") or source.get("path") or "")
    source_ref_name = Path(source_ref).name if source_ref else ""
    target_id = str(target.get("source_id") or target.get("id") or "")
    target_ref = str(target.get("raw_ref") or target.get("original_path") or target.get("path") or "")
    target_ref_name = Path(target_ref).name if target_ref else ""
    target_title = str(target.get("title") or "")
    if contains_identifier_reference(source_text, target_id):
        return True
    if target_ref and target_ref != source_ref and target_ref in source_text:
        return True
    if target_ref_name and target_ref_name != source_ref_name and target_ref_name in source_text:
        return True
    if target_title and target_title != source_title and target_title in source_text:
        return True
    if has_temporal_proximity(source, target):
        return True
    if has_path_family_proximity(source, target):
        return True
    return False


def has_temporal_proximity(source: dict[str, Any], target: dict[str, Any]) -> bool:
    for key in ["week", "lecture_week", "date", "session", "round"]:
        if source.get(key) is not None and source.get(key) == target.get(key):
            return True
    return False


def has_path_family_proximity(source: dict[str, Any], target: dict[str, Any]) -> bool:
    source_ref = str(source.get("raw_ref") or source.get("original_path") or source.get("path") or "")
    target_ref = str(target.get("raw_ref") or target.get("original_path") or target.get("path") or "")
    if not source_ref or not target_ref:
        return False
    if source_ref == target_ref:
        return False
    if is_aggregate_artifact_ref(source_ref) or is_aggregate_artifact_ref(target_ref):
        return False
    source_parent = Path(source_ref).parent
    target_parent = Path(target_ref).parent
    return str(source_parent) == str(target_parent) and str(source_parent) not in {"", "."}


def is_aggregate_artifact_ref(ref: str) -> bool:
    path = Path(ref)
    basename = path.name.lower()
    if basename in {"candidate_records.json", "evidence_ledger.json", "selected.json", "coverage_report.json", "evaluation.json"}:
        return True
    return path.parent.name.lower() in {"for_eval", "artifacts", "outputs"} and path.suffix.lower() in {".json", ".jsonl", ".csv"}


def relation_anchor_terms(text: str, max_terms: int = 24, min_len: int = 2) -> list[str]:
    terms: list[str] = []
    for token in tokenize(text):
        token = normalize_repetition_token(token)
        if len(token) < min_len and not token.isdigit():
            continue
        if token in STOP_TOKENS:
            continue
        terms.append(token)
    return unique_preserve_order(terms)[:max_terms]


def is_modality_reference_compatible(source_text: str, target: dict[str, Any]) -> bool:
    target_kind_text = " ".join(
        str(target.get(key, ""))
        for key in ["kind", "title", "raw_ref", "original_path", "path"]
    ).lower()
    compatibility_groups = [
        (["표", "table"], ["표", "table", ".csv", ".xlsx"]),
        (["영상", "강의", "video"], ["영상", "video", ".mp4", ".mkv", ".mov"]),
        (["사진", "이미지", "그림", "image"], ["사진", "이미지", "그림", "image", ".png", ".jpg", ".jpeg"]),
        (["녹음", "음성", "audio"], ["녹음", "음성", "audio", ".m4a", ".mp3", ".wav"]),
        (["자료", "문서", "file"], ["자료", "문서", "file", ".md", ".pdf", ".txt"]),
    ]
    return any(
        any(anchor in source_text.lower() for anchor in source_anchors)
        and any(anchor in target_kind_text for anchor in target_anchors)
        for source_anchors, target_anchors in compatibility_groups
    )


def extract_relation_excerpt(text: str, max_chars: int = 260) -> str:
    text = " ".join((text or "").split())
    if len(text) <= max_chars:
        return text
    for anchor in deictic_reference_anchors_in_text(text):
        index = text.find(anchor)
        if index >= 0:
            start = max(0, index - max_chars // 3)
            return text[start : start + max_chars]
    return text[:max_chars]


def is_audio_like(source: dict[str, Any]) -> bool:
    text = " ".join(str(source.get(key, "")) for key in ["kind", "title", "raw_ref", "original_path"]).lower()
    return any(anchor in text for anchor in ["audio", "recording", "녹음", ".m4a", ".mp3", ".wav"])


def is_video_like(source: dict[str, Any]) -> bool:
    text = " ".join(str(source.get(key, "")) for key in ["kind", "title", "raw_ref", "original_path"]).lower()
    return any(anchor in text for anchor in ["video", "강의영상", "영상", ".mp4", ".mkv", ".mov"])


def build_generic_memory_atoms(sources: list[dict[str, Any]], max_atoms_per_source: int = 80) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for source in sources:
        source_id = source.get("source_id") or source.get("id") or ""
        raw_ref = source.get("raw_ref") or source.get("original_path") or source.get("path") or ""
        raw_text = source.get("raw_text") or source.get("text") or ""
        if not source_id or not raw_text:
            continue
        spans = iter_text_spans(raw_text)
        kept = 0
        for span_index, span_start, span_end, span_text in spans:
            anchor_terms = relation_anchor_terms(span_text, max_terms=12)
            deictic_anchors = deictic_reference_anchors_in_text(span_text)
            if not anchor_terms and not deictic_anchors:
                continue
            text = "\n".join(
                str(part)
                for part in [
                    "generic_memory_atom",
                    f"source_id={source_id}",
                    f"raw_ref={raw_ref}",
                    f"span_start={span_start}",
                    f"span_end={span_end}",
                    f"anchor_terms={','.join(anchor_terms)}",
                    f"deictic_anchors={','.join(deictic_anchors)}",
                    source.get("title", ""),
                    span_text,
                ]
                if part is not None and str(part) != ""
            )
            docs.append(
                make_doc(
                    f"atomic_generic::{source_id}::{span_index:04d}",
                    source.get("domain", "generic"),
                    "generic_memory_atom",
                    text,
                    source_id,
                    raw_ref,
                    {
                        "source_id": source_id,
                        "raw_ref": raw_ref,
                        "span_index": span_index,
                        "span_start": span_start,
                        "span_end": span_end,
                        "anchor_terms": anchor_terms,
                        "deictic_anchors": deictic_anchors,
                    },
                )
            )
            kept += 1
            if max_atoms_per_source is not None and kept >= max_atoms_per_source:
                break
        if max_atoms_per_source is not None and len(spans) > kept:
            omitted = len(spans) - kept
            docs.append(
                make_doc(
                    f"atomic_generic_overflow::{source_id}",
                    source.get("domain", "generic"),
                    "generic_memory_atom_overflow",
                    "\n".join(
                        [
                            "generic_memory_atom_overflow projection capped",
                            f"source_id={source_id}",
                            f"raw_ref={raw_ref}",
                            f"kept_atom_count={kept}",
                            f"omitted_atom_count={omitted}",
                            "raw source remains authoritative; rerun atomization with a higher cap or use raw_leaf chunks for missing spans",
                        ]
                    ),
                    source_id,
                    raw_ref,
                    {
                        "source_id": source_id,
                        "raw_ref": raw_ref,
                        "kept_atom_count": kept,
                        "omitted_atom_count": omitted,
                        "span_count": len(spans),
                    },
                )
            )
    return docs


def iter_text_spans(text: str, max_chars: int = 900) -> list[tuple[int, int, int, str]]:
    spans: list[tuple[int, int, int, str]] = []
    span_index = 0
    for match in re.finditer(r"[^\n.!?。]+(?:[.!?。]+|$)", text or ""):
        raw_span = match.group(0)
        span_text = raw_span.strip()
        if not span_text:
            continue
        start = match.start() + (len(raw_span) - len(raw_span.lstrip()))
        end = start + len(span_text)
        if len(span_text) <= max_chars:
            spans.append((span_index, start, end, span_text))
            span_index += 1
            continue
        for chunk in split_raw_chunks(span_text, max_chars=max_chars, overlap_chars=80):
            chunk_start = text.find(chunk[: min(80, len(chunk))], start, end)
            if chunk_start < 0:
                chunk_start = start
            spans.append((span_index, chunk_start, chunk_start + len(chunk), chunk))
            span_index += 1
    return spans


def build_repetition_count_docs(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sources_by_token: dict[str, set[str]] = defaultdict(set)
    for source in sources:
        source_id = source.get("source_id") or source.get("id") or ""
        raw_text = source.get("raw_text") or source.get("text") or ""
        for token in set(tokenize(raw_text)):
            normalized_token = normalize_repetition_token(token)
            if len(normalized_token) >= 3 and normalized_token not in STOP_TOKENS:
                sources_by_token[normalized_token].add(source_id)

    docs: list[dict[str, Any]] = []
    for token, source_ids in sorted(sources_by_token.items()):
        if len(source_ids) < 2:
            continue
        doc_id = f"atomic_repetition::{token}"
        text = f"repetition_count term={token} mention_count={len(source_ids)} sources={','.join(sorted(source_ids))} {token}"
        docs.append(
            make_doc(
                doc_id,
                "generic",
                "atomic_repetition_count",
                text,
                "",
                "",
                {"term": token, "mention_count": len(source_ids), "source_ids": sorted(source_ids)},
            )
        )
    return docs


def normalize_repetition_token(token: str) -> str:
    for suffix in ["에서는", "에게는", "으로는", "이라는", "이라는", "입니다", "됩니다", "에서는", "하고", "하며", "하면", "들은", "으로", "에서", "에게", "까지", "부터", "라고", "라는", "이며", "이고", "이다", "한다", "된다", "입니다", "합니다", "들은", "은", "는", "이", "가", "을", "를", "도", "만", "에"]:
        if token.endswith(suffix) and len(token) > len(suffix) + 1:
            return token[: -len(suffix)]
    return token


def build_lossy_projection_text(title: str, summary: str, raw_text: str, source_id: str, raw_ref: str) -> str:
    tokens = unique_preserve_order(tokenize(raw_text))
    keep: list[str] = []
    for token in tokens:
        if len(token) >= 2 or re.search(r"\d", token):
            keep.append(token)
    return " ".join(unique_preserve_order([source_id, raw_ref, *tokenize(title), *tokenize(summary), *keep]))


def extract_lead_and_key_sentences(text: str, max_sentences: int = 6) -> str:
    sentences = [part.strip() for part in re.split(r"(?<=[.!?。])|\n", text or "") if part.strip()]
    if not sentences:
        return ""
    key_words = {"시험", "중간", "기말", "중요", "반드시", "문제", "범위", "정정", "취소", "변경", "인증토큰", "메신저", "비밀번호", "회로"}
    selected = sentences[:2]
    for sentence in sentences[2:]:
        if any(key in sentence for key in key_words):
            selected.append(sentence)
        if len(selected) >= max_sentences:
            break
    return "\n".join(unique_preserve_order(selected))


def build_variant_docs(pack: dict[str, Any], variant: str) -> list[dict[str, Any]]:
    docs_by_variant = normalize_pack(pack)
    if variant not in docs_by_variant:
        raise ValueError(f"unknown variant: {variant}")
    return docs_by_variant[variant]


def build_search_index(docs: list[dict[str, Any]]) -> dict[str, Any]:
    tokenized = [tokenize(doc["text"]) for doc in docs]
    counts = [Counter(tokens) for tokens in tokenized]
    length_norms = [math.sqrt(max(1, len(tokens))) for tokens in tokenized]
    df = Counter(token for tokens in tokenized for token in set(tokens))
    postings: dict[str, list[int]] = defaultdict(list)
    for doc_index, tokens in enumerate(tokenized):
        for token in set(tokens):
            postings[token].append(doc_index)
            normalized_token = normalize_repetition_token(token)
            if normalized_token and normalized_token != token:
                postings[normalized_token].append(doc_index)
    return {
        "docs": docs,
        "tokens": tokenized,
        "counts": counts,
        "length_norms": length_norms,
        "df": df,
        "postings": dict(postings),
    }


def score_index(index: dict[str, Any], query: str, top_k: int = 8) -> list[dict[str, Any]]:
    expanded_query = expand_query_text(query)
    query_tokens = tokenize(expanded_query)
    if not query_tokens:
        return []
    query_counts = Counter(query_tokens)
    docs = index["docs"]
    doc_counts = index["counts"]
    length_norms = index["length_norms"]
    df = index["df"]
    postings = index.get("postings", {})
    total_docs = max(1, len(docs))
    candidate_indices: set[int] = set()
    for token in query_counts:
        candidate_indices.update(postings.get(token, []))
    if not candidate_indices:
        return []
    scored: list[dict[str, Any]] = []
    for doc_index in sorted(candidate_indices):
        doc = docs[doc_index]
        counts = doc_counts[doc_index]
        length_norm = length_norms[doc_index]
        score = 0.0
        for token, q_count in query_counts.items():
            if counts[token]:
                idf = math.log((1 + total_docs) / (1 + df[token])) + 1
                score += (counts[token] / length_norm) * idf * q_count
        text_lower = doc["text"].lower()
        for token in query_tokens:
            if len(token) >= 3 and token in text_lower:
                score += 0.15
        score += query_aware_boost(doc, expanded_query, query_tokens)
        if score > 0:
            scored.append({**doc, "score": score})
    return sorted(scored, key=lambda item: (-item["score"], item["doc_id"]))[:top_k]


def query_aware_boost(doc: dict[str, Any], query: str, query_tokens: list[str]) -> float:
    text = doc.get("text", "")
    metadata = doc.get("metadata", {})
    boost = 0.0
    exact_ids = exact_query_ids_from_tokens(query_tokens)
    if exact_ids:
        exact_matches = exact_id_match_count(doc, exact_ids)
        if exact_matches:
            boost += 35.0 * exact_matches
        elif doc.get("kind") in {"atomic_visual", "atomic_local_meta", "atomic_global_meta", "raw_observation", "relation"}:
            boost -= 2.0
    is_count_query = "몇" in query and any(anchor in query for anchor in ["언급", "중복", "집계", "횟수"])
    if doc.get("kind") == "atomic_repetition_count" and not is_count_query:
        boost -= 5.0
    if any(anchor in query for anchor in ["최신", "정정", "최근", "취소", "변경"]):
        wants_correction = any(anchor in query for anchor in ["정정", "취소"])
        correction_match = any(anchor in text for anchor in ["정정", "취소"]) if wants_correction else any(anchor in text for anchor in ["정정", "취소", "변경"])
        if correction_match:
            boost += 8.0
            if doc.get("kind") in {"raw", "raw_chunk", "raw_observation", "hierarchical_anchor"}:
                boost += 6.0
            if doc.get("kind") == "generic_memory_atom":
                boost -= 2.0
            week = metadata.get("week")
            if isinstance(week, int | float):
                boost += float(week) * 0.05
        elif wants_correction:
            boost -= 4.0
        if doc.get("kind") == "relation":
            boost -= 10.0
    if is_count_query:
        if doc.get("kind") == "atomic_repetition_count":
            boost += 12.0
    if any(anchor in query for anchor in ["어떤 영상", "녹음본", "매칭", "보고 말"]):
        if doc.get("kind") == "relation" and ("inferred_same_week_audio_video" in text or "support_mapped_source" in text or "강의영상" in text):
            boost += 12.0
    if any(anchor in query.lower() for anchor in ["사진", "image", "png", "page image", "회로 사진"]):
        if doc.get("kind") == "atomic_visual" and any(anchor in text for anchor in ["page_image", ".png", "needs_model_verification"]):
            boost += 10.0
    if doc.get("kind") == "atomic_source_provenance":
        if "source provenance" in query.lower():
            boost += 20.0
        else:
            boost -= 8.0
    if "global_meta" in query and doc.get("kind") == "atomic_global_meta":
        boost += 14.0
    if "local_meta" in query and doc.get("kind") == "atomic_local_meta":
        boost += 14.0
    return boost


def score_docs(docs: list[dict[str, Any]], query: str, top_k: int = 8) -> list[dict[str, Any]]:
    return score_index(build_search_index(docs), query, top_k)


def rrf_fuse(result_sets: list[list[dict[str, Any]]], top_k: int = 8, k: int = 60) -> list[dict[str, Any]]:
    fused_scores: dict[str, float] = defaultdict(float)
    doc_by_id: dict[str, dict[str, Any]] = {}
    evidence_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for results in result_sets:
        for rank, doc in enumerate(results, start=1):
            fused_scores[doc["doc_id"]] += 1 / (k + rank)
            fused_scores[doc["doc_id"]] += min(float(doc.get("score", 0.0)), 30.0) * 0.001
            doc_by_id[doc["doc_id"]] = doc
            evidence_by_id[doc["doc_id"]].append(doc)
    fused: list[dict[str, Any]] = []
    for doc_id, score in fused_scores.items():
        doc = dict(doc_by_id[doc_id])
        doc["score"] = score
        rrf_kinds = unique_preserve_order([str(evidence.get("kind", "")) for evidence in evidence_by_id[doc_id]])
        if len(evidence_by_id[doc_id]) > 1:
            doc["kind"] = "rrf_fused"
        doc["metadata"] = {
            **doc.get("metadata", {}),
            "rrf_evidence_count": len(evidence_by_id[doc_id]),
            "rrf_kinds": rrf_kinds,
        }
        fused.append(doc)
    return sorted(fused, key=lambda item: (-item["score"], item["doc_id"]))[:top_k]


def search_variant(pack: dict[str, Any], variant: str, query: str, top_k: int = 8) -> list[dict[str, Any]]:
    return search_indexes(build_indexes(pack), variant, query, top_k)


def build_indexes(pack: dict[str, Any]) -> dict[str, dict[str, Any]]:
    docs_by_variant = normalize_pack(pack)
    return {
        variant: build_search_index(docs)
        for variant, docs in docs_by_variant.items()
        if variant != "ultimate_rrf"
    }


def search_indexes(indexes: dict[str, dict[str, Any]], variant: str, query: str, top_k: int = 8) -> list[dict[str, Any]]:
    if variant == "ultimate_rrf":
        component_variants = ["raw_leaf", "compressed_projection", "hierarchical_anchor", "graph_relation", "atomic_kag", "summary_only"]
        component_top_k = min(max(top_k * 6, top_k + 24), 80)
        fused = rrf_fuse([score_index(indexes[name], query, top_k=component_top_k) for name in component_variants], top_k=component_top_k)
        return rerank_fused_results(fused, query)[:top_k]
    return score_index(indexes[variant], query, top_k=top_k)


def rerank_fused_results(hits: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    reranked: list[dict[str, Any]] = []
    for hit in hits:
        adjusted = dict(hit)
        adjusted["score"] = float(hit.get("score", 0.0)) + final_fusion_boost(hit, query)
        reranked.append(adjusted)
    return sorted(reranked, key=lambda item: (-item["score"], item["doc_id"]))


def final_fusion_boost(doc: dict[str, Any], query: str) -> float:
    text = doc.get("text", "")
    boost = query_term_coverage_boost(doc, query)
    exact_ids = exact_query_ids_from_tokens(tokenize(expand_query_text(query)))
    if exact_ids:
        exact_matches = exact_id_match_count(doc, exact_ids)
        if exact_matches:
            boost += 1.50 * exact_matches
        elif doc_has_any_kind(doc, {"atomic_visual", "atomic_local_meta", "atomic_global_meta", "raw_observation", "relation"}):
            boost -= 0.10
    if doc_has_kind(doc, "relation") and any(
        anchor in query for anchor in ["어떤 영상", "녹음본", "매칭", "연결", "보고 말", "어느 원본", "가리키"]
    ):
        if any(
            anchor in text
            for anchor in [
                "relation",
                "supports",
                "refers_to",
                "target_id",
                "source_id",
                "강의영상",
                "보고 말",
                "보조 녹음",
            ]
        ):
            boost += 0.80
    if "global_meta" in query and doc_has_kind(doc, "atomic_global_meta"):
        boost += 0.12
    if "local_meta" in query and doc_has_kind(doc, "atomic_local_meta"):
        boost += 0.12
    if any(anchor in query for anchor in ["최신", "정정", "최근", "취소", "변경"]):
        if any(anchor in text for anchor in ["정정", "취소", "변경"]):
            if doc_has_any_kind(doc, {"raw", "raw_chunk", "raw_observation", "hierarchical_anchor"}):
                boost += 0.04
            if doc_has_kind(doc, "generic_memory_atom"):
                boost -= 0.01
    return boost


def doc_has_kind(doc: dict[str, Any], kind: str) -> bool:
    if doc.get("kind") == kind:
        return True
    rrf_kinds = doc.get("metadata", {}).get("rrf_kinds", [])
    return isinstance(rrf_kinds, list) and kind in rrf_kinds


def doc_has_any_kind(doc: dict[str, Any], kinds: set[str]) -> bool:
    return any(doc_has_kind(doc, kind) for kind in kinds)


def query_term_coverage_boost(doc: dict[str, Any], query: str) -> float:
    text = doc.get("text", "")
    text_lower = text.lower()
    query_tokens = [
        token
        for token in unique_preserve_order(tokenize(expand_query_text(query)))
        if len(token) >= 2 and token not in STOP_TOKENS
    ]
    if not text or not query_tokens:
        return 0.0
    matched = sum(1 for token in query_tokens if token in text_lower)
    coverage_ratio = matched / max(1, len(query_tokens))
    boost = min(0.18, matched * 0.02)
    boost += (coverage_ratio ** 2) * 0.35
    if matched == len(query_tokens):
        boost += 0.20
    elif coverage_ratio < 0.55:
        boost -= 0.03
    if doc.get("kind") in {"raw", "raw_chunk", "raw_observation", "hierarchical_anchor"} and matched >= 3:
        boost += 0.02
    if doc.get("kind") == "generic_memory_atom" and matched <= 2:
        boost -= 0.005
    return boost


def default_synthetic_contracts() -> list[dict[str, Any]]:
    return [
        {
            "contract_id": "domain_messenger",
            "query": "검토기관이 장치 인증토큰을 빼서 메신저 로그인한 기록",
            "required": ["인증토큰", "메신저", "로그인"],
        },
        {
            "contract_id": "domain_passcode_repeated_attempt",
            "query": "장치 비밀번호 반복시도 순차 대입 구형 기기 요건",
            "required": ["구형 장치", "네 자리 비밀번호", "순차 대입"],
        },
        {
            "contract_id": "course_all_problems",
            "query": "회로이론 문제 전부 VOC RTH",
            "required": ["문제 A", "VOC", "RTH"],
        },
        {
            "contract_id": "course_problem_explanation",
            "query": "VOC RTH 문제 설명 그대로",
            "required": ["독립전원을 끄고", "입력 단자", "등가저항"],
        },
        {
            "contract_id": "course_exam_scope",
            "query": "회로이론 중간고사 시험범위",
            "required": ["1주차부터 6주차", "테브난 등가회로", "중첩정리"],
        },
        {
            "contract_id": "course_exam_date",
            "query": "회로이론 중간고사 언제 시험일",
            "required": ["2026년 4월 22일", "수요일"],
        },
        {
            "contract_id": "course_circuit_image",
            "query": "회로 사진 VOC RTH 크롭 이미지",
            "required": ["voc_rth_crop.png", "테브난 등가회로"],
        },
        {
            "contract_id": "course_professor_important",
            "query": "교수가 중요하다고 말한 것 시험에 낸다",
            "required": ["시험에 그대로 응용", "반드시 표시"],
        },
        {
            "contract_id": "course_problem_tendency",
            "query": "교수 출제 성향 판단",
            "required": ["단순 암기", "회로를 변형", "등가회로"],
        },
        {
            "contract_id": "course_recording_relation",
            "query": "6주차 녹음본 어떤 영상 자료와 연결",
            "required": ["GDrive 6주차 녹음본", "회로이론 6주차 강의영상", "보조 녹음"],
            "same_hit": True,
        },
    ]


def extract_anchor_terms(text: str, max_terms: int = 3, min_len: int = 2) -> list[str]:
    anchors: list[str] = []
    for match in TOKEN_RE.finditer(text or ""):
        token = match.group(0)
        if len(token) < min_len:
            continue
        if token.lower() in STOP_TOKENS:
            continue
        anchors.append(token)
    return unique_preserve_order(anchors)[:max_terms]


def derive_audit_contracts(pack: dict[str, Any], max_per_kind: int = 5) -> list[dict[str, Any]]:
    """Create generic self-audit contracts from structured pack facts.

    These contracts do not replace evaluator contracts; they broaden the local
    audit surface so the selected retriever must recover structured problems,
    visuals, relations, metadata, and source provenance from the pack itself.
    """
    sources = pack.get("sources", [])
    source_by_id = {source.get("source_id"): source for source in sources}
    contracts: list[dict[str, Any]] = []

    source_contract_count = 0
    for source in sources:
        source_id = source.get("source_id") or source.get("id") or ""
        title = str(source.get("title") or "")
        raw_ref = str(source.get("raw_ref") or source.get("original_path") or source.get("path") or "")
        raw_text = str(source.get("raw_text") or source.get("text") or "")
        if is_aggregate_artifact_ref(raw_ref):
            continue
        if source_contract_count >= max_per_kind:
            break
        source_contract_count += 1
        atom_required = [anchor for anchor in ["generic_memory_atom", source_id, raw_ref] if anchor]
        atom_required.extend(relation_anchor_terms(raw_text, max_terms=2))
        if source_id and raw_text and len(atom_required) >= 3:
            contracts.append(
                {
                    "contract_id": f"derived_atom::{source_id}",
                    "query": f"generic_memory_atom raw span {title} {source_id}",
                    "required": unique_preserve_order(atom_required)[:5],
                    "same_hit": True,
                }
            )
        required_any = [[anchor] for anchor in [source_id, raw_ref] if anchor]
        if not source_id or not required_any:
            continue
        contracts.append(
            {
                "contract_id": f"derived_source::{source_id}",
                "query": f"source provenance {title} {source_id}",
                "required_any": required_any,
            }
        )

    for problem in pack.get("problems", [])[:max_per_kind]:
        problem_id = problem.get("problem_id") or ""
        question_text = str(problem.get("question_text") or "")
        explanation = str(problem.get("official_explanation") or "")
        local_meta = problem.get("local_meta") or []
        if isinstance(local_meta, str):
            local_meta_text = local_meta
        else:
            local_meta_text = " ".join(str(item) for item in local_meta)
        required = extract_anchor_terms(question_text, max_terms=2)
        required.extend(extract_anchor_terms(explanation or local_meta_text, max_terms=2))
        required = unique_preserve_order(required)[:4]
        if not problem_id or len(required) < 2:
            continue
        contracts.append(
            {
                "contract_id": f"derived_problem::{problem_id}",
                "query": f"문제 원문 해설 {question_text[:120]}",
                "required": required,
                "same_hit": True,
            }
        )

    relation_items = list(pack.get("relations", [])) + list(pack.get("recording_relations", []))
    for index, relation in enumerate(relation_items[:max_per_kind]):
        relation_id = relation.get("relation_id") or relation.get("id") or f"recording_relation_{index}"
        source_id = relation.get("source_id") or relation.get("recording_source_id") or ""
        target_id = relation.get("target_id") or relation.get("candidate_source_id") or ""
        source_title = str(relation.get("recording_title") or source_by_id.get(source_id, {}).get("title") or "")
        target_title = str(relation.get("candidate_title") or source_by_id.get(target_id, {}).get("title") or "")
        kind = str(relation.get("kind") or relation.get("use_policy") or "")
        required = [anchor for anchor in [source_title, target_title] if anchor]
        if kind:
            required.append(kind)
        if len(required) < 2:
            continue
        contracts.append(
            {
                "contract_id": f"derived_relation::{relation_id}",
                "query": f"자료 관계 매칭 {source_title} {target_title} {kind}",
                "required": required[:3],
                "same_hit": True,
            }
        )

    for visual in pack.get("visual_atoms", [])[:max_per_kind]:
        visual_id = visual.get("visual_atom_id") or ""
        image_path = str(visual.get("image_path") or "")
        text_hint = str(visual.get("text_hint") or "")
        basename = Path(image_path).name if image_path else ""
        required = [anchor for anchor in [visual_id, basename, *extract_anchor_terms(text_hint, max_terms=2)] if anchor]
        if not visual_id or not required:
            continue
        contracts.append(
            {
                "contract_id": f"derived_visual::{visual_id}",
                "query": f"시각 증거 이미지 회로 사진 {visual_id} {text_hint} {basename}",
                "required": required[:3],
                "same_hit": True,
            }
        )

    for meta_key in ["global_meta", "local_meta"]:
        for meta in pack.get(meta_key, [])[:max_per_kind]:
            meta_id = meta.get("meta_id") or ""
            text = str(meta.get("text") or "")
            required = extract_anchor_terms(text, max_terms=3)
            if not meta_id or len(required) < 2:
                continue
            contracts.append(
                {
                    "contract_id": f"derived_{meta_key}::{meta_id}",
                    "query": f"{meta_key} 메타 정보 {text[:120]}",
                    "required": required,
                    "same_hit": True,
                }
            )

    contracts.extend(derive_correction_audit_contracts(sources, max_per_kind=max_per_kind))

    return contracts


def derive_correction_audit_contracts(sources: list[dict[str, Any]], max_per_kind: int = 5) -> list[dict[str, Any]]:
    """Derive contrastive latest/correction contracts from raw source text.

    This is intentionally domain-neutral: if a later/source document says a
    prior statement was corrected/cancelled/changed, the audit contract should
    require the correcting fact while forbidding the older statement as a whole
    phrase. The forbidden phrase is not tokenized, because correction sentences
    often quote the obsolete anchor while negating it.
    """
    contracts: list[dict[str, Any]] = []
    for source in sources:
        if len(contracts) >= max_per_kind:
            break
        source_id = source.get("source_id") or source.get("id") or ""
        raw_ref = str(source.get("raw_ref") or source.get("original_path") or source.get("path") or "")
        raw_text = str(source.get("raw_text") or source.get("text") or "")
        if is_aggregate_artifact_ref(raw_ref):
            continue
        if not source_id or not raw_text or not has_correction_signal(raw_text):
            continue
        correction_text = extract_correction_context(raw_text)
        required = extract_anchor_terms(correction_text, max_terms=8)
        required = [
            anchor
            for anchor in required
            if not any(generic in anchor for generic in ["정정합니다", "취소합니다", "변경합니다"])
        ]
        if len(required) < 2:
            continue
        prior_sentence = find_prior_statement_for_correction(source, sources, correction_text)
        contract: dict[str, Any] = {
            "contract_id": f"derived_correction::{source_id}",
            "query": f"최신 정정 취소 변경 {source.get('title', '')} {source_id} {correction_text[:160]}",
            "required": unique_preserve_order(required)[:6],
            "top_hit": True,
        }
        if prior_sentence:
            contract["forbidden"] = [prior_sentence]
        contracts.append(contract)
    return contracts


def has_correction_signal(text: str) -> bool:
    if not text:
        return False
    return bool(
        re.search(
            r"(정정|번복|수정|"
            r"변경(합니다|했습니다|되었습니다|됐습니다|사항|내용)|"
            r"취소(합니다|했습니다)|"
            r"(앞서|이전|저번|지난|말한|언급한|안내한|공지한).{0,40}(취소|변경))",
            text,
        )
    )


def split_audit_sentences(text: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?<=[.!?。])|\n", text or "") if part.strip()]


def extract_correction_context(text: str, window_sentences: int = 3) -> str:
    sentences = split_audit_sentences(text)
    for index, sentence in enumerate(sentences):
        if has_correction_signal(sentence):
            return " ".join(sentences[index : index + window_sentences])
    return " ".join(sentences[:window_sentences])


def find_prior_statement_for_correction(
    correction_source: dict[str, Any],
    sources: list[dict[str, Any]],
    correction_text: str,
) -> str:
    correction_terms = set(extract_anchor_terms(correction_text, max_terms=16))
    correction_terms.discard("정정합니다")
    correction_terms.discard("취소합니다")
    correction_terms.discard("변경합니다")
    source_id = correction_source.get("source_id") or correction_source.get("id") or ""
    source_domain = correction_source.get("domain")
    candidates: list[tuple[int, str]] = []
    for source in sources:
        candidate_id = source.get("source_id") or source.get("id") or ""
        if candidate_id == source_id:
            continue
        if source_domain and source.get("domain") and source.get("domain") != source_domain:
            continue
        raw_text = str(source.get("raw_text") or source.get("text") or "")
        for sentence in split_audit_sentences(raw_text):
            if has_correction_signal(sentence):
                continue
            sentence_terms = set(extract_anchor_terms(sentence, max_terms=16))
            overlap = len(correction_terms & sentence_terms)
            if overlap >= 2:
                candidates.append((overlap, sentence))
    if not candidates:
        return ""
    _, sentence = sorted(candidates, key=lambda item: (-item[0], len(item[1]), item[1]))[0]
    return sentence[:180]


def default_actual_contracts() -> list[dict[str, Any]]:
    return [
        {
            "contract_id": "actual_domain_token_messenger",
            "query": "검토기관이 장치 인증토큰을 빼서 메신저 로그인한 사례",
            "required": ["인증토큰", "메신저"],
        },
        {
            "contract_id": "actual_domain_passcode_attempt_status",
            "query": "장치 비밀번호 반복시도 반복 대입 잠금해제 사례 요건 또는 부정근거",
            "required_any": [
                ["반복시도"],
                ["반복 대입"],
                ["반복", "비밀번호"],
                ["비밀번호", "시도"],
                ["phone_password_repeated_attempt"],
                ["화면잠금번호"],
                ["negative_search_report"],
                ["찾지 못했다"],
            ],
        },
        {
            "contract_id": "actual_course_exam_scope",
            "query": "회로이론 중간고사 시험범위",
            "required": ["중간고사", "거의 오늘 하는 내용까지"],
        },
        {
            "contract_id": "actual_course_exam_date",
            "query": "회로이론 중간고사 시험일 언제",
            "required_any": [["중간고사가", "그 다음 주"], ["중간고사", "다음주"]],
        },
        {
            "contract_id": "actual_course_all_problems",
            "query": "회로이론 문제 전부 Q Find HW V0 VOC RTH",
            "required_any": [["Q)", "Find"], ["VOC", "RTH"], ["H.W", "V_0"]],
        },
        {
            "contract_id": "actual_course_problem_explanation",
            "query": "VOC RTH 문제 교수 설명 그대로",
            "required_any": [["VOC", "RTH"], ["Short Circuit", "ISC"], ["Thevenin", "Norton"]],
        },
        {
            "contract_id": "actual_course_circuit_image",
            "query": "회로 사진 page image png needs_model_verification",
            "required": ["page_image", ".png", "needs_model_verification"],
        },
        {
            "contract_id": "actual_course_professor_important",
            "query": "교수가 중요하다고 말한 것 시험에 나온다 표시",
            "required_any": [
                ["중요", "시험"],
                ["중요한", "개념"],
                ["시험에", "나오지"],
                ["답안 표시", "최종 답", "단위"],
                ["나와봤자", "시험"],
            ],
        },
        {
            "contract_id": "actual_course_problem_tendency",
            "query": "교수 출제 성향 문제 스타일 솔루션 보지 말고 직접 풀기",
            "required_any": [
                ["솔루션을 보지 말고", "문제들을 풀"],
                ["눈으로만 보지 말고", "직접 손으로 풀"],
                ["출제", "성향"],
                ["문제를", "풀"],
            ],
        },
        {
            "contract_id": "actual_course_recording_relation",
            "query": "구글드라이브 녹음본 어떤 영상 자료와 매칭",
            "required_any": [
                ["GDrive", "녹음본", "강의영상"],
                ["녹음본", "강의영상", "support_mapped_source"],
                ["recording", "candidate", "source"],
            ],
            "combine_top_k": 8,
        },
    ]


def contract_passed(hits: list[dict[str, Any]], contract: dict[str, Any]) -> bool:
    required = contract.get("required", [])
    required_any = contract.get("required_any", [])
    if not required and not required_any:
        return bool(hits)
    forbidden = contract.get("forbidden", [])
    if contract.get("top_hit"):
        top_text = hits[0].get("text", "") if hits else ""
        if any(anchor in top_text for anchor in forbidden):
            return False
        if required and not all(anchor in top_text for anchor in required):
            return False
        if required_any and not any(all(anchor in top_text for anchor in anchor_group) for anchor_group in required_any):
            return False
        return bool(top_text)
    if contract.get("same_hit"):
        return any(
            all(anchor in hit.get("text", "") for anchor in required)
            and not any(anchor in hit.get("text", "") for anchor in forbidden)
            for hit in hits
        )
    combined = "\n".join(hit.get("text", "") for hit in hits[: contract.get("combine_top_k", 5)])
    if any(anchor in combined for anchor in forbidden):
        return False
    if required and not all(anchor in combined for anchor in required):
        return False
    if required_any and not any(all(anchor in combined for anchor in anchor_group) for anchor_group in required_any):
        return False
    return True


def evaluate_variant(
    pack: dict[str, Any],
    variant: str,
    contracts: list[dict[str, Any]],
    indexes: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    indexes = indexes or build_indexes(pack)
    contract_results: dict[str, Any] = {}
    passed_count = 0
    for contract in contracts:
        hits = search_indexes(indexes, variant, contract["query"], top_k=contract.get("top_k", 8))
        passed = contract_passed(hits, contract)
        passed_count += int(passed)
        contract_results[contract["contract_id"]] = {
            "passed": passed,
            "query": contract["query"],
            "required": contract.get("required", []),
            "required_any": contract.get("required_any", []),
            "hits": hits,
        }
    return {
        "variant": variant,
        "passed_count": passed_count,
        "total_count": len(contracts),
        "contracts": contract_results,
    }


def run_coverage_patch(
    pack: dict[str, Any],
    base_variant: str,
    contracts: list[dict[str, Any]],
    indexes: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    indexes = indexes or build_indexes(pack)
    base = evaluate_variant(pack, base_variant, contracts, indexes=indexes)
    rerun_contract_ids = [
        contract_id for contract_id, result in base["contracts"].items() if not result["passed"]
    ]
    patched_contracts = dict(base["contracts"])
    contracts_by_id = {contract["contract_id"]: contract for contract in contracts}
    for contract_id in rerun_contract_ids:
        contract = contracts_by_id[contract_id]
        previous_result = patched_contracts[contract_id]
        missing_anchors = missing_contract_anchors(previous_result.get("hits", []), contract)
        patch_query = build_iterative_patch_query(contract, missing_anchors)
        top_k = max(int(contract.get("top_k", 12)), int(contract.get("coverage_top_k", 12)))
        hits = search_indexes(indexes, "coverage_patch", patch_query, top_k=top_k)
        patched_contracts[contract_id] = {
            "passed": contract_passed(hits, contract),
            "query": contract["query"],
            "patch_query": patch_query,
            "required": contract.get("required", []),
            "required_any": contract.get("required_any", []),
            "missing_anchors": missing_anchors,
            "hits": hits,
            "patched_from": base_variant,
        }
    passed_count = sum(1 for result in patched_contracts.values() if result["passed"])
    return {
        "variant": f"coverage_patch_from_{base_variant}",
        "rerun_contract_ids": rerun_contract_ids,
        "passed_count": passed_count,
        "total_count": len(contracts),
        "contracts": patched_contracts,
    }


def run_iterative_coverage_loop(
    pack: dict[str, Any],
    base_variant: str,
    contracts: list[dict[str, Any]],
    indexes: dict[str, dict[str, Any]] | None = None,
    max_rounds: int = 3,
) -> dict[str, Any]:
    """Patch only still-missing contracts and remove passed ones each round."""
    indexes = indexes or build_indexes(pack)
    base = evaluate_variant(pack, base_variant, contracts, indexes=indexes)
    contracts_by_id = {contract["contract_id"]: contract for contract in contracts}
    patched_contracts = dict(base["contracts"])
    remaining = [
        contract_id
        for contract_id, result in base["contracts"].items()
        if not result["passed"]
    ]
    initial_missing = list(remaining)
    rerun_contract_ids: list[str] = []
    rerun_seen: set[str] = set()
    rounds: list[dict[str, Any]] = []
    stopped_reason = "all_passed" if not remaining else "max_rounds"

    for round_index in range(max_rounds):
        if not remaining:
            stopped_reason = "all_passed"
            break
        input_contract_ids = list(remaining)
        next_remaining: list[str] = []
        newly_passed: list[str] = []
        round_contracts: dict[str, Any] = {}

        for contract_id in input_contract_ids:
            contract = contracts_by_id[contract_id]
            previous_result = patched_contracts[contract_id]
            missing_anchors = missing_contract_anchors(previous_result.get("hits", []), contract)
            patch_query = build_iterative_patch_query(contract, missing_anchors)
            top_k = max(int(contract.get("top_k", 8)), int(contract.get("coverage_top_k", 16)))
            hits = search_indexes(indexes, "coverage_patch", patch_query, top_k=top_k)
            passed = contract_passed(hits, contract)
            result = {
                "passed": passed,
                "query": contract["query"],
                "patch_query": patch_query,
                "required": contract.get("required", []),
                "required_any": contract.get("required_any", []),
                "missing_anchors": missing_anchors,
                "hits": hits,
                "patched_from": base_variant,
                "patch_round": round_index + 1,
            }
            patched_contracts[contract_id] = result
            round_contracts[contract_id] = result
            if contract_id not in rerun_seen:
                rerun_seen.add(contract_id)
                rerun_contract_ids.append(contract_id)
            if passed:
                newly_passed.append(contract_id)
            else:
                next_remaining.append(contract_id)

        rounds.append(
            {
                "round_index": round_index + 1,
                "input_contract_ids": input_contract_ids,
                "newly_passed_contract_ids": newly_passed,
                "remaining_contract_ids": next_remaining,
                "contracts": round_contracts,
            }
        )
        if next_remaining == input_contract_ids and not newly_passed:
            remaining = next_remaining
            stopped_reason = "no_progress"
            break
        remaining = next_remaining

    passed_count = sum(1 for result in patched_contracts.values() if result["passed"])
    return {
        "variant": f"iterative_coverage_loop_from_{base_variant}",
        "initial_missing_contract_ids": initial_missing,
        "rerun_contract_ids": rerun_contract_ids,
        "remaining_contract_ids": remaining,
        "stopped_reason": stopped_reason,
        "rounds": rounds,
        "passed_count": passed_count,
        "total_count": len(contracts),
        "contracts": patched_contracts,
    }


def missing_contract_anchors(hits: list[dict[str, Any]], contract: dict[str, Any]) -> list[str]:
    combined = "\n".join(hit.get("text", "") for hit in hits[: contract.get("combine_top_k", 5)])
    missing = [anchor for anchor in contract.get("required", []) if anchor not in combined]
    required_any = contract.get("required_any", [])
    if required_any and not any(all(anchor in combined for anchor in group) for group in required_any):
        best_group = max(
            required_any,
            key=lambda group: sum(1 for anchor in group if anchor in combined),
            default=[],
        )
        missing.extend(anchor for anchor in best_group if anchor not in combined)
    return unique_preserve_order(missing)


def build_iterative_patch_query(contract: dict[str, Any], missing_anchors: list[str]) -> str:
    return " ".join(
        part
        for part in [
            contract.get("query", ""),
            " ".join(missing_anchors),
        ]
        if part
    )


def run_fixture_experiment(
    pack: dict[str, Any] | None = None,
    out_dir: str | Path | None = None,
    contracts: list[dict[str, Any]] | None = None,
    slim_only: bool = False,
) -> dict[str, Any]:
    pack = pack or {"sources": []}
    contracts = contracts or default_synthetic_contracts()
    indexes = build_indexes(pack)
    variants = ["summary_only", "raw_leaf", "graph_relation", "atomic_kag", "coverage_patch", "ultimate_rrf"]
    results = {variant: evaluate_variant(pack, variant, contracts, indexes=indexes) for variant in variants if variant != "coverage_patch"}
    results["coverage_patch"] = run_coverage_patch(pack, "raw_leaf", contracts, indexes=indexes)
    results["iterative_coverage_loop"] = run_iterative_coverage_loop(pack, "raw_leaf", contracts, indexes=indexes)
    selected_variant = select_variant_key(results)
    report = {
        "selected_variant": selected_variant,
        "contracts": [contract["contract_id"] for contract in contracts],
        "variants": results,
    }
    if out_dir:
        write_report(report, Path(out_dir), slim_only=slim_only)
    return report


def select_variant_key(results: dict[str, dict[str, Any]]) -> str:
    """Select a report key, not an internal patch result name.

    Patch-style variants keep provenance in their inner `variant` field
    (`coverage_patch_from_raw_leaf`, `iterative_coverage_loop_from_raw_leaf`),
    but report consumers need a key that exists in `report["variants"]`.
    """
    best_count = max(result["passed_count"] for result in results.values())
    if results.get("ultimate_rrf", {}).get("passed_count") == best_count:
        return "ultimate_rrf"
    return max(
        results.items(),
        key=lambda item: (
            item[1]["passed_count"],
            item[0] == "coverage_patch",
            item[0] == "iterative_coverage_loop",
            item[0],
        ),
    )[0]


def write_report(report: dict[str, Any], out_dir: Path, slim_only: bool = False) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if not slim_only:
        (out_dir / "retrieval_variant_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    (out_dir / "retrieval_variant_report.slim.json").write_text(
        json.dumps(make_slim_report(report), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    lines = ["# Generic Memory Retrieval Variant Report", ""]
    lines.append(f"- selected_variant: `{report['selected_variant']}`")
    lines.append("")
    for variant, result in report["variants"].items():
        lines.append(f"## {variant}")
        lines.append(f"- passed: {result['passed_count']} / {result['total_count']}")
        for contract_id, contract_result in result["contracts"].items():
            status = "PASS" if contract_result["passed"] else "FAIL"
            lines.append(f"- {status}: {contract_id}")
        lines.append("")
    (out_dir / "RETRIEVAL_VARIANT_REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def make_slim_report(report: dict[str, Any]) -> dict[str, Any]:
    slim = {
        "selected_variant": report.get("selected_variant"),
        "contracts": report.get("contracts", []),
        "variants": {},
    }
    for variant, result in report.get("variants", {}).items():
        slim_contracts: dict[str, Any] = {}
        for contract_id, contract_result in result.get("contracts", {}).items():
            slim_hits = []
            for hit in contract_result.get("hits", []):
                slim_hit = {key: strip_slim_payload(value) for key, value in hit.items() if key != "text"}
                slim_hit["text_chars"] = len(hit.get("text", ""))
                slim_hits.append(slim_hit)
            slim_contracts[contract_id] = {
                **{key: value for key, value in contract_result.items() if key != "hits"},
                "hits": slim_hits,
            }
        slim["variants"][variant] = {
            **{key: value for key, value in result.items() if key != "contracts"},
            "contracts": slim_contracts,
        }
    return slim


def strip_slim_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: strip_slim_payload(item)
            for key, item in value.items()
            if key not in SLIM_PAYLOAD_KEYS
        }
    if isinstance(value, list):
        return [strip_slim_payload(item) for item in value]
    return value


def load_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_course_pack_docs(path: str | Path) -> dict[str, Any]:
    data = load_json(path)
    if isinstance(data, dict):
        return data
    raise ValueError(f"course pack must be a JSON object: {path}")


def load_domain_for_eval_docs(path: str | Path) -> dict[str, Any]:
    base = Path(path)
    sources: list[dict[str, Any]] = []
    for name in ["candidate_records.json", "selected.json", "evidence_ledger.json"]:
        file_path = base / name
        if not file_path.exists():
            continue
        data = load_json(file_path)
        records = data if isinstance(data, list) else data.get("records") or data.get("items") or data.get("cases") or data.get("spans") or []
        if isinstance(records, dict):
            records = list(records.values())
        for index, record in enumerate(records[:1000]):
            if not isinstance(record, dict):
                continue
            text_parts = [
                record.get("case_name"),
                record.get("case_number") or record.get("case_no"),
                record.get("title"),
                record.get("summary"),
                record.get("reason"),
                record.get("text"),
                record.get("body"),
                record.get("excerpt"),
                record.get("supporting_text"),
            ]
            for key in ["snippets", "evidence_spans", "matched_terms", "verified_aspects", "aspect_verifications"]:
                value = record.get(key)
                if isinstance(value, list):
                    text_parts.extend(json.dumps(item, ensure_ascii=False) if isinstance(item, (dict, list)) else str(item) for item in value)
                elif isinstance(value, dict):
                    text_parts.append(json.dumps(value, ensure_ascii=False))
            raw_text = "\n".join(str(part) for part in text_parts if part)
            if raw_text:
                sources.append(
                    {
                        "source_id": f"domain_actual_{name}_{index}",
                        "domain": "domain",
                        "kind": "record",
                        "title": str(record.get("case_name") or record.get("title") or name),
                        "summary": str(record.get("summary") or record.get("holding") or ""),
                        "raw_text": raw_text,
                        "raw_ref": str(file_path),
                    }
                )
    for name in ["coverage_report.json", "evaluation.json"]:
        file_path = base / name
        if not file_path.exists():
            continue
        text = file_path.read_text(encoding="utf-8")
        sources.append(
            {
                "source_id": f"domain_actual_{name}",
                "domain": "domain",
                "kind": "evaluation_artifact",
                "title": name,
                "summary": name,
                "raw_text": text[:500000],
                "raw_ref": str(file_path),
            }
        )
    return {"sources": sources}


def merge_packs(*packs: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "sources": [],
        "relations": [],
        "recording_relations": [],
        "observations": [],
        "global_meta": [],
        "local_meta": [],
        "problems": [],
        "visual_atoms": [],
    }
    for pack in packs:
        for key in merged:
            value = pack.get(key, [])
            if isinstance(value, list):
                merged[key].extend(value)
    return merged


def run_actual_experiment(
    course_pack: str | None,
    domain_for_eval: str | None,
    out_dir: str | Path,
    append_derived_contracts: bool = False,
    derived_max_per_kind: int = 5,
    slim_only: bool = False,
) -> dict[str, Any]:
    packs = []
    if course_pack:
        packs.append(load_course_pack_docs(course_pack))
    if domain_for_eval:
        domain_pack = load_domain_for_eval_docs(domain_for_eval)
        if not domain_pack.get("sources"):
            raise ValueError(f"no usable domain for_eval artifacts: {domain_for_eval}")
        packs.append(domain_pack)
    merged_pack = merge_packs(*packs)
    contracts = default_actual_contracts()
    if append_derived_contracts:
        contracts = contracts + derive_audit_contracts(merged_pack, max_per_kind=derived_max_per_kind)
    report = run_fixture_experiment(merged_pack, out_dir=out_dir, contracts=contracts, slim_only=slim_only)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--course-pack")
    parser.add_argument("--domain-for-eval")
    parser.add_argument("--out-dir", default="tmp/generic_memory_retrieval_variants_20260412/fixture")
    parser.add_argument("--append-derived-contracts", action="store_true")
    parser.add_argument("--derived-max-per-kind", type=int, default=5)
    parser.add_argument("--slim-only", action="store_true")
    args = parser.parse_args()

    if args.course_pack or args.domain_for_eval:
        report = run_actual_experiment(
            args.course_pack,
            args.domain_for_eval,
            args.out_dir,
            append_derived_contracts=args.append_derived_contracts,
            derived_max_per_kind=args.derived_max_per_kind,
            slim_only=args.slim_only,
        )
    else:
        report = run_fixture_experiment(out_dir=args.out_dir, slim_only=args.slim_only)
    print(json.dumps({"selected_variant": report["selected_variant"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
