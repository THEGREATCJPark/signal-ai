#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?。！？다])\s+|\n+")


@dataclass(frozen=True)
class RoleRule:
    name: str
    terms: tuple[str, ...]


@dataclass(frozen=True)
class AspectProfile:
    name: str
    event_roles: tuple[RoleRule, ...]
    conclusion_roles: tuple[RoleRule, ...]
    direct_conclusion_terms: tuple[str, ...]
    allegation_markers: tuple[str, ...]
    procedural_markers: tuple[str, ...]
    window_chars: int = 420


def _as_terms(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(str(item) for item in value if str(item).strip())


def load_profile(path: Path) -> dict[str, AspectProfile]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    aspects: dict[str, AspectProfile] = {}
    for raw in payload.get("aspects", []):
        event_roles = tuple(
            RoleRule(str(role["name"]), _as_terms(role.get("terms", [])))
            for role in raw.get("event_roles", [])
        )
        conclusion_roles = tuple(
            RoleRule(str(role["name"]), _as_terms(role.get("terms", [])))
            for role in raw.get("conclusion_roles", raw.get("event_roles", []))
        )
        aspects[str(raw["name"])] = AspectProfile(
            name=str(raw["name"]),
            event_roles=event_roles,
            conclusion_roles=conclusion_roles,
            direct_conclusion_terms=_as_terms(raw.get("direct_conclusion_terms", [])),
            allegation_markers=_as_terms(raw.get("allegation_markers", [])),
            procedural_markers=_as_terms(raw.get("procedural_markers", [])),
            window_chars=int(raw.get("window_chars", 420)),
        )
    return aspects


def iter_jsonl_records(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            record.setdefault("source_id", f"{path.name}::line:{line_no}")
            yield record


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]


def split_windows(text: str, *, window_chars: int) -> list[tuple[int, int, str]]:
    windows: list[tuple[int, int, str]] = []
    cursor = 0
    for sentence in SENTENCE_SPLIT_RE.split(text):
        sentence = sentence.strip()
        if not sentence:
            continue
        start = text.find(sentence, cursor)
        if start < 0:
            start = cursor
        end = start + len(sentence)
        cursor = end
        if len(sentence) <= window_chars:
            windows.append((start, end, sentence))
            continue
        step = max(80, window_chars // 2)
        for offset in range(0, len(sentence), step):
            chunk = sentence[offset : offset + window_chars]
            if chunk:
                windows.append((start + offset, start + offset + len(chunk), chunk))
    return windows


def role_hits(text: str, roles: tuple[RoleRule, ...]) -> dict[str, list[str]]:
    lowered = text.lower()
    hits: dict[str, list[str]] = {}
    for role in roles:
        matched = [term for term in role.terms if term.lower() in lowered]
        if matched:
            hits[role.name] = matched
    return hits


def has_all_roles(hits: dict[str, list[str]], roles: tuple[RoleRule, ...]) -> bool:
    return all(role.name in hits for role in roles)


def contains_any(text: str, terms: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(term.lower() in lowered for term in terms)


def classify_conclusion_window(text: str, aspect: AspectProfile) -> str:
    if contains_any(text, aspect.allegation_markers):
        return "allegation"
    if contains_any(text, aspect.procedural_markers):
        return "procedural"
    if contains_any(text, aspect.direct_conclusion_terms):
        return "direct"
    return "none"


def span_payload(
    *,
    record: dict[str, Any],
    start: int,
    end: int,
    text: str,
    hits: dict[str, list[str]],
    label: str,
) -> dict[str, Any]:
    body = str(record.get("text", ""))
    return {
        "source_id": str(record.get("source_id")),
        "record_hash": content_hash(body),
        "span_start": start,
        "span_end": end,
        "label": label,
        "role_hits": hits,
        "text": text,
    }


def audit_record(record: dict[str, Any], aspects: dict[str, AspectProfile]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    text = str(record.get("text", ""))
    findings: dict[str, dict[str, list[dict[str, Any]]]] = {
        name: {
            "event_spans": [],
            "direct_method_conclusion_spans": [],
            "allegation_spans": [],
            "procedural_spans": [],
            "near_miss_spans": [],
        }
        for name in aspects
    }
    for aspect_name, aspect in aspects.items():
        for start, end, window in split_windows(text, window_chars=aspect.window_chars):
            event_hits = role_hits(window, aspect.event_roles)
            conclusion_hits = role_hits(window, aspect.conclusion_roles)
            if has_all_roles(event_hits, aspect.event_roles):
                findings[aspect_name]["event_spans"].append(
                    span_payload(record=record, start=start, end=end, text=window, hits=event_hits, label="event")
                )
            elif event_hits:
                findings[aspect_name]["near_miss_spans"].append(
                    span_payload(record=record, start=start, end=end, text=window, hits=event_hits, label="near_miss")
                )
            if has_all_roles(conclusion_hits, aspect.conclusion_roles):
                kind = classify_conclusion_window(window, aspect)
                if kind == "direct":
                    findings[aspect_name]["direct_method_conclusion_spans"].append(
                        span_payload(record=record, start=start, end=end, text=window, hits=conclusion_hits, label="direct")
                    )
                elif kind == "allegation":
                    findings[aspect_name]["allegation_spans"].append(
                        span_payload(record=record, start=start, end=end, text=window, hits=conclusion_hits, label="allegation")
                    )
                elif kind == "procedural":
                    findings[aspect_name]["procedural_spans"].append(
                        span_payload(record=record, start=start, end=end, text=window, hits=conclusion_hits, label="procedural")
                    )
    return findings


def merge_findings(target: dict[str, Any], record_findings: dict[str, dict[str, list[dict[str, Any]]]]) -> None:
    for aspect, groups in record_findings.items():
        slot = target.setdefault(
            aspect,
            {
                "event_spans": [],
                "direct_method_conclusion_spans": [],
                "allegation_spans": [],
                "procedural_spans": [],
                "near_miss_spans": [],
            },
        )
        for key, spans in groups.items():
            slot[key].extend(spans)


def summarize_aspect(aspect: str, groups: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    event_count = len(groups.get("event_spans", []))
    direct_count = len(groups.get("direct_method_conclusion_spans", []))
    allegation_count = len(groups.get("allegation_spans", []))
    procedural_count = len(groups.get("procedural_spans", []))
    if event_count and direct_count:
        status = "grounded_answer"
    elif event_count:
        status = "event_found_direct_conclusion_missing"
    elif groups.get("near_miss_spans"):
        status = "near_miss_only"
    else:
        status = "not_found"
    return {
        "aspect": aspect,
        "status": status,
        "answer_pass": status == "grounded_answer",
        "event_count": event_count,
        "direct_conclusion_count": direct_count,
        "allegation_count": allegation_count,
        "procedural_count": procedural_count,
        "near_miss_count": len(groups.get("near_miss_spans", [])),
    }


def trim_groups(groups: dict[str, list[dict[str, Any]]], *, limit: int) -> dict[str, list[dict[str, Any]]]:
    return {key: spans[:limit] for key, spans in groups.items()}


def run_audit(profile_path: Path, records_path: Path, out_dir: Path, *, span_limit: int = 20) -> dict[str, Any]:
    aspects = load_profile(profile_path)
    findings: dict[str, Any] = {}
    records_seen = 0
    for record in iter_jsonl_records(records_path):
        records_seen += 1
        merge_findings(findings, audit_record(record, aspects))
    summary = {
        "records_seen": records_seen,
        "aspects": [summarize_aspect(aspect, groups) for aspect, groups in sorted(findings.items())],
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    slim_findings = {aspect: trim_groups(groups, limit=span_limit) for aspect, groups in findings.items()}
    (out_dir / "findings.json").write_text(json.dumps(slim_findings, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Config-driven source-grounded method audit scanner")
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--records", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--span-limit", default=20, type=int)
    args = parser.parse_args()
    summary = run_audit(args.profile, args.records, args.out, span_limit=args.span_limit)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
