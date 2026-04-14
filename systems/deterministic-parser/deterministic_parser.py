#!/usr/bin/env python3
"""Extract course exam-scope evidence from local workspace artifacts only.

This tool intentionally does not call an LLM. It reports explicit evidence when
the files contain it, and leaves a course unconfirmed otherwise.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TEXT_EXTENSIONS = {".md", ".txt", ".json", ".jsonl"}
MAX_TEXT_FILE_BYTES = 2_000_000
DEFAULT_ROOTS = ("document_artifacts", "lms_downloads")
SKIP_DIRS = {
    ".git",
    "__pycache__",
    "node_modules",
    "chunk_logs",
    "assets",
    "lazy_bundle_sessions",
    "youtube_downloads",
    "screenshots",
    "frames",
    "pages",
    "crops",
    "llm_cache",
}

COURSE_NAME_OVERRIDES = {
    "288799": "인체생리학_(MBE2003.01-00)",
    "288800": "회로이론및실습_(MBE2004.01-00)",
    "288804": "바이오공학_(MBE2020.01-00)",
    "288805": "기초실험(1)_(MBE2022.01-00)",
    "292139": "채플_(YHA1002.03-00)",
    "292429": "한국전통문화의유산_(YHG1007.01-00)",
    "292738": "컴퓨터프로그래밍_(YHX1009.07-00)",
    "292811": "진로지도_의공학_(YHZ1001.66-00)",
}

COURSE_PART_RE = re.compile(r"^(?P<id>\d{6})_(?P<name>[^/\\]+)$")
LEARNUS_COURSE_RE = re.compile(r"^course_(?P<id>\d{6})(?:_|$)")
ANY_COURSE_RE = re.compile(r"(?:^|[_/\\])(?P<id>\d{6})(?:[_/\\]|$)")
WEEK_RE = re.compile(r"(?:^|[_/\\-])week\s*0?(?P<week>\d{1,2})(?:[_/\\.-]|$)", re.I)
KOREAN_LESSON_RE = re.compile(r"(?P<week>\d{1,2})\s*강")

STRUCTURED_FIELD_RES = {
    "studyExamTarget": re.compile(r'"studyExamTarget"\s*:\s*"(?P<value>[^"]*)"'),
    "studyExamRangeLabel": re.compile(r'"studyExamRangeLabel"\s*:\s*"(?P<value>[^"]*)"'),
    "studyExamRangeConfirmed": re.compile(r'"studyExamRangeConfirmed"\s*:\s*(?P<value>true|false)', re.I),
}

EVIDENCE_PATTERNS = [
    (
        "explicit_range",
        re.compile(
            r"(시험\s*범위|중간고사\s*범위|기말고사\s*범위|(?:중간|기말|시험).{0,20}범위|범위.{0,20}(?:중간|기말|시험))",
            re.I,
        ),
    ),
    (
        "out_of_scope",
        re.compile(r"(범위에\s*해당하지\s*않|범위가\s*아님|범위\s*밖)", re.I),
    ),
    (
        "no_exam",
        re.compile(r"(시험도\s*없|시험\s*없|시험\s*위주의\s*과목과는.{0,30}다름|pass\s*/\s*np|p\s*/\s*np)", re.I),
    ),
    (
        "assessment",
        re.compile(r"((평가|성적).{0,80}(중간|기말|시험|퀴즈)|(?:중간|기말|중간/기말고사).{0,40}\d+\s*%)", re.I),
    ),
    (
        "exam_schedule",
        re.compile(r"(중간고사\s*기간|학기말\s*시험|기말\s*시험|중간고사|기말고사|midterm\s+exam|final\s+exam|exam\s+period)", re.I),
    ),
    (
        "exam_hint",
        re.compile(r"(출제|시험\s*문제|시험에\s*나오는|시험\s*대비|exam_hint|exam_frequent|문제\s*패턴)", re.I),
    ),
]

CLAUSE_SPLIT_RE = re.compile(r"[,;，；]|(?:\s+(?:그리고|하지만|다만|또는)\s+)")
EXAM_EXCLUSION_RE = re.compile(
    r"(?P<target>.+?)(?:은|는|이|가|도|을|를)?\s*"
    r"(?:시험(?:에|에는)?\s*)?"
    r"(?P<marker>안\s*나(?:오(?:는|고|지|도록|면|음|습니다|다)?|옴)"
    r"|나오지\s*않(?:습니다|음|는다|다)?"
    r"|출제(?:되지|하지)\s*않(?:습니다|음|는다|다)?"
    r"|시험\s*제외|출제\s*제외)"
)
EXAM_INCLUSION_RE = re.compile(
    r"(?P<target>.+?)(?:은|는|이|가|도|을|를)?\s*"
    r"(?:시험(?:에|에는)?\s*)?"
    r"(?P<marker>출제(?:됩니다|된다|됨|예정|될|합니다|한다)?"
    r"|나(?:옵니다|온다|옴|오는))"
)
COURSE_SCOPE_EXCLUSION_RE = re.compile(
    r"(?P<target>.+?)(?:은|는|이|가|도|을|를)\s+"
    r"(?P<context>.{0,80}?범위에\s*해당하지\s*않(?:습니다|음|는다|다)?)"
)
COURSE_SCOPE_OUTSIDE_RE = re.compile(
    r"(?P<target>.+?)(?:은|는|이|가|도|을|를)\s+"
    r"(?P<context>범위\s*밖|범위가\s*아님)"
)
POST_MARKER_EXAM_EXCLUSION_RE = re.compile(
    r"(?:시험(?:에|에는)?\s*)?"
    r"(?:안\s*나(?:오(?:는|고|지|도록|면|음|습니다|다)?|옴)"
    r"|나오지\s*않(?:습니다|음|는다|다)?"
    r"|출제(?:되지|하지)\s*않(?:습니다|음|는다|다)?"
    r"|시험\s*제외|출제\s*제외)"
    r"(?:\s*(?:내용|부분|범위|것|항목))?\s*[:：]\s*(?P<target>.+)$"
)


@dataclass(frozen=True)
class Evidence:
    category: str
    path: str
    line: int
    text: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "path": self.path,
            "line": self.line,
            "text": self.text,
        }


def clean_line(line: str) -> str:
    return re.sub(r"\s+", " ", line).strip()


def display_line(raw_line: str) -> str:
    line = clean_line(raw_line)
    trimmed = line.rstrip(",")
    if re.match(r'^"[A-Za-z0-9_ -]+"\s*:', trimmed):
        try:
            parsed = json.loads("{" + trimmed + "}")
            value = next(iter(parsed.values()))
            if isinstance(value, str):
                return clean_line(value)
        except (json.JSONDecodeError, StopIteration):
            return line
    return line


def iter_clauses(line: str) -> list[tuple[str, int]]:
    clauses: list[tuple[str, int]] = []
    start = 0
    for match in CLAUSE_SPLIT_RE.finditer(line):
        raw = line[start : match.start()]
        stripped = raw.strip()
        if stripped:
            clauses.append((stripped, start + len(raw) - len(raw.lstrip())))
        start = match.end()
    raw = line[start:]
    stripped = raw.strip()
    if stripped:
        clauses.append((stripped, start + len(raw) - len(raw.lstrip())))
    return clauses


def clean_claim_target(raw_target: str) -> str:
    target = clean_line(raw_target)
    target = re.sub(r"^[#>*\-\s]+", "", target)
    target = re.sub(r"^\d+[.)]\s*", "", target)
    target = re.sub(r"^(?:시험|중간고사|기말고사)(?:에는|에|은|는)?\s*", "", target)
    target = re.sub(r"^(?:그리고|하지만|다만|또는)\s+", "", target)
    target = re.sub(r"\s*(?:은|는|이|가|도|을|를)$", "", target)
    return target.strip(" \"'“”‘’[]()。.;；")


def scope_claim_is_noise(claim_type: str, target_text: str, clause: str, match: re.Match[str]) -> bool:
    if claim_type == "exclude_from_exam" and "출제" not in match.group(0) and "시험" not in match.group(0):
        return True
    if claim_type != "include_in_exam":
        return False
    if "출제" not in match.group(0) and "시험" not in match.group(0):
        return True
    compact = re.sub(r"[\W_]+", "", target_text)
    if len(compact) < 4:
        return True
    if re.search(r"(문제\s*패턴|질문\s*패턴|출제자의\s*의도|출현\s*빈도|출제\s*빈도|빈도)", target_text):
        return True
    if clause.lstrip().startswith("#") and re.search(r"(패턴|출제자의\s*의도)", clause):
        return True
    return False


def build_scope_claim(
    claim_type: str,
    line: str,
    line_no: int,
    path: Path,
    base: Path,
    clause: str,
    clause_start: int,
    match: re.Match[str],
    boundary_method: str,
) -> dict[str, Any] | None:
    raw_target = match.group("target")
    target_text = clean_claim_target(raw_target)
    if not target_text or scope_claim_is_noise(claim_type, target_text, clause, match):
        return None
    raw_target_start = clause_start + match.start("target")
    target_offset = raw_target.find(target_text)
    if target_offset < 0:
        target_offset = 0
    target_char_start = raw_target_start + target_offset + 1
    target_char_end = target_char_start + len(target_text) - 1
    return {
        "claim_type": claim_type,
        "target_text": target_text,
        "claim_text": clean_line(clause),
        "source_path": relpath(path, base),
        "line_start": line_no,
        "line_end": line_no,
        "target_char_start": target_char_start,
        "target_char_end": target_char_end,
        "boundary_method": boundary_method,
        "confidence": "high",
    }


def extract_scope_claims(line: str, line_no: int, path: Path, base: Path) -> list[dict[str, Any]]:
    if len(line) > 1200 or "data:image" in line:
        return []
    claims: list[dict[str, Any]] = []
    for match in POST_MARKER_EXAM_EXCLUSION_RE.finditer(line):
        claim = build_scope_claim(
            "exclude_from_exam",
            line,
            line_no,
            path,
            base,
            line,
            0,
            match,
            "after_marker_to_line_end",
        )
        if claim:
            claims.append(claim)
    for clause, clause_start in iter_clauses(line):
        for claim_type, pattern in (
            ("exclude_from_course_scope", COURSE_SCOPE_EXCLUSION_RE),
            ("exclude_from_course_scope", COURSE_SCOPE_OUTSIDE_RE),
            ("exclude_from_exam", EXAM_EXCLUSION_RE),
        ):
            for match in pattern.finditer(clause):
                claim = build_scope_claim(
                    claim_type,
                    line,
                    line_no,
                    path,
                    base,
                    clause,
                    clause_start,
                    match,
                    "same_clause_before_marker",
                )
                if claim:
                    claims.append(claim)
        if EXAM_EXCLUSION_RE.search(clause):
            continue
        for match in EXAM_INCLUSION_RE.finditer(clause):
            claim = build_scope_claim(
                "include_in_exam",
                line,
                line_no,
                path,
                base,
                clause,
                clause_start,
                match,
                "same_clause_before_marker",
            )
            if claim:
                claims.append(claim)
    return claims


def relpath(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def course_from_path(path: Path) -> tuple[str | None, str | None]:
    parts = path.parts
    for part in parts:
        match = COURSE_PART_RE.match(part)
        if match:
            course_id = match.group("id")
            return course_id, COURSE_NAME_OVERRIDES.get(course_id, match.group("name"))
        match = LEARNUS_COURSE_RE.match(part)
        if match:
            course_id = match.group("id")
            return course_id, COURSE_NAME_OVERRIDES.get(course_id)
    joined = path.as_posix()
    match = ANY_COURSE_RE.search(joined)
    if match:
        course_id = match.group("id")
        return course_id, COURSE_NAME_OVERRIDES.get(course_id)
    return None, None


def week_from_path(path: Path) -> str | None:
    text = path.as_posix()
    match = WEEK_RE.search(text)
    if match:
        return f"week{int(match.group('week')):02d}"
    for part in path.parts:
        match = KOREAN_LESSON_RE.search(part)
        if match:
            return f"week{int(match.group('week')):02d}"
    return None


def iter_text_files(base: Path, roots: tuple[str, ...] = DEFAULT_ROOTS) -> list[Path]:
    files: list[Path] = []
    for root_name in roots:
        root = base / root_name
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if any(part in SKIP_DIRS for part in path.parts):
                continue
            if path.suffix.lower() not in TEXT_EXTENSIONS:
                continue
            try:
                if path.stat().st_size > MAX_TEXT_FILE_BYTES:
                    continue
            except OSError:
                continue
            files.append(path)
    return sorted(files)


def blank_course(course_id: str, course_name: str | None = None) -> dict[str, Any]:
    return {
        "course_id": course_id,
        "course_name": course_name or COURSE_NAME_OVERRIDES.get(course_id) or course_id,
        "available_weeks": set(),
        "state_weeks": [],
        "structured_fields": {
            "studyExamTarget": [],
            "studyExamRangeLabel": [],
            "studyExamRangeConfirmed": [],
        },
        "evidence": [],
        "scope_claims": [],
    }


def normalize_course_name(course_id: str, raw_name: str) -> str:
    name = clean_line(raw_name)
    name = re.sub(r"^(교과|비교과)\s+", "", name)
    name = name.replace(" ", "_")
    return COURSE_NAME_OVERRIDES.get(course_id, name or course_id)


def state_week_label(week_key: str) -> str | None:
    try:
        return f"week{int(week_key):02d}"
    except (TypeError, ValueError):
        return None


def item_title(item: dict[str, Any]) -> str:
    for key in ("title", "name", "activityName"):
        value = clean_line(str(item.get(key) or ""))
        if value:
            return value
    return ""


def load_state_courses(state_path: Path, term: str = "2026-1") -> dict[str, dict[str, Any]]:
    if not state_path.exists():
        return {}
    state = json.loads(state_path.read_text(encoding="utf-8"))
    courses = ((state.get("terms") or {}).get(term) or {}).get("courses") or {}
    out: dict[str, dict[str, Any]] = {}
    for course_id, raw_course in courses.items():
        course_id = str(course_id)
        raw_name = str(raw_course.get("courseName") or raw_course.get("name") or raw_course.get("title") or course_id)
        if raw_name.strip().startswith("비교과"):
            continue
        weeks = []
        for week_key, raw_week in (raw_course.get("weeks") or {}).items():
            label = state_week_label(str(week_key))
            if not label:
                continue
            items = raw_week.get("items") or []
            titles = [item_title(item) for item in items if isinstance(item, dict)]
            titles = [title for title in titles if title]
            weeks.append(
                {
                    "week": label,
                    "week_number": int(str(week_key)),
                    "item_count": len(items),
                    "titles": titles,
                }
            )
        out[course_id] = {
            "course_id": course_id,
            "course_name": normalize_course_name(course_id, raw_name),
            "state_weeks": sorted(weeks, key=lambda item: item["week_number"]),
        }
    return out


def merge_state_courses(courses: dict[str, dict[str, Any]], state_courses: dict[str, dict[str, Any]]) -> None:
    for course_id, state_course in state_courses.items():
        course = courses.setdefault(course_id, blank_course(course_id, state_course["course_name"]))
        course["course_name"] = state_course["course_name"]
        course["state_weeks"] = state_course["state_weeks"]
        for week in state_course["state_weeks"]:
            if week["item_count"] > 0:
                course["available_weeks"].add(week["week"])


def line_has_filename_noise(line: str) -> bool:
    lowered = line.lower()
    if "filename" not in lowered and "path" not in lowered:
        return False
    return bool(re.search(r"\bfinal\b", lowered)) and not re.search(r"(시험|고사|exam)", line, re.I)


def category_for_line(line: str) -> str | None:
    if len(line) > 1200 or "data:image" in line:
        return None
    if re.match(r'^"(kind|exam_frequency|exam_frequent|source)"\s*:', line):
        return None
    if line in {'"exam_hint"', '"exam_frequent"'}:
        return None
    if line_has_filename_noise(line):
        return None
    for category, pattern in EVIDENCE_PATTERNS:
        if pattern.search(line):
            return category
    return None


def update_structured_fields(fields: dict[str, list[str]], line: str) -> None:
    for key, pattern in STRUCTURED_FIELD_RES.items():
        match = pattern.search(line)
        if not match:
            continue
        value = match.group("value").strip()
        if value and value not in fields[key]:
            fields[key].append(value)


def scan_exam_scopes(base: Path, state_path: Path | None = None) -> dict[str, Any]:
    base = Path(base).resolve()
    courses: dict[str, dict[str, Any]] = {}
    scanned_files = 0
    skipped_no_course = 0

    for path in iter_text_files(base):
        course_id, course_name = course_from_path(path.relative_to(base))
        if not course_id:
            skipped_no_course += 1
            continue
        scanned_files += 1
        course = courses.setdefault(course_id, blank_course(course_id, course_name))
        if course_name and not COURSE_NAME_OVERRIDES.get(course_id):
            course["course_name"] = course_name
        week = week_from_path(path.relative_to(base))
        if week:
            course["available_weeks"].add(week)

        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for line_no, raw_line in enumerate(lines, start=1):
            update_structured_fields(course["structured_fields"], raw_line)
            line = display_line(raw_line)
            if not line:
                continue
            course["scope_claims"].extend(extract_scope_claims(line, line_no, path, base))
            category = category_for_line(line)
            if not category:
                continue
            course["evidence"].append(
                Evidence(
                    category=category,
                    path=relpath(path, base),
                    line=line_no,
                    text=line[:500],
                ).to_dict()
            )

    if state_path is None:
        default_state = base.parent / "lms_scout" / "state" / "lms_state.json"
        state_path = default_state if default_state.exists() else None
    state_path_text = ""
    if state_path:
        state_path = Path(state_path).resolve()
        state_path_text = state_path.as_posix()
        merge_state_courses(courses, load_state_courses(state_path))

    for course in courses.values():
        course["available_weeks"] = sorted(course["available_weeks"])
        course["evidence"] = dedupe_evidence(course["evidence"])
        course["scope_claims"] = dedupe_scope_claims(course["scope_claims"])
        course["confirmed_exam_range"] = confirmed_exam_range(course)
        course["candidate_exam_scope"] = candidate_exam_scope(course)
        course["status"] = course_status(course)
    return {
        "base": base.as_posix(),
        "state_path": state_path_text,
        "scanned_files": scanned_files,
        "skipped_no_course": skipped_no_course,
        "courses": dict(sorted(courses.items())),
    }


def dedupe_evidence(evidence: list[dict[str, Any]], limit_per_category: int = 25) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    counts: dict[str, int] = {}
    out: list[dict[str, Any]] = []
    for item in evidence:
        key = (item["category"], item["text"])
        if key in seen:
            continue
        seen.add(key)
        category = item["category"]
        counts[category] = counts.get(category, 0) + 1
        if counts[category] > limit_per_category:
            continue
        out.append(item)
    order = {name: idx for idx, (name, _pattern) in enumerate(EVIDENCE_PATTERNS)}
    return sorted(out, key=lambda item: (order.get(item["category"], 99), item["path"], item["line"]))


def dedupe_scope_claims(claims: list[dict[str, Any]], limit: int = 50) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for item in claims:
        key = (
            item["claim_type"],
            item["target_text"],
            item["claim_text"],
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
        if len(out) >= limit:
            break
    order = {
        "exclude_from_exam": 0,
        "include_in_exam": 1,
        "exclude_from_course_scope": 2,
    }
    return sorted(out, key=lambda item: (item["source_path"], item["line_start"], order.get(item["claim_type"], 99)))


def confirmed_exam_range(course: dict[str, Any]) -> str | None:
    fields = course["structured_fields"]
    confirmed_values = [v.lower() for v in fields.get("studyExamRangeConfirmed", [])]
    labels = [v for v in fields.get("studyExamRangeLabel", []) if v]
    targets = [v for v in fields.get("studyExamTarget", []) if v]
    if "true" in confirmed_values and (labels or targets):
        return " / ".join(labels or targets)
    for item in course["evidence"]:
        if item["category"] == "explicit_range":
            return item["text"]
    return None


def pre_midterm_state_weeks(course: dict[str, Any], max_week: int = 7) -> list[dict[str, Any]]:
    weeks = []
    for week in course.get("state_weeks") or []:
        if int(week.get("week_number") or 0) <= max_week and int(week.get("item_count") or 0) > 0:
            weeks.append(week)
    return weeks


def has_no_exam_signal(course: dict[str, Any]) -> bool:
    name = str(course.get("course_name") or "")
    if "진로지도" in name or "채플" in name:
        return True
    return any(item["category"] == "no_exam" for item in course.get("evidence") or [])


def candidate_exam_scope(course: dict[str, Any]) -> str | None:
    if course.get("confirmed_exam_range"):
        return str(course["confirmed_exam_range"])
    weeks = pre_midterm_state_weeks(course)
    if not weeks:
        return None
    parts = []
    for week in weeks:
        titles = week.get("titles") or []
        title_text = " / ".join(titles[:4]) if titles else "항목 제목 없음"
        more = int(week.get("item_count") or 0) - len(titles[:4])
        if more > 0:
            title_text += f" / +{more}개"
        parts.append(f"{week['week']}: {title_text}")
    prefix = "시험범위 대신 이수요건 후보" if has_no_exam_signal(course) else "중간고사 대비 후보 범위(확정 아님)"
    return f"{prefix}: " + "; ".join(parts)


def course_status(course: dict[str, Any]) -> str:
    if course.get("confirmed_exam_range"):
        return "confirmed_explicit_range"
    if has_no_exam_signal(course) and course.get("candidate_exam_scope"):
        return "no_written_exam_or_pnp"
    if course.get("candidate_exam_scope"):
        return "derived_pre_midterm_candidate"
    return "no_local_scope_evidence"


def render_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# Work16 시험범위 코드 전용 추출 리포트",
        "",
        "- 원칙: LLM/수동추론 없이 로컬 파일의 명시 근거만 사용.",
        "- 판정: `confirmed_explicit_range`는 직접 시험범위 문구 또는 confirmed 필드가 있을 때만 부여.",
        "- `derived_pre_midterm_candidate`는 LMS state의 중간고사 전 주차 항목 기반 후보이며 확정 범위가 아님.",
        "- 명시적 제외/포함 문구는 `scope_claims`에 대상 텍스트와 파일/라인/문자 범위로 따로 기록.",
        "- `no_written_exam_or_pnp`는 시험범위보다 이수요건/P-NP 성격이 강한 과목으로 분리.",
        f"- 스캔 파일 수: {result['scanned_files']}",
        f"- LMS state: {result.get('state_path') or '없음'}",
        "",
        "## 결론 표",
        "",
        "| 과목 | 상태 | 확정/후보 범위 | 코드가 확인한 주차 파일 |",
        "| --- | --- | --- | --- |",
    ]
    for course in result["courses"].values():
        weeks = ", ".join(course["available_weeks"]) if course["available_weeks"] else "없음"
        scope = course["confirmed_exam_range"] or course.get("candidate_exam_scope") or "직접/파생 근거 없음"
        lines.append(f"| {course['course_id']} {course['course_name']} | {course['status']} | {scope} | {weeks} |")
    lines.append("")
    for course in result["courses"].values():
        lines.extend(
            [
                f"## {course['course_id']} {course['course_name']}",
                f"- 상태: {course['status']}",
                f"- 확정 시험범위: {course['confirmed_exam_range'] or '직접 확정 근거 없음'}",
                f"- 코드 파생 후보 범위: {course.get('candidate_exam_scope') or '없음'}",
                f"- 코드가 확인한 주차 파일: {', '.join(course['available_weeks']) if course['available_weeks'] else '없음'}",
            ]
        )
        if course.get("state_weeks"):
            state_bits = []
            for week in course["state_weeks"]:
                if week["item_count"] <= 0:
                    continue
                titles = " / ".join((week.get("titles") or [])[:3])
                state_bits.append(f"{week['week']}({week['item_count']}): {titles}")
            if state_bits:
                lines.append(f"- LMS state 주차 항목: {'; '.join(state_bits)}")
        fields = course["structured_fields"]
        field_bits = []
        for key in ("studyExamTarget", "studyExamRangeLabel", "studyExamRangeConfirmed"):
            values = fields.get(key) or []
            if values:
                field_bits.append(f"{key}={', '.join(values)}")
        if field_bits:
            lines.append(f"- 구조화 필드: {'; '.join(field_bits)}")
        claims = course.get("scope_claims") or []
        if claims:
            lines.append("- 명시적 제외/포함 클레임:")
            for claim in claims[:30]:
                char_span = f"C{claim['target_char_start']}-C{claim['target_char_end']}"
                lines.append(
                    f"  - `{claim['claim_type']}` 대상=`{claim['target_text']}` "
                    f"{claim['source_path']}:{claim['line_start']}:{char_span} :: {claim['claim_text']}"
                )
            if len(claims) > 30:
                lines.append(f"  - ... {len(claims) - 30}개 추가 클레임은 JSON 참조")
        evidence = course["evidence"]
        if not evidence:
            lines.append("- 근거 라인: 없음")
            lines.append("")
            continue
        lines.append("- 근거 라인:")
        for item in evidence[:30]:
            lines.append(f"  - `{item['category']}` {item['path']}:{item['line']} :: {item['text']}")
        if len(evidence) > 30:
            lines.append(f"  - ... {len(evidence) - 30}개 추가 근거는 JSON 참조")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_outputs(result: dict[str, Any], out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "exam_scope_evidence.json"
    md_path = out_dir / "exam_scope_report.md"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(result), encoding="utf-8")
    return json_path, md_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--out-dir", type=Path, default=Path("tmp/exam_scope_code_only"))
    parser.add_argument("--state-path", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    base = args.base.resolve()
    out_dir = args.out_dir
    if not out_dir.is_absolute():
        out_dir = base / out_dir
    result = scan_exam_scopes(base, state_path=args.state_path)
    _json_path, md_path = write_outputs(result, out_dir)
    print(md_path.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
