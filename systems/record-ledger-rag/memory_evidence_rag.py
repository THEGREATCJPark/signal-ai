#!/usr/bin/env python3

import argparse
import copy
import dataclasses
import hashlib
import json
import math
import os
import re
import subprocess
import tempfile
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

import fitz  # type: ignore
import requests
import tiktoken


DEFAULT_CHAT_API_URL = "http://172.21.32.1:8046/v1/chat/completions"
DEFAULT_SELECT_MODEL = "gemini-3.1-flash-lite-preview"
DEFAULT_ANALYZE_MODEL = "gemini-3.1-flash-lite-preview"
DEFAULT_DRAFT_MODEL = "gemini-3.1-flash-lite-preview"
DEFAULT_EMBED_MODEL = "gemini-embedding-002"
ANALYZE_CACHE_SCHEMA_VERSION = "evidence-id-v2"
ALL_VARIANTS = [
    "variant_llm_select",
    "variant_embedding_select",
    "variant_full_scan",
    "variant_hybrid",
]
DEFAULT_MAX_REQUEST_TOKENS = 100_000
DEFAULT_CHUNK_TOKENS = 12_000
DEFAULT_QUESTION_CHUNK_TOKENS = 50_000
DEFAULT_LLM_SELECT_PACK_TOKENS = 12_000
DEFAULT_ANCHOR_TEXT_CHARS = 2_000
DEFAULT_QUOTE_CHARS = 220
DEFAULT_EMBED_WORKERS = 10
DEFAULT_ANALYZE_WORKERS = 8
DEFAULT_CHUNK_BUILD_WORKERS = 4
# Follow the HTML reference scheduler: always keep a 3s minimum per-key gap.
GEMINI_KEY_MIN_GAP_MS = 3000
GEMINI_KEY_MAX_INFLIGHT = 1
GEMINI_KEY_RPM_LIMIT = 20
GEMINI_KEY_TPM_LIMIT = 0
GEMINI_GLOBAL_MAX_INFLIGHT = 0
GEMINI_GLOBAL_RETRY_ROUNDS = 4
TOKENIZER_NAME = "cl100k_base"
EMBED_GROUP_ORDER = ["military_casebook", "general_casebook", "other", "regulation"]
EMBED_GROUP_CAP_RATIO = {
    "military_casebook": 0.45,
    "general_casebook": 0.35,
    "other": 0.30,
    "regulation": 0.35,
}
DIRECT_CHAT_MODEL_FALLBACKS = {
    "gemma-4-31b-it": ["gemma-4-27b", "gemma-4-26b", "gemma-3-27b-it", "gemini-3.1-pro"],
    "gemma-4-27b": ["gemma-4-26b", "gemma-3-27b-it", "gemini-3.1-pro"],
    "gemma-4-26b": ["gemma-4-27b", "gemma-3-27b-it", "gemini-3.1-pro"],
    "gemini-3.1-pro": ["gemini-2.5-pro", "gemini-2.5-flash"],
    "gemini-3-flash": ["gemini-2.5-flash"],
    "gemini-3.1-flash-lite-preview": ["gemini-flash-lite-latest", "gemini-2.5-flash-lite"],
}
EMBED_MODEL_FALLBACKS = {
    "gemini-embedding-002": ["gemini-embedding-2-preview", "gemini-embedding-001"],
    "gemini-embedding-2-preview": ["gemini-embedding-001"],
}
EMBED_CACHE_DIR = Path("/path/to/workspace/.cache/memory_evidence_rag_embeddings")
ENV_FALLBACK_FILES = [
    Path("~/.config/memory_evidence_rag/keys.env"),
    Path("/path/to/server/.env.local"),
]
SEPARATOR_RE = re.compile(r"^[=\-_*#~]{3,}$")
HEADING_PREFIX_RE = re.compile(r"^(?:제\s*\d+\s*조|[0-9]+\.|[가-하]\.|\([0-9]+\)|\[[^\]]+\]|레코드번호\s*:|요약\s*:|쟁점사항\s*:|이유\s*:)")
JSON_KEY_RE = re.compile(r'^\s*"?([A-Za-z0-9_가-힣]+)"?\s*[:=]')
WHITESPACE_RE = re.compile(r"\s+")
VIRTUAL_DOC_SEPARATOR_LINE = "=" * 40

DIRECT_EVIDENCE_FILES = {
    "dataset/심사위원회.txt",
    "dataset/증거/쟁점의결요구고지서.pdf",
    "dataset/증거/쟁점처분서.pdf",
}
DISCIPLINE_CASE_KEYWORDS = [
    "쟁점",
    "인사",
    "복무",
    "품위",
    "성실",
    "강요",
    "직권남용",
    "근무태만",
    "군기교육",
    "모욕",
    "강제추행",
    "절차령",
    "절차규정",
    "군인사법",
    "인사관리",
    "행동강령",
    "복무",
    "보안규정",
    "비밀엄수",
    "심부름",
]
CERTAINTY_ORDER = {
    "very_high": 5,
    "high": 4,
    "medium": 3,
    "low": 2,
    "speculative": 1,
}
CERTAINTY_LABELS = {
    "매우높음": "very_high",
    "높음": "high",
    "보통": "medium",
    "낮음": "low",
    "추측": "speculative",
}


ENCODER = tiktoken.get_encoding(TOKENIZER_NAME)


@dataclasses.dataclass
class SelectionParseResult:
    reasoning: str
    selected_ids: list[str]


@dataclasses.dataclass
class FileRecord:
    file_id: str
    relative_path: str
    absolute_path: str
    document_title: str
    doc_type: str
    source_group: str
    token_count: int
    anchor_text: str
    extracted_text: str
    candidate_boundaries: list[dict[str, Any]]
    is_direct_evidence: bool
    is_format_sample: bool = False
    content_hash: str = ""
    duplicate_paths: list[str] = dataclasses.field(default_factory=list)
    record_number: str = ""
    source_org: str = ""
    record_date: str = ""
    record_title: str = ""


@dataclasses.dataclass
class ChunkRecord:
    chunk_id: str
    file_id: str
    document_title: str
    same_document_group: str
    relative_path: str
    start_char: int
    end_char: int
    text: str
    token_count: int
    record_number: str = ""
    source_org: str = ""
    record_date: str = ""
    record_title: str = ""
    source_segments: list[dict[str, Any]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class GeminiKeyState:
    last_sent_at_ms: float = 0.0
    cooldown_until_ms: float = 0.0
    cooldown_retry_count: int = 0
    inflight_count: int = 0
    window_start_ms: float = 0.0
    rpm_count: int = 0
    tpm_count: int = 0


@dataclasses.dataclass
class MemoryEvidenceRequest:
    base_user_task: str
    target_files: list[str]
    core_issues: list[str]
    incident_data: str
    incident_material_paths: list[str]


class GeminiKeyScheduler:
    def __init__(self, keys: list[str]):
        self.keys = list(keys)
        self.states = {key: GeminiKeyState() for key in self.keys}
        self.next_idx = 0
        self.cv = threading.Condition()

    def _reset_window_unlocked(self, state: GeminiKeyState, now_ms: float) -> None:
        window_start_ms = now_ms - (now_ms % 60000)
        if not state.window_start_ms or window_start_ms != state.window_start_ms:
            state.window_start_ms = window_start_ms
            state.rpm_count = 0
            state.tpm_count = 0

    def runtime_snapshot(
        self,
        *,
        now_ms: float,
        key_min_gap_ms: int,
        key_max_inflight: int,
        key_rpm_limit: int = GEMINI_KEY_RPM_LIMIT,
        key_tpm_limit: int = GEMINI_KEY_TPM_LIMIT,
        global_max_inflight: int = GEMINI_GLOBAL_MAX_INFLIGHT,
    ) -> dict[str, int]:
        cooling = 0
        inflight = 0
        best_ready_at = math.inf
        for state in self.states.values():
            self._reset_window_unlocked(state, now_ms)
            inflight += state.inflight_count
            if state.cooldown_until_ms and now_ms < state.cooldown_until_ms:
                cooling += 1
            ready_at = now_ms
            if state.cooldown_until_ms and now_ms < state.cooldown_until_ms:
                ready_at = max(ready_at, state.cooldown_until_ms)
            if state.inflight_count >= key_max_inflight:
                ready_at = max(ready_at, now_ms + 50.0)
            else:
                next_by_gap = state.last_sent_at_ms + key_min_gap_ms if state.last_sent_at_ms else now_ms
                if next_by_gap and now_ms < next_by_gap:
                    ready_at = max(ready_at, next_by_gap)
                if key_rpm_limit > 0 and state.rpm_count >= key_rpm_limit:
                    ready_at = max(ready_at, state.window_start_ms + 60025.0)
            best_ready_at = min(best_ready_at, ready_at)
        next_ready_in_ms = 0 if not math.isfinite(best_ready_at) else max(0, int(best_ready_at - now_ms))
        return {
            "gemini_keys_total": len(self.states),
            "gemini_keys_cooling_down": cooling,
            "gemini_scheduler_inflight": inflight,
            "gemini_next_ready_in_ms": next_ready_in_ms,
            "gemini_key_min_gap_ms": key_min_gap_ms,
            "gemini_key_max_inflight": key_max_inflight,
            "gemini_key_rpm_limit": key_rpm_limit,
            "gemini_key_tpm_limit": key_tpm_limit,
            "gemini_global_max_inflight": global_max_inflight,
        }

    def _emit_runtime_snapshot_unlocked(
        self,
        *,
        now_ms: float,
        key_min_gap_ms: int,
        key_max_inflight: int,
        key_rpm_limit: int,
        key_tpm_limit: int,
        global_max_inflight: int,
    ) -> None:
        status_path = _RUNTIME_STATUS_PATH
        if status_path is None:
            return
        merge_runtime_status(
            status_path,
            self.runtime_snapshot(
                now_ms=now_ms,
                key_min_gap_ms=key_min_gap_ms,
                key_max_inflight=key_max_inflight,
                key_rpm_limit=key_rpm_limit,
                key_tpm_limit=key_tpm_limit,
                global_max_inflight=global_max_inflight,
            ),
        )

    def acquire(self, *, est_tokens: int = 0) -> str:
        with self.cv:
            while True:
                limits = get_runtime_limits()
                key_max_inflight = limits["gemini_key_max_inflight"]
                key_min_gap_ms = limits["gemini_key_min_gap_ms"]
                key_rpm_limit = limits["gemini_key_rpm_limit"]
                key_tpm_limit = limits["gemini_key_tpm_limit"]
                global_max_inflight = limits["gemini_global_max_inflight"]
                now_ms = time.time() * 1000
                chosen_key: str | None = None
                chosen_idx = -1
                best_ready_at = math.inf
                total_inflight = sum(state.inflight_count for state in self.states.values())

                for offset in range(len(self.keys)):
                    idx = (self.next_idx + offset) % len(self.keys)
                    key = self.keys[idx]
                    state = self.states[key]
                    self._reset_window_unlocked(state, now_ms)
                    if state.cooldown_until_ms and now_ms >= state.cooldown_until_ms:
                        state.cooldown_until_ms = 0.0

                    ready_at = now_ms
                    if state.cooldown_until_ms and now_ms < state.cooldown_until_ms:
                        ready_at = max(ready_at, state.cooldown_until_ms)
                    if global_max_inflight > 0 and total_inflight >= global_max_inflight:
                        ready_at = max(ready_at, now_ms + 50.0)
                    elif state.inflight_count >= key_max_inflight:
                        ready_at = max(ready_at, now_ms + 50.0)
                    else:
                        next_by_gap = state.last_sent_at_ms + key_min_gap_ms if state.last_sent_at_ms else 0.0
                        if next_by_gap and now_ms < next_by_gap:
                            ready_at = max(ready_at, next_by_gap)
                        if key_rpm_limit > 0 and state.rpm_count >= key_rpm_limit:
                            ready_at = max(ready_at, state.window_start_ms + 60025.0)
                        if key_tpm_limit > 0:
                            projected_tpm = state.tpm_count + max(0, est_tokens)
                            if est_tokens > key_tpm_limit:
                                if state.tpm_count >= key_tpm_limit:
                                    ready_at = max(ready_at, state.window_start_ms + 60025.0)
                            elif projected_tpm > key_tpm_limit:
                                ready_at = max(ready_at, state.window_start_ms + 60025.0)

                    if chosen_key is None and ready_at <= now_ms and state.inflight_count < key_max_inflight:
                        chosen_key = key
                        chosen_idx = idx
                    if ready_at < best_ready_at:
                        best_ready_at = ready_at

                if chosen_key is not None:
                    state = self.states[chosen_key]
                    self._reset_window_unlocked(state, now_ms)
                    state.last_sent_at_ms = now_ms
                    state.inflight_count += 1
                    state.rpm_count += 1
                    state.tpm_count += max(0, est_tokens)
                    self.next_idx = (chosen_idx + 1) % len(self.keys)
                    self._emit_runtime_snapshot_unlocked(
                        now_ms=now_ms,
                        key_min_gap_ms=key_min_gap_ms,
                        key_max_inflight=key_max_inflight,
                        key_rpm_limit=key_rpm_limit,
                        key_tpm_limit=key_tpm_limit,
                        global_max_inflight=global_max_inflight,
                    )
                    return chosen_key

                wait_ms = 100.0
                if math.isfinite(best_ready_at):
                    wait_ms = max(50.0, min(1000.0, best_ready_at - now_ms))
                self._emit_runtime_snapshot_unlocked(
                    now_ms=now_ms,
                    key_min_gap_ms=key_min_gap_ms,
                    key_max_inflight=key_max_inflight,
                    key_rpm_limit=key_rpm_limit,
                    key_tpm_limit=key_tpm_limit,
                    global_max_inflight=global_max_inflight,
                )
                self.cv.wait(wait_ms / 1000.0)

    def complete(
        self,
        key: str,
        *,
        success: bool,
        transient: bool = False,
        retry_after_ms: int | None = None,
        http_status: int | None = None,
    ) -> None:
        with self.cv:
            limits = get_runtime_limits()
            state = self.states[key]
            state.inflight_count = max(0, state.inflight_count - 1)
            now_ms = time.time() * 1000
            if success:
                state.cooldown_until_ms = 0.0
                state.cooldown_retry_count = 0
            elif transient:
                state.cooldown_retry_count += 1
                delay_ms = max(
                    retry_after_ms or 0,
                    compute_http_aware_backoff_ms(state.cooldown_retry_count, http_status),
                )
                state.cooldown_until_ms = max(state.cooldown_until_ms, now_ms + delay_ms)
            self._emit_runtime_snapshot_unlocked(
                now_ms=now_ms,
                key_min_gap_ms=limits["gemini_key_min_gap_ms"],
                key_max_inflight=limits["gemini_key_max_inflight"],
                key_rpm_limit=limits["gemini_key_rpm_limit"],
                key_tpm_limit=limits["gemini_key_tpm_limit"],
                global_max_inflight=limits["gemini_global_max_inflight"],
            )
            self.cv.notify_all()


_GEMINI_SCHEDULER_LOCK = threading.Lock()
_GEMINI_SCHEDULERS: dict[tuple[str, ...], GeminiKeyScheduler] = {}
_RUNTIME_CONTROL_LOCK = threading.Lock()
_RUNTIME_CONTROL_PATH: Path | None = None
_RUNTIME_CONTROL_MTIME_NS: int | None = None
_RUNTIME_CONTROL_CACHE: dict[str, int] = {}
_RUNTIME_STATUS_LOCK = threading.Lock()
_RUNTIME_STATUS_PATH: Path | None = None
_RUNTIME_HTTP_STATE = {
    "http_started": 0,
    "http_finished": 0,
    "http_inflight": 0,
}


def _normalize_space(text: str | None) -> str:
    return WHITESPACE_RE.sub(" ", str(text or "")).strip()


def load_memory_evidence_request(path: Path) -> MemoryEvidenceRequest:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("request json must be an object")

    base_user_task = _normalize_space(payload.get("base_user_task"))
    target_files = [_normalize_space(item) for item in payload.get("target_files") or [] if _normalize_space(item)]
    core_issues = [_normalize_space(item) for item in payload.get("core_issues") or [] if _normalize_space(item)]
    incident_data = str(payload.get("incident_data") or "").strip()
    incident_material_paths = [
        _normalize_space(item) for item in payload.get("incident_material_paths") or [] if _normalize_space(item)
    ]

    missing: list[str] = []
    if not target_files:
        missing.append("target_files")
    if not core_issues:
        missing.append("core_issues")
    if not incident_data:
        missing.append("incident_data")
    if not incident_material_paths:
        missing.append("incident_material_paths")
    if missing:
        raise ValueError("missing required request fields: " + ", ".join(missing))

    return MemoryEvidenceRequest(
        base_user_task=base_user_task or "구조화 의견서 작성",
        target_files=target_files,
        core_issues=core_issues,
        incident_data=incident_data,
        incident_material_paths=incident_material_paths,
    )


def build_request_user_task(request: MemoryEvidenceRequest) -> str:
    parts = [request.base_user_task.strip()]
    if request.core_issues:
        parts.append("[핵심쟁점]\n" + "\n".join(f"- {issue}" for issue in request.core_issues))
    if request.incident_data.strip():
        parts.append("[사건데이터]\n" + request.incident_data.strip())
    return "\n\n".join(part for part in parts if part)


def filter_records_to_target_files(records: list[FileRecord], target_files: list[str]) -> list[FileRecord]:
    normalized = {_normalize_space(item).replace("\\", "/") for item in target_files if _normalize_space(item)}
    if not normalized:
        return list(records)

    out: list[FileRecord] = []
    for record in records:
        rel = record.relative_path.replace("\\", "/")
        abs_path = record.absolute_path.replace("\\", "/")
        if rel in normalized or abs_path in normalized:
            out.append(record)
    return out


def set_runtime_control_file(path: Path | None) -> None:
    global _RUNTIME_CONTROL_PATH, _RUNTIME_CONTROL_MTIME_NS, _RUNTIME_CONTROL_CACHE
    with _RUNTIME_CONTROL_LOCK:
        _RUNTIME_CONTROL_PATH = path
        _RUNTIME_CONTROL_MTIME_NS = None
        _RUNTIME_CONTROL_CACHE = {}


def set_runtime_status_file(path: Path | None) -> None:
    global _RUNTIME_STATUS_PATH
    with _RUNTIME_STATUS_LOCK:
        _RUNTIME_STATUS_PATH = path
        _RUNTIME_HTTP_STATE["http_started"] = 0
        _RUNTIME_HTTP_STATE["http_finished"] = 0
        _RUNTIME_HTTP_STATE["http_inflight"] = 0


def get_runtime_limits() -> dict[str, int]:
    global _RUNTIME_CONTROL_MTIME_NS, _RUNTIME_CONTROL_CACHE
    defaults = {
        "gemini_key_min_gap_ms": GEMINI_KEY_MIN_GAP_MS,
        "gemini_key_max_inflight": GEMINI_KEY_MAX_INFLIGHT,
        "gemini_key_rpm_limit": GEMINI_KEY_RPM_LIMIT,
        "gemini_key_tpm_limit": GEMINI_KEY_TPM_LIMIT,
        "gemini_global_max_inflight": GEMINI_GLOBAL_MAX_INFLIGHT,
    }
    with _RUNTIME_CONTROL_LOCK:
        path = _RUNTIME_CONTROL_PATH
        if path is None or not path.exists():
            return dict(defaults)
        stat = path.stat()
        if _RUNTIME_CONTROL_MTIME_NS == stat.st_mtime_ns and _RUNTIME_CONTROL_CACHE:
            return {**defaults, **_RUNTIME_CONTROL_CACHE}
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            _RUNTIME_CONTROL_CACHE = {}
        else:
            cache: dict[str, int] = {}
            for key in defaults:
                value = payload.get(key)
                if isinstance(value, int) and value > 0:
                    cache[key] = value
            _RUNTIME_CONTROL_CACHE = cache
        _RUNTIME_CONTROL_MTIME_NS = stat.st_mtime_ns
        return {**defaults, **_RUNTIME_CONTROL_CACHE}


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^0-9A-Za-z가-힣._-]+", "_", value)
    slug = slug.strip("._")
    return slug[:120] or "item"


def _count_tokens(text: str) -> int:
    if not text:
        return 0
    return len(ENCODER.encode(text))


def _estimate_token_count(text: str, *, sample_chars: int = 50000) -> int:
    if not text:
        return 0
    if len(text) <= sample_chars:
        return _count_tokens(text)
    sample = text[:sample_chars]
    sample_tokens = _count_tokens(sample)
    if sample_tokens <= 0:
        return 0
    chars_per_token = len(sample) / sample_tokens
    return max(sample_tokens, int(len(text) / max(0.1, chars_per_token)))


def load_env_value(name: str) -> str:
    value = os.environ.get(name)
    if value:
        return value.strip()
    for path in ENV_FALLBACK_FILES:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith(f"{name}="):
                return line.split("=", 1)[1].strip()
    return ""


def load_env_values(name: str) -> list[str]:
    values: list[str] = []
    raw = os.environ.get(name, "")
    if raw:
        for line in re.split(r"[\n,]+", raw):
            line = line.strip()
            if line:
                values.append(line)
    for path in ENV_FALLBACK_FILES:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith(f"{name}="):
                for part in re.split(r"[\n,]+", line.split("=", 1)[1]):
                    part = part.strip()
                    if part:
                        values.append(part)
    return list(dict.fromkeys(values))


def load_gemini_api_keys() -> list[str]:
    disable_flag = _normalize_space(os.environ.get("MEMORY_EVIDENCE_DISABLE_GEMINI_KEYS")).lower()
    if disable_flag in {"1", "true", "yes", "on"}:
        return []
    keys: list[str] = []
    for env_name in ["GEMINI_API_KEYS", "GOOGLE_API_KEYS"]:
        keys.extend(load_env_values(env_name))
    single = load_env_value("GEMINI_API_KEY")
    if single:
        keys.append(single)
    single_google = load_env_value("GOOGLE_API_KEY")
    if single_google:
        keys.append(single_google)
    return list(dict.fromkeys(key.strip() for key in keys if key.strip()))


def load_chat_api_url() -> str:
    return load_env_value("MEMORY_EVIDENCE_CHAT_API_URL") or DEFAULT_CHAT_API_URL


def load_gemini_thinking_level() -> str:
    value = _normalize_space(os.environ.get("MEMORY_EVIDENCE_GEMINI_THINKING_LEVEL")).lower()
    if value in {"", "default"}:
        return "high"
    if value in {"off", "none", "disable", "disabled", "0"}:
        return ""
    if value in {"low", "medium", "high"}:
        return value
    return "high"


def should_use_gemini_thinking(model_name: str) -> bool:
    return "gemma" not in _normalize_space(model_name).lower()


def compute_http_aware_backoff_ms(retry_count: int, http_status: int | None = None) -> int:
    status = http_status if isinstance(http_status, int) else None
    is_429 = status == 429
    is_5xx = status is not None and 500 <= status < 600
    if is_429:
        seq = [10000, 30000, 60000, 60000]
    elif is_5xx:
        seq = [5000, 15000, 30000, 60000]
    else:
        seq = [3000, 10000, 30000, 60000]
    idx = max(0, min(len(seq) - 1, int(retry_count or 1) - 1))
    return int(seq[idx])


def _parse_retry_after_ms(response: requests.Response) -> int | None:
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        seconds = int(raw.strip())
    except ValueError:
        return None
    if seconds <= 0:
        return None
    return seconds * 1000


def get_gemini_key_scheduler(api_keys: list[str]) -> GeminiKeyScheduler:
    key_tuple = tuple(api_keys)
    with _GEMINI_SCHEDULER_LOCK:
        scheduler = _GEMINI_SCHEDULERS.get(key_tuple)
        if scheduler is None:
            scheduler = GeminiKeyScheduler(list(key_tuple))
            _GEMINI_SCHEDULERS[key_tuple] = scheduler
        return scheduler


def classify_doc_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".txt", ".md"}:
        return "txt"
    if suffix == ".pdf":
        return "pdf"
    if suffix in {".hwp", ".hwpx"}:
        return "hwp"
    if suffix in {".jpg", ".jpeg", ".png", ".bmp", ".webp"}:
        return "image"
    return "other"


def _line_fingerprint(line: str) -> str:
    text = line.strip()
    text = re.sub(r"\d{2,}", "<NUM>", text)
    text = re.sub(r"[A-Z]{2,}[0-9-]*", "<ID>", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _iter_line_records(text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    offset = 0
    for idx, line in enumerate(text.splitlines(keepends=True), start=1):
        bare = line.rstrip("\n")
        records.append(
            {
                "line_no": idx,
                "line_text": bare,
                "fingerprint": _line_fingerprint(bare),
                "char_start": offset,
                "char_end": offset + len(line),
            }
        )
        offset += len(line)
    return records


def find_boundary_candidates(text: str) -> list[dict[str, Any]]:
    line_records = _iter_line_records(text)
    if not line_records:
        return []

    fp_counter = Counter(record["fingerprint"] for record in line_records if record["fingerprint"])
    separator_counter = Counter(
        record["line_text"].strip() for record in line_records if SEPARATOR_RE.match(record["line_text"].strip())
    )
    key_seq_counter: Counter[tuple[str, ...]] = Counter()
    template_counter: Counter[str] = Counter()

    for idx in range(len(line_records) - 2):
        seq = []
        for record in line_records[idx : idx + 3]:
            m = JSON_KEY_RE.match(record["line_text"])
            if m:
                seq.append(m.group(1))
        if len(seq) >= 2:
            key_seq_counter[tuple(seq)] += 1

    for record in line_records:
        if HEADING_PREFIX_RE.match(record["line_text"].strip()):
            template_counter[record["fingerprint"]] += 1

    candidates: list[dict[str, Any]] = []
    for record in line_records:
        stripped = record["line_text"].strip()
        if not stripped:
            continue
        score = 0.0
        kind = "paragraph"
        if SEPARATOR_RE.match(stripped):
            if stripped in separator_counter and separator_counter[stripped] >= 2:
                score = 100 + separator_counter[stripped] * 5
                kind = "repeated_separator"
            else:
                score = 75
                kind = "separator"
        elif record["fingerprint"] in template_counter and template_counter[record["fingerprint"]] >= 2:
            score = 80 + template_counter[record["fingerprint"]] * 4
            kind = "repeated_template"
        elif HEADING_PREFIX_RE.match(stripped):
            score = 60
            kind = "heading"
        elif fp_counter[record["fingerprint"]] >= 2:
            score = 55 + fp_counter[record["fingerprint"]]
            kind = "repeated_line"
        else:
            # local schema-like key sequence support
            local_seq = []
            idx0 = record["line_no"] - 1
            for nearby in line_records[idx0 : min(idx0 + 3, len(line_records))]:
                m = JSON_KEY_RE.match(nearby["line_text"])
                if m:
                    local_seq.append(m.group(1))
            if len(local_seq) >= 2 and key_seq_counter[tuple(local_seq)] >= 2:
                score = 70 + key_seq_counter[tuple(local_seq)] * 3
                kind = "schema_sequence"
            elif record["line_text"].endswith(('.', ':')):
                score = 20
                kind = "paragraph"
        if score <= 0:
            continue
        candidates.append(
            {
                "kind": kind,
                "score": score,
                "line_no": record["line_no"],
                "char_index": record["char_start"],
                "line_text": stripped,
            }
        )
    candidates.sort(key=lambda item: (-item["score"], item["char_index"]))
    return candidates


def pick_split_index(text: str, max_tokens: int, *, search_radius_chars: int = 5000) -> int | None:
    if _count_tokens(text) <= max_tokens:
        return None
    target_ratio = max_tokens / max(1, _count_tokens(text))
    target_index = max(1, min(len(text) - 1, int(len(text) * target_ratio)))
    candidates = find_boundary_candidates(text)
    nearby = [
        c
        for c in candidates
        if abs(int(c["char_index"]) - target_index) <= search_radius_chars and 0 < int(c["char_index"]) < len(text)
    ]
    if nearby:
        nearby.sort(
            key=lambda item: (
                -float(item["score"]),
                abs(int(item["char_index"]) - target_index),
                int(item["char_index"]),
            )
        )
        return int(nearby[0]["char_index"])

    paragraph_break = text.rfind("\n\n", 0, target_index)
    if paragraph_break > 0:
        return paragraph_break + 2
    line_break = text.rfind("\n", 0, target_index)
    if line_break > 0:
        return line_break + 1
    return target_index


def chunk_document_text(
    *,
    text: str,
    max_tokens: int,
    overlap_chars: int,
    same_document_group: str,
    file_id: str = "",
    document_title: str = "",
    relative_path: str = "",
) -> list[dict[str, Any]]:
    if not text:
        return []
    chunks: list[dict[str, Any]] = []
    boundaries = sorted(find_boundary_candidates(text), key=lambda item: int(item["char_index"]))
    if len(text) <= 50_000:
        total_tokens = _count_tokens(text)
        avg_chars_per_token = max(1.0, len(text) / max(1, total_tokens))
    else:
        sample = text[:50_000]
        sample_tokens = _count_tokens(sample)
        avg_chars_per_token = max(1.0, len(sample) / max(1, sample_tokens))
    start = 0
    part = 1
    length = len(text)
    while start < length:
        max_char_guess = max(4000, int(max_tokens * avg_chars_per_token * 1.8))
        high = min(length, start + max_char_guess)
        if high >= length and _count_tokens(text[start:length]) <= max_tokens:
            end = length
            chunk_text = text[start:end]
        else:
            low = min(length, start + 1)
            best_end = low
            while low <= high:
                mid = (low + high) // 2
                candidate_tokens = _count_tokens(text[start:mid])
                if candidate_tokens <= max_tokens:
                    best_end = mid
                    low = mid + 1
                else:
                    high = mid - 1

            nearby = [
                item
                for item in boundaries
                if start < int(item["char_index"]) <= best_end + 2000 and int(item["char_index"]) >= max(start + 100, best_end - 6000)
            ]
            if nearby:
                nearby.sort(
                    key=lambda item: (
                        -float(item["score"]),
                        abs(int(item["char_index"]) - best_end),
                        int(item["char_index"]),
                    )
                )
                end = int(nearby[0]["char_index"])
            else:
                end = best_end
            if end <= start:
                end = min(length, best_end if best_end > start else start + max(1000, int(max_tokens * avg_chars_per_token)))
            chunk_text = text[start:end]
            while end < length and _count_tokens(chunk_text) > max_tokens and end > start + 100:
                end = max(start + 100, start + int((end - start) * 0.9))
                chunk_text = text[start:end]
        if not chunk_text:
            end = min(length, start + max(1000, int(max_tokens * avg_chars_per_token)))
            chunk_text = text[start:end]
        if _count_tokens(chunk_text) <= max_tokens and end < length:
            local_end = end
            paragraph_break = text.rfind("\n\n", start, local_end)
            if paragraph_break > start + 200 and paragraph_break + 2 != local_end:
                improved = text[start : paragraph_break + 2]
                if _count_tokens(improved) <= max_tokens:
                    end = paragraph_break + 2
                    chunk_text = improved
        chunks.append(
            {
                "chunk_id": f"{same_document_group}::part-{part:03d}",
                "file_id": file_id,
                "document_title": document_title,
                "same_document_group": same_document_group,
                "relative_path": relative_path,
                "start_char": start,
                "end_char": end,
                "text": chunk_text,
                "token_count": _count_tokens(chunk_text),
            }
        )
        if end >= length:
            break
        next_start = max(0, end - max(0, overlap_chars))
        if next_start <= start:
            next_start = end
        start = next_start
        part += 1
    return chunks


def parse_llm_selection_response(text: str) -> SelectionParseResult:
    raw = str(text or "")
    match = re.search(r"<selection>\s*(.*?)\s*</selection>", raw, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return SelectionParseResult(reasoning=raw.strip(), selected_ids=[])
    reasoning = raw[: match.start()].strip()
    body = match.group(1).strip()
    if re.search(r"<none\s*/>", body, flags=re.IGNORECASE):
        return SelectionParseResult(reasoning=reasoning, selected_ids=[])
    selected_ids = []
    for line in body.splitlines():
        item = line.strip().strip("-*")
        item = item.strip()
        if item:
            selected_ids.append(item)
    seen: set[str] = set()
    ordered = []
    for item in selected_ids:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return SelectionParseResult(reasoning=reasoning, selected_ids=ordered)


def _coerce_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        raw_items = value
    elif value in (None, ""):
        raw_items = []
    else:
        raw_items = [value]
    out: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        text = _normalize_space(item)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def parse_keyword_generation_response(text: str, *, keyword_count: int = 10) -> list[str]:
    raw = text.strip()
    keywords: list[str] = []
    try:
        obj = json.loads(_extract_json_block(raw))
        if isinstance(obj, dict):
            keywords = _coerce_string_list(obj.get("keywords"))
        elif isinstance(obj, list):
            keywords = _coerce_string_list(obj)
    except Exception:
        keywords = []
    if not keywords:
        for line in raw.splitlines():
            candidate = line.strip().strip("-*").strip()
            if not candidate:
                continue
            candidate = candidate.strip('"').strip("'")
            keywords.append(candidate)
    deduped: list[str] = []
    seen: set[str] = set()
    for keyword in keywords:
        normalized = _normalize_space(keyword)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
        if len(deduped) >= max(1, keyword_count):
            break
    return deduped


def _fallback_keywords_from_user_task(user_task: str, *, keyword_count: int = 10) -> list[str]:
    candidates = re.findall(r"[0-9A-Za-z가-힣]{2,20}", user_task)
    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = _normalize_space(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
        if len(deduped) >= max(1, keyword_count):
            break
    return deduped


def build_keyword_generation_prompt(user_task: str, *, keyword_count: int = 10) -> str:
    return (
        "아래 자료 질문을 기준으로 기억 레코드 검색용 핵심 키워드를 뽑아라.\n"
        "규칙:\n"
        f"- 반드시 서로 다른 키워드/짧은 구를 정확히 {keyword_count}개 만든다.\n"
        "- 분류A 방향, 분류B 방향, 유리한 포인트, 불리한 포인트를 함께 반영한다.\n"
        "- 너무 긴 문장 금지. 검색에 바로 넣을 수 있는 짧은 한국어 표현 위주로 쓴다.\n"
        "- JSON만 출력한다.\n"
        '- 스키마: {"keywords": ["...", "..."]}\n\n'
        f"질문:\n{user_task}"
    )


def generate_search_keywords(user_task: str, *, model: str, keyword_count: int = 10) -> list[str]:
    raw = call_chat(
        [
            {"role": "system", "content": "자료 기억 레코드 검색 키워드 생성기다. JSON만 출력하라."},
            {"role": "user", "content": build_keyword_generation_prompt(user_task, keyword_count=keyword_count)},
        ],
        model=model,
        timeout=180,
    )
    keywords = parse_keyword_generation_response(raw, keyword_count=keyword_count)
    if keywords:
        return keywords
    return _fallback_keywords_from_user_task(user_task, keyword_count=keyword_count)


def _record_date_sort_key(value: Any) -> int:
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    if not digits:
        return 0
    try:
        return int(digits[:8])
    except ValueError:
        return 0


def _score_structured_record_row(row: dict[str, Any], keywords: list[str]) -> tuple[int, int, list[str]]:
    title_text = _normalize_space(" ".join([str(row.get("record_title") or ""), str(row.get("record_number") or "")]))
    issue_text = _normalize_space(str(row.get("issue_text") or ""))
    summary_text = _normalize_space(str(row.get("summary_text") or ""))
    full_text = _normalize_space(str(row.get("full_text") or ""))
    hit_count = 0
    weighted_score = 0
    matched_keywords: list[str] = []
    for keyword in keywords:
        needle = _normalize_space(keyword)
        if not needle:
            continue
        local_score = 0
        if needle in title_text:
            local_score += 14
        if needle in issue_text:
            local_score += 10
        if needle in summary_text:
            local_score += 8
        if needle in full_text:
            local_score += 4
        if local_score:
            hit_count += 1
            weighted_score += local_score
            matched_keywords.append(needle)
    return hit_count, weighted_score, matched_keywords


def select_top_structured_record_rows(
    rows: list[dict[str, Any]],
    *,
    keywords: list[str],
    top_k: int,
) -> list[dict[str, Any]]:
    scored_rows: list[dict[str, Any]] = []
    for row in rows:
        hit_count, weighted_score, matched_keywords = _score_structured_record_row(row, keywords)
        if hit_count <= 0:
            continue
        scored_rows.append(
            {
                **row,
                "_keyword_hit_count": hit_count,
                "_keyword_weighted_score": weighted_score,
                "_matched_keywords": matched_keywords,
                "_record_date_sort_key": _record_date_sort_key(row.get("record_date")),
            }
        )
    scored_rows.sort(
        key=lambda item: (
            -int(item.get("_keyword_hit_count") or 0),
            -int(item.get("_keyword_weighted_score") or 0),
            -int(item.get("_record_date_sort_key") or 0),
            str(item.get("record_number") or ""),
        )
    )
    return scored_rows[: max(1, top_k)]


def merge_claim_ledgers(rows: list[dict[str, Any]], *, analysis_mode: str = "document") -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if analysis_mode == "question":
            key = _normalize_space(row.get("claim_axis")) or _normalize_space(row.get("claim_text"))
        else:
            key = _normalize_space(row.get("claim_text"))
        if not key:
            continue
        grouped[key].append(row)
    merged: list[dict[str, Any]] = []
    for group_key, items in grouped.items():
        support_spans = []
        oppose_spans = []
        certainty_value = max(CERTAINTY_ORDER.values())
        section_counter: Counter[str] = Counter()
        certainty_reason_parts: list[str] = []
        same_situation_case_exists = False
        inference_basis_parts: list[str] = []
        for item in items:
            support_spans.extend(item.get("support_spans") or [])
            oppose_spans.extend(item.get("oppose_spans") or [])
            section_counter[_normalize_space(item.get("recommended_section") or "나. 사안의 경우")] += 1
            current = CERTAINTY_ORDER.get(_normalize_space(item.get("certainty")).lower(), 3)
            certainty_value = min(certainty_value, current)
            same_situation_case_exists = same_situation_case_exists or bool(item.get("same_situation_case_exists"))
            if item.get("inference_basis"):
                inference_basis_parts.append(_normalize_space(item.get("inference_basis")))
            if item.get("certainty_reason"):
                certainty_reason_parts.append(_normalize_space(item.get("certainty_reason")))
        if oppose_spans:
            certainty_value = min(certainty_value, CERTAINTY_ORDER["high"])
        if analysis_mode == "question":
            supporting_cases: list[dict[str, Any]] = []
            seen_cases: set[str] = set()
            list_fields = [
                "favorable_basis",
                "unfavorable_basis",
                "usable_favorable_logic",
                "usable_unfavorable_logic",
                "favorable_factors",
                "unfavorable_factors",
                "required_facts",
                "missing_facts",
                "cautions",
                "counter_evidence",
            ]
            merged_lists: dict[str, list[str]] = {field: [] for field in list_fields}
            context_summaries: list[str] = []
            case_summaries: list[str] = []
            stance_values: list[str] = []
            primary_item = max(
                items,
                key=lambda item: (
                    len(item.get("support_spans") or []),
                    int(bool(_normalize_space(item.get("record_number")))),
                    int(bool(_normalize_space(item.get("source_org")))),
                    _record_date_sort_key(item.get("record_date")),
                ),
            )
            for item in items:
                for field in list_fields:
                    merged_lists[field].extend(_coerce_string_list(item.get(field)))
                context_summaries.extend(_coerce_string_list(item.get("context_summary")))
                case_summaries.extend(_coerce_string_list(item.get("case_summary")))
                stance_values.extend(_coerce_string_list(item.get("stance_to_user_goal")))
                case_key = "|".join(
                    [
                        _normalize_space(item.get("record_number")),
                        _normalize_space(item.get("source_org")),
                        _normalize_space(item.get("record_date")),
                        _normalize_space(item.get("record_title")),
                        _normalize_space(item.get("relative_path")),
                    ]
                )
                if case_key and case_key not in seen_cases:
                    seen_cases.add(case_key)
                    supporting_cases.append(
                        {
                            "record_number": _normalize_space(item.get("record_number")),
                            "source_org": _normalize_space(item.get("source_org")),
                            "record_date": _normalize_space(item.get("record_date")),
                            "record_title": _normalize_space(item.get("record_title")),
                            "document_title": _normalize_space(item.get("document_title")),
                            "relative_path": _normalize_space(item.get("relative_path")),
                        }
                    )
            if supporting_cases and len(supporting_cases) >= 2 and not oppose_spans:
                certainty_value = min(CERTAINTY_ORDER["very_high"], certainty_value + 1)
                certainty_reason_parts.append(f"동일 주장 축을 지지하는 별개 기억 레코드가 {len(supporting_cases)}건 확인되었다.")
            certainty = next((k for k, v in CERTAINTY_ORDER.items() if v == certainty_value), "medium")
            primary_support_case = supporting_cases[0] if supporting_cases else {}
            merged.append(
                {
                    "claim_axis": _normalize_space(items[0].get("claim_axis")) or group_key,
                    "claim_text": _normalize_space(items[0].get("claim_text")) or group_key,
                    "stance_to_user_goal": " / ".join(dict.fromkeys(stance_values))[:400],
                    "support_spans": _dedupe_dicts(support_spans),
                    "oppose_spans": _dedupe_dicts(oppose_spans),
                    "same_situation_case_exists": same_situation_case_exists,
                    "inference_basis": " / ".join(dict.fromkeys(inference_basis_parts))[:1200],
                    "certainty": certainty,
                    "certainty_reason": " / ".join(dict.fromkeys(certainty_reason_parts))[:1200],
                    "support_count": len(support_spans),
                    "oppose_count": len(oppose_spans),
                    "supporting_case_count": len(supporting_cases),
                    "supporting_cases": supporting_cases,
                    "source_file_id": _normalize_space(primary_item.get("source_file_id")),
                    "record_number": _normalize_space(primary_item.get("record_number")) or _normalize_space(primary_support_case.get("record_number")),
                    "source_org": _normalize_space(primary_item.get("source_org")) or _normalize_space(primary_support_case.get("source_org")),
                    "record_date": _normalize_space(primary_item.get("record_date")) or _normalize_space(primary_support_case.get("record_date")),
                    "record_title": _normalize_space(primary_item.get("record_title")) or _normalize_space(primary_support_case.get("record_title")),
                    "document_title": _normalize_space(primary_item.get("document_title")),
                    "relative_path": _normalize_space(primary_item.get("relative_path")),
                    "context_summary": " / ".join(dict.fromkeys(context_summaries))[:2000],
                    "case_summary": " / ".join(dict.fromkeys(case_summaries))[:2000],
                    **{field: list(dict.fromkeys(values)) for field, values in merged_lists.items()},
                }
            )
            continue
        certainty = next((k for k, v in CERTAINTY_ORDER.items() if v == certainty_value), "medium")
        merged.append(
            {
                "claim_text": group_key,
                "recommended_section": section_counter.most_common(1)[0][0] if section_counter else "나. 사안의 경우",
                "support_spans": _dedupe_dicts(support_spans),
                "oppose_spans": _dedupe_dicts(oppose_spans),
                "same_situation_case_exists": same_situation_case_exists,
                "inference_basis": " / ".join(dict.fromkeys(inference_basis_parts))[:1200],
                "certainty": certainty,
                "certainty_reason": " / ".join(dict.fromkeys(certainty_reason_parts))[:1200],
                "support_count": len(support_spans),
                "oppose_count": len(oppose_spans),
            }
        )
    merged.sort(key=lambda item: (-CERTAINTY_ORDER.get(item["certainty"], 0), -item["support_count"], item["claim_text"]))
    return merged


def _dedupe_dicts(items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in items:
        key = json.dumps(item, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _run(cmd: list[str], timeout: int = 600) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )


def _extract_hwp_text(path: Path) -> str:
    proc = _run(["hwp5txt", str(path)], timeout=600)
    if proc.returncode != 0:
        return ""
    return (proc.stdout or "").strip()


def _ocr_image(path: Path) -> str:
    proc = _run(["tesseract", str(path), "stdout", "-l", "kor+eng", "--psm", "6"], timeout=300)
    if proc.returncode != 0:
        return ""
    return (proc.stdout or "").strip()


def _extract_pdf_text(path: Path) -> str:
    out_parts: list[str] = []
    try:
        doc = fitz.open(path)
    except Exception:
        return ""
    with doc:
        for page in doc:
            text = page.get_text("text") or ""
            text = text.strip()
            if text:
                out_parts.append(text)
                continue
            # OCR fallback for image-like pages
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                tmp_path = Path(tmp.name)
            try:
                pix.save(tmp_path)
                ocr = _ocr_image(tmp_path)
                if ocr:
                    out_parts.append(ocr)
            finally:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
    return "\n\n".join(part for part in out_parts if part).strip()


def extract_text(path: Path) -> str:
    doc_type = classify_doc_type(path)
    if doc_type == "txt":
        return path.read_text(encoding="utf-8", errors="replace")
    if doc_type == "pdf":
        return _extract_pdf_text(path)
    if doc_type == "hwp":
        return _extract_hwp_text(path)
    if doc_type == "image":
        return _ocr_image(path)
    return ""


def _candidate_source_group(relative_path: str) -> str:
    rel = relative_path.replace("\\", "/")
    if rel in DIRECT_EVIDENCE_FILES or rel.startswith("dataset/증거/"):
        return "direct_evidence"
    if rel.startswith("군/법/"):
        return "regulation"
    if rel.startswith("군/자료 관련 기억 레코드집/"):
        return "military_casebook"
    if rel.startswith("기억 레코드집/"):
        return "general_casebook"
    return "other"


def iter_candidate_paths(root: Path) -> list[Path]:
    candidates: list[Path] = []
    seen: set[Path] = set()
    for rel in sorted(DIRECT_EVIDENCE_FILES):
        path = root / rel
        if path.exists() and path.is_file():
            seen.add(path)
            candidates.append(path)

    for sub in [root / "군" / "법"]:
        if not sub.exists():
            continue
        for path in sorted(sub.rglob("*")):
            if not path.is_file():
                continue
            if classify_doc_type(path) not in {"txt", "pdf", "hwp"}:
                continue
            if path.suffix.lower() in {".csv", ".json"}:
                continue
            if "/양정 원본/" in path.as_posix():
                continue
            if path not in seen:
                seen.add(path)
                candidates.append(path)

    for sub in [root / "군" / "국방부"]:
        if not sub.exists():
            continue
        for path in sorted(sub.rglob("*")):
            if not path.is_file():
                continue
            if classify_doc_type(path) not in {"txt", "pdf", "hwp"}:
                continue
            name = path.name
            if any(keyword in name for keyword in DISCIPLINE_CASE_KEYWORDS) or any(
                token in name for token in ["육규_110", "육규_112", "육규_113", "육규_180", "쟁점", "인사"]
            ):
                if path not in seen:
                    seen.add(path)
                    candidates.append(path)

    for sub in [root / "군" / "자료 관련 기억 레코드집", root / "기억 레코드집"]:
        if not sub.exists():
            continue
        for path in sorted(sub.rglob("*")):
            if not path.is_file():
                continue
            if classify_doc_type(path) != "txt":
                continue
            name = path.name
            if any(keyword in name for keyword in DISCIPLINE_CASE_KEYWORDS):
                if path not in seen:
                    seen.add(path)
                    candidates.append(path)

    for sub in [root / "domain" / "domain_records"]:
        if not sub.exists():
            continue
        for path in sorted(sub.rglob("*_extracted.txt")):
            if path not in seen:
                seen.add(path)
                candidates.append(path)
    return candidates


def build_file_record(root: Path, path: Path, cache_dir: Path | None = None) -> FileRecord:
    relative_path = path.relative_to(root).as_posix()
    source_group = _candidate_source_group(relative_path)
    doc_type = classify_doc_type(path)
    file_id = "file-" + hashlib.sha1(relative_path.encode("utf-8")).hexdigest()[:12]
    cache_path = cache_dir / f"{file_id}.txt" if cache_dir else None
    extracted_text = ""
    if cache_path and cache_path.exists():
        extracted_text = cache_path.read_text(encoding="utf-8", errors="replace")
    if not extracted_text:
        extracted_text = extract_text(path)
        if cache_path:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(extracted_text, encoding="utf-8")
    anchor_text = _normalize_space(extracted_text[:DEFAULT_ANCHOR_TEXT_CHARS])
    return FileRecord(
        file_id=file_id,
        relative_path=relative_path,
        absolute_path=str(path),
        document_title=path.stem,
        doc_type=doc_type,
        source_group=source_group,
        token_count=_estimate_token_count(extracted_text),
        anchor_text=anchor_text,
        extracted_text=extracted_text,
        candidate_boundaries=find_boundary_candidates(extracted_text)[:80],
        is_direct_evidence=source_group == "direct_evidence",
        content_hash=hashlib.sha1(extracted_text.encode("utf-8", errors="replace")).hexdigest() if extracted_text else "",
    )


def _iter_joonhok_record_rows(structured_dir: Path) -> Iterable[dict[str, Any]]:
    import pandas as pd

    columns = ["레코드명", "레코드번호", "기준일자", "출처명", "쟁점사항", "요약", "전문"]
    for parquet_path in sorted(structured_dir.glob("*.parquet")):
        df = pd.read_parquet(parquet_path, columns=columns)
        for row_index, row in enumerate(df.to_dict(orient="records")):
            issue_text = str(row.get("쟁점사항") or "").strip()
            summary_text = str(row.get("요약") or "").strip()
            full_text = str(row.get("전문") or "").strip() or "\n\n".join(part for part in [issue_text, summary_text] if part)
            yield {
                "record_title": _normalize_space(row.get("레코드명")),
                "record_number": _normalize_space(row.get("레코드번호")),
                "record_date": _normalize_space(row.get("기준일자")),
                "source_org": _normalize_space(row.get("출처명")),
                "issue_text": issue_text,
                "summary_text": summary_text,
                "full_text": full_text,
                "source_path": f"{parquet_path.as_posix()}::row-{row_index}",
                "source_dataset": structured_dir.name,
            }


def _iter_constitutional_record_rows(structured_dir: Path) -> Iterable[dict[str, Any]]:
    import pandas as pd

    columns = ["record_title", "record_number", "record_date", "summary", "full_text", "issues"]
    for parquet_path in sorted(structured_dir.glob("*.parquet")):
        df = pd.read_parquet(parquet_path, columns=columns)
        for row_index, row in enumerate(df.to_dict(orient="records")):
            issue_text = _normalize_space(row.get("issues"))
            summary_text = _normalize_space(row.get("summary"))
            full_text = str(row.get("full_text") or "").strip() or "\n\n".join(part for part in [issue_text, summary_text] if part)
            yield {
                "record_title": _normalize_space(row.get("record_title")),
                "record_number": _normalize_space(row.get("record_number")),
                "record_date": _normalize_space(row.get("record_date")),
                "source_org": "구조화출처",
                "issue_text": issue_text,
                "summary_text": summary_text,
                "full_text": full_text,
                "source_path": f"{parquet_path.as_posix()}::row-{row_index}",
                "source_dataset": structured_dir.name,
            }


def iter_structured_record_rows(structured_dir: Path) -> Iterable[dict[str, Any]]:
    name = structured_dir.name
    if name == "01_joonhok_records":
        yield from _iter_joonhok_record_rows(structured_dir)
        return
    if name == "05_constitutional_source_org":
        yield from _iter_constitutional_record_rows(structured_dir)
        return
    raise ValueError(f"unsupported structured record dir: {structured_dir}")


def structured_row_to_file_record(row: dict[str, Any]) -> FileRecord:
    record_number = _normalize_space(row.get("record_number"))
    record_title = _normalize_space(row.get("record_title"))
    source_org = _normalize_space(row.get("source_org"))
    record_date = _normalize_space(row.get("record_date"))
    source_path = _normalize_space(row.get("source_path"))
    full_text = str(row.get("full_text") or "").strip()
    title_parts = [part for part in [source_org, record_date, record_number, record_title] if part]
    document_title = " ".join(title_parts)[:240] or record_title or record_number or source_path
    relative_path = f"structured/{_normalize_space(row.get('source_dataset'))}/{Path(source_path.split('::row-')[0]).name}::{record_number or hashlib.sha1(source_path.encode('utf-8')).hexdigest()[:8]}"
    file_id = "structured-" + hashlib.sha1(f"{source_path}|{record_number}|{record_title}".encode("utf-8")).hexdigest()[:12]
    anchor_text = _normalize_space("\n".join([record_title, str(row.get("issue_text") or ""), str(row.get("summary_text") or ""), full_text[:1500]]))
    return FileRecord(
        file_id=file_id,
        relative_path=relative_path,
        absolute_path=source_path,
        document_title=document_title,
        doc_type="txt",
        source_group="structured_record",
        token_count=_estimate_token_count(full_text),
        anchor_text=anchor_text[:DEFAULT_ANCHOR_TEXT_CHARS],
        extracted_text=full_text,
        candidate_boundaries=find_boundary_candidates(full_text)[:80],
        is_direct_evidence=False,
        content_hash=hashlib.sha1(full_text.encode("utf-8", errors="replace")).hexdigest() if full_text else "",
        record_number=record_number,
        source_org=source_org,
        record_date=record_date,
        record_title=record_title,
    )


def _best_virtual_document_title(text: str, fallback: str) -> str:
    for line in text.splitlines():
        candidate = _normalize_space(line)
        if candidate:
            return candidate[:200]
    return fallback


def split_record_on_exact_virtual_separator(record: FileRecord) -> list[FileRecord]:
    if record.doc_type != "txt":
        return [record]
    if VIRTUAL_DOC_SEPARATOR_LINE not in record.extracted_text:
        return [record]
    parts = re.split(rf"(?:\r?\n){re.escape(VIRTUAL_DOC_SEPARATOR_LINE)}(?:\r?\n)", record.extracted_text)
    cleaned = [_normalize_space(part) for part in parts]
    meaningful_parts = [part for part in parts if _normalize_space(part)]
    if len(meaningful_parts) <= 1:
        return [record]

    split_records: list[FileRecord] = []
    for index, part in enumerate(parts, start=1):
        normalized = _normalize_space(part)
        if not normalized:
            continue
        suffix = f"::virtual-{index:03d}"
        text = part if part.endswith("\n") else part + "\n"
        split_records.append(
            FileRecord(
                file_id=f"{record.file_id}{suffix}",
                relative_path=f"{record.relative_path}{suffix}",
                absolute_path=record.absolute_path,
                document_title=_best_virtual_document_title(text, f"{record.document_title} {index}"),
                doc_type=record.doc_type,
                source_group=record.source_group,
                token_count=_estimate_token_count(text),
                anchor_text=_normalize_space(text[:DEFAULT_ANCHOR_TEXT_CHARS]),
                extracted_text=text,
                candidate_boundaries=find_boundary_candidates(text)[:80],
                is_direct_evidence=record.is_direct_evidence,
                is_format_sample=record.is_format_sample,
                content_hash=hashlib.sha1(text.encode("utf-8", errors="replace")).hexdigest() if text else "",
                duplicate_paths=list(record.duplicate_paths),
            )
        )
    return split_records or [record]


def call_chat(messages: list[dict[str, Any]], *, model: str, api_url: str | None = None, timeout: int = 300) -> str:
    if load_gemini_api_keys():
        return _call_gemini_generate_content(messages, model=model, timeout=timeout)
    resolved_api_url = api_url or load_chat_api_url()
    payload = {"model": model, "messages": messages, "temperature": 0.1}
    response = requests.post(resolved_api_url, json=payload, timeout=min(timeout, 600))
    response.raise_for_status()
    obj = response.json()
    return str(obj.get("choices", [{}])[0].get("message", {}).get("content", "") or "")


def _call_gemini_generate_content(messages: list[dict[str, Any]], *, model: str, timeout: int = 300) -> str:
    api_keys = load_gemini_api_keys()
    if not api_keys:
        raise RuntimeError("No Gemini API keys found in environment or fallback env file")
    scheduler = get_gemini_key_scheduler(api_keys)

    system_parts = []
    contents = []
    for message in messages:
        role = str(message.get("role") or "user")
        content = str(message.get("content") or "")
        if not content:
            continue
        if role == "system":
            system_parts.append(content)
            continue
        gemini_role = "model" if role == "assistant" else "user"
        contents.append({"role": gemini_role, "parts": [{"text": content}]})
    if not contents:
        contents = [{"role": "user", "parts": [{"text": ""}]}]

    last_error: str | None = None
    model_chain = list(dict.fromkeys([model, *DIRECT_CHAT_MODEL_FALLBACKS.get(model, [])]))
    missing_models: set[str] = set()
    started_at_s = time.monotonic()
    status_path = _RUNTIME_STATUS_PATH
    estimated_prompt_tokens = 0
    for part in system_parts:
        estimated_prompt_tokens += _estimate_token_count(part)
    for item in contents:
        for part in item.get("parts") or []:
            estimated_prompt_tokens += _estimate_token_count(str(part.get("text") or ""))
    for _ in range(GEMINI_GLOBAL_RETRY_ROUNDS):
        saw_transient = False
        for model_name in model_chain:
            if model_name in missing_models:
                continue
            generation_config: dict[str, Any] = {"temperature": 0.1}
            thinking_level = load_gemini_thinking_level()
            if thinking_level and should_use_gemini_thinking(model_name):
                generation_config["thinkingConfig"] = {"thinkingLevel": thinking_level}
            payload: dict[str, Any] = {
                "contents": contents,
                "generationConfig": generation_config,
            }
            if system_parts:
                payload["systemInstruction"] = {"parts": [{"text": "\n\n".join(system_parts)}]}
            for _ in range(len(api_keys)):
                attempt_timeout_cap_s = select_generate_timeout_cap(
                    model_name,
                    estimated_prompt_tokens=estimated_prompt_tokens,
                )
                request_timeout_s = compute_attempt_timeout_seconds(
                    total_timeout_s=float(timeout),
                    started_at_s=started_at_s,
                    now_s=time.monotonic(),
                    per_attempt_cap_s=attempt_timeout_cap_s,
                )
                if request_timeout_s is None:
                    raise RuntimeError(
                        f"Gemini logical timeout exhausted for {model}; last_error={last_error or 'none'}"
                    )
                api_key = scheduler.acquire(est_tokens=estimated_prompt_tokens)
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
                transient = False
                retry_after_ms = None
                record_runtime_http_activity(
                    status_path,
                    event="start",
                    kind="generateContent",
                    model_name=model_name,
                    detail=f"timeout={request_timeout_s:.1f}s",
                )
                try:
                    response = requests.post(url, json=payload, timeout=request_timeout_s)
                except requests.RequestException as exc:
                    last_error = f"{model_name}: request failed: {exc}"
                    transient = True
                    saw_transient = True
                    scheduler.complete(api_key, success=False, transient=transient, retry_after_ms=retry_after_ms, http_status=None)
                    record_runtime_http_activity(
                        status_path,
                        event="finish",
                        kind="generateContent",
                        model_name=model_name,
                        detail=f"request_exception: {type(exc).__name__}",
                    )
                    continue
                if response.status_code == 404:
                    last_error = f"{model_name}: HTTP 404"
                    scheduler.complete(api_key, success=False, transient=False, retry_after_ms=None, http_status=404)
                    record_runtime_http_activity(
                        status_path,
                        event="finish",
                        kind="generateContent",
                        model_name=model_name,
                        detail="http_404",
                    )
                    missing_models.add(model_name)
                    break
                if response.status_code in {429, 500, 502, 503, 504}:
                    last_error = f"{model_name}: HTTP {response.status_code}"
                    transient = True
                    saw_transient = True
                    retry_after_ms = _parse_retry_after_ms(response)
                    scheduler.complete(api_key, success=False, transient=transient, retry_after_ms=retry_after_ms, http_status=response.status_code)
                    record_runtime_http_activity(
                        status_path,
                        event="finish",
                        kind="generateContent",
                        model_name=model_name,
                        detail=f"http_{response.status_code}",
                    )
                    continue
                try:
                    response.raise_for_status()
                except requests.RequestException as exc:
                    last_error = f"{model_name}: {exc}"
                    transient = True
                    saw_transient = True
                    retry_after_ms = _parse_retry_after_ms(response)
                    scheduler.complete(api_key, success=False, transient=transient, retry_after_ms=retry_after_ms, http_status=response.status_code)
                    record_runtime_http_activity(
                        status_path,
                        event="finish",
                        kind="generateContent",
                        model_name=model_name,
                        detail=f"http_exception: {response.status_code}",
                    )
                    continue
                obj = response.json()
                parts = obj.get("candidates", [{}])[0].get("content", {}).get("parts", [])
                text = "".join(str(part.get("text") or "") for part in parts)
                scheduler.complete(api_key, success=True, transient=False, retry_after_ms=None, http_status=response.status_code)
                record_runtime_http_activity(
                    status_path,
                    event="finish",
                    kind="generateContent",
                    model_name=model_name,
                    detail="success" if text else "empty_body",
                )
                if text:
                    return text
                last_error = f"{model_name}: empty response body"
        if not saw_transient:
            break
    raise RuntimeError(f"No supported Gemini chat model available for {model}; last_error={last_error or 'none'}")


def call_gemini_embedding(text: str, *, model: str = DEFAULT_EMBED_MODEL) -> list[float]:
    api_keys = load_gemini_api_keys()
    if not api_keys:
        raise RuntimeError("No Gemini API keys found in environment or fallback env file")
    scheduler = get_gemini_key_scheduler(api_keys)

    cache_key = hashlib.sha1(f"{model}\n{text}".encode("utf-8")).hexdigest()
    cache_path = EMBED_CACHE_DIR / f"{cache_key}.json"
    if cache_path.exists():
        cached = json.loads(cache_path.read_text(encoding="utf-8"))
        if isinstance(cached, list):
            return cached
        values = list(cached.get("values") or [])
        if values:
            return values

    last_error: str | None = None
    fallback_chain = list(
        dict.fromkeys(
            [
                model,
                *EMBED_MODEL_FALLBACKS.get(model, []),
                DEFAULT_EMBED_MODEL,
                *EMBED_MODEL_FALLBACKS.get(DEFAULT_EMBED_MODEL, []),
                "gemini-embedding-001",
            ]
        )
    )
    missing_models: set[str] = set()
    for _ in range(GEMINI_GLOBAL_RETRY_ROUNDS):
        saw_transient = False
        for model_name in fallback_chain:
            if model_name in missing_models:
                continue
            payload = {
                "model": f"models/{model_name}",
                "taskType": "SEMANTIC_SIMILARITY",
                "content": {"parts": [{"text": text}]},
            }
            for _ in range(len(api_keys)):
                api_key = scheduler.acquire(est_tokens=_estimate_token_count(text))
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:embedContent?key={api_key}"
                transient = False
                retry_after_ms = None
                try:
                    response = requests.post(url, json=payload, timeout=60)
                except requests.RequestException as exc:
                    last_error = f"{model_name}: request failed: {exc}"
                    transient = True
                    saw_transient = True
                    scheduler.complete(api_key, success=False, transient=transient, retry_after_ms=retry_after_ms, http_status=None)
                    continue
                if response.status_code == 404:
                    last_error = f"{model_name}: HTTP 404"
                    scheduler.complete(api_key, success=False, transient=False, retry_after_ms=None, http_status=404)
                    missing_models.add(model_name)
                    break
                if response.status_code in {429, 500, 502, 503, 504}:
                    last_error = f"{model_name}: HTTP {response.status_code}"
                    transient = True
                    saw_transient = True
                    retry_after_ms = _parse_retry_after_ms(response)
                    scheduler.complete(api_key, success=False, transient=transient, retry_after_ms=retry_after_ms, http_status=response.status_code)
                    continue
                try:
                    response.raise_for_status()
                except requests.RequestException as exc:
                    last_error = f"{model_name}: {exc}"
                    transient = True
                    saw_transient = True
                    retry_after_ms = _parse_retry_after_ms(response)
                    scheduler.complete(api_key, success=False, transient=transient, retry_after_ms=retry_after_ms, http_status=response.status_code)
                    continue
                obj = response.json()
                values = list(obj.get("embedding", {}).get("values") or [])
                if values:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_text(json.dumps({"resolved_model": model_name, "values": values}), encoding="utf-8")
                    scheduler.complete(api_key, success=True, transient=False, retry_after_ms=None, http_status=response.status_code)
                    return values
                scheduler.complete(api_key, success=True, transient=False, retry_after_ms=None, http_status=response.status_code)
                last_error = f"{model_name}: empty embedding values"
        if not saw_transient:
            break
    raise RuntimeError(f"No supported Gemini embedding model available; last_error={last_error or 'none'}")


def cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return -1.0
    num = sum(x * y for x, y in zip(a, b))
    den_a = math.sqrt(sum(x * x for x in a))
    den_b = math.sqrt(sum(y * y for y in b))
    if not den_a or not den_b:
        return -1.0
    return num / (den_a * den_b)


def _render_anchor_block(records: list[FileRecord]) -> str:
    lines: list[str] = []
    for record in records:
        lines.extend(
            [
                f"[{record.file_id}] {record.document_title}",
                f"- relative_path: {record.relative_path}",
                f"- source_group: {record.source_group}",
                f"- token_count: {record.token_count}",
                f"- anchor: {record.anchor_text[:1200] or '(empty)'}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def pack_text_blocks(blocks: list[tuple[str, str]], *, max_tokens: int) -> list[list[tuple[str, str]]]:
    packs: list[list[tuple[str, str]]] = []
    current: list[tuple[str, str]] = []
    current_tokens = 0
    for key, text in blocks:
        tokens = _count_tokens(text)
        if current and current_tokens + tokens > max_tokens:
            packs.append(current)
            current = []
            current_tokens = 0
        current.append((key, text))
        current_tokens += tokens
    if current:
        packs.append(current)
    return packs


def _run_llm_file_select_pack(
    pack_index: int,
    pack_total: int,
    pack: list[tuple[str, str]],
    *,
    user_task: str,
    model: str,
) -> tuple[int, SelectionParseResult]:
    processing_requirements = build_processing_analysis_requirements()
    print(f"[select] llm pack {pack_index}/{pack_total}", flush=True)
    prompt = (
        "당신은 쟁점 의견서 작성을 위한 파일 선별기다.\n"
        "규칙:\n"
        "- 아래 파일 anchor들 중 관련 파일만 고른다.\n"
        "- 찬성 근거뿐 아니라 반대 근거가 될 파일도 고른다.\n"
        f"- 추가 지침: {processing_requirements}\n"
        "- 설명을 먼저 쓰고, 마지막에는 반드시 <selection> ... </selection> 블록만 따로 낸다.\n"
        "- <selection> 안에는 file_id만 한 줄에 하나씩 적는다.\n"
        "- 관련 파일이 없으면 <none/> 만 넣는다.\n\n"
        f"사용자 과제:\n{user_task}\n\n"
        f"파일 묶음 {pack_index}/{pack_total}:\n" + "\n\n".join(text for _, text in pack)
    )
    raw = call_chat(
        [
            {"role": "system", "content": "자료 파일 선택을 정확히 수행하고, 지정한 선택 블록 형식을 반드시 지켜라."},
            {"role": "user", "content": prompt},
        ],
        model=model,
    )
    parsed = parse_llm_selection_response(raw)
    print(
        f"[select] llm pack {pack_index}/{pack_total} selected={len(parsed.selected_ids)}",
        flush=True,
    )
    return pack_index, parsed


def run_llm_file_select(records: list[FileRecord], *, user_task: str, model: str) -> tuple[list[str], list[str]]:
    blocks = [(record.file_id, _render_anchor_block([record])) for record in records]
    packs = pack_text_blocks(blocks, max_tokens=DEFAULT_LLM_SELECT_PACK_TOKENS)
    selected: list[str] = []
    reasons: list[str] = []
    results: dict[int, SelectionParseResult] = {}
    max_workers = max(1, min(4, len(packs)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                _run_llm_file_select_pack,
                index,
                len(packs),
                pack,
                user_task=user_task,
                model=model,
            ): index
            for index, pack in enumerate(packs, start=1)
        }
        for future in as_completed(future_map):
            index, parsed = future.result()
            results[index] = parsed
    for index in range(1, len(packs) + 1):
        parsed = results[index]
        reasons.append(parsed.reasoning)
        selected.extend(parsed.selected_ids)
    selected = list(dict.fromkeys(selected))
    return selected, reasons


def build_embedding_queries(user_task: str) -> list[str]:
    return [
        user_task,
        "쟁점권자가 일부 비위사실을 이미 알고 있었는데 당시 쟁점하지 않고 나중에 다른 비위와 합산해 쟁점할 수 있는지",
        "선행 인지 후 미조치 비위를 후행 비위와 합산한 쟁점의 적법성",
        "쟁점권자의 선행 인지, 신뢰보호, 비례, 이중평가, 추가쟁점 관련 기억 레코드와 규정",
        "군인 절차령 군인사법 절차규정 인사관리 규정 중 선행 인지 비위와 후행 합산 쟁점 관련 규정",
    ]


def _score_record_for_embedding_select(record: FileRecord, query_vectors: list[list[float]]) -> tuple[str, float, dict[str, Any]]:
    anchor_text = f"{record.document_title}\n{record.relative_path}\n{record.anchor_text[:1500]}"
    vector = call_gemini_embedding(anchor_text)
    score = max((cosine_similarity(vector, qv) for qv in query_vectors), default=-1.0)
    detail = {"score": score, "title": record.document_title, "relative_path": record.relative_path}
    return record.file_id, score, detail


def select_embedding_candidates(
    records: list[FileRecord],
    scored: list[tuple[str, float]],
    *,
    top_k: int,
) -> tuple[list[str], dict[str, Any]]:
    if not scored:
        direct_ids = [record.file_id for record in records if record.is_direct_evidence]
        return direct_ids, {"adaptive_floor": 0.0, "pool_target_rank": 0, "group_counts": {}, "selected_ranked": []}

    record_by_id = {record.file_id: record for record in records}
    direct_ids = [record.file_id for record in records if record.is_direct_evidence]
    direct_set = set(direct_ids)
    remaining_slots = max(0, top_k - len(direct_ids))
    if remaining_slots == 0:
        return direct_ids[:top_k], {
            "adaptive_floor": 0.0,
            "pool_target_rank": 0,
            "group_counts": {"direct_evidence": len(direct_ids[:top_k])},
            "selected_ranked": [(fid, next((score for sid, score in scored if sid == fid), 0.0)) for fid in direct_ids[:top_k]],
        }

    pool_target_rank = min(len(scored), max(remaining_slots * 2, remaining_slots + 4))
    nth_score = scored[pool_target_rank - 1][1]
    top_score = scored[0][1]
    adaptive_floor = max(0.70, nth_score, top_score - 0.08)

    non_direct_scored = [(file_id, score) for file_id, score in scored if file_id not in direct_set]
    candidate_scored = [(file_id, score) for file_id, score in non_direct_scored if score >= adaptive_floor]
    if len(candidate_scored) < remaining_slots:
        candidate_scored = non_direct_scored[: min(len(non_direct_scored), max(remaining_slots * 2, remaining_slots + 4))]

    group_buckets: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for file_id, score in candidate_scored:
        group = record_by_id[file_id].source_group
        group_buckets[group].append((file_id, score))

    group_order = [group for group in EMBED_GROUP_ORDER if group_buckets.get(group)]
    extras = sorted(
        [group for group in group_buckets if group not in EMBED_GROUP_ORDER],
        key=lambda group: group_buckets[group][0][1],
        reverse=True,
    )
    group_order.extend(extras)
    group_caps = {
        group: max(1, math.ceil(remaining_slots * EMBED_GROUP_CAP_RATIO.get(group, 0.25)))
        for group in group_order
    }

    selected_ranked: list[tuple[str, float]] = []
    selected_ids: set[str] = set()
    group_counts: Counter[str] = Counter()
    progress = True
    while len(selected_ranked) < remaining_slots and progress:
        progress = False
        for group in group_order:
            bucket = group_buckets[group]
            while bucket and bucket[0][0] in selected_ids:
                bucket.pop(0)
            if not bucket:
                continue
            if group_counts[group] >= group_caps[group]:
                continue
            file_id, score = bucket.pop(0)
            selected_ranked.append((file_id, score))
            selected_ids.add(file_id)
            group_counts[group] += 1
            progress = True
            if len(selected_ranked) >= remaining_slots:
                break

    for file_id, score in candidate_scored:
        if len(selected_ranked) >= remaining_slots:
            break
        if file_id in selected_ids:
            continue
        selected_ranked.append((file_id, score))
        selected_ids.add(file_id)
        group_counts[record_by_id[file_id].source_group] += 1

    for file_id, score in non_direct_scored:
        if len(selected_ranked) >= remaining_slots:
            break
        if file_id in selected_ids:
            continue
        selected_ranked.append((file_id, score))
        selected_ids.add(file_id)
        group_counts[record_by_id[file_id].source_group] += 1

    final_selected = list(dict.fromkeys(direct_ids + [file_id for file_id, _ in selected_ranked]))
    return final_selected, {
        "adaptive_floor": adaptive_floor,
        "pool_target_rank": pool_target_rank,
        "group_counts": dict(group_counts),
        "selected_ranked": selected_ranked,
    }


def run_embedding_file_select(
    records: list[FileRecord],
    *,
    user_task: str,
    top_k: int = 18,
    workers: int = DEFAULT_EMBED_WORKERS,
) -> tuple[list[str], dict[str, Any]]:
    query_vectors = [call_gemini_embedding(q) for q in build_embedding_queries(user_task)]
    scored = []
    details = {}
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(records)))) as executor:
        future_map = {
            executor.submit(_score_record_for_embedding_select, record, query_vectors): record.file_id for record in records
        }
        for future in as_completed(future_map):
            file_id, score, detail = future.result()
            scored.append((file_id, score))
            details[file_id] = detail
    scored.sort(key=lambda item: item[1], reverse=True)
    selected, selection_meta = select_embedding_candidates(records, scored, top_k=max(1, min(top_k, len(scored))))
    return selected, {
        "scores": details,
        "selected_ranked": selection_meta["selected_ranked"],
        "adaptive_floor": selection_meta["adaptive_floor"],
        "pool_target_rank": selection_meta["pool_target_rank"],
        "group_counts": selection_meta["group_counts"],
    }


def _quote_obj(quote: str, chunk: ChunkRecord) -> dict[str, Any]:
    quote = quote.strip()
    idx = chunk.text.find(quote)
    if idx < 0:
        compact = _normalize_space(quote)
        idx = _normalize_space(chunk.text).find(compact)
        if idx < 0:
            return {"quote": quote, "char_start": chunk.start_char, "char_end": chunk.start_char + min(len(quote), len(chunk.text)), "resolved": False}
    return {
        "quote": quote,
        "char_start": chunk.start_char + idx,
        "char_end": chunk.start_char + idx + len(quote),
        "resolved": True,
    }


def _default_evidence_prefix(chunk: ChunkRecord) -> str:
    candidate = Path(chunk.relative_path).stem.strip()
    if candidate:
        return candidate
    candidate = str(chunk.document_title or "").strip()
    return candidate or chunk.file_id


def _default_evidence_prefix_from_fields(*, relative_path: str, document_title: str, file_id: str) -> str:
    candidate = Path(relative_path).stem.strip()
    if candidate:
        return candidate
    candidate = str(document_title or "").strip()
    return candidate or file_id


def _coerce_evidence_spans(entries: list[Any], chunk: ChunkRecord, *, default_prefix: str | None = None) -> list[dict[str, Any]]:
    spans: list[dict[str, Any]] = []
    base = default_prefix or _default_evidence_prefix(chunk)
    for index, entry in enumerate(entries, start=1):
        evidence_id = ""
        quote = ""
        if isinstance(entry, dict):
            evidence_id = _normalize_space(entry.get("evidence_id"))
            quote = str(entry.get("quote") or "")
        else:
            quote = str(entry or "")
        quote = quote.strip()
        if not quote:
            continue
        span = _quote_obj(quote, chunk)
        if not span.get("resolved"):
            continue
        span["evidence_id"] = evidence_id or f"{base}-근거{index}"
        spans.append(span)
    return spans


def _chunk_source_lookup(chunk: ChunkRecord) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    if chunk.source_segments:
        for segment in chunk.source_segments:
            file_id = _normalize_space(segment.get("file_id"))
            if file_id:
                lookup[file_id] = segment
    if not lookup:
        lookup[chunk.file_id] = {
            "file_id": chunk.file_id,
            "document_title": chunk.document_title,
            "relative_path": chunk.relative_path,
            "record_number": chunk.record_number,
            "source_org": chunk.source_org,
            "record_date": chunk.record_date,
            "record_title": chunk.record_title,
            "text": chunk.text,
            "excerpt": " ".join(chunk.text.split())[:220],
        }
    return lookup


def _extract_json_block(text: str) -> str:
    fenced = re.findall(r"```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```", text, flags=re.DOTALL)
    if fenced:
        return fenced[0]
    candidates: list[tuple[int, int, str]] = []
    for start_char, end_char in [("{", "}"), ("[", "]")]:
        start = text.find(start_char)
        end = text.rfind(end_char)
        if start >= 0 and end > start:
            candidates.append((start, end, text[start : end + 1]))
    if candidates:
        candidates.sort(key=lambda item: item[0])
        return candidates[0][2]
    raise ValueError("No JSON block found")


def _normalize_chunk_analysis_obj(obj: Any) -> dict[str, Any]:
    if isinstance(obj, list):
        return {"claims_proposed": obj}
    if isinstance(obj, dict):
        claims = obj.get("claims_proposed")
        if isinstance(claims, list):
            return obj
        claim_cards = obj.get("claim_cards")
        if isinstance(claim_cards, list):
            normalized = dict(obj)
            normalized["claims_proposed"] = claim_cards
            return normalized
    return {"claims_proposed": []}


def build_processing_analysis_requirements() -> str:
    return (
        "사용자의 요구사항에 부합하기 위해서 자료를 해석하고 분석할 것.\n"
        "꼭 사용자에게 유리한쪽으로만 해석하지말고, 객관적으로 해석하여 불리하다면 불리하다고 말하고 그 구체적 이유를 말해야 한다.\n"
        "판단하기 어렵다면 어려운 이유를 설명하고, 불리하다면 그럼에도 최대한 유리하게 만들 수 있는 실제 사례 기반 대응 방향이 있는지 함께 찾아야 한다."
    )


def analyze_chunk(
    chunk: ChunkRecord,
    *,
    user_task: str,
    model: str,
    analysis_mode: str = "document",
) -> dict[str, Any]:
    processing_requirements = build_processing_analysis_requirements()
    source_lookup = _chunk_source_lookup(chunk)
    source_manifest = "\n".join(
        f"- {source_id}: {segment.get('record_number') or '(불명)'} / {segment.get('source_org') or '(불명)'} / {segment.get('record_date') or '(불명)'} / {segment.get('record_title') or segment.get('document_title') or ''}"
        for source_id, segment in source_lookup.items()
    )
    if analysis_mode == "question":
        prompt = (
            "아래는 자료 질문에 대응하기 위해 검토하는 기억 레코드의 일부 또는 전체 청크다.\n"
            "반드시 JSON만 출력하라.\n"
            "규칙:\n"
            "- 이 청크만으로 뒷받침되는 주장 카드만 적는다.\n"
            "- 청크 안에 여러 기억 레코드 조각이 들어 있을 수 있다. 각 주장 카드마다 반드시 source_file_id를 적어 어느 기억 레코드 조각에서 나온 주장인지 명시한다.\n"
            "- source_file_id는 아래 source manifest에 나온 값 중 하나만 사용한다.\n"
            "- support_evidence, oppose_evidence는 청크 원문에서 정확히 복붙한 짧은 인용과 evidence_id를 함께 넣는다.\n"
            "- support_evidence는 반드시 최소 1개 이상 있어야 한다. 정확한 짧은 인용을 고를 수 없으면 그 주장은 쓰지 말라.\n"
            "- evidence_id는 지어내지 말고 문서 제목 또는 확정 가능한 기억 레코드명을 바탕으로 `...-근거N` 형식으로 쓴다.\n"
            "- 이 청크에서 확인되지 않는 요소는 지어내지 말고 빈 배열 또는 빈 문자열로 남긴다.\n"
            "- 최소 필수는 source_file_id, claim_axis, claim_text, stance_to_user_goal, context_summary, support_evidence, certainty다.\n"
            "- case_summary는 기억 레코드의 결론과 핵심 이유가 드러나게 1~2문장으로 적는다. 추상 법리만 적지 말라.\n"
            "- 추가 필드는 최소화하라. 확실하지 않으면 억지로 채우지 말라.\n"
            "- 관련이 없으면 claims_proposed를 빈 배열로 둔다.\n"
            f"- 추가 지침: {processing_requirements}\n"
            "certainty는 very_high/high/medium/low/speculative 중 하나다.\n\n"
            f"사용자 질문:\n{user_task}\n\n"
            f"문서 제목: {chunk.document_title}\n"
            f"경로: {chunk.relative_path}\n"
            f"레코드번호: {chunk.record_number or '(불명)'}\n"
            f"출처: {chunk.source_org or '(불명)'}\n"
            f"기준일자: {chunk.record_date or '(불명)'}\n"
            f"레코드명: {chunk.record_title or '(불명)'}\n"
            f"청크 ID: {chunk.chunk_id}\n\n"
            "source manifest:\n"
            f"{source_manifest}\n\n"
            "청크 원문:\n"
            f"{chunk.text}\n\n"
            "출력 JSON 스키마:\n"
            "{\n"
            '  "claims_proposed": [\n'
            "    {\n"
            '      "source_file_id": "...",\n'
            '      "claim_axis": "...",\n'
            '      "claim_text": "...",\n'
            '      "stance_to_user_goal": "유리 | 불리 | 혼합",\n'
            '      "context_summary": "...",\n'
            '      "case_summary": "...",\n'
            '      "support_evidence": [{"evidence_id": "...-근거1", "quote": "..."}],\n'
            '      "oppose_evidence": [{"evidence_id": "...-근거1", "quote": "..."}],\n'
            '      "same_situation_case_exists": true,\n'
            '      "inference_basis": "",\n'
            '      "certainty": "medium",\n'
            '      "certainty_reason": "..."\n'
            "    }\n"
            "  ]\n"
            "}"
        )
    else:
        prompt = (
            "아래는 쟁점 의견서 작성을 위한 자료 자료의 일부 청크다.\n"
            "반드시 JSON만 출력하라.\n"
            "규칙:\n"
            "- 이 청크에서만 뒷받침되는 claim만 적는다.\n"
            "- claim도 네가 작성한다.\n"
            "- support_evidence, oppose_evidence는 청크 원문에서 정확히 복붙한 짧은 인용과 evidence_id를 함께 넣는다.\n"
            "- evidence_id는 지어내지 말고, 인용이 가장 직접적으로 속한 고유한 출처명을 써라.\n"
            "- 기억 레코드 일부면 `대출처 2002. 9. 24. 발행 2002두6620 자료-근거1`처럼 정확한 자료명+근거번호로 쓴다.\n"
            "- 규정/자료 일부면 `군인사법-근거2`, `육군규정 180-근거3`처럼 정확한 규정명/자료명+근거번호로 쓴다.\n"
            "- `지방출처기억 레코드`, `규정`, `기억 레코드집`처럼 포괄적이고 비고유한 이름은 금지한다.\n"
            "- 청크 안에서 더 구체적인 출처명을 확정할 수 없으면 문서 제목 또는 원본 파일명을 그대로 쓰고 `-근거N`을 붙인다.\n"
            "- 근거가 없으면 확신하지 말고 certainty를 낮춘다.\n"
            "- 같은 상황의 직접 적용 기억 레코드/규정인지, 유추인지 구분한다.\n"
            "- 반대 근거가 있으면 반드시 oppose_evidence에 넣는다.\n"
            "- 관련이 없으면 claims_proposed를 빈 배열로 둔다.\n"
            f"- 추가 지침: {processing_requirements}\n"
            "certainty는 very_high/high/medium/low/speculative 중 하나다.\n\n"
            f"사용자 과제:\n{user_task}\n\n"
            f"문서 제목: {chunk.document_title}\n"
            f"경로: {chunk.relative_path}\n"
            f"원본 파일명: {Path(chunk.relative_path).name}\n"
            f"청크 ID: {chunk.chunk_id}\n\n"
            "청크 원문:\n"
            f"{chunk.text}\n\n"
            "출력 JSON 스키마:\n"
            "{\n"
            '  "claims_proposed": [\n'
            "    {\n"
            '      "claim_text": "...",\n'
            '      "recommended_section": "가. 관련 법령 및 기억 레코드 | 나. 사안의 적용 | 다. 반대논리 및 그 한계 | 4. 결어",\n'
            '      "support_evidence": [{"evidence_id": "...-근거1", "quote": "..."}],\n'
            '      "oppose_evidence": [{"evidence_id": "...-근거1", "quote": "..."}],\n'
            '      "same_situation_case_exists": true,\n'
            '      "inference_basis": "",\n'
            '      "certainty": "medium",\n'
            '      "certainty_reason": "..."\n'
            "    }\n"
            "  ]\n"
            "}"
    )
    raw = call_chat(
        [
            {"role": "system", "content": "자료 증거 청크를 분석하고 JSON만 출력하라."},
            {"role": "user", "content": prompt},
        ],
        model=model,
        timeout=600,
    )
    try:
        obj = _normalize_chunk_analysis_obj(json.loads(_extract_json_block(raw)))
    except Exception:
        obj = {"claims_proposed": []}
    out_claims = []
    for item in obj.get("claims_proposed") or []:
        claim_text = _normalize_space(item.get("claim_text"))
        if not claim_text:
            continue
        source_file_id = _normalize_space(item.get("source_file_id"))
        if not source_file_id and len(source_lookup) == 1:
            source_file_id = next(iter(source_lookup))
        source_meta = source_lookup.get(source_file_id)
        if source_meta is None:
            if len(source_lookup) == 1:
                source_file_id, source_meta = next(iter(source_lookup.items()))
            else:
                continue
        support_entries = item.get("support_evidence")
        if not isinstance(support_entries, list):
            support_entries = item.get("support_quotes") or []
        oppose_entries = item.get("oppose_evidence")
        if not isinstance(oppose_entries, list):
            oppose_entries = item.get("oppose_quotes") or []
        evidence_prefix = _default_evidence_prefix_from_fields(
            relative_path=str(source_meta.get("relative_path") or ""),
            document_title=str(source_meta.get("document_title") or ""),
            file_id=str(source_meta.get("file_id") or source_file_id),
        )
        support_spans = _coerce_evidence_spans(support_entries, chunk, default_prefix=evidence_prefix)
        oppose_spans = _coerce_evidence_spans(oppose_entries, chunk, default_prefix=evidence_prefix)
        if not support_spans:
            continue
        certainty = _normalize_space(item.get("certainty")).lower() or "medium"
        if certainty not in CERTAINTY_ORDER:
            certainty = CERTAINTY_LABELS.get(certainty, "medium")
        if oppose_spans and certainty == "very_high":
            certainty = "high"
        base_claim = {
            "claim_text": claim_text,
            "recommended_section": _normalize_space(item.get("recommended_section")) or "나. 사안의 적용",
            "support_spans": support_spans,
            "oppose_spans": oppose_spans,
            "same_situation_case_exists": bool(item.get("same_situation_case_exists")),
            "inference_basis": _normalize_space(item.get("inference_basis")),
            "certainty": certainty,
            "certainty_reason": _normalize_space(item.get("certainty_reason")),
            "source_file_id": source_file_id,
            "source_chunk_id": chunk.chunk_id,
            "document_title": str(source_meta.get("document_title") or chunk.document_title),
            "relative_path": str(source_meta.get("relative_path") or chunk.relative_path),
            "record_number": str(source_meta.get("record_number") or chunk.record_number),
            "source_org": str(source_meta.get("source_org") or chunk.source_org),
            "record_date": str(source_meta.get("record_date") or chunk.record_date),
            "record_title": str(source_meta.get("record_title") or chunk.record_title),
        }
        if analysis_mode == "question":
            base_claim.update(
                {
                    "claim_axis": _normalize_space(item.get("claim_axis")) or claim_text,
                    "stance_to_user_goal": _normalize_space(item.get("stance_to_user_goal")) or "혼합",
                    "favorable_basis": _coerce_string_list(item.get("favorable_basis")),
                    "unfavorable_basis": _coerce_string_list(item.get("unfavorable_basis")),
                    "usable_favorable_logic": _coerce_string_list(item.get("usable_favorable_logic")),
                    "usable_unfavorable_logic": _coerce_string_list(item.get("usable_unfavorable_logic")),
                    "favorable_factors": _coerce_string_list(item.get("favorable_factors")),
                    "unfavorable_factors": _coerce_string_list(item.get("unfavorable_factors")),
                    "required_facts": _coerce_string_list(item.get("required_facts")),
                    "missing_facts": _coerce_string_list(item.get("missing_facts")),
                    "cautions": _coerce_string_list(item.get("cautions")),
                    "counter_evidence": _coerce_string_list(item.get("counter_evidence")),
                    "context_summary": _normalize_space(item.get("context_summary")),
                    "case_summary": _normalize_space(item.get("case_summary")),
                }
            )
        out_claims.append(base_claim)
    return {"chunk_id": chunk.chunk_id, "file_id": chunk.file_id, "claims_proposed": out_claims, "raw": raw}


def analyze_chunk_cached(
    chunk: ChunkRecord,
    *,
    user_task: str,
    model: str,
    cache_dir: Path,
    analysis_mode: str = "document",
) -> dict[str, Any]:
    cache_key = _analyze_cache_key(chunk, user_task=user_task, model=model, analysis_mode=analysis_mode)
    cache_path = cache_dir / f"{cache_key}.json"
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))
    try:
        result = analyze_chunk(chunk, user_task=user_task, model=model, analysis_mode=analysis_mode)
    except Exception as exc:
        result = {
            "chunk_id": chunk.chunk_id,
            "file_id": chunk.file_id,
            "claims_proposed": [],
            "error": str(exc),
            "document_title": chunk.document_title,
            "relative_path": chunk.relative_path,
        }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    return result


def _analyze_cache_key(chunk: ChunkRecord, *, user_task: str, model: str, analysis_mode: str = "document") -> str:
    payload = f"{ANALYZE_CACHE_SCHEMA_VERSION}\n{analysis_mode}\n{model}\n{user_task}\n{chunk.chunk_id}\n{chunk.text}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def analyze_chunks_cached(
    chunks: list[ChunkRecord],
    *,
    user_task: str,
    model: str,
    cache_dir: Path,
    analysis_mode: str = "document",
    workers: int = DEFAULT_ANALYZE_WORKERS,
    status_path: Path | None = None,
    variant_name: str | None = None,
) -> list[dict[str, Any]]:
    ordered_results: list[dict[str, Any] | None] = [None] * len(chunks)
    completed = 0
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(chunks) or 1))) as executor:
        future_map = {
            executor.submit(
                analyze_chunk_cached,
                chunk,
                user_task=user_task,
                model=model,
                cache_dir=cache_dir,
                analysis_mode=analysis_mode,
            ): index
            for index, chunk in enumerate(chunks)
        }
        for future in as_completed(future_map):
            index = future_map[future]
            ordered_results[index] = future.result()
            completed += 1
            if status_path is not None:
                write_runtime_status(
                    status_path,
                    {
                        "phase": "variant",
                        "variant": variant_name,
                        "state": "analyzing_chunks",
                        "completed_chunks": completed,
                        "chunk_count": len(chunks),
                    },
                )
            if completed % 10 == 0 or completed == len(chunks):
                print(f"[analyze] {completed}/{len(chunks)}", flush=True)
    return [result for result in ordered_results if result is not None]


def _certainty_sort_value(value: str) -> int:
    return CERTAINTY_ORDER.get(value, 0)


def _render_claim_for_prompt(claim: dict[str, Any]) -> str:
    support = "\n".join(
        f"- [근거ID: {span.get('evidence_id') or '(없음)'}] [{span.get('char_start')}:{span.get('char_end')}] {span.get('quote')}"
        for span in claim.get("support_spans") or []
    ) or "- (없음)"
    oppose = "\n".join(
        f"- [근거ID: {span.get('evidence_id') or '(없음)'}] [{span.get('char_start')}:{span.get('char_end')}] {span.get('quote')}"
        for span in claim.get("oppose_spans") or []
    ) or "- (없음)"
    return (
        f"주장: {claim.get('claim_text')}\n"
        f"문서: {claim.get('document_title')} ({claim.get('relative_path')})\n"
        f"확실성: {claim.get('certainty')}\n"
        f"동일상황 직접 적용례 존재: {claim.get('same_situation_case_exists')}\n"
        f"유추 근거: {claim.get('inference_basis') or '(없음)'}\n"
        f"지지근거:\n{support}\n"
        f"반대근거:\n{oppose}\n"
    )


def _is_direct_opinion_adverse_claim(claim_text: str) -> bool:
    text = _normalize_space(claim_text)
    adverse_markers = [
        "부당성을 주장하기 어렵",
        "반대되는 근거",
        "위법하지 않",
        "적법할 수 있",
        "적법하다고",
        "남용으로 보기 어렵",
        "정당성이 인정",
        "권한이 있다",
        "가중할 수 있",
        "정당한 재량",
        "한계가 있",
    ]
    return any(marker in text for marker in adverse_markers)


def render_direct_opinion_claim_cards(claims: list[dict[str, Any]]) -> str:
    ordered = sorted(
        [
            claim
            for claim in claims
            if claim.get("support_spans") and not _is_direct_opinion_adverse_claim(claim.get("claim_text", ""))
        ],
        key=lambda item: (
            -_certainty_sort_value(item.get("certainty", "")),
            -(len(item.get("support_spans") or [])),
            item.get("claim_text", ""),
        ),
    )
    if not ordered:
        return "- (없음)"

    cards: list[str] = []
    for index, claim in enumerate(ordered, 1):
        support_lines = "\n".join(
            f"- {span.get('evidence_id') or '(없음)'}: {span.get('quote')}"
            for span in claim.get("support_spans") or []
        ) or "- (없음)"
        oppose_lines = "\n".join(
            f"- {span.get('evidence_id') or '(없음)'}: {span.get('quote')}"
            for span in claim.get("oppose_spans") or []
        ) or "- (없음)"
        cards.append(
            "\n".join(
                [
                    f"[주장 카드 {index}]",
                    f"주장: {claim.get('claim_text')}",
                    f"확실성: {claim.get('certainty')}",
                    f"동일상황 직접 적용례 존재: {claim.get('same_situation_case_exists')}",
                    f"유추 근거: {claim.get('inference_basis') or '(없음)'}",
                    "지지근거:",
                    support_lines,
                    "반대근거:",
                    oppose_lines,
                ]
            )
        )
    return "\n\n".join(cards)


def render_chunk_outputs_prompt_style(
    rows: list[tuple[int, dict[str, Any]]],
    *,
    skip_empty_chunks: bool = True,
) -> str:
    sep = "\n\n" + ("=" * 80) + "\n\n"
    subsep = "\n" + ("-" * 40) + "\n"
    kept = []
    for idx, row in rows:
        claims = row.get("claims_proposed") or []
        if skip_empty_chunks and not claims:
            continue
        header = [
            f"CHUNK_INDEX: {idx}",
            f"CHUNK_ID: {row.get('chunk_id', '')}",
            f"FILE_ID: {row.get('file_id', '')}",
        ]
        if claims:
            first = claims[0]
            header.extend(
                [
                    f"DOCUMENT_TITLE: {first.get('document_title', '')}",
                    f"RELATIVE_PATH: {first.get('relative_path', '')}",
                    f"CLAIM_COUNT: {len(claims)}",
                ]
            )
        else:
            header.extend(
                [
                    f"DOCUMENT_TITLE: {row.get('document_title', '')}",
                    f"RELATIVE_PATH: {row.get('relative_path', '')}",
                    "CLAIM_COUNT: 0",
                ]
            )
        block_parts = ["\n".join(header)]
        if not claims:
            block_parts.append(
                "주장: (없음)\n문서: (없음)\n확실성: (없음)\n동일상황 직접 적용례 존재: False\n유추 근거: (없음)\n지지근거:\n- (없음)\n반대근거:\n- (없음)"
            )
        else:
            for claim in claims:
                block_parts.append(_render_claim_for_prompt(claim).strip())
        kept.append(sep + subsep.join(block_parts))

    parts = [
        "# Variant Chunk Outputs In Prompt Style",
        "",
        f"- chunk_count_input: {len(rows)}",
        f"- chunk_count_rendered: {len(kept)}",
        f"- skip_empty_chunks: {skip_empty_chunks}",
        "- format: per-chunk separator + human-readable claim blocks",
    ]
    parts.extend(kept)
    return "\n".join(parts).strip() + "\n"


def collect_nonempty_chunk_ids_from_jsonl(path: Path) -> list[str]:
    out: list[str] = []
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            if not line.strip():
                continue
            row = json.loads(line)
            chunk_id = str(row.get("chunk_id") or "").strip()
            claims = row.get("claims_proposed") or []
            if chunk_id and claims:
                out.append(chunk_id)
    return out


def _pack_claims_for_writing(claims: list[dict[str, Any]], *, max_tokens: int) -> list[list[dict[str, Any]]]:
    blocks = [(str(index), _render_claim_for_prompt(claim)) for index, claim in enumerate(claims)]
    packs = pack_text_blocks(blocks, max_tokens=max_tokens)
    out: list[list[dict[str, Any]]] = []
    for pack in packs:
        ids = {key for key, _ in pack}
        out.append([claim for index, claim in enumerate(claims) if str(index) in ids])
    return out


def synthesize_section_packets(claims: list[dict[str, Any]], *, model: str, max_tokens: int) -> dict[str, list[str]]:
    by_section: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for claim in claims:
        by_section[claim.get("recommended_section") or "나. 사안의 적용"].append(claim)
    packets: dict[str, list[str]] = {}
    for section, rows in by_section.items():
        rows = sorted(rows, key=lambda item: (-_certainty_sort_value(item.get("certainty", "")), -len(item.get("support_spans") or []), item.get("claim_text", "")))
        pack_limit = max_tokens - 12_000
        for subset in _pack_claims_for_writing(rows, max_tokens=max(20_000, pack_limit)):
            prompt = (
                f"아래는 의견서의 섹션 `{section}` 에 들어갈 주장 ledger 다.\n"
                "주장들을 중복 없이 정리하되, 반대근거와 유추 여부를 누락하지 말고 한국어 정리문으로 써라.\n"
                "아직 최종 의견서 문체로 다듬지 말고, 이 섹션에 들어갈 핵심 논거 정리문만 써라.\n\n"
                + "\n\n".join(_render_claim_for_prompt(claim) for claim in subset)
            )
            text = call_chat(
                [
                    {"role": "system", "content": "자료 주장 ledger를 섹션용 정리문으로 합친다."},
                    {"role": "user", "content": prompt},
                ],
                model=model,
                timeout=600,
            )
            packets.setdefault(section, []).append(text.strip())
    return packets


def parse_used_evidence_ids(text: str) -> list[str]:
    lines = text.splitlines()
    in_block = False
    ids: list[str] = []
    seen: set[str] = set()
    for raw_line in lines:
        line = raw_line.strip()
        if not in_block:
            if line == "[사용한 근거 ID]" or line.endswith("[사용한 근거 ID]"):
                in_block = True
            continue
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            inner = line[1:-1].strip()
            if not inner:
                continue
            if inner == "사용한 근거 ID":
                continue
            if inner.endswith("-근거1") or "-근거" in inner:
                line = inner
            else:
                break
        if line.startswith("- "):
            line = line[2:].strip()
        elif line.startswith("* "):
            line = line[2:].strip()
        if not line or line in seen:
            continue
        seen.add(line)
        ids.append(line)
    return ids


def collect_selected_evidence_bundle(
    claims: list[dict[str, Any]],
    selected_evidence_ids: list[str],
) -> list[dict[str, Any]]:
    evidence_map: dict[str, dict[str, Any]] = {}
    for claim in claims:
        document_title = claim.get("document_title") or ""
        relative_path = claim.get("relative_path") or ""
        for side_key, evidence_side in (("support_spans", "support"), ("oppose_spans", "oppose")):
            for span in claim.get(side_key) or []:
                evidence_id = _normalize_space(span.get("evidence_id"))
                if not evidence_id or evidence_id in evidence_map:
                    continue
                evidence_map[evidence_id] = {
                    "evidence_id": evidence_id,
                    "document_title": document_title,
                    "relative_path": relative_path,
                    "quote": span.get("quote") or "",
                    "char_start": span.get("char_start"),
                    "char_end": span.get("char_end"),
                    "evidence_side": evidence_side,
                }
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for evidence_id in selected_evidence_ids:
        normalized = _normalize_space(evidence_id)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        row = evidence_map.get(normalized)
        if row:
            out.append(row)
    return out


def render_selected_evidence_bundle(selected_evidence_bundle: list[dict[str, Any]]) -> str:
    if not selected_evidence_bundle:
        return "- (없음)"
    parts: list[str] = []
    for item in selected_evidence_bundle:
        parts.append(
            "\n".join(
                [
                    f"근거ID: {item.get('evidence_id')}",
                    f"문서: {item.get('document_title')} ({item.get('relative_path')})",
                    f"유형: {item.get('evidence_side')}",
                    f"원문: [{item.get('char_start')}:{item.get('char_end')}] {item.get('quote')}",
                ]
            )
        )
    return "\n\n".join(parts)


DOCUMENT_META_LEAK_PATTERNS = (
    re.compile(r"though the user'?s task", re.IGNORECASE),
    re.compile(r"Wait,\s*let'?s re-read", re.IGNORECASE),
    re.compile(r"Subject Matter\s*:", re.IGNORECASE),
    re.compile(r"\bClaim\s+\d+\s*:", re.IGNORECASE),
    re.compile(r"\bStructure\s*:", re.IGNORECASE),
    re.compile(r"\bTone\s*:", re.IGNORECASE),
    re.compile(r"Drafting Content\s*:", re.IGNORECASE),
    re.compile(r"Refining the", re.IGNORECASE),
    re.compile(r"Check against constraints", re.IGNORECASE),
    re.compile(r"Final Review of the Ledger usage", re.IGNORECASE),
    re.compile(r"Formatting\s*:\s*Plain text", re.IGNORECASE),
    re.compile(r"Drafting the final response", re.IGNORECASE),
    re.compile(r"Proceeding to generate", re.IGNORECASE),
    re.compile(r"Self[-\s]?Correction", re.IGNORECASE),
)

DOCUMENT_OUTPUT_START_PATTERNS = (
    re.compile(
        r"(?P<start>자료\s*검토\s*의견서|구조화\s*의견서|구조화의견서|통지문|요청서|신청서|준비문서|답변문서|이의제기서|의견서|접수문서)\b"
    ),
    re.compile(r"(?m)(?P<start>^사\s*건\s+.+$)"),
    re.compile(r"(?m)(?P<start>^수\s*신(?:인)?\s*[:：]?\s*.+$)"),
    re.compile(r"(?m)(?P<start>^발\s*신(?:인)?\s*[:：]?\s*.+$)"),
    re.compile(r"(?m)(?P<start>^제\s*목\s*[:：]?\s*.+$)"),
)


def _document_output_has_meta_leak(text: str) -> bool:
    raw = str(text or "")
    return any(pattern.search(raw) for pattern in DOCUMENT_META_LEAK_PATTERNS)


def _find_document_output_start(raw: str, *, after: int) -> int | None:
    search_area = raw[max(0, after) :]
    candidates: list[int] = []
    for pattern in DOCUMENT_OUTPUT_START_PATTERNS:
        match = pattern.search(search_area)
        if match:
            candidates.append(max(0, after) + match.start("start"))
    return min(candidates) if candidates else None


def sanitize_document_output(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    if not _document_output_has_meta_leak(raw):
        return raw
    marker_ends = [
        match.end()
        for pattern in DOCUMENT_META_LEAK_PATTERNS
        for match in pattern.finditer(raw)
    ]
    after = max(marker_ends) if marker_ends else 0
    start = _find_document_output_start(raw, after=after)
    if start is None:
        return raw
    stripped = raw[start:].strip()
    return stripped or raw


def write_final_opinion(
    *,
    user_task: str,
    sample_texts: list[str],
    claims: list[dict[str, Any]],
    section_packets: dict[str, list[str]],
    model: str,
    extra_requirements: str = "",
) -> str:
    prompt = build_final_opinion_prompt(
        user_task=user_task,
        sample_texts=sample_texts,
        claims=claims,
        section_packets=section_packets,
        extra_requirements=extra_requirements,
    )
    output = call_chat(
        [
            {"role": "system", "content": "자료 의견서 작성자다. 샘플 형식은 따르되 이번 ledger만 근거로 사용하라."},
            {"role": "user", "content": prompt},
        ],
        model=model,
        timeout=900,
    ).strip()
    return sanitize_document_output(output)


def build_draft2_extra_requirements() -> str:
    return (
        "예시 형식을 철저하게 따라서, 예시가 자료문서라면 기억 레코드를 인용하는 방식, 논리를 적용하는 방식, 주장을 전개하는 방식을 그대로 따를 것.\n"
        "근거들은 광범위한 자료 속에서 관대한 조건으로 수집된 것이므로, 근거 자체의 존재는 의심하지 말되 해당 근거가 이 사안에 실제로 적합한지, 해당 근거 해석이 정확한지는 엄밀하게 다시 판단할 것.\n"
        "근거는 쓸 만하지만 기존 주장이 잘못 해석한 경우, 그 근거를 비약 없이 다시 해석하여 사용할 수 있다. 다만 근거를 과장하거나 잘못 읽어 새로운 논리를 함부로 만들어내지는 말 것.\n"
        "실제로 이것과 같은 상황에서 적용된 사례가 있는 주장만 본문 핵심 논거로 채용할 것. 직접 적용 사례가 없으면 그 한계를 분명히 적고, 유추 적용임을 숨기지 말 것.\n"
        "근거가 기억 레코드라면 본문에서 그 자료명이나 발행 정보가 드러나게 쓰고, 근거가 자료이나 규정이라면 그 법령명 또는 규정명을 정확히 적을 것. 다만 근거 ID 표기를 기계적으로 나열할 필요는 없다."
    )


def build_case_analysis_extra_requirements() -> str:
    source_ref_requirements = "\n".join(build_draft2_extra_requirements().splitlines()[1:])
    objectivity_requirements = (
        "꼭 사용자에게 유리한쪽으로만 해석하지말고, 객관적으로 해석하여 불리하다면 불리하다고 말하고 그 구체적 이유를 말해야 한다.\n"
        "판단하기 어렵다면 어려운 이유를 설명해야 한다.\n"
        "불리하다면 불리한 이유를 말한 뒤, 그럼에도 최대한 유리하게 만들 방법을 실제 유사 사례가 있는 범위에서 설명해야 한다.\n"
        "유리하다면 그 유리함을 유지하거나 강화하기 위해 실무적으로 어떤 대응을 해야 하는지도 실제 근거에 기대어 설명해야 한다.\n"
        "불리하거나 어렵게 보이는 쟁점이라도, 완전히 무리한 주장이 아니고 근거가 있다면 버리지 말 것.\n"
        "가능성이 낮더라도 써볼 만한 유리한 주장은 별도의 대응 카드로 정리하고, 받아들여진 사례와 받아들여지지 않은 사례가 함께 있으면 둘 다 설명할 것."
    )
    return source_ref_requirements + "\n" + objectivity_requirements


def build_final_opinion_prompt(
    *,
    user_task: str,
    sample_texts: list[str],
    claims: list[dict[str, Any]],
    section_packets: dict[str, list[str]],
    extra_requirements: str = "",
) -> str:
    top_claims = claims[:50]
    extra_block = f"\n[추가 작성 지침]\n{extra_requirements}\n" if extra_requirements.strip() else ""
    prompt = (
        "아래 두 개의 구조화 의견서 샘플 텍스트를 형식 예시로 삼아, 이번 사건에 대한 구조화 의견서 텍스트를 작성하라.\n"
        "중요:\n"
        "- 샘플의 문체와 구성감을 따르되 그대로 베끼지 말 것\n"
        "- 사용자 지시에 최대한 부합하고 사용자에게 유리하게 작성하되, 근거 없는 주장이나 과장은 하지 말 것\n"
        "- 불리한 내용을 독립된 본론으로 크게 부각하지 말 것. 불리한 사정이 필요하다면 반박, 구별, 완화, 양정 감경 논리와 함께 최소한으로만 다룰 것\n"
        "- 받아들여질 가능성이 낮더라도 완전히 무리한 주장이 아니고 근거가 있다면, 사용자에게 유리한 주장으로 정리하여 포함할 것\n"
        "- 아래 ledger와 section packet 에 있는 내용만 근거로 사용할 것\n"
        "- support/opposition/certainty를 반영할 것\n"
        "- 반대근거가 있는 쟁점은 단정하지 말 것\n"
        "- 결과물은 평문 텍스트로만 출력할 것\n"
        "- 1. 2. 3. / 가. 나. 다. / 4. / 첨부서류 구조를 자연스럽게 따를 것\n\n"
        f"사용자 과제:\n{user_task}\n\n"
        f"{extra_block}"
        "[샘플 1 전체]\n"
        f"{sample_texts[0]}\n\n"
        "[샘플 2 전체]\n"
        f"{sample_texts[1]}\n\n"
        "[핵심 claim ledger]\n"
        + "\n\n".join(_render_claim_for_prompt(claim) for claim in top_claims)
    )
    if section_packets:
        prompt += "\n\n[section packets]\n" + "\n\n".join(
            f"## {section}\n" + "\n\n".join(parts) for section, parts in section_packets.items()
        )
    return prompt


def build_final_opinion_from_analysis_prompt(
    *,
    user_task: str,
    sample_texts: list[str],
    case_analysis_text: str,
    selected_evidence_bundle: list[dict[str, Any]],
    extra_requirements: str = "",
) -> str:
    extra_block = f"\n[추가 작성 지침]\n{extra_requirements}\n" if extra_requirements.strip() else ""
    return (
        "아래 두 개의 구조화 의견서 샘플 텍스트를 형식 예시로 삼아, 이번 사건에 대한 구조화 의견서 텍스트를 작성하라.\n"
        "중요:\n"
        "- 샘플의 문체와 구성감을 따르되 그대로 베끼지 말 것\n"
        "- 사용자 지시에 최대한 부합하고 사용자에게 유리하게 작성하되, 근거 없는 주장이나 과장은 하지 말 것\n"
        "- 불리한 내용을 독립된 본론으로 크게 부각하지 말 것. 불리한 사정이 필요하다면 반박, 구별, 완화, 양정 감경 논리와 함께 최소한으로만 다룰 것\n"
        "- 받아들여질 가능성이 낮더라도 완전히 무리한 주장이 아니고 근거가 있다면, 사용자에게 유리한 주장으로 정리하여 포함할 것\n"
        "- 유리한 논거는 추상적 공정성 문구보다 명시적 법리 명칭으로 제시할 것. 가능한 경우 `이중쟁점`, `신의칙` 또는 `신뢰보호`, `재량권 일탈·남용`, `방어권 침해`, `비례원칙 위반` 같은 법리 이름을 표제와 첫 문장에서 분명히 밝힐 것\n"
        "- 같은 사실관계에서 복수의 유리한 법리가 각자 성립 가능하다면, 이를 하나의 보수적인 공정성 문구로만 축소하지 말 것. 본문에서 병렬적 또는 예비적으로 구분하여 제시할 것\n"
        "- 사건분석 문서가 가능성 낮음 또는 난점이 있다고 정리한 쟁점이라도, 선택 근거 원문에 그 쟁점을 직접 지지하는 구체적 법령이나 기억 레코드가 있으면 보조적 또는 예비적 주장으로 포함할 것\n"
        "- 아래 사건분석 문서와 선택 근거 원문에 있는 내용만 근거로 사용할 것\n"
        "- 사건분석 문서는 내부 검토 메모로만 취급할 것. 그 문서의 균형적 서술이나 위험 정리 구조를 최종 의견서의 구조로 답습하지 말 것\n"
        "- 사건분석 문서 안의 `불리한 쟁점`, `불리한 이유` 같은 표제어나 배열을 최종 의견서의 표제어나 본문 구조로 옮기지 말 것\n"
        "- 사건분석 문서의 서술을 그대로 복사하지 말고, 선택 근거 원문으로 다시 확인하여 의견서 문장으로 재구성할 것\n"
        "- 반대근거나 한계가 드러난 쟁점은 무리하게 단정하지 말 것\n"
        "- 결과물은 평문 텍스트로만 출력할 것\n"
        "- 1. 2. 3. / 가. 나. 다. / 4. / 첨부서류 구조를 자연스럽게 따를 것\n\n"
        f"사용자 과제:\n{user_task}\n\n"
        f"{extra_block}"
        "[샘플 1 전체]\n"
        f"{sample_texts[0]}\n\n"
        "[샘플 2 전체]\n"
        f"{sample_texts[1]}\n\n"
        "[사건분석 및 대응 문서]\n"
        f"{case_analysis_text.strip()}\n\n"
        "[선택 근거 원문]\n"
        f"{render_selected_evidence_bundle(selected_evidence_bundle)}\n"
    )


def build_direct_final_opinion_prompt(
    *,
    user_task: str,
    sample_texts: list[str],
    incident_material_texts: list[tuple[str, str]],
    claims: list[dict[str, Any]],
    extra_requirements: str = "",
) -> str:
    extra_block = f"\n[추가 작성 지침]\n{extra_requirements}\n" if extra_requirements.strip() else ""
    incident_blocks = []
    for index, (name, text) in enumerate(incident_material_texts, 1):
        incident_blocks.append(f"[사건자료 원문 {index}]\n자료명: {name}\n{text.strip()}\n")
    return (
        "아래 두 개의 구조화 의견서 샘플 텍스트를 형식 예시로 삼아, 이번 사건에 대한 구조화 의견서 텍스트를 작성하라.\n"
        "중요:\n"
        "- 샘플의 문체와 구성감을 따르되 그대로 베끼지 말 것\n"
        "- 사용자 지시에 최대한 부합하고 사용자에게 유리하게 작성하되, 근거 없는 주장이나 과장은 하지 말 것\n"
        "- 아래 사건자료 원문과 주장별 근거 카드만 근거로 사용할 것\n"
        "- 후보 주장 카드에는 사용자에게 유리한 주장과 불리한 주장, 반대근거가 함께 섞여 있을 수 있다. 최종 의견서에는 사용자에게 유리한 주장 카드만 채택할 것\n"
        "- 불리한 카드나 반대 카드의 문구를 독립된 본론으로 쓰지 말 것\n"
        "- 사건자료 원문과 맞지 않는 주장 카드는 채택하지 말 것\n"
        "- 받아들여질 가능성이 낮더라도 완전히 무리한 주장이 아니고 사건자료와 근거카드가 함께 뒷받침하면 보조적 또는 예비적 주장으로 포함할 것\n"
        "- 사건자료 원문이 특정 법리의 모든 사실요건을 완전히 닫아주지 않더라도, 그 법리가 사건자료에 의해 명백히 배척되지 않고 근거카드에 직접적인 기억 레코드나 법령이 있으면 예비적 주장으로 포함할 것\n"
        "- 같은 사실관계에서 복수의 유리한 법리가 각자 성립 가능하다면 하나의 뭉뚱그린 공정성 표현으로 축소하지 말고, 별개의 유리한 공격축으로 병렬적 또는 예비적으로 구분하여 제시할 것\n"
        "- 주장별 근거 카드에 직접적인 기억 레코드나 법령이 붙은 별도 공격축은, 사건자료가 이를 명시적으로 배척하지 않는 한 누락하지 말고 조건부 문장이나 예비적 주장 형식으로라도 유지할 것\n"
        "- 가능한 경우 각 논점 문단 안에 사용한 기억 레코드명이나 법령명을 직접 드러낼 것\n"
        "- 결과물은 평문 텍스트로만 출력할 것\n"
        "- 1. 2. 3. / 가. 나. 다. / 4. / 첨부서류 구조를 자연스럽게 따를 것\n\n"
        f"사용자 과제:\n{user_task}\n\n"
        f"{extra_block}"
        "[샘플 1 전체]\n"
        f"{sample_texts[0]}\n\n"
        "[샘플 2 전체]\n"
        f"{sample_texts[1]}\n\n"
        + "\n\n".join(incident_blocks)
        + "\n\n[주장별 근거 카드]\n"
        + render_direct_opinion_claim_cards(claims)
    )


def build_case_analysis_prompt(
    *,
    user_task: str,
    claims: list[dict[str, Any]],
    extra_requirements: str = "",
) -> str:
    top_claims = claims[:80]
    extra_block = f"\n[추가 작성 지침]\n{extra_requirements}\n" if extra_requirements.strip() else ""
    return (
        "아래 claim ledger만을 근거로 사건분석 및 대응 문서를 작성하라.\n"
        "중요:\n"
        "- 형식 샘플은 따르지 말고, 분석 문서로서 명확하고 체계적으로 쓸 것\n"
        "- 아래 ledger에 있는 내용만 근거로 사용할 것\n"
        "- 사용자에게 유리한 점과 불리한 점을 함께 정리할 것\n"
        "- 불리한 경우에는 왜 불리한지, 그럼에도 어떤 대응이 가능한지 실제 사례와 연결해 설명할 것\n"
        "- 판단이 어려운 쟁점은 그 이유를 숨기지 말고 적을 것\n"
        "- 문서 맨 마지막에 `[사용한 근거 ID]` 섹션을 두고, 본문 작성에 실제로 사용한 정확한 근거 ID만 한 줄에 하나씩 적을 것\n"
        "- 근거 ID는 아래 ledger에 적힌 정확한 문자열만 그대로 사용할 것\n"
        "- 결과물은 평문 텍스트로만 출력할 것\n\n"
        f"사용자 과제:\n{user_task}\n\n"
        f"{extra_block}"
        "[핵심 claim ledger]\n"
        + "\n\n".join(_render_claim_for_prompt(claim) for claim in top_claims)
    )


def write_case_analysis_document(
    *,
    user_task: str,
    claims: list[dict[str, Any]],
    model: str,
    extra_requirements: str = "",
) -> str:
    prompt = build_case_analysis_prompt(
        user_task=user_task,
        claims=claims,
        extra_requirements=extra_requirements,
    )
    return call_chat(
        [
            {
                "role": "system",
                "content": "자료 사건을 객관적으로 분석하고, 유불리와 대응 방향을 근거 중심으로 정리하는 실무 문서 작성자다.",
            },
            {"role": "user", "content": prompt},
        ],
        model=model,
        timeout=900,
    ).strip()


def write_final_opinion_from_analysis(
    *,
    user_task: str,
    sample_texts: list[str],
    case_analysis_text: str,
    selected_evidence_bundle: list[dict[str, Any]],
    model: str,
    extra_requirements: str = "",
) -> str:
    prompt = build_final_opinion_from_analysis_prompt(
        user_task=user_task,
        sample_texts=sample_texts,
        case_analysis_text=case_analysis_text,
        selected_evidence_bundle=selected_evidence_bundle,
        extra_requirements=extra_requirements,
    )
    output = call_chat(
        [
            {
                "role": "system",
                "content": "자료 의견서 작성자다. 샘플 형식은 따르되 사건분석과 선택 근거 원문만을 기반으로 사용하라.",
            },
            {"role": "user", "content": prompt},
        ],
        model=model,
        timeout=900,
    ).strip()
    return sanitize_document_output(output)


def write_direct_final_opinion(
    *,
    user_task: str,
    sample_texts: list[str],
    incident_material_texts: list[tuple[str, str]],
    claims: list[dict[str, Any]],
    model: str,
    extra_requirements: str = "",
) -> str:
    prompt = build_direct_final_opinion_prompt(
        user_task=user_task,
        sample_texts=sample_texts,
        incident_material_texts=incident_material_texts,
        claims=claims,
        extra_requirements=extra_requirements,
    )
    output = call_chat(
        [
            {
                "role": "system",
                "content": "자료 의견서 작성자다. 샘플 형식은 따르되 사건자료 원문과 주장별 근거 카드만을 기반으로 사용하라.",
            },
            {"role": "user", "content": prompt},
        ],
        model=model,
        timeout=900,
    ).strip()
    return sanitize_document_output(output)


def _read_sample_texts(sample_paths: list[Path], cache_dir: Path) -> list[str]:
    texts = []
    for sample in sample_paths:
        texts.append(build_file_record(sample.parent, sample, cache_dir=cache_dir).extracted_text)
    return texts


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_runtime_status(path: Path, payload: dict[str, Any]) -> None:
    merge_runtime_status(path, payload, replace=True)


def merge_runtime_status(path: Path, payload: dict[str, Any], *, replace: bool = False) -> None:
    with _RUNTIME_STATUS_LOCK:
        data: dict[str, Any] = {}
        if not replace and path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(existing, dict):
                    data.update(existing)
            except Exception:
                data = {}
        data.update(payload)
        data["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        limits = get_runtime_limits()
        data.setdefault("runtime_limits", limits)
        _write_json(path, data)


def compute_attempt_timeout_seconds(
    *,
    total_timeout_s: float,
    started_at_s: float,
    now_s: float,
    per_attempt_cap_s: float = 30.0,
    min_attempt_timeout_s: float = 5.0,
) -> float | None:
    remaining = float(total_timeout_s) - max(0.0, float(now_s) - float(started_at_s))
    if remaining <= 0:
        return None
    if remaining <= min_attempt_timeout_s:
        return float(min_attempt_timeout_s)
    return float(min(per_attempt_cap_s, remaining))


def select_generate_timeout_cap(model_name: str, *, estimated_prompt_tokens: int) -> float:
    return 600.0


def record_runtime_http_activity(
    status_path: Path | None,
    *,
    event: str,
    kind: str,
    model_name: str,
    detail: str = "",
) -> None:
    if status_path is None:
        return
    with _RUNTIME_STATUS_LOCK:
        if event == "start":
            _RUNTIME_HTTP_STATE["http_started"] += 1
            _RUNTIME_HTTP_STATE["http_inflight"] += 1
        elif event == "finish":
            _RUNTIME_HTTP_STATE["http_finished"] += 1
            _RUNTIME_HTTP_STATE["http_inflight"] = max(0, _RUNTIME_HTTP_STATE["http_inflight"] - 1)
        payload = {
            "last_api_activity_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "last_api_kind": kind,
            "last_api_model": model_name,
            "last_api_event": event,
            "last_api_result": detail,
            **_RUNTIME_HTTP_STATE,
        }
    merge_runtime_status(status_path, payload)


def _ensure_runtime_control_file(path: Path) -> None:
    if path.exists():
        return
    _write_json(
        path,
        {
            "gemini_key_min_gap_ms": GEMINI_KEY_MIN_GAP_MS,
            "gemini_key_max_inflight": GEMINI_KEY_MAX_INFLIGHT,
            "gemini_key_rpm_limit": GEMINI_KEY_RPM_LIMIT,
            "gemini_key_tpm_limit": GEMINI_KEY_TPM_LIMIT,
            "gemini_global_max_inflight": GEMINI_GLOBAL_MAX_INFLIGHT,
        },
    )


def _apply_runtime_control_overrides(
    path: Path,
    *,
    gemini_key_min_gap_ms: int | None = None,
    gemini_key_max_inflight: int | None = None,
    gemini_key_rpm_limit: int | None = None,
    gemini_key_tpm_limit: int | None = None,
    gemini_global_max_inflight: int | None = None,
) -> None:
    payload = {}
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            payload.update(loaded)
    if isinstance(gemini_key_min_gap_ms, int) and gemini_key_min_gap_ms > 0:
        payload["gemini_key_min_gap_ms"] = gemini_key_min_gap_ms
    if isinstance(gemini_key_max_inflight, int) and gemini_key_max_inflight > 0:
        payload["gemini_key_max_inflight"] = gemini_key_max_inflight
    if isinstance(gemini_key_rpm_limit, int) and gemini_key_rpm_limit >= 0:
        payload["gemini_key_rpm_limit"] = gemini_key_rpm_limit
    if isinstance(gemini_key_tpm_limit, int) and gemini_key_tpm_limit >= 0:
        payload["gemini_key_tpm_limit"] = gemini_key_tpm_limit
    if isinstance(gemini_global_max_inflight, int) and gemini_global_max_inflight >= 0:
        payload["gemini_global_max_inflight"] = gemini_global_max_inflight
    _write_json(path, payload or {
        "gemini_key_min_gap_ms": GEMINI_KEY_MIN_GAP_MS,
        "gemini_key_max_inflight": GEMINI_KEY_MAX_INFLIGHT,
        "gemini_key_rpm_limit": GEMINI_KEY_RPM_LIMIT,
        "gemini_key_tpm_limit": GEMINI_KEY_TPM_LIMIT,
        "gemini_global_max_inflight": GEMINI_GLOBAL_MAX_INFLIGHT,
    })


def _variant_dir(root: Path, name: str) -> Path:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _records_by_id(records: list[FileRecord]) -> dict[str, FileRecord]:
    return {record.file_id: record for record in records}


def _build_chunks_for_record(record: FileRecord, *, max_tokens: int) -> list[ChunkRecord]:
    raw_chunks = chunk_document_text(
        text=record.extracted_text,
        max_tokens=max_tokens,
        overlap_chars=DEFAULT_QUOTE_CHARS,
        same_document_group=record.file_id,
        file_id=record.file_id,
        document_title=record.document_title,
        relative_path=record.relative_path,
    )
    return [
        ChunkRecord(
            **raw,
            record_number=record.record_number,
            source_org=record.source_org,
            record_date=record.record_date,
            record_title=record.record_title,
            source_segments=[
                {
                    "file_id": record.file_id,
                    "document_title": record.document_title,
                    "relative_path": record.relative_path,
                    "record_number": record.record_number,
                    "source_org": record.source_org,
                    "record_date": record.record_date,
                    "record_title": record.record_title,
                    "start_char": raw["start_char"],
                    "end_char": raw["end_char"],
                    "text": raw["text"],
                    "token_count": raw["token_count"],
                    "excerpt": " ".join(str(raw["text"]).split())[:220],
                }
            ],
        )
        for raw in raw_chunks
    ]


def _build_question_source_segment_from_record(record: FileRecord) -> dict[str, Any]:
    return {
        "file_id": record.file_id,
        "document_title": record.document_title,
        "relative_path": record.relative_path,
        "record_number": record.record_number,
        "source_org": record.source_org,
        "record_date": record.record_date,
        "record_title": record.record_title,
        "start_char": 0,
        "end_char": len(record.extracted_text),
        "text": record.extracted_text,
        "token_count": _count_tokens(record.extracted_text),
        "excerpt": " ".join(record.extracted_text.split())[:220],
    }


def _render_question_source_segment(segment: dict[str, Any], *, index: int) -> str:
    return (
        f"[SOURCE {index}]\n"
        f"source_file_id: {segment.get('file_id') or ''}\n"
        f"document_title: {segment.get('document_title') or ''}\n"
        f"relative_path: {segment.get('relative_path') or ''}\n"
        f"record_number: {segment.get('record_number') or ''}\n"
        f"source_org: {segment.get('source_org') or ''}\n"
        f"record_date: {segment.get('record_date') or ''}\n"
        f"record_title: {segment.get('record_title') or ''}\n"
        "본문:\n"
        f"{segment.get('text') or ''}\n"
        f"[END SOURCE {index}]"
    )


def _build_question_pack_chunk(
    segments: list[dict[str, Any]],
    *,
    pack_index: int,
    chunk_id: str | None = None,
    file_id: str | None = None,
    same_document_group: str | None = None,
    relative_path: str | None = None,
) -> ChunkRecord:
    rendered_parts = [_render_question_source_segment(segment, index=index) for index, segment in enumerate(segments, start=1)]
    text = "\n\n" + ("\n\n" + ("=" * 72) + "\n\n").join(rendered_parts) + "\n"
    lead = segments[0] if segments else {}
    resolved_chunk_id = chunk_id or f"question-pack-{pack_index:03d}"
    resolved_file_id = file_id or resolved_chunk_id
    return ChunkRecord(
        chunk_id=resolved_chunk_id,
        file_id=resolved_file_id,
        document_title=f"질문 기억 레코드 팩 {pack_index}",
        same_document_group=same_document_group or resolved_file_id,
        relative_path=relative_path or f"packed/question/{pack_index:03d}",
        start_char=0,
        end_char=len(text),
        text=text,
        token_count=_count_tokens(text),
        record_number=str(lead.get("record_number") or ""),
        source_org=str(lead.get("source_org") or ""),
        record_date=str(lead.get("record_date") or ""),
        record_title=str(lead.get("record_title") or ""),
        source_segments=segments,
    )


def _question_source_segment_token_count(segment: dict[str, Any], *, index: int = 1) -> int:
    return _count_tokens(_render_question_source_segment(segment, index=index))


def build_question_chunks_for_records(
    records: list[FileRecord],
    *,
    max_tokens: int,
    workers: int = DEFAULT_CHUNK_BUILD_WORKERS,
    status_path: Path | None = None,
    variant_name: str | None = None,
) -> list[ChunkRecord]:
    if not records:
        return []

    packs: list[ChunkRecord] = []
    current_segments: list[dict[str, Any]] = []
    current_token_count = 0
    pack_index = 1

    def flush() -> None:
        nonlocal current_segments, current_token_count, pack_index
        if not current_segments:
            return
        packs.append(_build_question_pack_chunk(current_segments, pack_index=pack_index))
        pack_index += 1
        current_segments = []
        current_token_count = 0

    if status_path is not None:
        write_runtime_status(
            status_path,
            {
                "phase": "variant",
                "variant": variant_name,
                "state": "chunking_records",
                "selected_file_count": len(records),
                "chunk_count": 0,
            },
        )

    for index, record in enumerate(records, start=1):
        full_segment = _build_question_source_segment_from_record(record)
        full_rendered_tokens = _question_source_segment_token_count(full_segment, index=1)

        if full_rendered_tokens <= max_tokens:
            separator_tokens = 0 if not current_segments else 32
            if current_segments and current_token_count + separator_tokens + full_rendered_tokens > max_tokens:
                flush()
                separator_tokens = 0
            current_segments.append(full_segment)
            current_token_count += separator_tokens + full_rendered_tokens
        else:
            flush()
            split_limit = max(256, int(max_tokens * 0.8))
            split_chunks = _build_chunks_for_record(record, max_tokens=split_limit)
            for split_chunk in split_chunks:
                segment = split_chunk.source_segments[0] if split_chunk.source_segments else _build_question_source_segment_from_record(record)
                packs.append(
                    _build_question_pack_chunk(
                        [segment],
                        pack_index=pack_index,
                        chunk_id=split_chunk.chunk_id,
                        file_id=split_chunk.file_id,
                        same_document_group=split_chunk.same_document_group,
                        relative_path=split_chunk.relative_path,
                    )
                )
                pack_index += 1

        if status_path is not None:
            merge_runtime_status(
                status_path,
                {
                    "phase": "variant",
                    "variant": variant_name,
                    "state": "chunking_records",
                    "selected_file_count": len(records),
                    "completed_records": index,
                    "total_records": len(records),
                },
            )
    flush()
    if status_path is not None:
        write_runtime_status(
            status_path,
            {
                "phase": "variant",
                "variant": variant_name,
                "state": "packing_chunks",
                "selected_file_count": len(records),
                "chunk_count": len(packs),
            },
        )
    return packs


def _build_chunks_for_records(
    records: list[FileRecord],
    *,
    max_tokens: int,
    workers: int = DEFAULT_CHUNK_BUILD_WORKERS,
    status_path: Path | None = None,
    variant_name: str | None = None,
) -> list[ChunkRecord]:
    if not records:
        return []
    ordered_chunks: list[list[ChunkRecord] | None] = [None] * len(records)
    completed = 0
    estimated_chunk_count = 0
    max_workers = max(1, min(workers, len(records)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_build_chunks_for_record, record, max_tokens=max_tokens): index
            for index, record in enumerate(records)
        }
        for future in as_completed(future_map):
            index = future_map[future]
            record_chunks = future.result()
            ordered_chunks[index] = record_chunks
            completed += 1
            estimated_chunk_count += len(record_chunks)
            if status_path is not None and (completed == 1 or completed % 5 == 0 or completed == len(records)):
                write_runtime_status(
                    status_path,
                    {
                        "phase": "variant",
                        "variant": variant_name,
                        "state": "chunking_records",
                        "selected_file_count": len(records),
                        "chunked_file_count": completed,
                        "estimated_chunk_count": estimated_chunk_count,
                    },
                )
    chunks: list[ChunkRecord] = []
    for item in ordered_chunks:
        if item:
            chunks.extend(item)
    return chunks


def _selected_records_from_ids(records: list[FileRecord], selected_ids: list[str]) -> list[FileRecord]:
    lookup = _records_by_id(records)
    out = []
    for file_id in selected_ids:
        record = lookup.get(file_id)
        if record:
            out.append(record)
    return out


def _load_selected_ids_snapshot(path: Path) -> list[str]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return []
    out: list[str] = []
    for row in payload:
        if isinstance(row, dict) and row.get("file_id"):
            out.append(str(row["file_id"]))
    return out


def resolve_requested_variants(names: list[str] | None) -> list[str]:
    if not names:
        return list(ALL_VARIANTS)
    requested = set(names)
    return [name for name in ALL_VARIANTS if name in requested]


def get_embedding_selection(
    records: list[FileRecord],
    *,
    user_task: str,
    out_dir: Path,
    require_selection: bool,
) -> tuple[list[str], dict[str, Any], bool]:
    snapshot_path = out_dir / "variant_embedding_select" / "selected_files.json"
    scores_path = out_dir / "variant_embedding_select" / "embedding_scores.json"
    selected = _load_selected_ids_snapshot(snapshot_path)
    if selected:
        meta: dict[str, Any] = {}
        if scores_path.exists():
            payload = json.loads(scores_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                meta = payload
        print(f"[select] embedding selection reused={len(selected)}", flush=True)
        return selected, meta, True
    if not require_selection:
        return [], {}, False
    print("[select] embedding file selection", flush=True)
    selected, meta = run_embedding_file_select(records, user_task=user_task)
    _write_json(scores_path, meta)
    _write_json(
        snapshot_path,
        [dataclasses.asdict(record) for record in _selected_records_from_ids(records, selected)],
    )
    print(f"[select] embedding selected={len(selected)}", flush=True)
    return selected, meta, False


def _base_user_task(case_theme: str) -> str:
    return (
        "주제: 의뢰인이 2025년 1월부터 3월까지의 사건으로 쟁점를 받았고, 그중 일부 사건은 쟁점권자가 당시 이미 알고 있었는데 그때는 쟁점하지 않았다가 나중에 다른 사안과 합산해 다시 쟁점하려고 한다. "
        "이 합산 쟁점가 부당한지에 관하여 구조화 의견서 텍스트를 작성한다. "
        f"세부 사건 메모: {case_theme}"
    )


def _format_question_case_source_ref(record_number: str, source_org: str, record_date: str, record_title: str) -> str:
    source_org = _normalize_space(source_org)
    record_date = _normalize_space(record_date)
    digits = "".join(ch for ch in record_date if ch.isdigit())
    if len(digits) >= 8:
        record_date = f"{digits[:4]}. {digits[4:6]}. {digits[6:8]}."
    record_number = _normalize_space(record_number)
    record_title = _normalize_space(record_title)
    if record_title.isdigit() or (len(record_title) <= 2 and record_title.isalnum()):
        record_title = ""
    parts: list[str] = []
    if source_org:
        parts.append(source_org)
    if record_date:
        parts.append(record_date)
    if record_number:
        parts.append(f"발행 {record_number} 자료")
    source_ref = " ".join(parts).strip()
    if record_title and record_title not in source_ref:
        source_ref = f"{source_ref} ({record_title})".strip()
    return source_ref or record_title or record_number or ""


def _hydrate_question_claims(claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    hydrated: list[dict[str, Any]] = []
    for claim in claims:
        copied = copy.deepcopy(claim)
        primary_case = next(
            (
                row
                for row in copied.get("supporting_cases") or []
                if _normalize_space(row.get("record_number"))
            ),
            {},
        )
        if not _normalize_space(copied.get("record_number")):
            copied["record_number"] = _normalize_space(primary_case.get("record_number"))
        if not _normalize_space(copied.get("source_org")):
            copied["source_org"] = _normalize_space(primary_case.get("source_org"))
        if not _normalize_space(copied.get("record_date")):
            copied["record_date"] = _normalize_space(primary_case.get("record_date"))
        if not _normalize_space(copied.get("record_title")):
            copied["record_title"] = _normalize_space(primary_case.get("record_title"))
        copied["source_ref"] = _format_question_case_source_ref(
            str(copied.get("record_number") or ""),
            str(copied.get("source_org") or ""),
            str(copied.get("record_date") or ""),
            str(copied.get("record_title") or ""),
        )
        for side_key in ("support_spans", "oppose_spans"):
            normalized_spans: list[dict[str, Any]] = []
            for span in copied.get(side_key) or []:
                span_copy = dict(span)
                span_copy["source_ref"] = copied["source_ref"]
                normalized_spans.append(span_copy)
            copied[side_key] = normalized_spans
        hydrated.append(copied)
    return hydrated


def _render_question_claim_for_prompt(claim: dict[str, Any]) -> str:
    primary_source_ref = _format_question_case_source_ref(
        str(claim.get("record_number") or ""),
        str(claim.get("source_org") or ""),
        str(claim.get("record_date") or ""),
        str(claim.get("record_title") or ""),
    )
    support = "\n".join(
        f"- ({primary_source_ref}) {span.get('quote')}"
        for span in claim.get("support_spans") or []
    ) or "- (없음)"
    oppose = "\n".join(
        f"- ({primary_source_ref}) {span.get('quote')}"
        for span in claim.get("oppose_spans") or []
    ) or "- (없음)"
    supporting_cases = "\n".join(
        f"- {row.get('source_org') or ''} {row.get('record_date') or ''} {row.get('record_number') or ''} {row.get('record_title') or ''}".strip()
        for row in claim.get("supporting_cases") or []
    ) or "- (없음)"
    list_fields = [
        ("유리하게 쓸 논리", claim.get("usable_favorable_logic")),
        ("불리하게 작용할 논리", claim.get("usable_unfavorable_logic")),
        ("유리해지는 요소", claim.get("favorable_factors")),
        ("불리해지는 요소", claim.get("unfavorable_factors")),
        ("주의점", claim.get("cautions")),
        ("반대근거", claim.get("counter_evidence")),
    ]
    detail_blocks = []
    for label, values in list_fields:
        values = values or []
        if not values:
            continue
        lines = "\n".join(f"- {item}" for item in values)
        detail_blocks.append(f"{label}:\n{lines}")
    detail_text = ("\n".join(detail_blocks) + "\n") if detail_blocks else ""
    return (
        f"claim_id: {claim.get('claim_id')}\n"
        f"주장 축: {claim.get('claim_axis')}\n"
        f"주장: {claim.get('claim_text')}\n"
        f"사용자 목표 기준: {claim.get('stance_to_user_goal')}\n"
        f"확실성: {claim.get('certainty')} / {claim.get('certainty_reason')}\n"
        f"동일상황 직접 적용례 존재: {claim.get('same_situation_case_exists')}\n"
        f"문맥 요약: {claim.get('context_summary') or '(없음)'}\n"
        f"기억 레코드 요약: {claim.get('case_summary') or '(없음)'}\n"
        f"지지 기억 레코드 수: {claim.get('supporting_case_count') or 0}\n"
        f"지지 기억 레코드:\n{supporting_cases}\n"
        + detail_text
        + f"지지 인용:\n{support}\n반대 인용:\n{oppose}\n"
    )


def _normalize_for_coverage(text: str) -> str:
    return _normalize_space(text).lower()


def _assign_question_claim_ids(claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for index, claim in enumerate(claims, start=1):
        copied = dict(claim)
        copied["claim_id"] = copied.get("claim_id") or f"CLAIM-{index:03d}"
        out.append(copied)
    return out


def _build_question_record_catalog(claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    catalog: dict[str, dict[str, Any]] = {}
    for claim in claims:
        for case in claim.get("supporting_cases") or []:
            record_number = _normalize_space(case.get("record_number"))
            if not record_number:
                continue
            row = catalog.setdefault(
                record_number,
                {
                    "record_number": record_number,
                    "source_org": _normalize_space(case.get("source_org")),
                    "record_date": _normalize_space(case.get("record_date")),
                    "record_title": _normalize_space(case.get("record_title")),
                    "supported_claim_ids": [],
                    "supported_claim_axes": [],
                },
            )
            claim_id = _normalize_space(claim.get("claim_id"))
            claim_axis = _normalize_space(claim.get("claim_axis"))
            if claim_id and claim_id not in row["supported_claim_ids"]:
                row["supported_claim_ids"].append(claim_id)
            if claim_axis and claim_axis not in row["supported_claim_axes"]:
                row["supported_claim_axes"].append(claim_axis)
    rows = list(catalog.values())
    rows.sort(key=lambda item: (item.get("record_date", ""), item.get("record_number", "")), reverse=True)
    return rows


def _render_question_record_catalog(catalog: list[dict[str, Any]]) -> str:
    if not catalog:
        return "- (없음)"
    return "\n".join(
        f"- {item.get('record_number')} | {item.get('source_org')} | {item.get('record_date')} | {item.get('record_title')} | 관련 주장ID: {', '.join(item.get('supported_claim_ids') or [])}"
        for item in catalog
    )


def _normalize_record_buckets(raw: Any, catalog: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    allowed = ("very_similar", "similar", "usable", "other")
    catalog_by_case = {
        _normalize_space(item.get("record_number")): item
        for item in catalog
        if _normalize_space(item.get("record_number"))
    }
    buckets: dict[str, list[dict[str, Any]]] = {key: [] for key in allowed}
    seen_cases: set[str] = set()
    if not isinstance(raw, dict):
        return buckets
    for key in allowed:
        values = raw.get(key)
        if not isinstance(values, list):
            continue
        for value in values:
            if not isinstance(value, dict):
                continue
            record_number = _normalize_space(value.get("record_number"))
            if not record_number or record_number in seen_cases:
                continue
            catalog_row = catalog_by_case.get(record_number)
            if not catalog_row:
                continue
            seen_cases.add(record_number)
            buckets[key].append(
                {
                    "record_number": record_number,
                    "source_org": catalog_row.get("source_org") or "",
                    "record_date": catalog_row.get("record_date") or "",
                    "record_title": catalog_row.get("record_title") or "",
                    "supported_claim_ids": list(catalog_row.get("supported_claim_ids") or []),
                    "supported_claim_axes": list(catalog_row.get("supported_claim_axes") or []),
                    "why": _normalize_space(value.get("why")) or "",
                }
            )
    return buckets


_CLAIM_GROUP_STOPWORDS = {
    "주장",
    "원칙",
    "여부",
    "조건",
    "적용",
    "한계",
    "판단",
    "효력",
    "사유",
    "근거",
    "무효",
    "취소",
    "도과",
    "및",
    "따른",
    "대한",
    "관련",
}


def _tokenize_claim_axis(axis: str) -> list[str]:
    tokens = re.findall(r"[가-힣A-Za-z0-9]+", _normalize_space(axis))
    out: list[str] = []
    for token in tokens:
        normalized = token.strip()
        if len(normalized) < 2:
            continue
        if normalized in _CLAIM_GROUP_STOPWORDS:
            continue
        out.append(normalized)
    return out


def _claim_group_label(axis: str) -> str:
    normalized = _normalize_space(axis)
    if not normalized:
        return "기타 주장"
    rules = [
        ("쟁점시효", ("쟁점시효", "기산점", "계속적 위반", "시효")),
        ("이중쟁점", ("이중쟁점",)),
        ("신뢰보호", ("신뢰보호", "신의칙")),
        ("재량권 남용", ("재량권", "재량권남용", "양정", "과중", "비례")),
        ("절차적 하자 및 권한", ("절차", "정족수", "추인", "방어권", "권한", "특정", "의결")),
        ("확인의 이익", ("확인의 이익", "소송 요건")),
    ]
    for label, keywords in rules:
        if any(keyword in normalized for keyword in keywords):
            return label
    return normalized


def _build_fallback_claim_groups(claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    token_counts: Counter[str] = Counter()
    axis_to_tokens: dict[str, list[str]] = {}
    for claim in claims:
        axis = _normalize_space(claim.get("claim_axis"))
        if not axis:
            continue
        tokens = _tokenize_claim_axis(axis)
        axis_to_tokens[axis] = tokens
        token_counts.update(set(tokens))
    grouped: dict[str, list[str]] = {}
    for claim in claims:
        claim_id = _normalize_space(claim.get("claim_id"))
        axis = _normalize_space(claim.get("claim_axis"))
        if not claim_id or not axis:
            continue
        tokens = sorted(
            [token for token in axis_to_tokens.get(axis, []) if token_counts.get(token, 0) >= 2 and len(token) >= 3],
            key=lambda item: (-len(item), axis.find(item)),
        )
        label = _claim_group_label(tokens[0] if tokens else axis)
        grouped.setdefault(label, []).append(claim_id)
    rows = [{"label": label, "claim_ids": claim_ids} for label, claim_ids in grouped.items() if claim_ids]
    rows.sort(key=lambda item: (-len(item.get("claim_ids") or []), item.get("label") or ""))
    return rows


def _normalize_claim_groups(raw: Any, claims: list[dict[str, Any]]) -> list[dict[str, Any]]:
    claim_ids = {_normalize_space(item.get("claim_id")) for item in claims}
    groups: list[dict[str, Any]] = []
    if not isinstance(raw, list):
        return groups
    seen_ids: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        label = _claim_group_label(_normalize_space(item.get("label")))
        if not label:
            continue
        group_claim_ids: list[str] = []
        for value in item.get("claim_ids") or []:
            claim_id = _normalize_space(value)
            if claim_id and claim_id in claim_ids and claim_id not in seen_ids:
                group_claim_ids.append(claim_id)
                seen_ids.add(claim_id)
        if group_claim_ids:
            groups.append({"label": label, "claim_ids": group_claim_ids})
    return groups


def _render_question_claim_groups(claim_groups: list[dict[str, Any]], claims: list[dict[str, Any]]) -> str:
    if not claim_groups:
        return "- (없음)"
    claim_by_id = {_normalize_space(item.get("claim_id")): item for item in claims}
    rows: list[str] = []
    for group in claim_groups:
        label = _normalize_space(group.get("label"))
        claim_ids = [_normalize_space(item) for item in group.get("claim_ids") or [] if _normalize_space(item)]
        if not label or not claim_ids:
            continue
        bullet_lines = []
        for claim_id in claim_ids:
            claim = claim_by_id.get(claim_id)
            if not claim:
                continue
            axis = _normalize_space(claim.get("claim_axis")) or claim_id
            support_count = int(claim.get("supporting_case_count") or 0)
            bullet_lines.append(f"- {claim_id} | {axis} | 지지 기억 레코드 수 {support_count}")
        if bullet_lines:
            rows.append(f"[{label}]\n" + "\n".join(bullet_lines))
    return "\n\n".join(rows) if rows else "- (없음)"


def build_question_answer_plan_prompt(*, user_task: str, claims: list[dict[str, Any]], catalog: list[dict[str, Any]]) -> str:
    top_claims = sorted(
        claims,
        key=lambda item: (
            -int(item.get("supporting_case_count") or 0),
            -_certainty_sort_value(item.get("certainty", "")),
            item.get("claim_axis", ""),
        ),
    )[:120]
    return (
        "아래 claim ledger와 기억 레코드 catalog를 보고 JSON만 출력하라.\n"
        "목표는 본문에 넣을 주장과 참고 기억 레코드 목록의 위계를 고르는 것이다.\n"
        "중요:\n"
        "- body_claim_ids에는 본문에서 다룰 주장만 넣는다.\n"
        "- 반드시 claim ledger에 있는 exact claim_id만 사용한다.\n"
        "- 같은 주장 축을 지지하는 별개 기억 레코드 수가 많을수록 신뢰도가 더 강하다고 보고 우선한다.\n"
        "- likely_outcome에는 현재 자료상 가장 가능성이 높은 결론을 한 문장으로 적는다.\n"
        "- confidence_basis에는 왜 그 결론이 더 유력한지 2~4개 bullet로 적는다.\n"
        "- helpful_facts에는 질문자가 유리하게 만들기 위해 더 확보·정리해야 할 사실을 적는다.\n"
        "- harmful_facts에는 질문자에게 불리하게 작용할 사실·행동을 적는다.\n"
        "- claim_groups에는 본문에서 함께 다룰 주장 묶음을 넣는다.\n"
        "- 같은 법리·논리 축이면 claim_axis가 조금 달라도 하나의 group label 아래로 묶는다.\n"
        "- claim_groups에는 exact claim_id만 사용한다.\n"
        "- record_buckets는 very_similar, similar, usable, other 네 키만 사용한다.\n"
        "- 매우 유사한 기억 레코드: 유형 및 정황 모두 매우 가까운 기억 레코드\n"
        "- 유사한 기억 레코드: 유형은 같고 정황이 조금 다르지만 유불리 조건을 뽑아낼 수 있는 기억 레코드\n"
        "- 이용할 만한 기억 레코드: 유형은 달라도 내부 논리를 쓸 수 있는 기억 레코드\n"
        "- 기타: 직접적 관련이 낮거나 분류 자신이 없는 기억 레코드\n"
        "- 각 기억 레코드는 한 버킷에만 넣는다. 자신 없으면 other다.\n"
        "- very_similar/similar/usable에 들어간 기억 레코드만 coverage 대상으로 간주한다.\n"
        "- 새 주장, 새 레코드번호, 새 사실을 만들지 말라.\n\n"
        f"질문:\n{user_task}\n\n"
        "[claim ledger]\n"
        + "\n\n".join(_render_question_claim_for_prompt(claim) for claim in top_claims)
        + "\n\n[record catalog]\n"
        + _render_question_record_catalog(catalog)
        + "\n\n출력 JSON 스키마:\n"
        + '{\n  "likely_outcome": "...",\n  "confidence_basis": ["..."],\n  "helpful_facts": ["..."],\n  "harmful_facts": ["..."],\n  "body_claim_ids": ["CLAIM-001"],\n  "claim_groups": [{"label": "쟁점시효", "claim_ids": ["CLAIM-001", "CLAIM-002"]}],\n  "record_buckets": {"very_similar": [{"record_number": "...", "why": "..."}], "similar": [], "usable": [], "other": []}\n}'
    )


def _extract_question_answer_plan(*, raw: str, claims: list[dict[str, Any]], catalog: list[dict[str, Any]]) -> dict[str, Any]:
    claim_ids = {_normalize_space(item.get("claim_id")) for item in claims}
    try:
        obj = json.loads(_extract_json_block(raw))
    except Exception:
        obj = {}
    body_claim_ids: list[str] = []
    for value in obj.get("body_claim_ids") or []:
        claim_id = _normalize_space(value)
        if claim_id and claim_id in claim_ids and claim_id not in body_claim_ids:
            body_claim_ids.append(claim_id)
    buckets = _normalize_record_buckets(obj.get("record_buckets"), catalog)
    return {
        "likely_outcome": _normalize_space(obj.get("likely_outcome")),
        "confidence_basis": _coerce_string_list(obj.get("confidence_basis")),
        "helpful_facts": _coerce_string_list(obj.get("helpful_facts")),
        "harmful_facts": _coerce_string_list(obj.get("harmful_facts")),
        "body_claim_ids": body_claim_ids,
        "claim_groups": _normalize_claim_groups(obj.get("claim_groups"), claims),
        "record_buckets": buckets,
    }


def _claim_certainty_points(value: str) -> int:
    normalized = _normalize_space(value).lower()
    return {
        "very_high": 5,
        "high": 4,
        "medium": 3,
        "low": 2,
        "speculative": 1,
    }.get(normalized, 2)


def _claim_strength_score(claim: dict[str, Any]) -> int:
    support_cases = int(claim.get("supporting_case_count") or 0)
    support_spans = len(claim.get("support_spans") or [])
    same_situation = 2 if claim.get("same_situation_case_exists") else 0
    return (support_cases * 5) + (_claim_certainty_points(str(claim.get("certainty") or "")) * 3) + support_spans + same_situation


def _fallback_question_answer_plan(claims: list[dict[str, Any]], catalog: list[dict[str, Any]]) -> dict[str, Any]:
    hydrated_claims = _hydrate_question_claims(claims)
    ranked_claims = sorted(hydrated_claims, key=lambda item: (-_claim_strength_score(item), item.get("claim_axis") or ""))
    body_claim_ids = [
        str(item.get("claim_id") or "")
        for item in ranked_claims[: min(8, len(ranked_claims))]
        if _normalize_space(item.get("claim_id"))
    ]
    favorable_score = sum(_claim_strength_score(item) for item in hydrated_claims if _normalize_space(item.get("stance_to_user_goal")) == "유리")
    unfavorable_score = sum(_claim_strength_score(item) for item in hydrated_claims if _normalize_space(item.get("stance_to_user_goal")) == "불리")
    top_favorable = next((item for item in ranked_claims if _normalize_space(item.get("stance_to_user_goal")) == "유리"), None)
    top_unfavorable = next((item for item in ranked_claims if _normalize_space(item.get("stance_to_user_goal")) == "불리"), None)
    top_claim = ranked_claims[0] if ranked_claims else None
    top_axis = _normalize_space((top_claim or {}).get("claim_axis"))
    top_support = int((top_claim or {}).get("supporting_case_count") or 0)
    top_direction = _normalize_space((top_claim or {}).get("stance_to_user_goal"))
    support_examples = [
        _normalize_space(item.get("record_number"))
        for item in ((top_claim or {}).get("supporting_cases") or [])
        if _normalize_space(item.get("record_number"))
    ][:2]
    repeated_line = ""
    if top_support >= 2:
        if support_examples:
            repeated_line = f"{', '.join(support_examples)} 등 {top_support}건 안팎의 기억 레코드가 같은 방향으로 반복적으로 뒷받침한다."
        else:
            repeated_line = f"복수 기억 레코드 {top_support}건 안팎이 같은 방향으로 반복적으로 뒷받침한다."
    if favorable_score > unfavorable_score * 1.08 or (top_direction == "유리" and top_support >= 2):
        axis = _normalize_space((top_favorable or top_claim or {}).get("claim_axis"))
        likely_outcome = (
            f"현재 자료상 질문자에게 유리한 결론이 가장 가능성이 높고, 특히 `{axis}` 묶음이 가장 강하다. {repeated_line}".strip()
            if axis
            else f"현재 자료상 질문자에게 유리한 결론이 가장 가능성이 높다. {repeated_line}".strip()
        )
    elif unfavorable_score > favorable_score * 1.08 or (top_direction == "불리" and top_support >= 2):
        axis = _normalize_space((top_unfavorable or top_claim or {}).get("claim_axis"))
        likely_outcome = (
            f"현재 자료상 질문자에게 불리한 결론이 가장 가능성이 높고, 특히 `{axis}` 묶음이 가장 위험하다. {repeated_line}".strip()
            if axis
            else f"현재 자료상 질문자에게 불리한 결론이 가장 가능성이 높다. {repeated_line}".strip()
        )
    elif top_axis:
        likely_outcome = f"현재 자료상 `{top_axis}` 묶음이 가장 직접적인 판단축이며, 이 축이 결론을 좌우할 가능성이 가장 높다. {repeated_line}".strip()
    else:
        likely_outcome = "현재 자료상 핵심 사실관계에 따라 결론이 갈릴 여지가 크지만, 반복적으로 지지되는 주장 축부터 우선 검토해야 한다."
    confidence_basis: list[str] = []
    for item in ranked_claims[:4]:
        claim_axis = _normalize_space(item.get("claim_axis"))
        if not claim_axis:
            continue
        support_cases = int(item.get("supporting_case_count") or 0)
        if support_cases >= 2:
            confidence_basis.append(f"`{claim_axis}`는 복수 기억 레코드가 같은 방향으로 지지해 신뢰도가 높다.")
        elif item.get("same_situation_case_exists"):
            confidence_basis.append(f"`{claim_axis}`는 현재 사안과 직접 맞닿는 기억 레코드가 확인된다.")
        elif item.get("certainty_reason"):
            confidence_basis.append(f"`{claim_axis}`는 {item.get('certainty_reason')}")
        else:
            confidence_basis.append(f"`{claim_axis}`는 현재 자료상 비교적 강한 주장 축으로 평가된다.")
    helpful_facts: list[str] = []
    harmful_facts: list[str] = []
    for item in ranked_claims:
        for source in ("favorable_factors", "required_facts", "favorable_basis"):
            for fact in _coerce_string_list(item.get(source)):
                if fact and fact not in helpful_facts:
                    helpful_facts.append(fact)
        for source in ("unfavorable_factors", "cautions", "counter_evidence", "unfavorable_basis"):
            for fact in _coerce_string_list(item.get(source)):
                if fact and fact not in harmful_facts:
                    harmful_facts.append(fact)
    if not helpful_facts:
        helpful_facts = [
            _normalize_space(item.get("claim_text"))
            for item in ranked_claims
            if _normalize_space(item.get("stance_to_user_goal")) == "유리" and _normalize_space(item.get("claim_text"))
        ][:6]
    if not harmful_facts:
        harmful_facts = [
            _normalize_space(item.get("claim_text"))
            for item in ranked_claims
            if _normalize_space(item.get("stance_to_user_goal")) == "불리" and _normalize_space(item.get("claim_text"))
        ][:6]
    return {
        "likely_outcome": likely_outcome,
        "confidence_basis": confidence_basis[:4],
        "helpful_facts": helpful_facts[:6],
        "harmful_facts": harmful_facts[:6],
        "body_claim_ids": body_claim_ids,
        "claim_groups": _build_fallback_claim_groups([item for item in hydrated_claims if str(item.get("claim_id") or "") in body_claim_ids]),
        "record_buckets": _normalize_record_buckets(
            {
                "very_similar": [{"record_number": row.get("record_number"), "why": ""} for row in catalog[:3]],
                "similar": [{"record_number": row.get("record_number"), "why": ""} for row in catalog[3:8]],
                "usable": [{"record_number": row.get("record_number"), "why": ""} for row in catalog[8:12]],
                "other": [],
            },
            catalog,
        ),
    }


def _required_record_record_numbers(record_buckets: dict[str, list[dict[str, Any]]]) -> list[str]:
    required: list[str] = []
    for key in ("very_similar", "similar", "usable"):
        for row in record_buckets.get(key) or []:
            record_number = _normalize_space(row.get("record_number"))
            if record_number and record_number not in required:
                required.append(record_number)
    return required


_MARKDOWN_HEADING_RE = re.compile(r"(?m)^[ \t]{0,3}(#{1,6})\s+(.+?)\s*$")


def _clean_markdown_heading_title(value: str) -> str:
    return _normalize_space(
        re.sub(r"[*_`#]+", " ", str(value or "")).replace(":", " ")
    )


def _find_markdown_section(answer_text: str, heading_title: str, *, min_level: int = 1, max_level: int = 6) -> tuple[int, int, int, int] | None:
    target = _normalize_for_coverage(heading_title)
    if not target:
        return None
    headings = list(_MARKDOWN_HEADING_RE.finditer(answer_text))
    for index, match in enumerate(headings):
        level = len(match.group(1))
        if level < min_level or level > max_level:
            continue
        title = _clean_markdown_heading_title(match.group(2))
        if _normalize_for_coverage(title) != target:
            continue
        section_end = len(answer_text)
        for next_match in headings[index + 1 :]:
            if len(next_match.group(1)) <= level:
                section_end = next_match.start()
                break
        return (match.start(), match.end(), section_end, level)
    return None


def _reference_list_start(answer_text: str) -> int | None:
    for match in _MARKDOWN_HEADING_RE.finditer(answer_text):
        title = _normalize_for_coverage(_clean_markdown_heading_title(match.group(2)))
        if "참고기억 레코드목록" in re.sub(r"\s+", "", title):
            return match.start()
    marker = re.search(r"(?m)^\s*참고\s*기억 레코드\s*목록\s*$", answer_text)
    if marker:
        return marker.start()
    return None


def _answer_without_reference_list(answer_text: str) -> str:
    ref_start = _reference_list_start(answer_text)
    if ref_start is None:
        return answer_text
    return answer_text[:ref_start]


def _insert_before_reference_list(answer_text: str, content: str) -> str:
    insert = content.strip()
    if not insert:
        return answer_text
    ref_start = _reference_list_start(answer_text)
    if ref_start is None:
        return f"{answer_text.rstrip()}\n\n{insert}".strip()
    return f"{answer_text[:ref_start].rstrip()}\n\n{insert}\n\n{answer_text[ref_start:].lstrip()}".strip()


def _normalize_patch_snippet(text: str) -> str:
    return re.sub(r"[^0-9A-Za-z가-힣]+", "", str(text or "")).lower()


def _find_patch_snippet_range(text: str, snippet: str, from_index: int = 0) -> tuple[int, int] | None:
    raw = str(snippet or "")
    if not raw.strip():
        return None
    sub = text[from_index:]
    exact = sub.find(raw)
    if exact >= 0:
        return (from_index + exact, from_index + exact + len(raw))

    normalized_snippet = _normalize_patch_snippet(raw)
    if not normalized_snippet:
        return None
    normalized_chars: list[str] = []
    index_map: list[int] = []
    for index, char in enumerate(sub):
        normalized = _normalize_patch_snippet(char)
        if not normalized:
            continue
        for normalized_char in normalized:
            normalized_chars.append(normalized_char)
            index_map.append(index)
    normalized_text = "".join(normalized_chars)
    found = normalized_text.find(normalized_snippet)
    if found < 0:
        return None
    start = index_map[found]
    end = index_map[found + len(normalized_snippet) - 1] + 1
    return (from_index + start, from_index + end)


def _parse_tag_value(text: str, tag_name: str) -> str:
    match = re.search(rf"<\s*{re.escape(tag_name)}\s*>([\s\S]*?)<\s*/\s*{re.escape(tag_name)}\s*>", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _parse_question_coverage_patch_blocks(raw: str) -> list[dict[str, str]]:
    blocks = re.findall(r"<\s*patch\s*>([\s\S]*?)<\s*/\s*patch\s*>", str(raw or ""), flags=re.IGNORECASE)
    patches: list[dict[str, str]] = []
    for block in blocks:
        content = _parse_tag_value(block, "content")
        if not content.strip():
            continue
        patches.append(
            {
                "section": _parse_tag_value(block, "section"),
                "op": (_parse_tag_value(block, "op") or "append").lower(),
                "before": _parse_tag_value(block, "before"),
                "after": _parse_tag_value(block, "after"),
                "content": content,
            }
        )
    return patches


def _looks_like_markdown_block(text: str) -> bool:
    stripped = str(text or "").lstrip()
    return bool(re.match(r"^(\||[-*+]\s+|\d+\.\s+|#{1,6}\s+|>\s+|```)", stripped))


def _glue_patch_content(prefix: str, content: str, suffix: str) -> str:
    if not _looks_like_markdown_block(content):
        return content
    before = "\n" if prefix and not prefix.endswith("\n") and not content.startswith("\n") else ""
    after = "\n" if suffix and not suffix.startswith("\n") and not content.endswith("\n") else ""
    return f"{before}{content}{after}"


def _is_inside_table_row(text: str, index: int) -> bool:
    last_newline = text.rfind("\n", 0, max(index, 0))
    line_start = 0 if last_newline < 0 else last_newline + 1
    next_newline = text.find("\n", index)
    line_end = len(text) if next_newline < 0 else next_newline
    return bool(re.match(r"^\s*\|", text[line_start:line_end]))


def _question_patch_section_range(answer_text: str, section_name: str) -> tuple[int, int]:
    section_name = _normalize_space(section_name)
    if section_name:
        section = _find_markdown_section(answer_text, section_name, min_level=1, max_level=6)
        if section:
            start, _heading_end, section_end, _level = section
            return (start, section_end)
    ref_start = _reference_list_start(answer_text)
    return (0, ref_start if ref_start is not None else len(answer_text))


def _apply_question_coverage_patch_block(answer_text: str, patch: dict[str, str]) -> tuple[str, bool]:
    op = (patch.get("op") or "append").lower()
    if op not in {"append", "insert_after", "insert_before", "insert_between"}:
        return (answer_text, False)
    content = str(patch.get("content") or "").strip()
    if not content:
        return (answer_text, False)
    section_start, section_end = _question_patch_section_range(answer_text, patch.get("section") or "")
    section_text = answer_text[section_start:section_end]
    before = patch.get("before") or ""
    after = patch.get("after") or ""

    if op == "append":
        insert_pos = section_end
    else:
        before_range = _find_patch_snippet_range(section_text, before, 0) if before else None
        if not before_range:
            return (answer_text, False)
        if op == "insert_after":
            insert_pos = section_start + before_range[1]
        elif op == "insert_before":
            insert_pos = section_start + before_range[0]
        else:
            after_range = _find_patch_snippet_range(section_text, after, before_range[1]) if after else None
            if not after_range:
                return (answer_text, False)
            insert_pos = section_start + after_range[0]
    if _is_inside_table_row(answer_text, insert_pos):
        return (answer_text, False)
    prefix = answer_text[:insert_pos]
    suffix = answer_text[insert_pos:]
    glued = _glue_patch_content(prefix, content, suffix)
    return (f"{prefix.rstrip()}\n\n{glued.strip()}\n\n{suffix.lstrip()}".strip(), True)


def _apply_question_coverage_patch_blocks(answer_text: str, patches: list[dict[str, str]]) -> tuple[str, int]:
    output = answer_text
    applied_count = 0
    for patch in patches:
        output, applied = _apply_question_coverage_patch_block(output, patch)
        if applied:
            applied_count += 1
    return (output, applied_count)


def _format_claim_support_quote(claim: dict[str, Any]) -> str:
    for source_ref, quote, evidence_id in _claim_support_markers(claim):
        if not quote:
            continue
        if source_ref:
            return f"**({source_ref})** \"{quote}\""
        if evidence_id:
            return f"[{evidence_id}] \"{quote}\""
        return f"\"{quote}\""
    return ""


def _format_claim_coverage_block(claim: dict[str, Any]) -> str:
    axis = _normalize_space(claim.get("claim_axis")) or "관련 주장"
    body = _normalize_space(claim.get("claim_text") or claim.get("context_summary") or claim.get("case_summary"))
    quote = _format_claim_support_quote(claim)
    lines = [f"### {axis}"]
    if body:
        lines.append(body)
    if quote:
        lines.extend(["근거 인용", quote])
    return "\n\n".join(lines)


def _append_claim_quote_to_existing_section(answer_text: str, claim: dict[str, Any]) -> str:
    axis = _normalize_space(claim.get("claim_axis"))
    quote = _format_claim_support_quote(claim)
    if not axis or not quote:
        return answer_text
    section = _find_markdown_section(answer_text, axis, min_level=2, max_level=6)
    if not section:
        return _insert_before_reference_list(answer_text, _format_claim_coverage_block(claim))
    start, heading_end, section_end, _level = section
    section_text = answer_text[heading_end:section_end]
    if _normalize_for_coverage(quote) in _normalize_for_coverage(section_text):
        return answer_text
    insert = f"\n\n근거 인용\n\n{quote}\n\n"
    return f"{answer_text[:section_end].rstrip()}{insert}{answer_text[section_end:].lstrip()}".strip()


def _find_required_record_row(record_buckets: dict[str, list[dict[str, Any]]], record_number: str) -> tuple[str, dict[str, Any]] | None:
    needle = _normalize_space(record_number)
    labels = {"very_similar": "매우 유사한 기억 레코드", "similar": "유사한 기억 레코드", "usable": "이용할 만한 기억 레코드"}
    for key in ("very_similar", "similar", "usable"):
        for row in record_buckets.get(key) or []:
            if _normalize_space(row.get("record_number")) == needle:
                return (labels[key], row)
    return None


def _format_record_body_sentence(row: dict[str, Any]) -> str:
    source_ref = _format_question_case_source_ref(
        str(row.get("record_number") or ""),
        str(row.get("source_org") or ""),
        str(row.get("record_date") or ""),
        str(row.get("record_title") or ""),
    )
    axes = ", ".join(_coerce_string_list(row.get("supported_claim_axes")))
    why = _normalize_space(row.get("why"))
    if axes and why:
        return f"또한 **({source_ref})** 역시 {axes}와 관련하여 {why}"
    if axes:
        return f"또한 **({source_ref})** 역시 {axes}와 관련해 같은 방향의 판단 근거로 검토할 수 있다."
    if why:
        return f"또한 **({source_ref})** 역시 {why}"
    return f"또한 **({source_ref})** 역시 같은 법리 축에서 참고할 수 있다."


def _append_record_sentence_to_body_section(
    answer_text: str,
    row: dict[str, Any],
    claims: list[dict[str, Any]],
) -> str:
    sentence = _format_record_body_sentence(row)
    if _normalize_for_coverage(row.get("record_number")) in _normalize_for_coverage(_answer_without_reference_list(answer_text)):
        return answer_text
    claim_by_axis = {
        _normalize_for_coverage(claim.get("claim_axis")): claim
        for claim in claims
        if _normalize_space(claim.get("claim_axis"))
    }
    target_axis = ""
    for axis in _coerce_string_list(row.get("supported_claim_axes")):
        if _normalize_for_coverage(axis) in claim_by_axis:
            target_axis = axis
            break
    if target_axis:
        section = _find_markdown_section(answer_text, target_axis, min_level=2, max_level=6)
        if section:
            _start, _heading_end, section_end, _level = section
            return f"{answer_text[:section_end].rstrip()}\n\n{sentence}\n\n{answer_text[section_end:].lstrip()}".strip()
    block = f"### {target_axis or '관련 참고 기억 레코드'}\n\n{sentence}"
    return _insert_before_reference_list(answer_text, block)


def _apply_question_coverage_fallback(
    *,
    answer_text: str,
    claims: list[dict[str, Any]],
    record_buckets: dict[str, list[dict[str, Any]]],
    gaps: dict[str, list[str]],
) -> str:
    output = answer_text.strip()
    hydrated_claims = _hydrate_question_claims(claims)
    claim_by_axis = {
        _normalize_space(claim.get("claim_axis")): claim
        for claim in hydrated_claims
        if _normalize_space(claim.get("claim_axis"))
    }
    seen_axes: set[str] = set()
    for axis in [*(gaps.get("missing_axes") or []), *(gaps.get("missing_source_refs") or [])]:
        normalized_axis = _normalize_space(axis)
        if not normalized_axis or normalized_axis in seen_axes:
            continue
        seen_axes.add(normalized_axis)
        claim = claim_by_axis.get(normalized_axis)
        if not claim:
            continue
        if normalized_axis in (gaps.get("missing_axes") or []):
            output = _insert_before_reference_list(output, _format_claim_coverage_block(claim))
        else:
            output = _append_claim_quote_to_existing_section(output, claim)
    for record_number in gaps.get("missing_records") or []:
        found = _find_required_record_row(record_buckets, record_number)
        if not found:
            continue
        _bucket_label, row = found
        output = _append_record_sentence_to_body_section(output, row, hydrated_claims)
    return _rewrite_question_answer_source_refs(output, hydrated_claims)


def _claim_support_markers(claim: dict[str, Any]) -> list[tuple[str, str, str]]:
    markers: list[tuple[str, str, str]] = []
    source_ref = _format_question_case_source_ref(
        str(claim.get("record_number") or ""),
        str(claim.get("source_org") or ""),
        str(claim.get("record_date") or ""),
        str(claim.get("record_title") or ""),
    )
    for span in claim.get("support_spans") or []:
        quote = _normalize_space(span.get("quote"))
        if quote:
            markers.append((source_ref, quote, _normalize_space(span.get("evidence_id"))))
    return markers


def find_question_coverage_gaps(
    answer_text: str,
    claims: list[dict[str, Any]],
    *,
    record_buckets: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, list[str]]:
    normalized_answer = _normalize_for_coverage(answer_text)
    missing_axes: list[str] = []
    missing_source_refs: list[str] = []
    missing_records: list[str] = []
    for claim in claims:
        claim_axis = _normalize_space(claim.get("claim_axis"))
        if not claim_axis:
            continue
        normalized_axis = _normalize_for_coverage(claim_axis)
        if normalized_axis not in normalized_answer:
            missing_axes.append(claim_axis)
            missing_source_refs.append(claim_axis)
            continue
        support_markers = _claim_support_markers(claim)
        if not support_markers:
            continue
        has_source_ref = any(
            (
                source_ref
                and _normalize_for_coverage(source_ref) in normalized_answer
                and _normalize_for_coverage(quote) in normalized_answer
            )
            or (
                evidence_id
                and _normalize_for_coverage(evidence_id) in normalized_answer
                and _normalize_for_coverage(quote) in normalized_answer
            )
            for source_ref, quote, evidence_id in support_markers
        )
        if not has_source_ref:
            missing_source_refs.append(claim_axis)
    normalized_body_answer = _normalize_for_coverage(_answer_without_reference_list(answer_text))
    for record_number in _required_record_record_numbers(record_buckets or {}):
        if _normalize_for_coverage(record_number) not in normalized_body_answer:
            missing_records.append(record_number)
    return {
        "missing_axes": missing_axes,
        "missing_source_refs": missing_source_refs,
        "missing_records": missing_records,
    }


def _render_question_record_buckets(record_buckets: dict[str, list[dict[str, Any]]]) -> str:
    order = [
        ("very_similar", "매우 유사한 기억 레코드"),
        ("similar", "유사한 기억 레코드"),
        ("usable", "이용할 만한 기억 레코드"),
        ("other", "기타"),
    ]
    parts: list[str] = []
    for key, label in order:
        rows = record_buckets.get(key) or []
        if rows:
            body = "\n".join(
                f"- {_format_question_case_source_ref(str(row.get('record_number') or ''), str(row.get('source_org') or ''), str(row.get('record_date') or ''), str(row.get('record_title') or ''))} | 관련 주장ID: {', '.join(row.get('supported_claim_ids') or [])} | 사유: {row.get('why') or ''}"
                for row in rows
            )
        else:
            body = "- (없음)"
        parts.append(f"[{label}]\n{body}")
    return "\n\n".join(parts)


def build_question_answer_prompt(
    *,
    user_task: str,
    claims: list[dict[str, Any]],
    record_buckets: dict[str, list[dict[str, Any]]],
    answer_plan: dict[str, Any] | None = None,
) -> str:
    answer_plan = answer_plan or {}
    planner_block_parts = []
    if answer_plan.get("likely_outcome"):
        planner_block_parts.append(f"가장 가능성 높은 결론:\n- {answer_plan.get('likely_outcome')}")
    if answer_plan.get("confidence_basis"):
        planner_block_parts.append(
            "결론 근거:\n" + "\n".join(f"- {item}" for item in answer_plan.get("confidence_basis") or [])
        )
    if answer_plan.get("helpful_facts"):
        planner_block_parts.append(
            "유리하게 만들 요소:\n" + "\n".join(f"- {item}" for item in answer_plan.get("helpful_facts") or [])
        )
    if answer_plan.get("harmful_facts"):
        planner_block_parts.append(
            "불리하게 만들 요소:\n" + "\n".join(f"- {item}" for item in answer_plan.get("harmful_facts") or [])
        )
    if answer_plan.get("claim_groups"):
        planner_block_parts.append(
            "주장 묶음:\n" + _render_question_claim_groups(answer_plan.get("claim_groups") or [], claims)
        )
    planner_block = "\n\n[사전 판단]\n" + "\n\n".join(planner_block_parts) if planner_block_parts else ""
    return (
        "아래 selected claim ledger와 참고 기억 레코드 위계를 바탕으로 자료 질문에 대한 실무형 답변을 작성하라.\n"
        "중요:\n"
        "- 질문 모드다. 유리한 내용과 불리한 내용을 모두 빠뜨리지 말고 정리할 것\n"
        "- selected claim만 본문 대상으로 사용하고, other 버킷 기억 레코드는 본문 논증 대상으로 쓰지 말 것\n"
        "- 같은 주장 축을 지지하는 별개 기억 레코드 수가 많을수록 신뢰도가 더 높다는 점을 반영해 설명할 것\n"
        "- supporting_case_count가 2건 이상인 주장은 `복수 기억 레코드가 같은 방향을 지지한다`는 취지로 본문에서 명시할 것\n"
        "- claim_groups가 주어지면 같은 그룹 안의 주장들을 한 덩어리의 논리 흐름으로 정리할 것\n"
        "- 예를 들어 `쟁점시효` 그룹 안에서는 기산점, 도과, 계속적 위반 여부를 따로 흩뜨리지 말고 같이 설명할 것\n"
        "- 본문 첫머리에 `## 종합 판단` 섹션을 두고, 현재 자료상 가장 가능성이 높은 결론을 분명하게 적을 것\n"
        "- `종합 판단`에서는 양쪽 가능성을 나열만 하지 말고 어떤 방향이 더 가능성이 높은지와 그 이유를 말할 것\n"
        "- 반복적으로 뒷받침되는 주장 축은 `여러 기억 레코드가 같은 방향을 지지하므로 신뢰도가 높다`는 취지로 평가할 것\n"
        "- 결론을 애매하게 얼버무리지 말고, 현재 자료상 어떤 주장이 가장 강한지 우선순위를 세워 말할 것\n"
        "- `## 종합 판단`의 첫 문장은 `현재 자료상 ... 가능성이 가장 높다` 또는 `현재 자료상 ... 주장이 가장 강하다`처럼 단정형으로 쓸 것\n"
        "- 단순히 양쪽 논리를 병렬 나열하지 말고, 반복적으로 뒷받침되는 주장 축을 기준으로 우세한 방향을 먼저 판단할 것\n"
        "- `## 종합 판단`의 앞 두 문장 안에 가장 강한 주장 묶음, 그 묶음을 반복적으로 지지하는 기억 레코드 수, 그래서 왜 그 방향이 우세한지를 반드시 적을 것\n"
        "- `## 종합 판단`에서 `주장할 수 있다`, `반면`, `다만` 식의 병렬 나열로 끝내지 말고, 현재 기준 가장 우세한 방향을 먼저 확정적으로 적을 것\n"
        "- claim_groups가 있으면 가장 강한 group label을 결론의 주축으로 삼고, 그 아래 하위 주장들이 어떻게 쌓이는지 설명할 것\n"
        "- 같은 그룹 안의 하위 주장들이 모두 같은 방향이면 `이 묶음은 복수 기억 레코드가 반복적으로 뒷받침하는 핵심 논리`라는 취지로 적을 것\n"
        "- `## 유리하게 만들 요소`와 `## 불리하게 만들 요소` 섹션을 두고, 사전 판단의 helpful_facts/harmful_facts를 자연스럽게 반영할 것\n"
        "- 질문자가 자신에게 유리한 결과를 만들려면 어떤 사실을 입증·정리·확보해야 하는지 실무적으로 적을 것\n"
        "- 질문자에게 불리해질 수 있는 사실이나 행동도 분명하게 적을 것\n"
        "- 질문자가 유리하게 만들려면 어떤 사실을 더 확보·정리해야 하는지도 구체적으로 적을 것\n"
        "- 각 주장 축은 본문에서 `### <claim_axis>` 형식의 소제목으로 정확히 한 번 이상 다룰 것\n"
        "- 각 주장 축 아래에는 `근거 인용` 소제목을 두고, support_spans 중 최소 1개의 인용을 `**(출처 기준일 발행 레코드번호 자료)** \"인용문\"` 형식으로 적을 것\n"
        "- 본문에 들어간 각 주장 축은 최소 1개의 직접 인용을 반드시 포함해야 한다\n"
        "- 질문에 맞는 적절한 섹션 구조를 스스로 정할 것\n"
        "- 예시는 유리한 논리, 불리한 논리, 유리/불리 조건, 주의점, 대응 포인트, 참조 기억 레코드 요약 등이지만, 질문과 맞지 않는 섹션 제목은 만들지 말 것\n"
        "- 무엇을 하면 유리해지고 무엇을 하면 불리해지는지 구체적으로 적을 것\n"
        "- 마지막에는 `참고 기억 레코드 목록`을 두고 `매우 유사한 기억 레코드`, `유사한 기억 레코드`, `이용할 만한 기억 레코드`, `기타` 네 제목으로 정확히 정리할 것\n"
        "- `기타`는 간단히만 적고, coverage 대상은 매우 유사/유사/이용할 만한 기억 레코드뿐이다.\n"
        "- 근거 없는 추측 금지. 아래 selected claim ledger 안에 있는 내용만 사용한다.\n"
        "- 결과물은 평문 또는 마크다운 텍스트로만 출력한다.\n\n"
        f"질문:\n{user_task}\n\n"
        + planner_block
        + "\n\n"
        "[selected claim ledger]\n"
        + "\n\n".join(_render_question_claim_for_prompt(claim) for claim in claims)
        + "\n\n[참고 기억 레코드 위계]\n"
        + _render_question_record_buckets(record_buckets)
    )


def _question_answer_has_meta_leak(text: str) -> bool:
    lowered = text.lower()
    leak_markers = [
        "input: a draft",
        "goal: rewrite",
        "constraints:",
        "selected claim ledger",
        "self-correction",
        "self correction",
        "final check",
        "drafting the final response",
        "let's go",
        "wait, the instruction says",
        "wait, the prompt says",
        "the prompt says",
        "지시문",
        "제공된 `selected claim ledger`",
        "[selected claim ledger]",
        "[참고 기억 레코드 위계]",
        "*   `## 종합 판단`",
    ]
    return any(marker in lowered or marker in text for marker in leak_markers)


def _strip_question_answer_meta_prefix(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    if not _question_answer_has_meta_leak(raw):
        return raw
    matches = list(re.finditer(r"##\s*종합\s*판단", raw))
    if not matches:
        return raw
    start = matches[-1].start()
    stripped = raw[start:].strip()
    stripped = re.sub(r"^\*?\s*(?:let'?s go\.?|이제 작성한다\.?)\s*\*?\s*", "", stripped, flags=re.IGNORECASE).strip()
    return stripped or raw


def _question_answer_missing_required_sections(text: str) -> bool:
    required = [
        "## 종합 판단",
        "## 유리하게 만들 요소",
        "## 불리하게 만들 요소",
        "참고 기억 레코드 목록",
        "매우 유사한 기억 레코드",
        "유사한 기억 레코드",
        "이용할 만한 기억 레코드",
    ]
    return any(section not in text for section in required)


def _rewrite_question_answer_source_refs(text: str, claims: list[dict[str, Any]]) -> str:
    evidence_map: dict[str, str] = {}
    for claim in _hydrate_question_claims(claims):
        source_ref = _normalize_space(claim.get("source_ref")) or _format_question_case_source_ref(
            str(claim.get("record_number") or ""),
            str(claim.get("source_org") or ""),
            str(claim.get("record_date") or ""),
            str(claim.get("record_title") or ""),
        )
        for span in claim.get("support_spans") or []:
            evidence_id = _normalize_space(span.get("evidence_id"))
            if evidence_id and source_ref:
                evidence_map[evidence_id] = source_ref
    rewritten = text
    for evidence_id, source_ref in evidence_map.items():
        rewritten = re.sub(
            rf"\[\s*{re.escape(evidence_id)}\s*\]",
            f"**({source_ref})**",
            rewritten,
        )
        rewritten = re.sub(
            rf"(?<!\S){re.escape(evidence_id)}(?!\S)",
            source_ref,
            rewritten,
        )
    rewritten = re.sub(r"\*\*\s*\*\*", "", rewritten)
    rewritten = re.sub(r"\n{3,}", "\n\n", rewritten)
    return rewritten.strip()


def _repair_question_answer_output(
    *,
    user_task: str,
    draft_text: str,
    claims: list[dict[str, Any]],
    record_buckets: dict[str, list[dict[str, Any]]],
    answer_plan: dict[str, Any] | None,
    model: str,
) -> str:
    repair_prompt = (
        "아래 초안은 메타 설명, 지시문, self-check, 중간 메모가 섞여 있다.\n"
        "사용자에게 바로 보여줄 완성 답변만 다시 작성하라.\n"
        "규칙:\n"
        "- 지시문, 단계 설명, 영어 메모, self-correction, claim_id 나열을 쓰지 말 것\n"
        "- `## 종합 판단`에서 가장 가능성이 높은 결론을 먼저 분명히 적을 것\n"
        "- `## 종합 판단`의 첫 문장은 단정형으로 시작하고, 가장 강한 주장 축과 그 이유를 곧바로 적을 것\n"
        "- `## 종합 판단`의 앞 두 문장 안에 가장 강한 주장 묶음, 반복적으로 지지하는 기억 레코드 수, 왜 그 방향이 우세한지를 반드시 적을 것\n"
        "- `## 유리하게 만들 요소`와 `## 불리하게 만들 요소` 섹션을 반드시 둘 것\n"
        "- claim_groups가 있으면 같은 묶음 안의 주장들을 함께 정리하고, 서로 다른 주장처럼 흩뿌리지 말 것\n"
        "- 같은 주장 축을 지지하는 별개 기억 레코드가 2건 이상이면 그 점을 신뢰도 근거로 자연스럽게 적을 것\n"
        "- 각 주장 축은 `### <claim_axis>`로 다루고 `근거 인용` 아래에 최소 1개의 직접 인용을 넣을 것\n"
        "- 인용 형식은 `**(출처 기준일 발행 레코드번호 자료)** \"인용문\"`이다\n"
        "- 질문자에게 유리하게 만들 요소와 불리하게 만들 요소를 구체적으로 적을 것\n"
        "- 마지막에 `참고 기억 레코드 목록`을 두고 `매우 유사한 기억 레코드`, `유사한 기억 레코드`, `이용할 만한 기억 레코드`, `기타`를 구분할 것\n"
        "- `기타`는 간단히만 적을 것\n\n"
        f"[질문]\n{user_task}\n\n"
        f"[사전 판단]\n{json.dumps(answer_plan or {}, ensure_ascii=False, indent=2)}\n\n"
        "[선택된 주장]\n"
        + "\n\n".join(_render_question_claim_for_prompt(claim) for claim in claims)
        + "\n\n[참고 기억 레코드 위계]\n"
        + _render_question_record_buckets(record_buckets)
        + "\n\n[초안]\n"
        + draft_text
    )
    return call_chat(
        [
            {"role": "system", "content": "자료 답변 정리자다. 메타 설명 없이 사용자에게 바로 보여줄 한국어 최종답만 출력하라."},
            {"role": "user", "content": repair_prompt},
        ],
        model=model,
        timeout=600,
    ).strip()


def write_question_answer(
    *,
    user_task: str,
    claims: list[dict[str, Any]],
    record_buckets: dict[str, list[dict[str, Any]]],
    answer_plan: dict[str, Any] | None = None,
    model: str,
) -> str:
    prompt = build_question_answer_prompt(
        user_task=user_task,
        claims=claims,
        record_buckets=record_buckets,
        answer_plan=answer_plan,
    )
    output = call_chat(
        [
            {"role": "system", "content": "자료 질문에 대한 실무형 기억 레코드 답변을 작성하라. 지시문, 메모, self-check를 노출하지 말고 한국어 최종답만 출력하라."},
            {"role": "user", "content": prompt},
        ],
        model=model,
        timeout=600,
    ).strip()
    if _question_answer_has_meta_leak(output) or _question_answer_missing_required_sections(output):
        output = _repair_question_answer_output(
            user_task=user_task,
            draft_text=output,
            claims=claims,
            record_buckets=record_buckets,
            answer_plan=answer_plan,
            model=model,
        )
    output = _strip_question_answer_meta_prefix(output)
    output = _rewrite_question_answer_source_refs(output, claims)
    return output


def build_question_coverage_patch_prompt(
    *,
    user_task: str,
    claims: list[dict[str, Any]],
    current_answer: str,
    record_buckets: dict[str, list[dict[str, Any]]],
    gaps: dict[str, list[str]],
    answer_plan: dict[str, Any] | None = None,
) -> str:
    answer_plan = answer_plan or {}
    planner_block_parts = []
    if answer_plan.get("likely_outcome"):
        planner_block_parts.append(f"가장 가능성 높은 결론:\n- {answer_plan.get('likely_outcome')}")
    if answer_plan.get("confidence_basis"):
        planner_block_parts.append(
            "결론 근거:\n" + "\n".join(f"- {item}" for item in answer_plan.get("confidence_basis") or [])
        )
    if answer_plan.get("helpful_facts"):
        planner_block_parts.append(
            "유리하게 만들 요소:\n" + "\n".join(f"- {item}" for item in answer_plan.get("helpful_facts") or [])
        )
    if answer_plan.get("harmful_facts"):
        planner_block_parts.append(
            "불리하게 만들 요소:\n" + "\n".join(f"- {item}" for item in answer_plan.get("harmful_facts") or [])
        )
    if answer_plan.get("claim_groups"):
        planner_block_parts.append(
            "주장 묶음:\n" + _render_question_claim_groups(answer_plan.get("claim_groups") or [], claims)
        )
    planner_block = "\n\n[사전 판단]\n" + "\n\n".join(planner_block_parts) if planner_block_parts else ""
    missing_axes = set(gaps.get("missing_axes") or []) | set(gaps.get("missing_source_refs") or [])
    missing_claims = [
        claim for claim in claims if _normalize_space(claim.get("claim_axis")) in missing_axes
    ]
    missing_record_rows: list[dict[str, Any]] = []
    for record_number in gaps.get("missing_records") or []:
        found = _find_required_record_row(record_buckets, record_number)
        if found:
            _bucket, row = found
            missing_record_rows.append(row)
    missing_payload_parts: list[str] = []
    if missing_claims:
        missing_payload_parts.append(
            "[본문에 패치해야 할 주장]\n"
            + "\n\n".join(_render_question_claim_for_prompt(claim) for claim in missing_claims)
        )
    if missing_record_rows:
        missing_payload_parts.append(
            "[본문에 패치해야 할 기억 레코드]\n"
            + "\n".join(
                "- "
                + _format_question_case_source_ref(
                    str(row.get("record_number") or ""),
                    str(row.get("source_org") or ""),
                    str(row.get("record_date") or ""),
                    str(row.get("record_title") or ""),
                )
                + f" | 관련 주장: {', '.join(_coerce_string_list(row.get('supported_claim_axes')))}"
                + f" | 사유: {_normalize_space(row.get('why'))}"
                for row in missing_record_rows
            )
        )
    missing_payload = "\n\n".join(missing_payload_parts) or "(없음)"
    return (
        "아래는 이미 작성된 자료 질문 답변과 selected claim ledger, 참고 기억 레코드 위계다.\n"
        "목표는 선택된 주장 축이 최종 답변 본문에서 명시적으로 하나도 누락되지 않게 하는 것이다.\n"
        "PATCH MODE다. coverage bundle maker처럼 전체 답변을 다시 쓰지 말고, 누락된 분석결과를 어디에 넣을지 패치 지시만 출력하라.\n"
        "중요:\n"
        "- 절대 완성본 전체를 다시 출력하지 말 것\n"
        "- 절대 현재 답변을 요약하거나 재작성하지 말 것\n"
        "- 누락된 claim/기억 레코드 분석결과만 보고, 기존 본문에 넣을 짧은 local edit만 만들 것\n"
        "- 출력은 반드시 `<patches>...</patches>` 안의 `<patch>` 블록들만 낼 것\n"
        "- `<patch>`에는 `<section>`, `<op>`, `<before>`, `<after>`, `<content>`만 넣을 것\n"
        "- `<op>`는 `append`, `insert_after`, `insert_before`, `insert_between` 중 하나다\n"
        "- `<section>`에는 기존 답변의 마크다운 섹션 제목을 적을 것. 모르면 빈 값으로 두면 참고 기억 레코드 목록 앞 본문에 적용된다\n"
        "- `<before>`와 `<after>`는 현재 답변에서 짧게 그대로 찾을 수 있는 anchor 문구를 적을 것. append면 비워도 된다\n"
        "- `<content>`에는 실제로 끼워 넣을 1~3문단 markdown만 적을 것\n"
        "- 이미 들어간 내용을 장황하게 반복하지 말 것\n"
        "- 각 claim_axis가 본문에서 적어도 한 번은 명시적으로 드러나도록 보완할 것\n"
        "- 각 claim_axis는 `### <claim_axis>` 형식의 소제목으로 정확히 남겨둘 것\n"
        "- 각 claim_axis 아래에는 `근거 인용` 소제목을 두고, support_spans 중 최소 1개의 인용을 `**(출처 기준일 발행 레코드번호 자료)** \"인용문\"` 형식으로 반드시 넣을 것\n"
        "- very_similar/similar/usable에 들어간 레코드번호는 `참고 기억 레코드 목록`뿐 아니라 본문 논증 안에도 반드시 자연스럽게 들어가야 한다\n"
        "- other 버킷 기억 레코드는 coverage 대상이 아니다\n"
        "- 절대 `누락 보정`, `보강`, `coverage`, `패치` 같은 작업명 섹션을 만들지 말 것\n"
        "- 보완 내용은 기존 주장 섹션 안에 자연스럽게 흡수하거나, 필요한 경우 기존 섹션을 다시 정돈할 것\n"
        "- 새 주장을 만들지 말고, 누락된 claim_axis와 기억 레코드만 기존 논리 흐름에 짧게 반영할 것\n"
        "- claim ledger에 없는 새 사실을 만들지 말 것\n"
        "- 패치 후 검증은 시스템이 다시 수행하므로 스스로 전체 재검증 문구를 쓰지 말 것\n\n"
        "출력 형식:\n"
        "<patches>\n"
        "<patch>\n"
        "<section>기존 섹션 제목</section>\n"
        "<op>append</op>\n"
        "<before></before>\n"
        "<after></after>\n"
        "<content>여기에 넣을 짧은 markdown 문단</content>\n"
        "</patch>\n"
        "</patches>\n\n"
        f"질문:\n{user_task}\n\n"
        + planner_block
        + "\n\n"
        "[현재 답변]\n"
        f"{current_answer}\n\n"
        "[패치할 누락 분석결과]\n"
        + missing_payload
    )


def _question_coverage_has_gaps(gaps: dict[str, list[str]]) -> bool:
    return bool(gaps.get("missing_axes") or gaps.get("missing_source_refs") or gaps.get("missing_records"))


def _subset_gap_values(raw_values: Any, allowed_values: list[str]) -> list[str]:
    if not isinstance(raw_values, list):
        return []
    by_normalized = {_normalize_for_coverage(value): value for value in allowed_values}
    output: list[str] = []
    for raw_value in raw_values:
        normalized = _normalize_for_coverage(str(raw_value or ""))
        if normalized in by_normalized:
            value = by_normalized[normalized]
            if value not in output:
                output.append(value)
    return output


def _parse_question_coverage_verifier_gaps(
    raw: str,
    candidate_gaps: dict[str, list[str]],
) -> dict[str, list[str]] | None:
    try:
        parsed = json.loads(_extract_json_block(raw))
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    return {
        "missing_axes": _subset_gap_values(parsed.get("missing_axes"), candidate_gaps.get("missing_axes") or []),
        "missing_source_refs": _subset_gap_values(parsed.get("missing_source_refs"), candidate_gaps.get("missing_source_refs") or []),
        "missing_records": _subset_gap_values(parsed.get("missing_records"), candidate_gaps.get("missing_records") or []),
    }


def build_question_coverage_verifier_prompt(
    *,
    user_task: str,
    claims: list[dict[str, Any]],
    current_answer: str,
    record_buckets: dict[str, list[dict[str, Any]]],
    candidate_gaps: dict[str, list[str]],
) -> str:
    missing_axes = set(candidate_gaps.get("missing_axes") or []) | set(candidate_gaps.get("missing_source_refs") or [])
    missing_claims = [
        claim for claim in claims if _normalize_space(claim.get("claim_axis")) in missing_axes
    ]
    missing_rows: list[str] = []
    for record_number in candidate_gaps.get("missing_records") or []:
        found = _find_required_record_row(record_buckets, record_number)
        if not found:
            continue
        bucket, row = found
        missing_rows.append(
            "- "
            + _format_question_case_source_ref(
                str(row.get("record_number") or ""),
                str(row.get("source_org") or ""),
                str(row.get("record_date") or ""),
                str(row.get("record_title") or ""),
            )
            + f" | 분류: {bucket}"
            + f" | 관련 주장: {', '.join(_coerce_string_list(row.get('supported_claim_axes')))}"
            + f" | 사유: {_normalize_space(row.get('why'))}"
        )
    claim_block = "\n\n".join(_render_question_claim_for_prompt(claim) for claim in missing_claims) or "(없음)"
    record_block = "\n".join(missing_rows) or "(없음)"
    return (
        "COVERAGE VERIFIER다. 아래 현재 답변 본문과 후보 누락 목록을 비교해서, 아직 실제로 빠진 항목만 골라라.\n"
        "기계적 문자열 일치가 아니라 의미상 반영 여부를 판단한다.\n"
        "예를 들어 레코드번호가 정확히 같은 문자열로 없더라도, 같은 출처/기준일/레코드명/직접 인용문이 본문에 들어가 그 기억 레코드가 실질적으로 쓰였으면 누락으로 보지 않는다.\n"
        "반대로 `참고 기억 레코드 목록`에만 있고 본문 논증에는 없으면 누락이다.\n"
        "확실히 반영됐다고 판단할 수 없으면 누락으로 남겨라.\n"
        "other 버킷은 coverage 대상이 아니므로 판단하지 않는다.\n"
        "출력은 JSON 객체 하나만 반환한다. 후보 목록에 없는 새 값을 만들지 마라.\n"
        "{\n"
        '  "missing_axes": [],\n'
        '  "missing_source_refs": [],\n'
        '  "missing_records": []\n'
        "}\n\n"
        f"질문:\n{user_task}\n\n"
        "[현재 답변]\n"
        f"{current_answer}\n\n"
        "[후보 누락 claim]\n"
        f"{claim_block}\n\n"
        "[후보 누락 기억 레코드]\n"
        f"{record_block}\n\n"
        "[기계 후보 목록]\n"
        f"{json.dumps(candidate_gaps, ensure_ascii=False, indent=2)}"
    )


def filter_question_coverage_gaps_with_llm(
    *,
    user_task: str,
    claims: list[dict[str, Any]],
    current_answer: str,
    record_buckets: dict[str, list[dict[str, Any]]],
    candidate_gaps: dict[str, list[str]],
    model: str,
) -> dict[str, list[str]]:
    if not _question_coverage_has_gaps(candidate_gaps):
        return candidate_gaps
    prompt = build_question_coverage_verifier_prompt(
        user_task=user_task,
        claims=claims,
        current_answer=current_answer,
        record_buckets=record_buckets,
        candidate_gaps=candidate_gaps,
    )
    try:
        raw = call_chat(
            [
                {
                    "role": "system",
                    "content": "자료 답변 coverage verifier다. 후보 누락 중 아직 실제로 빠진 항목만 JSON으로 반환한다.",
                },
                {"role": "user", "content": prompt},
            ],
            model=model,
            timeout=120,
        ).strip()
    except Exception:
        return candidate_gaps
    verified = _parse_question_coverage_verifier_gaps(raw, candidate_gaps)
    return verified if verified is not None else candidate_gaps


def _build_question_coverage_supplement(
    *,
    claims: list[dict[str, Any]],
    record_buckets: dict[str, list[dict[str, Any]]],
    gaps: dict[str, list[str]],
) -> str:
    claim_by_axis = {
        _normalize_space(claim.get("claim_axis")): claim
        for claim in _hydrate_question_claims(claims)
        if _normalize_space(claim.get("claim_axis"))
    }
    sections: list[str] = []
    seen_axes: set[str] = set()
    for axis in [*(gaps.get("missing_axes") or []), *(gaps.get("missing_source_refs") or [])]:
        normalized_axis = _normalize_space(axis)
        if not normalized_axis or normalized_axis in seen_axes:
            continue
        seen_axes.add(normalized_axis)
        claim = claim_by_axis.get(normalized_axis)
        if not claim:
            continue
        lines = [f"### {normalized_axis}"]
        claim_text = _normalize_space(claim.get("claim_text") or claim.get("context_summary"))
        if claim_text:
            lines.append(claim_text)
        support_markers = _claim_support_markers(claim)
        source_ref, quote, evidence_id = support_markers[0] if support_markers else ("", "", "")
        if quote:
            lines.append("근거 인용")
            if source_ref:
                lines.append(f"**({source_ref})** \"{quote}\"")
            elif evidence_id:
                lines.append(f"[{evidence_id}] \"{quote}\"")
            else:
                lines.append(f"\"{quote}\"")
        sections.append("\n\n".join(lines))
    required_rows: dict[str, dict[str, Any]] = {}
    for key in ("very_similar", "similar", "usable"):
        for row in record_buckets.get(key) or []:
            record_number = _normalize_space(row.get("record_number"))
            if record_number and record_number not in required_rows:
                required_rows[record_number] = row
    record_lines: list[str] = []
    for record_number in gaps.get("missing_records") or []:
        row = required_rows.get(_normalize_space(record_number), {})
        source_ref = _format_question_case_source_ref(
            str(row.get("record_number") or record_number),
            str(row.get("source_org") or ""),
            str(row.get("record_date") or ""),
            str(row.get("record_title") or ""),
        )
        axes = ", ".join(row.get("supported_claim_axes") or [])
        why = _normalize_space(row.get("why"))
        suffix = " ".join(part for part in [f"관련 주장: {axes}" if axes else "", why] if part).strip()
        record_lines.append(f"- {source_ref or record_number}{(' - ' + suffix) if suffix else ''}")
    if record_lines:
        sections.append("## 참고 기억 레코드 목록\n\n" + "\n".join(record_lines))
    if not sections:
        return ""
    return "\n\n".join(sections)


def apply_question_coverage_patch(
    *,
    user_task: str,
    claims: list[dict[str, Any]],
    current_answer: str,
    record_buckets: dict[str, list[dict[str, Any]]],
    answer_plan: dict[str, Any] | None,
    model: str,
) -> str:
    output = current_answer.strip()
    coverage_claims = _hydrate_question_claims(claims)
    previous_gap_signature = ""
    while True:
        gaps = find_question_coverage_gaps(output, coverage_claims, record_buckets=record_buckets)
        gaps = filter_question_coverage_gaps_with_llm(
            user_task=user_task,
            claims=coverage_claims,
            current_answer=output,
            record_buckets=record_buckets,
            candidate_gaps=gaps,
            model=model,
        )
        gap_signature = json.dumps(gaps, ensure_ascii=False, sort_keys=True)
        if not gaps["missing_axes"] and not gaps["missing_source_refs"] and not gaps["missing_records"]:
            return output

        deterministic_patch = _apply_question_coverage_fallback(
            answer_text=output,
            claims=coverage_claims,
            record_buckets=record_buckets,
            gaps=gaps,
        )
        if deterministic_patch != output:
            output = deterministic_patch
            previous_gap_signature = ""
            continue

        missing_block = []
        if gaps["missing_axes"]:
            missing_block.append("누락된 claim_axis:\n" + "\n".join(f"- {item}" for item in gaps["missing_axes"]))
        if gaps["missing_source_refs"]:
            missing_block.append("인용이 없는 claim_axis:\n" + "\n".join(f"- {item}" for item in gaps["missing_source_refs"]))
        if gaps["missing_records"]:
            missing_block.append("본문 논증에서 빠진 레코드번호:\n" + "\n".join(f"- {item}" for item in gaps["missing_records"]))
        prompt = build_question_coverage_patch_prompt(
            user_task=user_task,
            claims=coverage_claims,
            current_answer=output,
            record_buckets=record_buckets,
            answer_plan=answer_plan,
            gaps=gaps,
        )
        patch_prompt = prompt + "\n\n[누락 진단]\n" + "\n\n".join(missing_block)
        raw_patch = call_chat(
            [
                {"role": "system", "content": "자료 질문 답변의 claim coverage patcher다. 전체 답변 재작성은 금지다. <patches> 안의 local edit 패치 블록만 출력하라."},
                {"role": "user", "content": patch_prompt},
            ],
            model=model,
            timeout=120,
        ).strip()
        patches = _parse_question_coverage_patch_blocks(raw_patch)
        if not patches:
            return output
        patched, applied_count = _apply_question_coverage_patch_blocks(output, patches)
        if applied_count <= 0:
            return output
        patched = _rewrite_question_answer_source_refs(patched, coverage_claims)
        if patched == output or gap_signature == previous_gap_signature:
            integrated = _apply_question_coverage_fallback(
                answer_text=patched,
                claims=coverage_claims,
                record_buckets=record_buckets,
                gaps=gaps,
            )
            if integrated == patched:
                return patched
            output = integrated
            previous_gap_signature = ""
            continue
        previous_gap_signature = gap_signature
        output = patched


def write_question_answer_plan(
    *,
    user_task: str,
    claims: list[dict[str, Any]],
    catalog: list[dict[str, Any]],
    model: str,
) -> dict[str, Any]:
    hydrated_claims = _hydrate_question_claims(claims)
    prompt = build_question_answer_plan_prompt(user_task=user_task, claims=hydrated_claims, catalog=catalog)
    raw = call_chat(
        [
            {"role": "system", "content": "자료 기억 레코드 planner다. JSON만 출력하라."},
            {"role": "user", "content": prompt},
        ],
        model=model,
        timeout=600,
    ).strip()
    plan = _extract_question_answer_plan(raw=raw, claims=hydrated_claims, catalog=catalog)
    fallback = _fallback_question_answer_plan(hydrated_claims, catalog)
    for key in ("likely_outcome",):
        if not _normalize_space(plan.get(key)):
            plan[key] = fallback.get(key)
    for key in ("confidence_basis", "helpful_facts", "harmful_facts", "body_claim_ids"):
        if not plan.get(key):
            plan[key] = fallback.get(key) or []
    if not plan.get("claim_groups"):
        plan["claim_groups"] = fallback.get("claim_groups") or []
    if not any((plan.get("record_buckets") or {}).get(bucket) for bucket in ("very_similar", "similar", "usable")):
        plan["record_buckets"] = fallback.get("record_buckets") or plan.get("record_buckets") or {}
    plan["raw"] = raw
    return plan


def select_top_structured_record_records(
    structured_dirs: list[Path],
    *,
    user_task: str,
    model: str,
    keyword_count: int,
    top_k: int,
) -> tuple[list[FileRecord], dict[str, Any]]:
    keywords = generate_search_keywords(user_task, model=model, keyword_count=keyword_count)
    rows: list[dict[str, Any]] = []
    for structured_dir in structured_dirs:
        rows.extend(iter_structured_record_rows(structured_dir))
    selected_rows = select_top_structured_record_rows(rows, keywords=keywords, top_k=top_k)
    records = [structured_row_to_file_record(row) for row in selected_rows]
    selection_meta = {
        "keywords": keywords,
        "selected_count": len(records),
        "selected_rows": [
            {
                "record_number": row.get("record_number"),
                "record_title": row.get("record_title"),
                "source_org": row.get("source_org"),
                "record_date": row.get("record_date"),
                "matched_keywords": row.get("_matched_keywords") or [],
                "keyword_hit_count": row.get("_keyword_hit_count") or 0,
                "keyword_weighted_score": row.get("_keyword_weighted_score") or 0,
                "source_path": row.get("source_path"),
            }
            for row in selected_rows
        ],
    }
    return records, selection_meta


def _coerce_selected_record_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = None
        for key in ("selected_rows", "selected_files", "records", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                items = value
                break
        if items is None:
            raise ValueError("selected record JSON must be a list or contain a list under selected_rows/selected_files/records/items")
    else:
        raise ValueError("selected record JSON must be a list or object")
    out: list[dict[str, Any]] = []
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(f"selected record item at index {index} is not an object")
        out.append(item)
    return out


def _file_record_from_payload(payload: dict[str, Any]) -> FileRecord:
    return FileRecord(
        file_id=str(payload.get("file_id") or "").strip(),
        relative_path=str(payload.get("relative_path") or "").strip(),
        absolute_path=str(payload.get("absolute_path") or "").strip(),
        document_title=str(payload.get("document_title") or "").strip(),
        doc_type=str(payload.get("doc_type") or "").strip() or "txt",
        source_group=str(payload.get("source_group") or "").strip() or "structured_record",
        token_count=int(payload.get("token_count") or 0),
        anchor_text=str(payload.get("anchor_text") or ""),
        extracted_text=str(payload.get("extracted_text") or ""),
        candidate_boundaries=list(payload.get("candidate_boundaries") or []),
        is_direct_evidence=bool(payload.get("is_direct_evidence")),
        is_format_sample=bool(payload.get("is_format_sample")),
        content_hash=str(payload.get("content_hash") or ""),
        duplicate_paths=[str(value) for value in (payload.get("duplicate_paths") or []) if str(value).strip()],
        record_number=str(payload.get("record_number") or "").strip(),
        source_org=str(payload.get("source_org") or "").strip(),
        record_date=str(payload.get("record_date") or "").strip(),
        record_title=str(payload.get("record_title") or "").strip(),
    )


def load_selected_question_records(path: Path) -> list[FileRecord]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    items = _coerce_selected_record_items(payload)
    records: list[FileRecord] = []
    for item in items:
        if {"file_id", "relative_path", "absolute_path", "document_title", "extracted_text"}.issubset(item):
            records.append(_file_record_from_payload(item))
            continue
        records.append(structured_row_to_file_record(item))
    return records


def _run_question_variant_from_selected_records(
    *,
    name: str,
    selected_records: list[FileRecord],
    selection_meta: dict[str, Any],
    selection_reasoning_text: str,
    user_task: str,
    out_dir: Path,
    analyze_model: str,
    draft_model: str,
    question_chunk_tokens: int,
    analyze_workers: int,
    chunk_build_workers: int,
) -> dict[str, Any]:
    variant_dir = _variant_dir(out_dir, name)
    runtime_dir = _variant_dir(out_dir, "_runtime")
    runtime_status_path = runtime_dir / "status.json"
    shared_cache_dir = out_dir / "_shared_cache" / "chunk_analysis"

    write_runtime_status(
        runtime_status_path,
        {
            "phase": "variant",
            "variant": name,
            "state": "starting",
            "selected_file_count": len(selected_records),
        },
    )
    _write_json(variant_dir / "selected_files.json", [dataclasses.asdict(record) for record in selected_records])
    _write_json(variant_dir / "selection_meta.json", selection_meta)
    (variant_dir / "selection_reasoning.txt").write_text(selection_reasoning_text.strip(), encoding="utf-8")

    chunks = build_question_chunks_for_records(
        selected_records,
        max_tokens=question_chunk_tokens,
        workers=chunk_build_workers,
        status_path=runtime_status_path,
        variant_name=name,
    )
    chunk_outputs = analyze_chunks_cached(
        chunks,
        user_task=user_task,
        model=analyze_model,
        cache_dir=shared_cache_dir,
        analysis_mode="question",
        workers=analyze_workers,
        status_path=runtime_status_path,
        variant_name=name,
    )
    jsonl_path = variant_dir / "chunk_outputs.jsonl"
    all_claims: list[dict[str, Any]] = []
    with jsonl_path.open("w", encoding="utf-8") as fp:
        for result in chunk_outputs:
            fp.write(json.dumps(result, ensure_ascii=False) + "\n")
            all_claims.extend(result.get("claims_proposed") or [])

    merged_claims = merge_claim_ledgers(all_claims, analysis_mode="question")
    merged_claims = _assign_question_claim_ids(merged_claims)
    claim_ledger_path = variant_dir / "claim_ledger.json"
    _write_json(claim_ledger_path, merged_claims)

    record_catalog = _build_question_record_catalog(merged_claims)
    answer_plan = write_question_answer_plan(
        user_task=user_task,
        claims=merged_claims,
        catalog=record_catalog,
        model=analyze_model,
    )
    _write_json(variant_dir / "answer_plan.json", answer_plan)

    selected_claim_ids = list(answer_plan.get("body_claim_ids") or [])
    selected_claims = [claim for claim in merged_claims if claim.get("claim_id") in selected_claim_ids]
    if not selected_claims:
        selected_claims = merged_claims[: min(12, len(merged_claims))]
    record_buckets = answer_plan.get("record_buckets") or {
        "very_similar": [],
        "similar": [],
        "usable": [],
        "other": [],
    }

    write_runtime_status(
        runtime_status_path,
        {
            "phase": "draft",
            "variant": name,
            "state": "writing_final_draft",
            "selected_file_count": len(selected_records),
            "chunk_count": len(chunks),
            "completed_chunks": len(chunks),
            "claim_count": len(merged_claims),
        },
    )
    first_answer = write_question_answer(
        user_task=user_task,
        claims=selected_claims,
        record_buckets=record_buckets,
        answer_plan=answer_plan,
        model=draft_model,
    )
    write_runtime_status(
        runtime_status_path,
        {
            "phase": "coverage",
            "variant": name,
            "state": "applying_coverage_patch",
            "selected_file_count": len(selected_records),
            "chunk_count": len(chunks),
            "completed_chunks": len(chunks),
            "claim_count": len(merged_claims),
        },
    )
    patched_answer = apply_question_coverage_patch(
        user_task=user_task,
        claims=selected_claims,
        current_answer=first_answer,
        record_buckets=record_buckets,
        answer_plan=answer_plan,
        model=draft_model,
    )
    patched_answer = _strip_question_answer_meta_prefix(patched_answer)
    final_answer_path = variant_dir / "final_answer.md"
    final_answer_path.write_text(patched_answer, encoding="utf-8")
    _write_json(
        variant_dir / "comparison_summary.json",
        {
            "variant": name,
            "selected_file_count": len(selected_records),
            "chunk_count": len(chunks),
            "claim_count": len(merged_claims),
            "selection_source": selection_meta.get("selection_source") or "",
            "keyword_count": len(selection_meta.get("keywords") or []),
        },
    )
    write_runtime_status(
        runtime_status_path,
        {
            "phase": "done",
            "variant": name,
            "state": "completed",
            "selected_file_count": len(selected_records),
            "chunk_count": len(chunks),
            "completed_chunks": len(chunks),
            "claim_count": len(merged_claims),
            "finished_at": time.time(),
        },
    )
    return {
        "variant": name,
        "selected_records": selected_records,
        "merged_claims": merged_claims,
        "final_draft_path": str(final_answer_path),
        "summary_path": str(variant_dir / "comparison_summary.json"),
    }


def run_question_variant(
    *,
    name: str,
    structured_dirs: list[Path],
    user_task: str,
    out_dir: Path,
    select_model: str,
    analyze_model: str,
    draft_model: str,
    keyword_count: int,
    top_k_records: int,
    question_chunk_tokens: int,
    analyze_workers: int,
    chunk_build_workers: int,
) -> dict[str, Any]:
    selected_records, selection_meta = select_top_structured_record_records(
        structured_dirs,
        user_task=user_task,
        model=select_model,
        keyword_count=keyword_count,
        top_k=top_k_records,
    )
    selection_meta = dict(selection_meta)
    selection_meta.setdefault("selection_source", "generated_keywords")
    return _run_question_variant_from_selected_records(
        name=name,
        selected_records=selected_records,
        selection_meta=selection_meta,
        selection_reasoning_text="keywords: " + ", ".join(selection_meta.get("keywords") or []),
        user_task=user_task,
        out_dir=out_dir,
        analyze_model=analyze_model,
        draft_model=draft_model,
        question_chunk_tokens=question_chunk_tokens,
        analyze_workers=analyze_workers,
        chunk_build_workers=chunk_build_workers,
    )


def run_variant(
    *,
    name: str,
    records: list[FileRecord],
    selected_ids: list[str],
    selection_reasoning: list[str],
    user_task: str,
    sample_texts: list[str],
    out_dir: Path,
    analyze_model: str,
    draft_model: str,
) -> dict[str, Any]:
    variant_dir = _variant_dir(out_dir, name)
    runtime_dir = _variant_dir(out_dir, "_runtime")
    runtime_status_path = runtime_dir / "status.json"
    shared_cache_dir = out_dir / "_shared_cache" / "chunk_analysis"
    selected_records = _selected_records_from_ids(records, selected_ids)
    final_draft_path = variant_dir / "final_draft.md"
    claim_ledger_path = variant_dir / "claim_ledger.json"
    summary_path = variant_dir / "comparison_summary.json"

    if final_draft_path.exists() and claim_ledger_path.exists() and summary_path.exists():
        merged_claims = json.loads(claim_ledger_path.read_text(encoding="utf-8"))
        write_runtime_status(
            runtime_status_path,
            {
                "phase": "variant",
                "variant": name,
                "state": "reused_complete",
                "selected_file_count": len(selected_records),
                "claim_count": len(merged_claims),
                "final_draft_path": str(final_draft_path),
            },
        )
        return {
            "variant": name,
            "selected_records": selected_records,
            "merged_claims": merged_claims,
            "final_draft_path": str(final_draft_path),
            "summary_path": str(summary_path),
        }

    write_runtime_status(
        runtime_status_path,
        {
            "phase": "variant",
            "variant": name,
            "state": "starting",
            "selected_file_count": len(selected_records),
        },
    )
    _write_json(
        variant_dir / "selected_files.json",
        [dataclasses.asdict(record) for record in selected_records],
    )
    (variant_dir / "selection_reasoning.txt").write_text("\n\n".join(reason for reason in selection_reasoning if reason).strip(), encoding="utf-8")

    chunks = _build_chunks_for_records(
        selected_records,
        max_tokens=DEFAULT_CHUNK_TOKENS,
        status_path=runtime_status_path,
        variant_name=name,
    )
    write_runtime_status(
        runtime_status_path,
        {
            "phase": "variant",
            "variant": name,
            "state": "analyzing_chunks",
            "selected_file_count": len(selected_records),
            "chunk_count": len(chunks),
            "completed_chunks": 0,
        },
    )
    chunk_outputs = analyze_chunks_cached(
        chunks,
        user_task=user_task,
        model=analyze_model,
        cache_dir=shared_cache_dir,
        status_path=runtime_status_path,
        variant_name=name,
    )
    all_claims = []
    jsonl_path = variant_dir / "chunk_outputs.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as fp:
        for result in chunk_outputs:
            fp.write(json.dumps(result, ensure_ascii=False) + "\n")
            all_claims.extend(result.get("claims_proposed") or [])

    merged_claims = merge_claim_ledgers(all_claims)
    _write_json(claim_ledger_path, merged_claims)
    write_runtime_status(
        runtime_status_path,
        {
            "phase": "variant",
            "variant": name,
            "state": "synthesizing_sections",
            "selected_file_count": len(selected_records),
            "chunk_count": len(chunks),
            "claim_count": len(merged_claims),
        },
    )

    section_packets = synthesize_section_packets(merged_claims, model=analyze_model, max_tokens=DEFAULT_MAX_REQUEST_TOKENS)
    section_dir = variant_dir / "section_packets"
    for section, texts in section_packets.items():
        (section_dir / f"{_safe_slug(section)}.md").parent.mkdir(parents=True, exist_ok=True)
        (section_dir / f"{_safe_slug(section)}.md").write_text("\n\n".join(texts).strip(), encoding="utf-8")

    write_runtime_status(
        runtime_status_path,
        {
            "phase": "variant",
            "variant": name,
            "state": "writing_final_draft",
            "selected_file_count": len(selected_records),
            "chunk_count": len(chunks),
            "claim_count": len(merged_claims),
            "section_count": len(section_packets),
        },
    )
    final_draft = write_final_opinion(
        user_task=user_task,
        sample_texts=sample_texts,
        claims=merged_claims,
        section_packets=section_packets,
        model=draft_model,
    )
    final_draft_path.write_text(final_draft, encoding="utf-8")
    _write_json(
        summary_path,
        {
            "variant": name,
            "selected_file_count": len(selected_records),
            "chunk_count": len(chunks),
            "claim_count": len(merged_claims),
            "opposing_claim_count": sum(1 for claim in merged_claims if claim.get("oppose_spans")),
            "direct_case_claim_count": sum(1 for claim in merged_claims if claim.get("same_situation_case_exists")),
            "certainty_distribution": Counter(claim.get("certainty") for claim in merged_claims),
        },
    )
    _write_json(
        variant_dir / "final_draft_prompt_input.json",
        {
            "user_task": user_task,
            "sample_text_lengths": [len(text) for text in sample_texts],
            "claim_count": len(merged_claims),
            "sections": list(section_packets.keys()),
        },
    )
    write_runtime_status(
        runtime_status_path,
        {
            "phase": "variant",
            "variant": name,
            "state": "completed",
            "selected_file_count": len(selected_records),
            "chunk_count": len(chunks),
            "claim_count": len(merged_claims),
            "final_draft_path": str(final_draft_path),
        },
    )
    return {
        "variant": name,
        "selected_records": selected_records,
        "merged_claims": merged_claims,
        "final_draft_path": str(final_draft_path),
        "summary_path": str(summary_path),
    }


def build_inventory(root: Path, *, cache_dir: Path) -> list[FileRecord]:
    records: list[FileRecord] = []
    canonical_by_hash: dict[str, FileRecord] = {}
    for path in iter_candidate_paths(root):
        record = build_file_record(root, path, cache_dir=cache_dir)
        for expanded in split_record_on_exact_virtual_separator(record):
            if expanded.content_hash and not expanded.is_direct_evidence:
                existing = canonical_by_hash.get(expanded.content_hash)
                if existing:
                    existing.duplicate_paths.append(expanded.relative_path)
                    continue
                canonical_by_hash[expanded.content_hash] = expanded
            records.append(expanded)
    records.sort(key=lambda item: (item.source_group, item.relative_path))
    return records


def write_comparison(out_dir: Path, variant_results: list[dict[str, Any]]) -> None:
    comparison_dir = _variant_dir(out_dir, "comparison")
    lines = ["# Variant Comparison", ""]
    rows = []
    for result in variant_results:
        rows.append(
            {
                "variant": result["variant"],
                "selected_file_count": len(result["selected_records"]),
                "claim_count": len(result["merged_claims"]),
                "final_draft_path": result["final_draft_path"],
            }
        )
        lines.extend(
            [
                f"## {result['variant']}",
                f"- selected files: {len(result['selected_records'])}",
                f"- merged claims: {len(result['merged_claims'])}",
                f"- final draft: {result['final_draft_path']}",
                "- selected files preview:",
            ]
        )
        for record in result["selected_records"][:20]:
            lines.append(f"  - {record.relative_path}")
        lines.append("")
    (comparison_dir / "variant_compare.md").write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    _write_json(comparison_dir / "variant_compare.json", rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/mnt/d/Downloads/60_폴더구조_개편/01_학습_자료/dataset_root")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--case-theme", default="선행 인지 후 미조치된 일부 비위를 나중에 다른 비위와 합산해 쟁점하려는 경우")
    parser.add_argument("--user-task", default="")
    parser.add_argument("--request-json", default="")
    parser.add_argument("--select-model", default=DEFAULT_SELECT_MODEL)
    parser.add_argument("--analyze-model", default=DEFAULT_ANALYZE_MODEL)
    parser.add_argument("--draft-model", default=DEFAULT_DRAFT_MODEL)
    parser.add_argument("--question-mode", action="store_true")
    parser.add_argument("--structured-record-dir", action="append", default=[])
    parser.add_argument("--selected-record-json", default="")
    parser.add_argument("--keyword-count", type=int, default=10)
    parser.add_argument("--top-k-records", type=int, default=100)
    parser.add_argument("--question-chunk-tokens", type=int, default=DEFAULT_QUESTION_CHUNK_TOKENS)
    parser.add_argument("--analyze-workers", type=int, default=DEFAULT_ANALYZE_WORKERS)
    parser.add_argument("--chunk-build-workers", type=int, default=DEFAULT_CHUNK_BUILD_WORKERS)
    parser.add_argument("--gemini-key-min-gap-ms", type=int, default=0)
    parser.add_argument("--gemini-key-max-inflight", type=int, default=0)
    parser.add_argument("--gemini-key-rpm-limit", type=int, default=-1)
    parser.add_argument("--gemini-key-tpm-limit", type=int, default=-1)
    parser.add_argument("--gemini-global-max-inflight", type=int, default=-1)
    parser.add_argument(
        "--format-sample",
        action="append",
        default=[],
        help="Repeatable path to a format sample HWP/TXT/PDF",
    )
    parser.add_argument(
        "--variant",
        action="append",
        choices=ALL_VARIANTS,
        default=[],
        help="Repeatable variant filter. Defaults to all variants in canonical order.",
    )
    args = parser.parse_args()
    requested_variants = resolve_requested_variants(args.variant)

    root = Path(args.root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    runtime_control_path = out_dir / "_runtime" / "control.json"
    _ensure_runtime_control_file(runtime_control_path)
    _apply_runtime_control_overrides(
        runtime_control_path,
        gemini_key_min_gap_ms=args.gemini_key_min_gap_ms or None,
        gemini_key_max_inflight=args.gemini_key_max_inflight or None,
        gemini_key_rpm_limit=args.gemini_key_rpm_limit if args.gemini_key_rpm_limit >= 0 else None,
        gemini_key_tpm_limit=args.gemini_key_tpm_limit if args.gemini_key_tpm_limit >= 0 else None,
        gemini_global_max_inflight=args.gemini_global_max_inflight if args.gemini_global_max_inflight >= 0 else None,
    )
    set_runtime_control_file(runtime_control_path)
    runtime_status_path = out_dir / "_runtime" / "status.json"
    set_runtime_status_file(runtime_status_path)
    cache_dir = out_dir / "inventory" / "text_cache"
    write_runtime_status(
        runtime_status_path,
        {
            "phase": "startup",
            "state": "building_inventory",
            "out_dir": str(out_dir),
        },
    )

    request_path = Path(args.request_json) if args.request_json else None
    request = load_memory_evidence_request(request_path) if request_path else None

    if args.question_mode:
        user_task = _normalize_space(args.user_task) or _normalize_space(args.case_theme)
        if not user_task:
            raise ValueError("--question-mode requires --user-task or --case-theme")
        selected_record_json = Path(args.selected_record_json) if args.selected_record_json else None
        if selected_record_json is not None:
            selected_records = load_selected_question_records(selected_record_json)
            result = _run_question_variant_from_selected_records(
                name="question_selected_manual",
                selected_records=selected_records,
                selection_meta={
                    "selection_source": str(selected_record_json),
                    "selected_count": len(selected_records),
                },
                selection_reasoning_text=f"selection_source: {selected_record_json}",
                user_task=user_task,
                out_dir=out_dir,
                analyze_model=args.analyze_model,
                draft_model=args.draft_model,
                question_chunk_tokens=max(4_000, args.question_chunk_tokens),
                analyze_workers=max(1, args.analyze_workers),
                chunk_build_workers=max(1, args.chunk_build_workers),
            )
        else:
            structured_dirs = [Path(path) for path in args.structured_record_dir]
            if not structured_dirs:
                raise ValueError("--question-mode requires --selected-record-json or at least one --structured-record-dir")
            result = run_question_variant(
                name="question_top100_gemma",
                structured_dirs=structured_dirs,
                user_task=user_task,
                out_dir=out_dir,
                select_model=args.select_model,
                analyze_model=args.analyze_model,
                draft_model=args.draft_model,
                keyword_count=max(1, args.keyword_count),
                top_k_records=max(1, args.top_k_records),
                question_chunk_tokens=max(4_000, args.question_chunk_tokens),
                analyze_workers=max(1, args.analyze_workers),
                chunk_build_workers=max(1, args.chunk_build_workers),
            )
        write_comparison(out_dir, [result])
        write_runtime_status(
            runtime_status_path,
            {
                "phase": "done",
                "state": "question_variant_completed",
                "variant_count": 1,
                "final_draft_path": result["final_draft_path"],
            },
        )
        print(f"[done] question variant -> {result['final_draft_path']}", flush=True)
        return 0

    inventory = build_inventory(root, cache_dir=cache_dir)
    if request is not None:
        filtered_inventory = filter_records_to_target_files(inventory, request.target_files)
        if not filtered_inventory:
            raise ValueError("request target_files did not match any inventory records")
        inventory = filtered_inventory
    _write_json(out_dir / "inventory" / "files.json", [dataclasses.asdict(record) for record in inventory])
    print(f"[inventory] files={len(inventory)}", flush=True)
    write_runtime_status(
        runtime_status_path,
        {
            "phase": "startup",
            "state": "inventory_built",
            "inventory_file_count": len(inventory),
            "out_dir": str(out_dir),
        },
    )

    sample_paths = [Path(p) for p in args.format_sample]
    sample_texts = [extract_text(path) for path in sample_paths]
    user_task = (
        build_request_user_task(request)
        if request is not None
        else (_normalize_space(args.user_task) or _base_user_task(args.case_theme))
    )

    llm_selected_snapshot = out_dir / "variant_llm_select" / "selected_files.json"
    llm_reason_path = out_dir / "variant_llm_select" / "selection_reasoning.txt"
    llm_selected = _load_selected_ids_snapshot(llm_selected_snapshot)
    llm_reasons = []
    if llm_selected and llm_reason_path.exists():
        llm_reasons = [llm_reason_path.read_text(encoding="utf-8", errors="replace")]
        print(f"[select] llm selection reused={len(llm_selected)}", flush=True)
    else:
        print("[select] llm file selection", flush=True)
        llm_selected, llm_reasons = run_llm_file_select(inventory, user_task=user_task, model=args.select_model)
        _write_json(
            llm_selected_snapshot,
            [dataclasses.asdict(record) for record in _selected_records_from_ids(inventory, llm_selected)],
        )
        llm_reason_path.write_text("\n\n".join(reason for reason in llm_reasons if reason).strip(), encoding="utf-8")
        print(f"[select] llm selected={len(llm_selected)}", flush=True)

    need_embedding_selection = any(
        variant in requested_variants for variant in ("variant_embedding_select", "variant_hybrid")
    )
    embedding_selected, embedding_meta, _ = get_embedding_selection(
        inventory,
        user_task=user_task,
        out_dir=out_dir,
        require_selection=need_embedding_selection,
    )

    direct_ids = [record.file_id for record in inventory if record.is_direct_evidence]
    full_scan_ids = [record.file_id for record in inventory]
    hybrid_ids = list(dict.fromkeys(direct_ids + llm_selected + embedding_selected))

    variant_results = []
    if "variant_llm_select" in requested_variants:
        print("[variant] variant_llm_select", flush=True)
        variant_results.append(
            run_variant(
                name="variant_llm_select",
                records=inventory,
                selected_ids=list(dict.fromkeys(direct_ids + llm_selected)),
                selection_reasoning=llm_reasons,
                user_task=user_task,
                sample_texts=sample_texts,
                out_dir=out_dir,
                analyze_model=args.analyze_model,
                draft_model=args.draft_model,
            )
        )
    if "variant_embedding_select" in requested_variants:
        print("[variant] variant_embedding_select", flush=True)
        variant_results.append(
            run_variant(
                name="variant_embedding_select",
                records=inventory,
                selected_ids=embedding_selected,
                selection_reasoning=["Embedding selector using Gemini embedding model."],
                user_task=user_task,
                sample_texts=sample_texts,
                out_dir=out_dir,
                analyze_model=args.analyze_model,
                draft_model=args.draft_model,
            )
        )
    if "variant_full_scan" in requested_variants:
        print("[variant] variant_full_scan", flush=True)
        variant_results.append(
            run_variant(
                name="variant_full_scan",
                records=inventory,
                selected_ids=full_scan_ids,
                selection_reasoning=["Full scan variant processes every candidate file."],
                user_task=user_task,
                sample_texts=sample_texts,
                out_dir=out_dir,
                analyze_model=args.analyze_model,
                draft_model=args.draft_model,
            )
        )
    if "variant_hybrid" in requested_variants:
        print("[variant] variant_hybrid", flush=True)
        variant_results.append(
            run_variant(
                name="variant_hybrid",
                records=inventory,
                selected_ids=hybrid_ids,
                selection_reasoning=llm_reasons
                + ["Hybrid retains the union of LLM-selected files and embedding-selected files."],
                user_task=user_task,
                sample_texts=sample_texts,
                out_dir=out_dir,
                analyze_model=args.analyze_model,
                draft_model=args.draft_model,
            )
        )
    if not variant_results:
        write_runtime_status(
            runtime_status_path,
            {
                "phase": "done",
                "state": "no_variants_requested",
                "variant_count": 0,
            },
        )
        print("[done] no variants requested", flush=True)
        return 0

    write_comparison(out_dir, variant_results)
    write_runtime_status(
        runtime_status_path,
        {
            "phase": "done",
            "state": "comparison_written",
            "variant_count": len(variant_results),
            "comparison_path": str(out_dir / "comparison" / "variant_compare.md"),
        },
    )
    print("[done] comparison written", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
