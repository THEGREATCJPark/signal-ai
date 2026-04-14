from __future__ import annotations

import dataclasses
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .config import (
    DEFAULT_ANALYZE_MODEL,
    DEFAULT_DRAFT_MODEL,
    DEFAULT_GLOBAL_MAX_INFLIGHT,
    DEFAULT_JOB_MAX_ATTEMPTS,
    DEFAULT_KEY_MAX_INFLIGHT,
    DEFAULT_KEY_MIN_GAP_MS,
    DEFAULT_KEY_RPM_LIMIT,
    DEFAULT_RETRY_BACKOFF_SECONDS,
    DEFAULT_KEY_TPM_LIMIT,
    DEFAULT_SELECT_MODEL,
    DEFAULT_TOP_K,
    DEFAULT_WORKER_COUNT,
    DOCUMENT_PRESETS,
    MEMORY_RAG_SCRIPT,
    QUESTION_VARIANT_NAME,
    RUNS_ROOT,
    WORKSPACE_SCRIPTS,
)
from .exporters import build_export_artifacts
from .models import build_result_payload, load_record_detail, project_live_status
from .search import select_top_records

if str(WORKSPACE_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_SCRIPTS))

import memory_evidence_rag as rag  # type: ignore


TERMINAL_JOB_STATES = {"completed", "failed", "cancelled", "interrupted"}
FINAL_ARTIFACT_COMPLETED_STATES = {"question_variant_completed"}


def _clean_headline_text(value: str, *, limit: int = 140) -> str:
    raw = str(value or "").replace("<br/>", "\n").replace("<br />", "\n").replace("<br>", "\n")
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    filtered: list[str] = []
    for line in lines:
        if re.fullmatch(r"\[기억 레코드\s*\d+\]", line, flags=re.IGNORECASE):
            continue
        if re.match(r"^(제목|확정\s*날짜|항소\s*날짜|날짜|url)\s*:", line, flags=re.IGNORECASE):
            continue
        if re.fullmatch(r"\[사건\s*정보\]", line, flags=re.IGNORECASE):
            continue
        if re.fullmatch(r"\d+\s*심", line, flags=re.IGNORECASE):
            continue
        if re.fullmatch(r"(취득세|소득세|법인세|부가가치세|양도소득세|상속세|증여세|지방세)", line, flags=re.IGNORECASE):
            continue
        if re.fullmatch(
            r"[가-힣A-Za-z0-9·()\s]+출처\s+\d{4}\.\s*\d{1,2}\.\s*\d{1,2}\.\s*발행\s*[0-9가-힣()누구합단도재마나카허저]+\s*자료(?:\s*\[[^\]]+\])?",
            line,
            flags=re.IGNORECASE,
        ):
            continue
        filtered.append(line)
    text = " ".join(filtered)
    text = re.sub(r"\[기억 레코드\s*\d+\]\s*", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b기억 레코드\s*\d+\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"(제목|확정\s*날짜|항소\s*날짜|날짜|url)\s*:\s*", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"【[^】]+】", " ", text)
    text = re.sub(r"▣[^\n]+", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\[사건\s*정보\]", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(항소|확정)\b", " ", text)
    text = re.sub(r"^(?:(?:\d+\s*심)|(?:취득세|소득세|법인세|부가가치세|양도소득세|상속세|증여세|지방세))\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^(?:(?:\d+\s*심)|(?:취득세|소득세|법인세|부가가치세|양도소득세|상속세|증여세|지방세))\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^(?:주문|이유)\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^\d+\.\s*", "", text)
    text = re.sub(r"\s+(?:주문|이유)\s+", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def _headline_excerpt_from_text(value: str, *, limit: int = 110) -> str:
    text = _clean_headline_text(value, limit=800)
    text = re.sub(r"-{6,}", " ", text)
    text = re.sub(r"^사\s*건\s*:?\s*", "", text)
    text = re.sub(r"^[가-힣A-Za-z0-9·\(\)\s]+출처\s+\d{4}\.\s*\d{1,2}\.\s*\d{1,2}\.\s*발행\s*[0-9가-힣\(\)누구합단도재마나카허저]+자료\s*", "", text)
    text = re.sub(r"^\[[^\]]+\]\s*", "", text)
    text = re.sub(r"^사\s*건\s+\S+\s*", "", text)
    text = re.sub(r"^\S+\s+발행\s+", "", text)
    text = re.sub(r"\b(원고|피고|청구인|피청구인|항소인|피항소인|상고인|피상고인|변론종결|주문|이유)\b.*$", "", text)
    text = re.sub(r"^(주문|이유)\s*", "", text)
    text = re.sub(r"^\S+\s+자료\s*", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    sentence = re.split(r"(?<=[\.\?!다])\s+", text, maxsplit=1)[0].strip()
    cleaned = sentence or text
    return cleaned[:limit]


def _is_meaningful_headline_body(value: str) -> bool:
    text = _clean_headline_text(value, limit=160)
    if not text:
        return False
    bare = text.strip("[]()·-–—. ").strip()
    if not bare:
        return False
    if re.fullmatch(r"[0-9.]+", bare):
        return False
    if len(bare) <= 2 and bare.replace(" ", "").isalnum():
        return False
    return True


def _normalize_client_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"[^A-Za-z0-9_.:-]", "", text)[:160]


def _token_count_for_runtime_text(text: str) -> int:
    try:
        return int(rag._count_tokens(str(text or "")))  # type: ignore[attr-defined]
    except Exception:
        return max(1, len(str(text or "")) // 4)


def _load_json_file(path: Path, fallback: Any) -> Any:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def _load_jsonl_file(path: Path, *, limit: int = 120) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as fp:
            for line in fp:
                if len(rows) >= limit:
                    break
                if not line.strip():
                    continue
                parsed = json.loads(line)
                if isinstance(parsed, dict):
                    rows.append(parsed)
    except Exception:
        return rows
    return rows


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


@dataclasses.dataclass
class JobRecord:
    job_id: str
    mode: str
    user_task: str
    run_dir: Path
    created_at: float
    status_path: Path
    selected_manifest_path: Path
    chunk_plan: list[dict[str, Any]]
    selected_count: int = 0
    process_pid: int | None = None
    finished_at: float | None = None
    error: str = ""
    document_preset_id: str = ""
    sample_path: str = ""
    runtime_control_path: Path | None = None
    cancel_requested: bool = False
    cancel_reason: str = ""
    client_id: str = ""


class MemoryRuntimeJobManager:
    def __init__(self, *, runs_root: Path = RUNS_ROOT) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._lock = threading.Lock()
        self._execution_slot = threading.Lock()
        self._runs_root = runs_root
        self._runs_root.mkdir(parents=True, exist_ok=True)

    def create_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        mode = str(payload.get("mode") or "question")
        user_task = str(payload.get("userTask") or "").strip()
        if not user_task:
            raise ValueError("userTask is required")
        job_id = f"job-{uuid.uuid4().hex[:12]}"
        run_dir = self._runs_root / job_id
        run_dir.mkdir(parents=True, exist_ok=True)
        status_path = run_dir / "_runtime" / "status.json"
        selected_manifest_path = run_dir / "selected_records.json"
        status_path.parent.mkdir(parents=True, exist_ok=True)
        job = JobRecord(
            job_id=job_id,
            mode=mode,
            user_task=user_task,
            run_dir=run_dir,
            created_at=time.time(),
            status_path=status_path,
            selected_manifest_path=selected_manifest_path,
            chunk_plan=[],
            document_preset_id=str(payload.get("documentPresetId") or ""),
            sample_path=str(payload.get("samplePath") or ""),
            runtime_control_path=run_dir / "_runtime" / "control.json",
            client_id=_normalize_client_id(payload.get("clientId") or payload.get("client_id") or ""),
        )
        with self._lock:
            self._jobs[job_id] = job
        self._write_job_meta(job)
        self._prepare_runtime_files(job)
        self._write_status(job, {"phase": "startup", "state": "queued", "selected_file_count": 0})
        thread = threading.Thread(target=self._run_job, args=(job,), daemon=True)
        thread.start()
        return {
            "jobId": job_id,
            "statusUrl": f"/api/jobs/{job_id}",
            "resultUrl": f"/api/jobs/{job_id}/result",
        }

    def list_jobs(self, client_id: str = "") -> list[dict[str, Any]]:
        normalized_client_id = _normalize_client_id(client_id)
        self._hydrate_jobs_from_disk()
        with self._lock:
            jobs = list(self._jobs.values())
        jobs.sort(key=lambda item: item.created_at, reverse=True)
        rows: list[dict[str, Any]] = []
        for job in jobs:
            if normalized_client_id and job.client_id != normalized_client_id:
                continue
            runtime = self._read_status(job)
            self._sync_job_with_runtime(job, runtime)
            runtime.setdefault("selected_file_count", job.selected_count)
            runtime["elapsed_seconds"] = int(time.time() - job.created_at)
            projected = project_live_status(runtime, chunk_plan=job.chunk_plan, worker_count=DEFAULT_WORKER_COUNT)
            projected.update(
                {
                    "jobId": job.job_id,
                    "clientId": job.client_id,
                    "mode": job.mode,
                    "userTask": job.user_task,
                    "error": job.error,
                    "createdAt": job.created_at,
                    "finishedAt": job.finished_at,
                    "queuePosition": self._queue_position(job.job_id),
                    "headlineFrames": self._headline_frames_for_job(job),
                }
            )
            rows.append(projected)
        return rows

    def get_job_status(self, job_id: str) -> dict[str, Any]:
        job = self._require_job(job_id)
        runtime = self._read_status(job)
        self._sync_job_with_runtime(job, runtime)
        runtime.setdefault("selected_file_count", job.selected_count)
        elapsed = int(time.time() - job.created_at)
        runtime["elapsed_seconds"] = elapsed
        projected = project_live_status(runtime, chunk_plan=job.chunk_plan, worker_count=DEFAULT_WORKER_COUNT)
        projected.update(
            {
                "jobId": job.job_id,
                "mode": job.mode,
                "userTask": job.user_task,
                "error": job.error,
                "createdAt": job.created_at,
                "finishedAt": job.finished_at,
                "queuePosition": self._queue_position(job.job_id),
                "headlineFrames": self._headline_frames_for_job(job),
            }
        )
        return projected

    def _hydrate_jobs_from_disk(self, limit: int = 24) -> None:
        candidates = sorted(
            [path for path in self._runs_root.glob("job-*") if path.is_dir()],
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )[:limit]
        if not candidates:
            return
        with self._lock:
            known_ids = set(self._jobs.keys())
        for run_dir in candidates:
            job_id = run_dir.name
            if job_id in known_ids:
                continue
            recovered = self._recover_job(job_id)
            if recovered is None:
                continue
            with self._lock:
                self._jobs.setdefault(job_id, recovered)

    def _sync_job_with_runtime(self, job: JobRecord, runtime: dict[str, Any]) -> None:
        if self._promote_final_artifact_completion(job, runtime):
            return
        finished_at = runtime.get("finished_at")
        state = str(runtime.get("state") or "")
        if finished_at and job.finished_at is None:
            job.finished_at = float(finished_at)
        if state in TERMINAL_JOB_STATES and job.finished_at is None:
            job.finished_at = time.time()
        if state == "completed":
            job.error = ""
        if state and runtime.get("error") and not job.error:
            job.error = str(runtime.get("error") or "")
        if not job.selected_count:
            selected_count = runtime.get("selected_file_count")
            try:
                job.selected_count = int(selected_count or 0)
            except (TypeError, ValueError):
                job.selected_count = 0

    def _candidate_final_artifact_path(self, run_dir: Path, runtime: dict[str, Any], *, mode: str) -> Path | None:
        candidates: list[Path] = []
        raw_path = str(runtime.get("final_draft_path") or "").strip()
        if raw_path:
            candidates.append(Path(raw_path))
        variant_dir = run_dir / QUESTION_VARIANT_NAME
        if mode == "document":
            candidates.append(variant_dir / "final_document.md")
        candidates.extend(
            [
                variant_dir / "final_answer.md",
                variant_dir / "final_answer_v2.md",
            ]
        )
        for candidate in candidates:
            try:
                if candidate.exists() and candidate.is_file() and candidate.stat().st_size > 0:
                    return candidate
            except OSError:
                continue
        return None

    def _promote_final_artifact_completion(self, job: JobRecord, runtime: dict[str, Any]) -> bool:
        state = str(runtime.get("state") or "")
        has_completed_state = state == "completed"
        if state not in FINAL_ARTIFACT_COMPLETED_STATES and not has_completed_state:
            return False
        artifact_path = self._candidate_final_artifact_path(job.run_dir, runtime, mode=job.mode)
        if artifact_path is None:
            return False
        try:
            finished_at = float(runtime.get("finished_at") or artifact_path.stat().st_mtime)
        except (OSError, TypeError, ValueError):
            finished_at = time.time()
        try:
            total_chunks = int(runtime.get("chunk_count") or runtime.get("total_chunks") or len(job.chunk_plan) or 0)
        except (TypeError, ValueError):
            total_chunks = len(job.chunk_plan)
        try:
            completed_chunks = int(runtime.get("completed_chunks") or total_chunks)
        except (TypeError, ValueError):
            completed_chunks = total_chunks
        promoted = dict(runtime)
        promoted.update(
            {
                "phase": "done",
                "state": "completed",
                "error": "",
                "finished_at": finished_at,
                "final_draft_path": str(artifact_path),
            }
        )
        if job.selected_count:
            promoted["selected_file_count"] = job.selected_count
        if total_chunks:
            promoted["chunk_count"] = total_chunks
            promoted["completed_chunks"] = max(completed_chunks, total_chunks)
        if job.created_at:
            promoted["elapsed_seconds"] = max(0, int(finished_at - job.created_at))
        self._write_status(job, promoted)
        runtime.clear()
        runtime.update(promoted)
        job.finished_at = finished_at
        job.error = ""
        return True

    def _headline_frames_for_job(self, job: JobRecord) -> list[str]:
        if not job.selected_manifest_path.exists():
            return []
        try:
            rows = json.loads(job.selected_manifest_path.read_text(encoding="utf-8"))
        except Exception:
            return []
        frames: list[str] = []
        for row in rows[:100]:
            record_number = str(row.get("record_number") or "").strip()
            title = _clean_headline_text(str(row.get("record_title") or row.get("document_title") or row.get("title") or ""), limit=110)
            raw_text = str(row.get("extracted_text") or row.get("full_text") or row.get("text") or "").strip()
            excerpt = title if _is_meaningful_headline_body(title) else _headline_excerpt_from_text(raw_text)
            if not _is_meaningful_headline_body(excerpt):
                continue
            headline = " ".join(part for part in [f"[{record_number}]" if record_number else "", excerpt] if part).strip()
            if headline and headline not in frames:
                frames.append(headline)
        return frames

    def _queue_position(self, job_id: str) -> int:
        with self._lock:
            pending = [job for job in self._jobs.values() if job.finished_at is None]
        pending.sort(key=lambda item: item.created_at)
        for index, job in enumerate(pending, start=1):
            if job.job_id == job_id:
                return index
        return 0

    def get_job_result(self, job_id: str) -> dict[str, Any]:
        job = self._require_job(job_id)
        payload = build_result_payload(job.run_dir, variant_name=QUESTION_VARIANT_NAME, job_id=job.job_id, mode=job.mode)
        payload.update({"jobId": job.job_id, "mode": job.mode})
        return payload

    def follow_up(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        job = self._require_job(job_id)
        user_task = str(payload.get("userTask") or "").strip()
        if not user_task:
            raise ValueError("userTask is required")
        client_id = _normalize_client_id(payload.get("clientId") or payload.get("client_id") or job.client_id)
        packs = self._build_follow_up_packs(job, user_task=user_task)
        missing_information: list[str] = []
        needed_keywords: list[str] = []
        if not packs:
            needed_keywords.extend(self._fallback_follow_up_keywords(user_task))
        for index, pack in enumerate(packs, start=1):
            parsed = self._ask_follow_up_pack(job, user_task=user_task, pack=pack, pack_index=index, pack_total=len(packs))
            missing_information.extend(str(item) for item in parsed.get("missing_information") or [])
            needed_keywords.extend(str(item) for item in parsed.get("needed_keywords") or [])
            answer = str(parsed.get("answer_markdown") or parsed.get("answerMarkdown") or "").strip()
            if parsed.get("answerable") and answer:
                return {
                    "mode": "answered_from_existing",
                    "sourceJobId": job.job_id,
                    "answerMarkdown": rag._strip_question_answer_meta_prefix(answer),  # type: ignore[attr-defined]
                    "neededKeywords": _dedupe_keep_order(needed_keywords),
                    "missingInformation": _dedupe_keep_order(missing_information),
                }
        needed_keywords = _dedupe_keep_order(needed_keywords or self._fallback_follow_up_keywords(user_task))
        missing_information = _dedupe_keep_order(missing_information)
        created = self.create_job(
            {
                "mode": "question",
                "userTask": self._build_follow_up_search_task(
                    original_job=job,
                    user_task=user_task,
                    needed_keywords=needed_keywords,
                    missing_information=missing_information,
                ),
                "clientId": client_id,
            }
        )
        return {
            "mode": "new_job_started",
            "sourceJobId": job.job_id,
            "neededKeywords": needed_keywords,
            "missingInformation": missing_information,
            **created,
        }

    def get_record_detail(self, job_id: str, record_id: str) -> dict[str, Any]:
        job = self._require_job(job_id)
        return load_record_detail(job.run_dir, record_id, variant_name=QUESTION_VARIANT_NAME)

    def list_document_presets(self) -> list[dict[str, Any]]:
        return [dict(item) for item in DOCUMENT_PRESETS]

    def save_uploaded_sample(self, filename: str, content: bytes) -> dict[str, str]:
        safe_name = re.sub(r"[^A-Za-z0-9._-가-힣]", "_", filename or "sample.txt").strip("._") or "sample.txt"
        upload_dir = self._runs_root / "_uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)
        stored = upload_dir / f"{uuid.uuid4().hex[:10]}_{safe_name}"
        stored.write_bytes(content)
        return {
            "path": str(stored),
            "label": filename or safe_name,
        }

    def document_preflight(self, payload: dict[str, Any]) -> dict[str, Any]:
        user_task = str(payload.get("userTask") or "").strip()
        if not user_task:
            raise ValueError("userTask is required")
        conversation_rows = payload.get("conversation") or []
        conversation: list[dict[str, str]] = []
        if isinstance(conversation_rows, list):
            for row in conversation_rows:
                if not isinstance(row, dict):
                    continue
                role = str(row.get("role") or "").strip()
                text = str(row.get("text") or "").strip()
                if role in {"user", "assistant"} and text:
                    conversation.append({"role": role, "text": text})
        sample_paths = self._resolve_sample_paths(
            document_preset_id=str(payload.get("documentPresetId") or "").strip(),
            sample_path=str(payload.get("samplePath") or "").strip(),
        )
        sample_excerpt_parts: list[str] = []
        for index, sample_path in enumerate(sample_paths, start=1):
            try:
                excerpt = rag.extract_text(sample_path)[:5000]
            except Exception:
                excerpt = ""
            if excerpt.strip():
                sample_excerpt_parts.append(f"--- 예시 문서 {index}: {sample_path.name} ---\n{excerpt.strip()}")
        sample_excerpt = "\n\n".join(sample_excerpt_parts)
        source_job_context = ""
        source_job_id = str(payload.get("sourceJobId") or payload.get("previousJobId") or "").strip()
        if source_job_id:
            try:
                source_job_context = self._build_source_job_context(source_job_id)
            except KeyError:
                source_job_context = ""
        try:
            raw = rag.call_chat(
                [
                    {
                        "role": "system",
                        "content": (
                            "자료 문서 초안 preflight planner다. JSON만 출력하라. "
                            "문서 작성 전 더 필요한 사실이 있으면 질문만 하고, 충분하면 기억 레코드 검색용 retrieval task를 만들어라."
                        ),
                    },
                    {
                        "role": "user",
                        "content": self._build_document_preflight_prompt(
                            user_task=user_task,
                            conversation=conversation,
                            sample_excerpt=sample_excerpt,
                            source_job_context=source_job_context,
                        ),
                    },
                ],
                model=DEFAULT_ANALYZE_MODEL,
                timeout=45,
            ).strip()
            parsed = self._extract_document_preflight(raw)
        except Exception:
            parsed = {
                "ready": False,
                "questions": [],
                "retrievalTask": "",
                "draftingGoal": "",
                "summary": "",
            }
        if not parsed["ready"] and not parsed["questions"]:
            parsed["questions"] = self._fallback_document_questions(user_task, conversation)
        if not parsed["draftingGoal"]:
            parsed["draftingGoal"] = self._build_document_goal_text(user_task, conversation)
        if parsed["ready"] and not parsed["retrievalTask"]:
            parsed["retrievalTask"] = self._build_document_retrieval_task(parsed["draftingGoal"], conversation)
        if not parsed["summary"]:
            if parsed["ready"]:
                parsed["summary"] = "문서 초안 작성을 위한 사실관계와 목표가 정리되어 기억 레코드 수집을 시작합니다."
            else:
                parsed["summary"] = "초안 품질을 높이려면 아래 사실을 먼저 확인해야 합니다."
        return parsed

    def _build_follow_up_packs(self, job: JobRecord, *, user_task: str) -> list[list[tuple[str, str]]]:
        variant_dir = job.run_dir / QUESTION_VARIANT_NAME
        blocks: list[tuple[str, str]] = []
        answer_path = variant_dir / ("final_document.md" if job.mode == "document" else "final_answer.md")
        if not answer_path.exists():
            fallback = variant_dir / "final_answer_v2.md"
            answer_path = fallback if fallback.exists() else answer_path
        if answer_path.exists():
            blocks.append(("기존 최종 본문", answer_path.read_text(encoding="utf-8", errors="ignore")))
        answer_plan = _load_json_file(variant_dir / "answer_plan.json", {})
        if answer_plan:
            blocks.append(("기존 답변 설계", json.dumps(answer_plan, ensure_ascii=False, indent=2)))
        claim_ledger = _load_json_file(variant_dir / "claim_ledger.json", [])
        if claim_ledger:
            blocks.append(("선택 주장 ledger", json.dumps(claim_ledger, ensure_ascii=False, indent=2)))
        for index, output in enumerate(_load_jsonl_file(variant_dir / "chunk_outputs.jsonl"), start=1):
            blocks.append((f"청크 분석 결과 {index}", json.dumps(output, ensure_ascii=False, indent=2)))
        selected_rows = _load_json_file(variant_dir / "selected_files.json", [])
        if not selected_rows:
            selected_rows = _load_json_file(job.selected_manifest_path, [])
        if isinstance(selected_rows, list):
            for index, row in enumerate(selected_rows[:100], start=1):
                if not isinstance(row, dict):
                    continue
                text = str(row.get("extracted_text") or row.get("full_text") or row.get("text") or row.get("anchor_text") or "").strip()
                metadata = {
                    "file_id": row.get("file_id"),
                    "record_number": row.get("record_number"),
                    "source_org": row.get("source_org"),
                    "record_date": row.get("record_date"),
                    "record_title": row.get("record_title") or row.get("document_title") or row.get("title"),
                    "relative_path": row.get("relative_path"),
                }
                block_text = f"{json.dumps(metadata, ensure_ascii=False)}\n\n{text}".strip()
                if block_text:
                    blocks.append((f"선택 기억 레코드 본문 {index}", block_text))
        budget = max(10_000, int(getattr(rag, "DEFAULT_MAX_REQUEST_TOKENS", 50_000)) - _token_count_for_runtime_text(user_task) - 2_500)
        return self._pack_follow_up_blocks(blocks, max_tokens=budget)

    def _pack_follow_up_blocks(self, blocks: list[tuple[str, str]], *, max_tokens: int) -> list[list[tuple[str, str]]]:
        packs: list[list[tuple[str, str]]] = []
        current: list[tuple[str, str]] = []
        current_tokens = 0
        for key, text in blocks:
            rendered = f"[{key}]\n{text}".strip()
            tokens = _token_count_for_runtime_text(rendered)
            if current and current_tokens + tokens > max_tokens:
                packs.append(current)
                current = []
                current_tokens = 0
            if tokens > max_tokens:
                # 단일 원자 블록은 반으로 나누지 않는다. 모델 한도를 넘는 경우 단독 pack으로 보내고
                # Gemini 쪽 제한/재시도 계층이 처리하도록 둔다.
                packs.append([(key, text)])
                continue
            current.append((key, text))
            current_tokens += tokens
        if current:
            packs.append(current)
        return packs

    def _ask_follow_up_pack(
        self,
        job: JobRecord,
        *,
        user_task: str,
        pack: list[tuple[str, str]],
        pack_index: int,
        pack_total: int,
    ) -> dict[str, Any]:
        evidence = "\n\n".join(f"[{key}]\n{text}" for key, text in pack)
        prompt = (
            "기존 MemoryRuntime 분석 산출물만 보고 추가 질문에 답할 수 있는지 판단하라.\n"
            "규칙:\n"
            "- JSON만 출력한다.\n"
            "- 기존 산출물만으로 답할 수 있으면 answerable=true와 answer_markdown을 쓴다.\n"
            "- answer_markdown에는 내부 사고, self-check, 영어 지시문, claim_id 나열을 넣지 않는다.\n"
            "- 답할 수 없으면 answerable=false, missing_information과 needed_keywords를 채운다.\n"
            "- needed_keywords는 새 기억 레코드 검색에 바로 쓸 한국어 키워드/쟁점어만 넣는다.\n\n"
            f"[원래 질문]\n{job.user_task}\n\n"
            f"[추가 질문]\n{user_task}\n\n"
            f"[산출물 묶음 {pack_index}/{pack_total}]\n{evidence}\n\n"
            '출력 JSON:\n{"answerable": true, "answer_markdown": "## 추가 답변\\n...", "missing_information": [], "needed_keywords": []}'
        )
        raw = rag.call_chat(
            [
                {"role": "system", "content": "기존 자료 분석 산출물의 추가 질문 답변 가능성을 엄격히 판단한다. JSON만 출력한다."},
                {"role": "user", "content": prompt},
            ],
            model=DEFAULT_ANALYZE_MODEL,
            timeout=600,
        )
        try:
            parsed = json.loads(rag._extract_json_block(raw))  # type: ignore[attr-defined]
        except Exception:
            parsed = {}
        return parsed if isinstance(parsed, dict) else {}

    def _fallback_follow_up_keywords(self, user_task: str) -> list[str]:
        tokens = re.findall(r"[가-힣A-Za-z0-9]{2,}", user_task)
        return _dedupe_keep_order(tokens[:8])

    def _build_follow_up_search_task(
        self,
        *,
        original_job: JobRecord,
        user_task: str,
        needed_keywords: list[str],
        missing_information: list[str],
    ) -> str:
        keyword_line = ", ".join(needed_keywords) if needed_keywords else "(모델이 별도 키워드를 특정하지 못함)"
        missing_line = "; ".join(missing_information) if missing_information else "(기존 산출물만으로 답변 가능 판단이 나오지 않음)"
        return (
            f"{user_task}\n\n"
            "[이전 분석에서 이어진 새 기억 레코드검색 요청]\n"
            f"- 이전 질문: {original_job.user_task}\n"
            f"- 기존 산출물로 부족했던 이유: {missing_line}\n"
            f"- 새 검색 키워드: {keyword_line}\n"
            "- 위 추가 질문에 직접 답할 수 있는 기억 레코드를 다시 선별하고, 기존 분석과 충돌/보완되는 논리도 함께 정리한다."
        )

    def _require_job(self, job_id: str) -> JobRecord:
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            job = self._recover_job(job_id)
            if job is not None:
                with self._lock:
                    self._jobs[job_id] = job
        if not job:
            raise KeyError(job_id)
        return job

    def _resolve_sample_paths(self, *, document_preset_id: str, sample_path: str) -> list[Path]:
        if sample_path:
            candidate = Path(sample_path)
            if candidate.exists():
                return [candidate]
        if document_preset_id:
            for preset in DOCUMENT_PRESETS:
                preset_ids = {str(preset.get("id") or "").strip(), *[str(item).strip() for item in (preset.get("aliases") or [])]}
                if document_preset_id in preset_ids:
                    raw_paths = preset.get("paths") or [preset.get("path")]
                    paths = [Path(str(path)) for path in raw_paths if str(path or "").strip()]
                    return [path for path in paths if path.exists()]
        return []

    def _build_document_preflight_prompt(
        self,
        *,
        user_task: str,
        conversation: list[dict[str, str]],
        sample_excerpt: str,
        source_job_context: str = "",
    ) -> str:
        transcript = "\n".join(
            f"- {row['role']}: {row['text']}" for row in conversation[-8:] if row.get("text")
        ) or "- (없음)"
        sample_block = sample_excerpt.strip() or "(예시 문서 없음)"
        source_block = source_job_context.strip() or "(이전 분석 산출물 없음)"
        return (
            "다음 정보를 바탕으로 문서 초안 작성 preflight를 수행하라.\n"
            "규칙:\n"
            "- JSON만 출력한다.\n"
            "- 더 필요한 사실이 있으면 questions에 짧고 구체적인 질문만 넣고 ready=false로 둔다.\n"
            "- 충분하면 ready=true, retrieval_task에는 기억 레코드 검색용 한 문단 메시지를 만든다.\n"
            "- drafting_goal에는 사용자에게 보여줄 문서 작성 목표를 한 문단으로 적는다.\n"
            "- summary에는 현재 판단을 1~2문장으로 적는다.\n"
            "- 과도한 스키마를 만들지 말라.\n\n"
            f"[현재 요청]\n{user_task}\n\n"
            f"[지금까지 대화]\n{transcript}\n\n"
            f"[이전 질문 분석 산출물]\n{source_block}\n\n"
            f"[예시 문서 발췌]\n{sample_block}\n\n"
            '출력 JSON:\n{"ready": true, "questions": [], "retrieval_task": "...", "drafting_goal": "...", "summary": "..."}'
        )

    def _build_source_job_context(self, job_id: str) -> str:
        job = self._require_job(job_id)
        payload = build_result_payload(job.run_dir, variant_name=QUESTION_VARIANT_NAME, job_id=job.job_id, mode=job.mode)
        parts: list[str] = []
        answer = str(payload.get("answerMarkdown") or "").strip()
        if answer:
            parts.append("[이전 최종 답변]\n" + answer[:10_000])
        answer_plan = payload.get("answerPlan") or {}
        if answer_plan:
            parts.append("[이전 답변 설계]\n" + json.dumps(answer_plan, ensure_ascii=False, indent=2)[:8_000])
        claims = payload.get("selectedClaims") or payload.get("claims") or []
        if claims:
            parts.append("[이전 선택 주장]\n" + json.dumps(claims[:30], ensure_ascii=False, indent=2)[:18_000])
        records = payload.get("selectedRecords") or []
        compact_records = []
        for row in records[:30]:
            if not isinstance(row, dict):
                continue
            compact_records.append(
                {
                    "recordNumber": row.get("recordNumber"),
                    "source_ref": row.get("source_ref"),
                    "title": row.get("title"),
                    "excerpt": row.get("excerpt"),
                }
            )
        if compact_records:
            parts.append("[이전 참고 기억 레코드]\n" + json.dumps(compact_records, ensure_ascii=False, indent=2)[:18_000])
        return "\n\n".join(parts).strip()

    def _extract_document_preflight(self, raw: str) -> dict[str, Any]:
        try:
            obj = json.loads(rag._extract_json_block(raw))
        except Exception:
            obj = {}
        questions = []
        for item in obj.get("questions") or []:
            text = str(item or "").strip()
            if text and text not in questions:
                questions.append(text)
        return {
            "ready": bool(obj.get("ready")) and not questions,
            "questions": questions[:3],
            "retrievalTask": str(obj.get("retrieval_task") or "").strip(),
            "draftingGoal": str(obj.get("drafting_goal") or "").strip(),
            "summary": str(obj.get("summary") or "").strip(),
        }

    def _fallback_document_questions(self, user_task: str, conversation: list[dict[str, str]]) -> list[str]:
        merged = " ".join(row.get("text") or "" for row in conversation[-6:]).strip()
        base = f"{user_task} {merged}".strip()
        questions: list[str] = []
        if len(base) < 80:
            questions.append("문서에서 가장 핵심적으로 주장하거나 요구할 결론을 한 문장으로 적어주세요.")
        if not re.search(r"(언제|일시|날짜|시각|\d{4}\.\s*\d{1,2}\.\s*\d{1,2})", base):
            questions.append("문제된 행위나 처분이 언제 있었는지 알 수 있는 날짜·시각을 적어주세요.")
        if not re.search(r"(누가|상대방|처분권자|관련자|학생|회사|학교|기관A|기관B|출처)", base):
            questions.append("문서의 당사자와 상대방이 누구인지, 관계가 무엇인지 적어주세요.")
        return questions[:3]

    def _build_document_goal_text(self, user_task: str, conversation: list[dict[str, str]]) -> str:
        merged = " ".join(row.get("text") or "" for row in conversation[-6:] if row.get("role") == "user").strip()
        if merged:
            return f"{user_task.strip()} / 추가 설명: {merged}".strip()
        return user_task.strip()

    def _build_document_retrieval_task(self, drafting_goal: str, conversation: list[dict[str, str]]) -> str:
        merged = " ".join(row.get("text") or "" for row in conversation[-6:] if row.get("role") == "user").strip()
        if merged and merged not in drafting_goal:
            return f"{drafting_goal}\n추가 사실관계: {merged}"
        return drafting_goal

    def _recover_job(self, job_id: str) -> JobRecord | None:
        run_dir = self._runs_root / job_id
        status_path = run_dir / "_runtime" / "status.json"
        selected_manifest_path = run_dir / "selected_records.json"
        if not run_dir.exists() or not status_path.exists():
            return None
        runtime_status = json.loads(status_path.read_text(encoding="utf-8"))
        meta = self._read_job_meta(run_dir)
        recovered_mode = str(meta.get("mode") or "question")
        if (
            runtime_status.get("state") in FINAL_ARTIFACT_COMPLETED_STATES
            and self._candidate_final_artifact_path(run_dir, runtime_status, mode=recovered_mode) is not None
        ):
            final_path = self._candidate_final_artifact_path(run_dir, runtime_status, mode=recovered_mode)
            finished_at = time.time()
            if final_path is not None:
                try:
                    finished_at = final_path.stat().st_mtime
                except OSError:
                    pass
            runtime_status.update(
                {
                    "phase": "done",
                    "state": "completed",
                    "error": "",
                    "finished_at": finished_at,
                }
            )
            status_path.write_text(json.dumps(runtime_status, ensure_ascii=False, indent=2), encoding="utf-8")
        elif runtime_status.get("state") not in TERMINAL_JOB_STATES and not runtime_status.get("finished_at"):
            finished_at = time.time()
            runtime_status.update(
                {
                    "phase": "done",
                    "state": "interrupted",
                    "error": "server restarted before this job completed",
                    "finished_at": finished_at,
                }
            )
            status_path.write_text(json.dumps(runtime_status, ensure_ascii=False, indent=2), encoding="utf-8")
        chunk_plan: list[dict[str, Any]] = []
        selected_count = 0
        if selected_manifest_path.exists():
            try:
                records = rag.load_selected_question_records(selected_manifest_path)
                selected_count = len(records)
                preview_chunks = rag.build_question_chunks_for_records(  # type: ignore[attr-defined]
                    records,
                    max_tokens=max(4000, rag.DEFAULT_QUESTION_CHUNK_TOKENS),  # type: ignore[attr-defined]
                    workers=1,
                )
                chunk_plan = []
                for chunk in preview_chunks:
                    first_segment = chunk.source_segments[0] if getattr(chunk, "source_segments", None) else {}
                    excerpt = str(first_segment.get("excerpt") or " ".join(chunk.text.split())[:220])
                    chunk_plan.append(
                        {
                            "chunk_id": chunk.chunk_id,
                            "file_id": chunk.file_id,
                            "record_number": str(first_segment.get("record_number") or chunk.record_number or ""),
                            "excerpt": _headline_excerpt_from_text(excerpt) or excerpt,
                        }
                    )
            except Exception:
                chunk_plan = []
                selected_count = 0
        created_at = float(meta.get("created_at") or run_dir.stat().st_mtime)
        return JobRecord(
            job_id=job_id,
            mode=str(meta.get("mode") or "question"),
            user_task=str(meta.get("user_task") or ""),
            run_dir=run_dir,
            created_at=created_at,
            status_path=status_path,
            selected_manifest_path=selected_manifest_path,
            chunk_plan=chunk_plan,
            selected_count=selected_count,
            finished_at=float(runtime_status.get("finished_at")) if runtime_status.get("finished_at") else None,
            error=str(runtime_status.get("error") or ""),
            document_preset_id=str(meta.get("document_preset_id") or ""),
            sample_path=str(meta.get("sample_path") or ""),
            runtime_control_path=run_dir / "_runtime" / "control.json",
            client_id=str(meta.get("client_id") or ""),
        )

    def _write_status(self, job: JobRecord, payload: dict[str, Any]) -> None:
        job.status_path.parent.mkdir(parents=True, exist_ok=True)
        job.status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _read_status(self, job: JobRecord) -> dict[str, Any]:
        if not job.status_path.exists():
            return {}
        return json.loads(job.status_path.read_text(encoding="utf-8"))

    def _job_meta_path(self, job: JobRecord | Path) -> Path:
        run_dir = job.run_dir if isinstance(job, JobRecord) else job
        return run_dir / "_runtime" / "job_meta.json"

    def _write_job_meta(self, job: JobRecord) -> None:
        meta_path = self._job_meta_path(job)
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "job_id": job.job_id,
            "mode": job.mode,
            "user_task": job.user_task,
            "created_at": job.created_at,
            "document_preset_id": job.document_preset_id,
            "sample_path": job.sample_path,
            "client_id": job.client_id,
        }
        meta_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _read_job_meta(self, run_dir: Path) -> dict[str, Any]:
        meta_path = self._job_meta_path(run_dir)
        if not meta_path.exists():
            return {}
        return json.loads(meta_path.read_text(encoding="utf-8"))

    def _resolve_sample_texts(self, job: JobRecord) -> list[str]:
        sample_paths: list[Path] = []
        if job.sample_path:
            sample_paths.append(Path(job.sample_path))
        elif job.document_preset_id:
            for preset in DOCUMENT_PRESETS:
                preset_ids = {str(preset.get("id") or "").strip(), *[str(item).strip() for item in (preset.get("aliases") or [])]}
                if job.document_preset_id in preset_ids:
                    raw_paths = preset.get("paths") or [preset.get("path")]
                    sample_paths.extend(Path(str(path)) for path in raw_paths if str(path or "").strip())
                    break
        if not sample_paths:
            for item in DOCUMENT_PRESETS:
                raw_paths = item.get("paths") or [item.get("path")]
                sample_paths.extend(Path(str(path)) for path in raw_paths if str(path or "").strip())
        cache_dir = job.run_dir / "_runtime" / "sample_cache"
        return rag._read_sample_texts(sample_paths, cache_dir)  # type: ignore[attr-defined]

    def _run_job(self, job: JobRecord) -> None:
        acquired_immediately = self._execution_slot.acquire(blocking=False)
        if not acquired_immediately:
            self._write_status(
                job,
                {
                    "phase": "queue",
                    "state": "waiting_for_capacity",
                    "selected_file_count": job.selected_count,
                },
            )
            self._execution_slot.acquire()
        try:
            if job.cancel_requested:
                job.finished_at = time.time()
                job.error = job.cancel_reason or "cancelled"
                self._write_status(
                    job,
                    {
                        "phase": "done",
                        "state": "cancelled",
                        "selected_file_count": job.selected_count,
                        "completed_chunks": 0,
                        "chunk_count": len(job.chunk_plan),
                        "elapsed_seconds": int(job.finished_at - job.created_at),
                        "error": job.error,
                        "finished_at": job.finished_at,
                    },
                )
                return
            self._bind_runtime(job)
            last_error = ""
            attempt = 0
            while True:
                if job.cancel_requested:
                    job.finished_at = time.time()
                    job.error = job.cancel_reason or "cancelled"
                    self._write_status(
                        job,
                        {
                            "phase": "done",
                            "state": "cancelled",
                            "selected_file_count": job.selected_count,
                            "completed_chunks": 0,
                            "chunk_count": len(job.chunk_plan),
                            "elapsed_seconds": int(job.finished_at - job.created_at),
                            "error": job.error,
                            "finished_at": job.finished_at,
                        },
                    )
                    return
                attempt += 1
                try:
                    self._ensure_selected_records(job)
                    self._ensure_chunk_plan(job)
                    self._run_question_subprocess(job)
                    if job.mode == "document":
                        self._write_document_output(job)
                        self._write_export_outputs(job)
                    job.finished_at = time.time()
                    job.error = ""
                    self._write_status(
                        job,
                        {
                            "phase": "done",
                            "state": "completed",
                            "selected_file_count": job.selected_count,
                            "completed_chunks": len(job.chunk_plan),
                            "chunk_count": len(job.chunk_plan),
                            "elapsed_seconds": int(job.finished_at - job.created_at),
                            "finished_at": job.finished_at,
                            "attempt_count": attempt,
                        },
                    )
                    return
                except Exception as exc:  # noqa: BLE001
                    if job.cancel_requested:
                        job.finished_at = time.time()
                        job.error = job.cancel_reason or "cancelled"
                        self._write_status(
                            job,
                            {
                                "phase": "done",
                                "state": "cancelled",
                                "selected_file_count": job.selected_count,
                                "completed_chunks": 0,
                                "chunk_count": len(job.chunk_plan),
                                "elapsed_seconds": int(job.finished_at - job.created_at),
                                "error": job.error,
                                "finished_at": job.finished_at,
                            },
                        )
                        return
                    last_error = str(exc)
                    if not self._is_retryable_job_error(exc):
                        raise
                    backoff_seconds = self._retry_backoff_seconds(attempt)
                    self._write_status(
                        job,
                        {
                            "phase": "retry",
                            "state": "retrying_after_transient_error",
                            "selected_file_count": job.selected_count,
                            "completed_chunks": 0,
                            "chunk_count": len(job.chunk_plan),
                            "attempt_count": attempt,
                            "max_attempts": None if DEFAULT_JOB_MAX_ATTEMPTS == 0 else DEFAULT_JOB_MAX_ATTEMPTS,
                            "retry_backoff_seconds": backoff_seconds,
                            "error": last_error,
                        },
                    )
                    time.sleep(backoff_seconds)
        except Exception as exc:  # noqa: BLE001
            job.finished_at = time.time()
            job.error = str(exc)
            self._write_status(
                job,
                {
                    "phase": "done",
                    "state": "failed",
                    "selected_file_count": job.selected_count,
                    "completed_chunks": 0,
                    "chunk_count": len(job.chunk_plan),
                    "elapsed_seconds": int(job.finished_at - job.created_at),
                    "error": job.error,
                    "finished_at": job.finished_at,
                },
            )
        finally:
            self._execution_slot.release()

    def _ensure_selected_records(self, job: JobRecord) -> None:
        if job.selected_manifest_path.exists():
            try:
                selected_records = json.loads(job.selected_manifest_path.read_text(encoding="utf-8"))
            except Exception:
                selected_records = []
            job.selected_count = len(selected_records)
            return
        self._write_status(job, {"phase": "selection", "state": "generating_keywords", "selected_file_count": 0})
        selected_records, selection_meta = select_top_records(
            job.user_task,
            top_k=DEFAULT_TOP_K,
            select_model=DEFAULT_SELECT_MODEL,
        )
        job.selected_count = len(selected_records)
        job.selected_manifest_path.write_text(json.dumps(selected_records, ensure_ascii=False, indent=2), encoding="utf-8")
        self._write_status(
            job,
            {
                "phase": "selection",
                "state": "building_chunk_plan",
                "selected_file_count": len(selected_records),
                "keywords": selection_meta.get("keywords") or [],
            },
        )

    def _ensure_chunk_plan(self, job: JobRecord) -> None:
        if job.chunk_plan:
            return
        rag_records = rag.load_selected_question_records(job.selected_manifest_path)
        preview_chunks = rag.build_question_chunks_for_records(  # type: ignore[attr-defined]
            rag_records,
            max_tokens=max(4000, rag.DEFAULT_QUESTION_CHUNK_TOKENS),  # type: ignore[attr-defined]
            workers=1,
        )
        job.chunk_plan = []
        for chunk in preview_chunks:
            first_segment = chunk.source_segments[0] if getattr(chunk, "source_segments", None) else {}
            excerpt = str(first_segment.get("excerpt") or " ".join(chunk.text.split())[:220])
            job.chunk_plan.append(
                {
                    "chunk_id": chunk.chunk_id,
                    "file_id": chunk.file_id,
                    "record_number": str(first_segment.get("record_number") or chunk.record_number or ""),
                    "excerpt": _headline_excerpt_from_text(excerpt) or excerpt,
                }
            )

    def _retry_backoff_seconds(self, attempt: int) -> int:
        index = max(0, min(attempt - 1, len(DEFAULT_RETRY_BACKOFF_SECONDS) - 1))
        return int(DEFAULT_RETRY_BACKOFF_SECONDS[index])

    def _is_retryable_job_error(self, exc: Exception) -> bool:
        text = str(exc).lower()
        transient_markers = [
            "429",
            "rate limit",
            "timed out",
            "timeout",
            "connection aborted",
            "connection reset",
            "temporarily unavailable",
            "service unavailable",
            "internal server error",
            "bad gateway",
            "question subprocess failed with exit code",
        ]
        return any(marker in text for marker in transient_markers)

    def _run_question_subprocess(self, job: JobRecord) -> None:
        command = [
            sys.executable,
            str(MEMORY_RAG_SCRIPT),
            "--question-mode",
            "--selected-record-json",
            str(job.selected_manifest_path),
            "--user-task",
            job.user_task,
            "--out-dir",
            str(job.run_dir),
            "--analyze-model",
            DEFAULT_ANALYZE_MODEL,
            "--draft-model",
            DEFAULT_DRAFT_MODEL,
            "--analyze-workers",
            str(DEFAULT_WORKER_COUNT),
            "--chunk-build-workers",
            str(DEFAULT_WORKER_COUNT),
            "--gemini-key-min-gap-ms",
            str(DEFAULT_KEY_MIN_GAP_MS),
            "--gemini-key-max-inflight",
            str(DEFAULT_KEY_MAX_INFLIGHT),
            "--gemini-key-rpm-limit",
            str(DEFAULT_KEY_RPM_LIMIT),
            "--gemini-key-tpm-limit",
            str(DEFAULT_KEY_TPM_LIMIT),
            "--gemini-global-max-inflight",
            str(DEFAULT_GLOBAL_MAX_INFLIGHT),
        ]
        log_path = job.run_dir / "_runtime" / "process.log"
        with log_path.open("w", encoding="utf-8") as log_fp:
            process = subprocess.Popen(command, stdout=log_fp, stderr=subprocess.STDOUT, cwd=str(WORKSPACE_SCRIPTS.parent))
            job.process_pid = process.pid
            return_code = process.wait()
        job.process_pid = None
        if return_code != 0 and job.cancel_requested:
            raise RuntimeError(job.cancel_reason or "cancelled")
        if return_code != 0:
            tail = ""
            if log_path.exists():
                try:
                    tail = log_path.read_text(encoding="utf-8", errors="ignore")[-2000:]
                except Exception:
                    tail = ""
            detail = f"question subprocess failed with exit code {return_code}"
            if tail.strip():
                detail += f"; tail={tail.strip()}"
            raise RuntimeError(detail)

    def _write_document_output(self, job: JobRecord) -> None:
        variant_dir = job.run_dir / QUESTION_VARIANT_NAME
        claim_path = variant_dir / "claim_ledger.json"
        if not claim_path.exists():
            raise FileNotFoundError(f"missing claim ledger: {claim_path}")
        claims = json.loads(claim_path.read_text(encoding="utf-8"))
        sample_texts = self._resolve_sample_texts(job)
        self._write_status(
            job,
            {
                "phase": "variant",
                "state": "writing_document",
                "selected_file_count": job.selected_count,
                "completed_chunks": len(job.chunk_plan),
                "chunk_count": len(job.chunk_plan),
            },
        )
        section_packets = rag.synthesize_section_packets(
            claims,
            model=DEFAULT_ANALYZE_MODEL,
            max_tokens=rag.DEFAULT_MAX_REQUEST_TOKENS,  # type: ignore[attr-defined]
        )
        final_document = rag.write_final_opinion(
            user_task=job.user_task,
            sample_texts=sample_texts,
            claims=claims,
            section_packets=section_packets,
            model=DEFAULT_DRAFT_MODEL,
        )
        final_document = rag.sanitize_document_output(final_document)
        (variant_dir / "final_document.md").write_text(final_document, encoding="utf-8")

    def _prepare_runtime_files(self, job: JobRecord) -> None:
        if job.runtime_control_path is None:
            return
        job.runtime_control_path.parent.mkdir(parents=True, exist_ok=True)
        job.runtime_control_path.write_text(
            json.dumps(
                {
                    "gemini_key_min_gap_ms": DEFAULT_KEY_MIN_GAP_MS,
                    "gemini_key_max_inflight": DEFAULT_KEY_MAX_INFLIGHT,
                    "gemini_key_rpm_limit": DEFAULT_KEY_RPM_LIMIT,
                    "gemini_key_tpm_limit": DEFAULT_KEY_TPM_LIMIT,
                    "gemini_global_max_inflight": DEFAULT_GLOBAL_MAX_INFLIGHT,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def _bind_runtime(self, job: JobRecord) -> None:
        if job.runtime_control_path is not None:
            rag.set_runtime_control_file(job.runtime_control_path)
        rag.set_runtime_status_file(job.status_path)

    def _write_export_outputs(self, job: JobRecord) -> None:
        if job.mode != "document":
            return
        variant_dir = job.run_dir / QUESTION_VARIANT_NAME
        markdown_path = variant_dir / ("final_document.md" if job.mode == "document" else "final_answer.md")
        if not markdown_path.exists():
            markdown_path = variant_dir / "final_answer_v2.md"
        if not markdown_path.exists():
            return
        title = markdown_path.stem.replace("_", " ").strip() or "MemoryRuntime AI 결과"
        markdown_text = markdown_path.read_text(encoding="utf-8")
        if job.mode == "document":
            sanitized_markdown = rag.sanitize_document_output(markdown_text)
            if sanitized_markdown != markdown_text:
                markdown_path.write_text(sanitized_markdown, encoding="utf-8")
            markdown_text = sanitized_markdown
        export_payload = build_export_artifacts(
            markdown_text,
            variant_dir=variant_dir,
            title=title,
            mode=job.mode,
            stem="final_document" if job.mode == "document" else "final_answer",
        )
        self._write_status(
            job,
            {
                "phase": "variant",
                "state": "exporting_artifacts",
                "selected_file_count": job.selected_count,
                "completed_chunks": len(job.chunk_plan),
                "chunk_count": len(job.chunk_plan),
                "export_ready": bool(export_payload.get("pdf_path") or export_payload.get("hwpx_path")),
            },
        )
