#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable


REQUIRED_TOP_LEVEL_KEYS = ("final_goal", "solver", "evaluators", "references")
REQUIRED_RESULT_KEYS = ("all_pass", "fail_tags", "pass_tags", "evidence")
VALID_VERDICTS = ("terminate", "progress", "regress", "no_progress")
VALID_ISSUE_SEVERITIES = ("high", "medium", "low")
VALID_EVALUATION_FLOW_MODES = ("all", "primary_then_confirm_terminate")
ISSUE_SEVERITY_SCORE = {
    "high": 100,
    "medium": 10,
    "low": 1,
}
ISSUE_SEVERITY_ALIASES = {
    "critical": "high",
    "severe": "high",
    "informational": "low",
    "info": "low",
}
VERDICT_ALIASES = {
    "not_ready_for_termination": "no_progress",
    "not-ready-for-termination": "no_progress",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _append_event(run_root: Path, event_type: str, **payload: Any) -> None:
    events_path = run_root / "meta" / "events.jsonl"
    event = {"event_type": event_type, **payload}
    with events_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")


def _copy_files(paths: list[str], destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for raw in paths:
        source = Path(raw).resolve()
        shutil.copy2(source, destination / source.name)


def _remove_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists() or path.is_symlink():
        path.unlink()


def _copy_path(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        _remove_path(destination)
    if source.is_dir():
        shutil.copytree(source, destination)
    else:
        shutil.copy2(source, destination)


def _try_read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return _read_json(path)
    except Exception:  # noqa: BLE001
        return None


def _relative_file_list(root: Path, *, base: Path) -> list[str]:
    if not root.exists():
        return []
    return sorted(str(path.relative_to(base)) for path in root.rglob("*") if path.is_file())


def _safe_relative_path(raw: Any) -> Path | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    path = Path(raw)
    if path.is_absolute() or any(part == ".." for part in path.parts):
        return None
    return path


def _copy_declared_replay_artifacts(workspace: Path, for_eval_root: Path) -> list[str]:
    project_root = workspace / "project"
    if not project_root.exists():
        return []

    created: list[str] = []
    for source in sorted(project_root.rglob("*.json")):
        payload = _try_read_json(source)
        if not isinstance(payload, dict):
            continue
        target_relative = _safe_relative_path(payload.get("replay_artifact"))
        if target_relative is None or not target_relative.parts or target_relative.parts[0] != "for_eval":
            continue
        target = workspace / target_relative
        try:
            target.resolve().relative_to(for_eval_root.resolve())
        except ValueError:
            continue
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        created.append(str(target_relative))
    return created


def _copy_evaluation_code_library_to_workspace(run_root: Path, workspace: Path) -> None:
    library_root = run_root / "evaluation_code_library"
    if library_root.exists():
        _copy_path(library_root, workspace / "evaluation_code_library")

    handoff_path = workspace / "loop_input" / "handoff.md"
    if not handoff_path.exists():
        return
    library_status = "available at `evaluation_code_library/`" if library_root.exists() else "not available yet"
    handoff = handoff_path.read_text(encoding="utf-8").rstrip()
    _write_text(
        handoff_path,
        "\n".join(
            [
                handoff,
                "",
                "## Evaluation Code Library",
                f"- Current status: {library_status}.",
                "- You may use this library only to evaluate candidates from multiple angles.",
                "- If you create reusable evaluation-only helper code, put it under `evaluation_code_candidates/<tool_name>/`.",
                "- Each tool candidate must include `manifest.json` with `purpose: evaluation_code` and `evaluation_only: true`.",
                "- Do not store solver policies, task solutions, benchmark compromises, or freeform critic memory in this library.",
                "",
            ]
        ),
    )


def _collect_evaluation_code_candidates(
    *,
    run_root: Path,
    round_dir: Path,
    evaluator_name: str,
    evaluator_workspace: Path,
    round_index: int,
) -> list[str]:
    candidates_root = evaluator_workspace / "evaluation_code_candidates"
    if not candidates_root.exists():
        return []

    promoted: list[str] = []
    for manifest_path in sorted(candidates_root.rglob("manifest.json")):
        manifest = _try_read_json(manifest_path)
        if not manifest or manifest.get("purpose") != "evaluation_code" or manifest.get("evaluation_only") is not True:
            continue
        candidate_root = manifest_path.parent
        try:
            candidate_relative = candidate_root.relative_to(candidates_root)
        except ValueError:
            continue
        target = run_root / "evaluation_code_library" / f"round_{round_index:04d}" / evaluator_name / candidate_relative
        audit_target = round_dir / "evaluation_code_library_delta" / evaluator_name / candidate_relative
        _copy_path(candidate_root, target)
        _copy_path(candidate_root, audit_target)
        promoted.append(str(target.relative_to(run_root)))
    return promoted


def _path_progress_signature(paths: list[Path] | None) -> tuple[Any, ...]:
    if not paths:
        return ()
    signature: list[Any] = []
    for path in paths:
        if not path.exists():
            signature.append((str(path), "missing"))
            continue
        if path.is_file():
            stat = path.stat()
            signature.append((str(path), "file", stat.st_mtime_ns, stat.st_size))
            continue
        max_mtime_ns = path.stat().st_mtime_ns
        file_count = 0
        total_size = 0
        for child in path.rglob("*"):
            if not child.is_file():
                continue
            stat = child.stat()
            file_count += 1
            total_size += stat.st_size
            max_mtime_ns = max(max_mtime_ns, stat.st_mtime_ns)
        signature.append((str(path), "dir", max_mtime_ns, file_count, total_size))
    return tuple(signature)


def _copy_seed_entries(entries: list[dict[str, str]], destination_root: Path) -> None:
    for entry in entries:
        source = Path(entry["source"]).resolve()
        target = destination_root / entry["target"]
        _copy_path(source, target)


def _get_surface_levels(spec: dict[str, Any]) -> list[dict[str, Any]]:
    workspace_seed = spec.get("workspace_seed") or {}
    surface_levels = workspace_seed.get("surface_levels")
    if isinstance(surface_levels, list) and surface_levels:
        return surface_levels
    return [
        {
            "name": "default",
            "solver_editable": list(workspace_seed.get("solver_editable", [])),
        }
    ]


def _get_active_surface(spec: dict[str, Any], status: dict[str, Any] | None) -> tuple[int, dict[str, Any], list[dict[str, str]]]:
    surface_levels = _get_surface_levels(spec)
    requested_level = int((status or {}).get("active_surface_level", 0))
    level_index = min(max(requested_level, 0), len(surface_levels) - 1)
    level = surface_levels[level_index]
    entries = list(level.get("solver_editable", []))
    return level_index, level, entries


def _render_command(template: list[str], workspace: Path, round_index: int, run_root: Path) -> list[str]:
    rendered: list[str] = []
    for part in template:
        rendered.append(
            part.format(
                workspace=str(workspace),
                round=round_index,
                run_root=str(run_root),
            )
        )
    return rendered


def load_loop_spec(spec_path: str | Path) -> dict[str, Any]:
    path = Path(spec_path).resolve()
    payload = _read_json(path)
    missing = [key for key in REQUIRED_TOP_LEVEL_KEYS if key not in payload]
    if missing:
        raise ValueError(f"loop spec missing required sections: {', '.join(missing)}")
    if not payload["solver"].get("command"):
        raise ValueError("loop spec solver.command is required")
    solver_timeout = payload["solver"].get("max_runtime_seconds")
    if solver_timeout is not None and float(solver_timeout) <= 0:
        raise ValueError("solver.max_runtime_seconds must be > 0 when provided")
    solver_stall_watchdog = payload["solver"].get("stall_watchdog_seconds")
    if solver_stall_watchdog is not None and float(solver_stall_watchdog) <= 0:
        raise ValueError("solver.stall_watchdog_seconds must be > 0 when provided")
    if not isinstance(payload["evaluators"], list) or not payload["evaluators"]:
        raise ValueError("loop spec evaluators must be a non-empty list")
    for evaluator in payload["evaluators"]:
        if not isinstance(evaluator, dict):
            raise ValueError("each evaluator must be an object")
        if not evaluator.get("name"):
            raise ValueError("each evaluator needs name")
        if not evaluator.get("command"):
            raise ValueError("each evaluator needs command")
        fallback = evaluator.get("fallback")
        if fallback is not None:
            if not isinstance(fallback, dict):
                raise ValueError("evaluator.fallback must be an object when provided")
            if not fallback.get("command"):
                raise ValueError("evaluator.fallback.command is required when fallback is provided")
    converger = payload.get("converger")
    if converger is not None:
        if not isinstance(converger, dict):
            raise ValueError("loop spec converger must be an object when provided")
        if not converger.get("name"):
            raise ValueError("loop spec converger.name is required when converger is provided")
        if not converger.get("command"):
            raise ValueError("loop spec converger.command is required when converger is provided")
    manager = payload.get("manager")
    if manager is not None:
        if not isinstance(manager, dict):
            raise ValueError("loop spec manager must be an object when provided")
        heartbeat_seconds = manager.get("heartbeat_seconds")
        if heartbeat_seconds is not None and float(heartbeat_seconds) <= 0:
            raise ValueError("manager.heartbeat_seconds must be > 0 when provided")
    if not payload["references"].get("solver_visible"):
        raise ValueError("loop spec references.solver_visible is required")
    if "evaluator_only" not in payload["references"]:
        raise ValueError("loop spec references.evaluator_only is required")
    workspace_seed = payload.get("workspace_seed")
    if workspace_seed is not None:
        solver_editable = workspace_seed.get("solver_editable", [])
        surface_levels = workspace_seed.get("surface_levels")
        if surface_levels is not None:
            if not isinstance(surface_levels, list) or not surface_levels:
                raise ValueError("workspace_seed.surface_levels must be a non-empty list")
            for index, level in enumerate(surface_levels):
                if not isinstance(level, dict):
                    raise ValueError("each workspace_seed.surface_levels entry must be an object")
                entries = level.get("solver_editable", [])
                if not isinstance(entries, list) or not entries:
                    raise ValueError("each workspace_seed.surface_levels entry needs a non-empty solver_editable list")
                for entry in entries:
                    if not isinstance(entry, dict) or "source" not in entry or "target" not in entry:
                        raise ValueError("each surface level solver_editable entry needs source and target")
                if not level.get("name"):
                    raise ValueError(f"workspace_seed.surface_levels[{index}] needs name")
        else:
            if not isinstance(solver_editable, list):
                raise ValueError("loop spec workspace_seed.solver_editable must be a list")
            for entry in solver_editable:
                if not isinstance(entry, dict) or "source" not in entry or "target" not in entry:
                    raise ValueError("each workspace_seed.solver_editable entry needs source and target")
        carry_forward_mode = workspace_seed.get("carry_forward_mode")
        allowed_carry_modes = {None, "", "best_checkpoint", "last_round_candidate"}
        if carry_forward_mode not in allowed_carry_modes:
            raise ValueError("unsupported workspace_seed.carry_forward_mode")
    mechanical_feedback = payload.get("mechanical_feedback")
    if mechanical_feedback is not None:
        if mechanical_feedback.get("enabled"):
            threshold = int(mechanical_feedback.get("no_progress_reset_after", 0))
            if threshold < 1:
                raise ValueError("mechanical_feedback.no_progress_reset_after must be >= 1 when enabled")
            surface_threshold = mechanical_feedback.get("surface_escalation_after")
            if surface_threshold is not None and int(surface_threshold) < 1:
                raise ValueError("mechanical_feedback.surface_escalation_after must be >= 1 when enabled")
            reset_policy = mechanical_feedback.get("reset_policy", "")
            allowed = {"keep_best_checkpoint_code_clear_solver_memory"}
            if reset_policy and reset_policy not in allowed:
                raise ValueError("unsupported mechanical_feedback.reset_policy")
    solver_memory = payload.get("solver_memory")
    if solver_memory is not None:
        if not solver_memory.get("root"):
            raise ValueError("solver_memory.root is required when solver_memory is configured")
        required_files = solver_memory.get("required_files", [])
        if not isinstance(required_files, list):
            raise ValueError("solver_memory.required_files must be a list")
    issue_ledger = payload.get("issue_ledger")
    if issue_ledger is not None and not isinstance(issue_ledger.get("enabled"), bool):
        raise ValueError("issue_ledger.enabled must be a boolean when issue_ledger is configured")
    evidence_gate = payload.get("evidence_gate")
    if evidence_gate is not None:
        if not isinstance(evidence_gate, dict):
            raise ValueError("evidence_gate must be an object when configured")
        if "enabled" in evidence_gate and not isinstance(evidence_gate.get("enabled"), bool):
            raise ValueError("evidence_gate.enabled must be a boolean when evidence_gate is configured")
    evaluation_flow = payload.get("evaluation_flow") or {}
    if evaluation_flow:
        mode = evaluation_flow.get("mode", "all")
        if mode not in VALID_EVALUATION_FLOW_MODES:
            raise ValueError(f"unsupported evaluation_flow.mode: {mode}")
        primary = evaluation_flow.get("primary")
        if primary is not None and primary not in [evaluator.get("name") for evaluator in payload["evaluators"]]:
            raise ValueError("evaluation_flow.primary must match one of evaluators[].name")
    return payload


def initialize_run_root(spec: dict[str, Any], run_root: str | Path) -> dict[str, Any]:
    root = Path(run_root).resolve()
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "references" / "solver_visible").mkdir(parents=True, exist_ok=True)
    (root / "references" / "evaluator_only").mkdir(parents=True, exist_ok=True)
    (root / "rounds").mkdir(parents=True, exist_ok=True)
    (root / "checkpoints").mkdir(parents=True, exist_ok=True)

    _copy_files(spec["references"]["solver_visible"], root / "references" / "solver_visible")
    _copy_files(spec["references"].get("evaluator_only", []), root / "references" / "evaluator_only")
    _write_json(root / "meta" / "spec.snapshot.json", spec)

    status_path = root / "meta" / "status.json"
    if not status_path.exists():
        surface_index, surface_level, _ = _get_active_surface(spec, None)
        initial_status = {
            "next_round": 1,
            "status": "initialized",
            "active_surface_level": surface_index,
            "surface_name": str(surface_level.get("name") or f"surface_{surface_index}"),
            "surface_escalation_count": 0,
        }
        _write_json(status_path, initial_status)
        _append_event(root, "run.initialized", status=initial_status)
    return {"run_root": root, "status_path": status_path}


def _load_best_checkpoint_summary(run_root: Path) -> dict[str, Any] | None:
    best_path = run_root / "meta" / "best_checkpoint.json"
    if not best_path.exists():
        return None
    return _read_json(best_path)


def _load_latest_scoreboard(run_root: Path) -> dict[str, Any] | None:
    scoreboard_path = run_root / "meta" / "scoreboard.json"
    if not scoreboard_path.exists():
        return None
    return _read_json(scoreboard_path)


def _load_issue_ledger(run_root: Path) -> dict[str, Any]:
    issue_path = run_root / "meta" / "issue_ledger.json"
    if not issue_path.exists():
        return {"issues": [], "issue_count": 0, "issue_score": 0, "issue_severity_counts": {}}
    return _read_json(issue_path)


def _load_converged_feedback(run_root: Path) -> str:
    feedback_path = run_root / "meta" / "converged_feedback.md"
    if not feedback_path.exists():
        return ""
    return feedback_path.read_text(encoding="utf-8").strip()


def _load_round_summaries(run_root: Path) -> list[dict[str, Any]]:
    round_root = run_root / "rounds"
    if not round_root.exists():
        return []
    summaries: list[dict[str, Any]] = []
    for path in sorted(round_root.glob("round_*/summary.json")):
        summaries.append(_read_json(path))
    return summaries


def _build_round_history(run_root: Path, *, limit: int = 8) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    for summary in _load_round_summaries(run_root)[-limit:]:
        solver_result = summary.get("solver_result", {})
        history.append(
            {
                "round_index": int(summary.get("round_index", 0)),
                "merged_verdict": summary.get("merged_verdict"),
                "agreed_fail_tags": list(summary.get("agreed_fail_tags", [])),
                "agreed_pass_tags": list(summary.get("agreed_pass_tags", [])),
                "issue_count": int(summary.get("issue_count", 0)),
                "issue_score": int(summary.get("issue_score", 0)),
                "policy": dict(solver_result.get("policy", {})) if isinstance(solver_result.get("policy"), dict) else {},
                "selected_ids": list(solver_result.get("selected_ids", []))
                if isinstance(solver_result.get("selected_ids"), list)
                else [],
            }
        )
    return history


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _render_markdown_section(title: str, lines: list[str]) -> str:
    rendered = [f"# {title}", ""]
    rendered.extend(lines)
    rendered.append("")
    return "\n".join(rendered)


def _render_round_context_markdown(context: dict[str, Any]) -> str:
    lines = [
        f"- Round index: {context.get('round_index')}",
        f"- Final goal: {context.get('final_goal')}",
        f"- Last feedback verdict: {context.get('last_feedback_verdict')}",
        f"- Consecutive no_progress: {context.get('consecutive_no_progress')}",
        f"- Current surface: {context.get('surface_name')} (level {context.get('active_surface_level', 0)})",
        f"- Prior agreed fail tags: {', '.join(context.get('prior_agreed_fail_tags', [])) or '(none)'}",
        f"- Prior agreed pass tags: {', '.join(context.get('prior_agreed_pass_tags', [])) or '(none)'}",
        f"- Solver reset pending: {bool(context.get('solver_reset_pending', False))}",
    ]
    return _render_markdown_section("Round Context", lines)


def _render_issue_ledger_markdown(issue_ledger: dict[str, Any]) -> str:
    lines = [
        f"- Round index: {issue_ledger.get('round_index')}",
        f"- Issue count: {issue_ledger.get('issue_count', 0)}",
        f"- Issue score: {issue_ledger.get('issue_score', 0)}",
    ]
    for issue in issue_ledger.get("issues", []):
        if not isinstance(issue, dict):
            continue
        summary = str(issue.get("summary") or "").strip() or "(missing summary)"
        severity = str(issue.get("severity") or "low")
        lines.append(f"- [{severity}] {summary}")
    return _render_markdown_section("Issue Ledger", lines)


def _render_stagnation_markdown(stagnation: dict[str, Any]) -> str:
    lines = [
        f"- Round count: {stagnation.get('round_count', 0)}",
        f"- Latest round index: {stagnation.get('latest_round_index', 0)}",
        f"- Consecutive no_progress: {stagnation.get('consecutive_no_progress', 0)}",
        f"- Latest verdict: {stagnation.get('latest_verdict')}",
        f"- Repeated verdict: {stagnation.get('repeated_verdict')}",
        f"- Repeated fail signature: {stagnation.get('repeated_fail_signature')}",
        f"- Repeated selected signature: {stagnation.get('repeated_selected_signature')}",
    ]
    return _render_markdown_section("Stagnation Summary", lines)


def _render_round_history_markdown(round_history: list[dict[str, Any]]) -> str:
    lines = [f"- History length: {len(round_history)}"]
    for item in round_history:
        lines.append(
            "- "
            + f"Round {item.get('round_index')}: verdict={item.get('merged_verdict')}, "
            + f"fail_tags={','.join(item.get('agreed_fail_tags', [])) or '(none)'}, "
            + f"selected={len(item.get('selected_ids', []))}"
        )
    return _render_markdown_section("Round History", lines)


def _render_manager_directive_markdown(manager_directive: dict[str, Any]) -> str:
    lines = [
        f"- Manager mode: {manager_directive.get('manager_mode')}",
        f"- Audit level: {manager_directive.get('audit', {}).get('level')}",
        f"- Audit reasons: {', '.join(manager_directive.get('audit', {}).get('reasons', [])) or '(none)'}",
        f"- Current surface: {manager_directive.get('surface', {}).get('name')} (level {manager_directive.get('surface', {}).get('level')})",
        f"- Allowed edit targets: {', '.join(manager_directive.get('surface', {}).get('allowed_edit_targets', [])) or '(none)'}",
        f"- Missing artifacts to backfill: {', '.join(manager_directive.get('missing_artifacts_to_backfill', [])) or '(none)'}",
        f"- Preserve pass tags: {', '.join(manager_directive.get('preserve', {}).get('agreed_pass_tags', [])) or '(none)'}",
        f"- Target fail tags: {', '.join(manager_directive.get('target', {}).get('agreed_fail_tags', [])) or '(none)'}",
        f"- Instructions: {' | '.join(manager_directive.get('instructions', [])) or '(none)'}",
        f"- Required move characteristics: {', '.join(manager_directive.get('required_move_characteristics', [])) or '(none)'}",
        f"- Prohibited patterns: {', '.join(manager_directive.get('prohibited_patterns', [])) or '(none)'}",
    ]
    return _render_markdown_section("Manager Directive", lines)


def _orchestrator_memory_path(run_root: Path) -> Path:
    return run_root / "meta" / "orchestrator_memory.md"


def _read_text_tail(path: Path, *, max_chars: int = 2000) -> str:
    if not path.exists():
        return ""
    text = path.read_text(encoding="utf-8")
    return text[-max_chars:]


def _append_orchestrator_memory(
    run_root: Path,
    manager_directive: dict[str, Any],
    *,
    actions: list[str],
) -> None:
    path = _orchestrator_memory_path(run_root)
    if not path.exists():
        _write_text(
            path,
            "\n".join(
                [
                    "# Orchestrator Memory",
                    "",
                    "Persistent self-memory for the top orchestration manager.",
                    "This records what the manager inspected, decided, and wrote each round.",
                    "",
                ]
            ),
        )

    target = manager_directive.get("target", {})
    preserve = manager_directive.get("preserve", {})
    audit = manager_directive.get("audit", {})
    surface = manager_directive.get("surface", {})
    observations = manager_directive.get("observations", [])
    status = manager_directive.get("status", {})
    best_checkpoint = _load_best_checkpoint_summary(run_root) or {}
    best_solver_result = best_checkpoint.get("solver_result") if isinstance(best_checkpoint, dict) else {}
    if not isinstance(best_solver_result, dict):
        best_solver_result = {}
    best_round = (
        best_checkpoint.get("round_index")
        or best_checkpoint.get("round")
        or best_solver_result.get("round")
        or "(none)"
    )
    checkpoint_path = best_checkpoint.get("checkpoint_dir") or best_checkpoint.get("for_eval_dir") or "(none)"
    latest_scoreboard = _load_latest_scoreboard(run_root) or {}
    latest_round = latest_scoreboard.get("round_index", "(none)")
    latest_verdict = latest_scoreboard.get("merged_verdict", "(none)")
    latest_fail_tags = latest_scoreboard.get("agreed_fail_tags", [])
    latest_pass_tags = latest_scoreboard.get("agreed_pass_tags", [])
    lines = [
        f"## Round {int(manager_directive.get('round_index', 0)):04d}",
        "### FINAL_GOAL",
        f"- {manager_directive.get('final_goal') or '(not provided)'}",
        "### CURRENT_FOCUS",
        f"- manager_mode: {manager_directive.get('manager_mode')}",
        f"- audit: {audit.get('level')} ({', '.join(audit.get('reasons', [])) or 'no reasons'})",
        f"- surface: {surface.get('name')} level {surface.get('level')}",
        f"- allowed edit targets: {', '.join(surface.get('allowed_edit_targets', [])) or '(none)'}",
        f"- preserve pass tags: {', '.join(preserve.get('agreed_pass_tags', [])) or '(none)'}",
        f"- target fail tags: {', '.join(target.get('agreed_fail_tags', [])) or '(none)'}",
        f"- highest severity issues: {', '.join(target.get('highest_severity_issue_keys', [])) or '(none)'}",
        f"- missing artifacts to backfill: {', '.join(manager_directive.get('missing_artifacts_to_backfill', [])) or '(none)'}",
        f"- loop status: consecutive_no_progress={status.get('consecutive_no_progress')}, last_feedback_verdict={status.get('last_feedback_verdict')}, surface={status.get('surface_name')}",
        "### TODO",
        f"- [ ] preserve: {', '.join(preserve.get('agreed_pass_tags', [])) or '(none)'}",
        f"- [ ] resolve: {', '.join(target.get('agreed_fail_tags', [])) or '(none)'}",
        f"- [ ] backfill artifacts: {', '.join(manager_directive.get('missing_artifacts_to_backfill', [])) or '(none)'}",
        "### PING_PONG_SUMMARY",
        f"- latest evaluated round: {latest_round}; verdict={latest_verdict}; pass={', '.join(latest_pass_tags) or '(none)'}; fail={', '.join(latest_fail_tags) or '(none)'}",
        f"- manager response: issue next solver directive in {manager_directive.get('manager_mode')} mode with {audit.get('level')} audit.",
        "### DECISION_LOG",
        f"- required move characteristics: {', '.join(manager_directive.get('required_move_characteristics', [])) or '(none)'}",
        f"- prohibited patterns: {', '.join(manager_directive.get('prohibited_patterns', [])) or '(none)'}",
        f"- observations: {json.dumps(observations, ensure_ascii=False, sort_keys=True)}",
        "### CHECKPOINT",
        f"- best checkpoint round: {best_round}",
        f"- best checkpoint path: {checkpoint_path}",
        f"- best checkpoint issue_score: {best_checkpoint.get('issue_score', '(unknown)')}",
        f"- best checkpoint solver status: {best_solver_result.get('status', '(unknown)')}; degraded={best_solver_result.get('degraded', '(unknown)')}",
        "### REFERENCES",
        "- meta/manager_history.jsonl",
        "- meta/scoreboard.json",
        "- meta/issue_ledger.json",
        "- meta/stagnation.json",
        "- meta/best_checkpoint.json",
        f"- rounds/round_{int(manager_directive.get('round_index', 0)):04d}/solver/workspace/loop_input/manager_directive.json",
    ]
    for action in actions:
        lines.append(f"- action: {action}")
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n\n")


def _render_converged_feedback_section(converged_feedback: str) -> str:
    feedback = converged_feedback.strip()
    if not feedback:
        return ""
    lines = [
        "This is the only critic feedback to feed back into the next solver turn.",
        "It should contain only stable overlap or runner-validated failure signals, not one-off critic opinions.",
        "",
        feedback,
    ]
    return _render_markdown_section("Converged Feedback", lines)


def _render_handoff_markdown(
    context: dict[str, Any],
    issue_ledger: dict[str, Any],
    stagnation: dict[str, Any],
    round_history: list[dict[str, Any]],
    manager_directive: dict[str, Any],
    converged_feedback: str = "",
) -> str:
    parts = [
        "# Turn Handoff",
        "",
        "Read this file first. JSON mirrors exist in the same directory for exact machine fields.",
        "",
        _render_round_context_markdown(context).strip(),
    ]
    if converged_feedback.strip():
        parts.append(_render_converged_feedback_section(converged_feedback).strip())
    parts.extend(
        [
        _render_issue_ledger_markdown(issue_ledger).strip(),
        _render_stagnation_markdown(stagnation).strip(),
        _render_round_history_markdown(round_history).strip(),
        _render_manager_directive_markdown(manager_directive).strip(),
        "",
        ]
    )
    return "\n".join(parts)


def _top_issue_keys(issue_ledger: dict[str, Any], *, limit: int = 3) -> list[str]:
    issues = issue_ledger.get("issues", [])
    if not isinstance(issues, list):
        return []
    ranked: list[tuple[int, str]] = []
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        key = str(issue.get("issue_key") or issue.get("issue_id") or issue.get("summary") or "").strip()
        if not key:
            continue
        severity = str(issue.get("severity") or "low")
        ranked.append((ISSUE_SEVERITY_SCORE.get(severity, 0), key))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [key for _, key in ranked[:limit]]


def _classify_policy_delta(previous_policy: dict[str, Any], latest_policy: dict[str, Any]) -> dict[str, list[str]]:
    delta = {
        "changed_keys": [],
        "boolean_flips": [],
        "numeric_increase_keys": [],
        "numeric_decrease_keys": [],
        "other_keys": [],
    }
    for key in sorted({*previous_policy.keys(), *latest_policy.keys()}):
        previous_value = previous_policy.get(key)
        latest_value = latest_policy.get(key)
        if previous_value == latest_value:
            continue
        delta["changed_keys"].append(key)
        if isinstance(previous_value, bool) and isinstance(latest_value, bool):
            delta["boolean_flips"].append(key)
        elif (
            isinstance(previous_value, (int, float))
            and not isinstance(previous_value, bool)
            and isinstance(latest_value, (int, float))
            and not isinstance(latest_value, bool)
        ):
            if latest_value > previous_value:
                delta["numeric_increase_keys"].append(key)
            elif latest_value < previous_value:
                delta["numeric_decrease_keys"].append(key)
            else:
                delta["other_keys"].append(key)
        else:
            delta["other_keys"].append(key)
    return delta


def _build_manager_directive(spec: dict[str, Any], run_root: Path, *, round_index: int, status: dict[str, Any]) -> dict[str, Any]:
    raw_summaries = _load_round_summaries(run_root)
    round_history = _build_round_history(run_root)
    stagnation = _load_stagnation_summary(run_root)
    issue_ledger = _load_issue_ledger(run_root)
    surface_index, surface_level, surface_entries = _get_active_surface(spec, status)
    latest = round_history[-1] if round_history else None
    previous = round_history[-2] if len(round_history) >= 2 else None
    latest_summary = raw_summaries[-1] if raw_summaries else None

    manager_mode = "monitor"
    observations: list[dict[str, Any]] = []
    instructions = [
        "Use round_history.json as the authoritative record of what was already tried.",
        "Describe the delta versus the immediately previous candidate, not versus the original seed.",
    ]
    required_move_characteristics: list[str] = []
    prohibited_patterns: list[str] = []
    preserve = {"agreed_pass_tags": []}
    target = {"agreed_fail_tags": [], "highest_severity_issue_keys": []}
    audit_reasons: list[str] = []
    audit_focus_checks: list[str] = []

    repeated_verdict = stagnation.get("repeated_verdict")
    repeated_selected_signature = stagnation.get("repeated_selected_signature")
    repeated_fail_signature = stagnation.get("repeated_fail_signature")
    repeated_issue_signature = stagnation.get("repeated_issue_signature")
    consecutive_no_progress = int(status.get("consecutive_no_progress", 0))
    top_issue_keys = _top_issue_keys(issue_ledger)

    latest_pass_tags = []
    latest_fail_tags = []
    latest_verdict = None
    latest_missing_artifacts: list[str] = []
    if latest:
        latest_pass_tags = list(latest.get("agreed_pass_tags", [])) if isinstance(latest.get("agreed_pass_tags"), list) else []
        latest_fail_tags = list(latest.get("agreed_fail_tags", [])) if isinstance(latest.get("agreed_fail_tags"), list) else []
        latest_verdict = str(latest.get("merged_verdict") or "")
        latest_solver_result = latest_summary.get("solver_result", {}) if isinstance(latest_summary, dict) and isinstance(latest_summary.get("solver_result"), dict) else {}
        latest_missing_artifacts = (
            [str(item) for item in latest_solver_result.get("missing_artifacts", [])]
            if isinstance(latest_solver_result.get("missing_artifacts"), list)
            else []
        )

    if latest_pass_tags:
        preserve["agreed_pass_tags"] = latest_pass_tags
        observations.append({"kind": "protected_pass_tags", "value": latest_pass_tags})
        instructions.append("Protect already-passing dimensions unless changing them is necessary to reduce the primary failure.")
        required_move_characteristics.append("preserve_passing_dimensions")

    if latest_fail_tags:
        target["agreed_fail_tags"] = latest_fail_tags
        observations.append({"kind": "active_fail_tags", "value": latest_fail_tags})
        instructions.append("Spend this round on unresolved fail signatures before polishing already-passing dimensions.")
        required_move_characteristics.append("target_primary_fail_signature")

    if top_issue_keys:
        target["highest_severity_issue_keys"] = top_issue_keys
        observations.append({"kind": "top_issue_keys", "value": top_issue_keys})
        instructions.append("Prioritize the highest-severity open issues before secondary cleanup.")
        required_move_characteristics.append("address_high_severity_issues")

    if latest_missing_artifacts:
        observations.append({"kind": "missing_artifacts_to_backfill", "value": latest_missing_artifacts})
        instructions.append("Backfill the previously missing round artifacts while still prioritizing the primary benchmark failure.")

    if bool(status.get("solver_reset_pending", False)):
        audit_reasons.append("solver_reset_pending")
        audit_focus_checks.extend(
            [
                "inspect_reset_boundary",
                "compare_checkpoint_to_recent_candidate",
            ]
        )
        manager_mode = "reset_enforced"
        observations.append({"kind": "solver_reset_pending", "value": True})
        instructions.extend(
            [
                "Treat this round as a reset: rebuild from the preserved checkpoint/seed rather than continuing the previous solver memory.",
                "Do not justify the new candidate by citing the immediately previous failed local direction as if it were still active.",
            ]
        )
    elif latest_verdict and latest_verdict != "terminate" and (latest_fail_tags or top_issue_keys):
        manager_mode = "directed_search"
        instructions.extend(
            [
                "Make one decisive move that should change the observable outcome signature, not just the explanation around it.",
                "State the hypothesis for why this move should reduce the targeted fail signature or issue pressure.",
            ]
        )
        required_move_characteristics.extend(
            [
                "single_decisive_delta",
                "observable_outcome_change",
                "hypothesis_driven_move",
            ]
        )

    if round_history and len(round_history) % 3 == 0:
        audit_reasons.append("periodic_cadence")
        audit_focus_checks.extend(
            [
                "review_recent_search_trajectory",
                "compare_recent_deltas_to_outcomes",
                "inspect_solver_memory_consistency",
            ]
        )

    if not bool(status.get("solver_reset_pending", False)) and (
        consecutive_no_progress >= 2
        or repeated_verdict == "no_progress"
        or repeated_selected_signature is not None
        or repeated_fail_signature is not None
        or repeated_issue_signature is not None
    ):
        manager_mode = "anti_stagnation"
        observations.append({"kind": "consecutive_no_progress", "value": consecutive_no_progress})
        if repeated_verdict:
            observations.append({"kind": "repeated_verdict", "value": repeated_verdict})
        instructions.extend(
            [
                "Do not repeat an already-failed candidate direction without a new state-based reason.",
                "Prefer a qualitatively different move over undoing and replaying a recent failed move.",
            ]
        )
        required_move_characteristics.extend(
            [
                "orthogonal_to_recent_failures",
                "observable_outcome_change",
            ]
        )
        if consecutive_no_progress >= 2:
            audit_reasons.append("consecutive_no_progress")
        if repeated_verdict == "no_progress":
            audit_reasons.append("repeated_verdict")
        if repeated_fail_signature is not None:
            audit_reasons.append("repeated_fail_signature")
        if repeated_selected_signature is not None:
            audit_reasons.append("repeated_selected_signature")
        if repeated_issue_signature is not None:
            audit_reasons.append("repeated_issue_signature")
        audit_focus_checks.extend(
            [
                "inspect_stagnation_root_cause",
                "verify_recent_move_orthogonality",
                "check_progress_claims_against_artifacts",
            ]
        )

    if repeated_fail_signature:
        observations.append({"kind": "repeated_fail_signature", "value": repeated_fail_signature})
        instructions.append("Prioritize changing the primary repeated fail signature before optimizing any secondary metric.")

    if repeated_selected_signature:
        observations.append({"kind": "repeated_selected_signature", "value": repeated_selected_signature})
        prohibited_patterns.append(f"repeat_selected_signature:{repeated_selected_signature}")
        instructions.append("Do not repeat a candidate direction that is likely to recreate the repeated selected frontier.")

    if latest and previous:
        latest_policy = latest.get("policy", {}) if isinstance(latest.get("policy"), dict) else {}
        previous_policy = previous.get("policy", {}) if isinstance(previous.get("policy"), dict) else {}
        policy_delta = _classify_policy_delta(previous_policy, latest_policy)
        if policy_delta["changed_keys"]:
            observations.append({"kind": "previous_policy_delta", "value": policy_delta})

        latest_selected_ids = latest.get("selected_ids", []) if isinstance(latest.get("selected_ids"), list) else []
        previous_selected_ids = previous.get("selected_ids", []) if isinstance(previous.get("selected_ids"), list) else []
        latest_fails = latest.get("agreed_fail_tags", []) if isinstance(latest.get("agreed_fail_tags"), list) else []
        previous_fails = previous.get("agreed_fail_tags", []) if isinstance(previous.get("agreed_fail_tags"), list) else []
        latest_passes = latest.get("agreed_pass_tags", []) if isinstance(latest.get("agreed_pass_tags"), list) else []
        previous_passes = previous.get("agreed_pass_tags", []) if isinstance(previous.get("agreed_pass_tags"), list) else []
        latest_issue_score = int(latest.get("issue_score", 0))
        previous_issue_score = int(previous.get("issue_score", 0))

        if latest_selected_ids == previous_selected_ids and latest_selected_ids:
            observations.append({"kind": "unchanged_selected_frontier", "value": len(latest_selected_ids)})
            instructions.append("The last move reproduced the same selected frontier; do not toggle back into it.")

        primary_fail_same = _signature_from_sequence(latest_fails) == _signature_from_sequence(previous_fails)
        issue_score_improved = latest_issue_score < previous_issue_score
        pass_signature_held = set(previous_passes).issubset(set(latest_passes))
        if policy_delta["changed_keys"] and primary_fail_same and not issue_score_improved:
            observations.append(
                {
                    "kind": "ineffective_last_delta",
                    "value": {
                        "changed_keys": policy_delta["changed_keys"],
                        "fail_signature": latest_fails,
                        "issue_score_delta": latest_issue_score - previous_issue_score,
                        "protected_passes_held": pass_signature_held,
                    },
                }
            )
            prohibited_patterns.append("blind_replay_of_last_unsuccessful_delta")
            instructions.append(
                "The last applied delta did not reduce the primary failure pressure; the next move must be orthogonal or introduce a genuinely new mechanism."
            )
            required_move_characteristics.append("orthogonal_to_last_ineffective_delta")

        capacity_only_expansion = (
            bool(policy_delta["numeric_increase_keys"])
            and not policy_delta["boolean_flips"]
            and not policy_delta["numeric_decrease_keys"]
            and not policy_delta["other_keys"]
        )
        if capacity_only_expansion and primary_fail_same and not issue_score_improved:
            observations.append(
                {
                    "kind": "capacity_only_expansion_without_primary_change",
                    "value": {
                        "numeric_increase_keys": policy_delta["numeric_increase_keys"],
                        "previous_selected_count": len(previous_selected_ids),
                        "latest_selected_count": len(latest_selected_ids),
                        "fail_signature": latest_fails,
                    },
                }
            )
            prohibited_patterns.append("capacity_only_expansion_without_primary_change")
            instructions.append(
                "If the last move only expanded capacity without changing the primary failure state, do not spend the next round on another capacity-only expansion."
            )

        if len(latest_selected_ids) > len(previous_selected_ids) and latest_fails == previous_fails and latest_fails:
            observations.append(
                {
                    "kind": "width_increase_without_primary_fail_change",
                    "value": {
                        "previous_selected_count": len(previous_selected_ids),
                        "latest_selected_count": len(latest_selected_ids),
                        "fail_signature": latest_fails,
                    },
                }
            )
            prohibited_patterns.append("pure_width_increase_without_primary_fail_change")
            instructions.append(
                "If the last move widened the output frontier without changing the primary fail signature, do not spend the next round on width-only expansion again."
            )

    directive = {
        "round_index": round_index,
        "final_goal": spec.get("final_goal"),
        "manager_mode": manager_mode,
        "orchestrator_memory": {
            "path": "meta/orchestrator_memory.md",
            "prior_tail": _read_text_tail(_orchestrator_memory_path(run_root)),
        },
        "surface": {
            "level": surface_index,
            "name": str(surface_level.get("name") or f"surface_{surface_index}"),
            "allowed_edit_targets": [str(entry.get("target")) for entry in surface_entries if entry.get("target")],
            "surface_escalation_count": int(status.get("surface_escalation_count", 0)),
        },
        "audit": {
            "level": "deep" if audit_reasons else "light",
            "reasons": list(dict.fromkeys(audit_reasons)),
            "focus_checks": list(dict.fromkeys(audit_focus_checks)),
        },
        "status": {
            "consecutive_no_progress": consecutive_no_progress,
            "last_feedback_verdict": status.get("last_feedback_verdict"),
            "reset_count": int(status.get("reset_count", 0)),
            "solver_reset_pending": bool(status.get("solver_reset_pending", False)),
            "active_surface_level": surface_index,
            "surface_name": str(surface_level.get("name") or f"surface_{surface_index}"),
            "surface_escalation_count": int(status.get("surface_escalation_count", 0)),
        },
        "observations": observations,
        "instructions": list(dict.fromkeys(instructions)),
        "required_move_characteristics": list(dict.fromkeys(required_move_characteristics)),
        "prohibited_patterns": list(dict.fromkeys(prohibited_patterns)),
        "missing_artifacts_to_backfill": latest_missing_artifacts,
        "preserve": preserve,
        "target": target,
        "issue_count": int(issue_ledger.get("issue_count", 0)),
        "issue_score": int(issue_ledger.get("issue_score", 0)),
        "history_length": len(round_history),
    }
    return directive


def _signature_from_sequence(items: list[Any]) -> str | None:
    normalized = [str(item) for item in items if str(item)]
    if not normalized:
        return None
    return "|".join(sorted(normalized))


def _build_stagnation_summary(run_root: Path, status: dict[str, Any] | None = None) -> dict[str, Any]:
    summaries = _load_round_summaries(run_root)
    verdict_window = [str(summary.get("merged_verdict")) for summary in summaries if summary.get("merged_verdict")]
    fail_signature_window = [
        _signature_from_sequence(list(summary.get("agreed_fail_tags", []))) for summary in summaries if summary.get("agreed_fail_tags")
    ]
    issue_signature_window = []
    for summary in summaries:
        issues = list(summary.get("issues", []))
        issue_keys = [
            str(issue.get("issue_key") or issue.get("issue_id") or issue.get("summary") or "")
            for issue in issues
            if str(issue.get("issue_key") or issue.get("issue_id") or issue.get("summary") or "")
        ]
        signature = _signature_from_sequence(issue_keys)
        if signature:
            issue_signature_window.append(signature)
    selected_signature_window = []
    for summary in summaries:
        solver_result = summary.get("solver_result", {})
        selected_ids = solver_result.get("selected_ids", [])
        if isinstance(selected_ids, list):
            signature = _signature_from_sequence(selected_ids)
            if signature:
                selected_signature_window.append(signature)

    def _repeat(window: list[str]) -> str | None:
        if len(window) < 2:
            return None
        if window[-1] and window[-1] == window[-2]:
            return window[-1]
        return None

    current_status = status or _load_status(run_root)
    return {
        "round_count": len(summaries),
        "latest_round_index": summaries[-1]["round_index"] if summaries else 0,
        "consecutive_no_progress": int(current_status.get("consecutive_no_progress", 0)),
        "latest_verdict": verdict_window[-1] if verdict_window else None,
        "verdict_window": verdict_window[-5:],
        "repeated_verdict": _repeat(verdict_window),
        "fail_signature_window": fail_signature_window[-5:],
        "repeated_fail_signature": _repeat(fail_signature_window),
        "issue_signature_window": issue_signature_window[-5:],
        "repeated_issue_signature": _repeat(issue_signature_window),
        "selected_signature_window": selected_signature_window[-5:],
        "repeated_selected_signature": _repeat(selected_signature_window),
    }


def _load_stagnation_summary(run_root: Path) -> dict[str, Any]:
    path = run_root / "meta" / "stagnation.json"
    if path.exists():
        return _read_json(path)
    return _build_stagnation_summary(run_root)


def _write_stagnation_summary(run_root: Path, status: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = _build_stagnation_summary(run_root, status=status)
    _write_json(run_root / "meta" / "stagnation.json", payload)
    return payload


def _copy_last_approved_bundle(run_root: Path, workspace: Path) -> None:
    best_summary = _load_best_checkpoint_summary(run_root)
    if not best_summary:
        return
    checkpoint_dir_raw = best_summary.get("checkpoint_dir")
    if not checkpoint_dir_raw:
        return
    checkpoint_for_eval = Path(str(checkpoint_dir_raw)).resolve() / "for_eval"
    if checkpoint_for_eval.exists():
        _copy_path(checkpoint_for_eval, workspace / "last_approved")


def _load_previous_round_workspace(run_root: Path, round_index: int) -> Path | None:
    if round_index <= 1:
        return None
    workspace = run_root / "rounds" / f"round_{round_index - 1:04d}" / "solver" / "workspace"
    if workspace.exists():
        return workspace
    return None


def _seed_solver_state(
    spec: dict[str, Any],
    run_root: Path,
    solver_workspace: Path,
    status: dict[str, Any],
    *,
    round_index: int,
) -> None:
    seed_cfg = spec.get("workspace_seed", {})
    _, _, solver_editable = _get_active_surface(spec, status)
    project_root = solver_workspace / "project"
    carry_mode = seed_cfg.get("carry_forward_mode")
    best_summary = _load_best_checkpoint_summary(run_root)
    previous_workspace = _load_previous_round_workspace(run_root, round_index)
    reset_pending = bool(status.get("solver_reset_pending", False))
    carried = False
    if carry_mode == "best_checkpoint" and best_summary:
        checkpoint_dir_raw = best_summary.get("checkpoint_dir")
        if checkpoint_dir_raw:
            checkpoint_project = Path(str(checkpoint_dir_raw)).resolve() / "project"
            if checkpoint_project.exists():
                _copy_path(checkpoint_project, project_root)
                carried = True
    elif carry_mode == "last_round_candidate" and previous_workspace is not None:
        previous_project = previous_workspace / "project"
        if previous_project.exists():
            _copy_path(previous_project, project_root)
            carried = True

    if solver_editable and not carried:
        _copy_seed_entries(solver_editable, solver_workspace)
    elif solver_editable and carried:
        for entry in solver_editable:
            target = solver_workspace / entry["target"]
            if not target.exists():
                _copy_path(Path(entry["source"]).resolve(), target)

    solver_memory_cfg = spec.get("solver_memory") or {}
    if not solver_memory_cfg.get("carry_forward") or reset_pending:
        return
    if carry_mode == "last_round_candidate" and previous_workspace is not None:
        previous_memory = previous_workspace / str(solver_memory_cfg["root"])
        if previous_memory.exists():
            _copy_path(previous_memory, solver_workspace / str(solver_memory_cfg["root"]))
        return
    if not best_summary:
        return

    checkpoint_dir_raw = best_summary.get("checkpoint_dir")
    if not checkpoint_dir_raw:
        return
    checkpoint_memory = Path(str(checkpoint_dir_raw)).resolve() / str(solver_memory_cfg["root"])
    if checkpoint_memory.exists():
        _copy_path(checkpoint_memory, solver_workspace / str(solver_memory_cfg["root"]))


def _write_round_context(
    spec: dict[str, Any],
    run_root: Path,
    workspace: Path,
    round_index: int,
    status: dict[str, Any],
    *,
    append_manager_history: bool = False,
) -> None:
    latest = _load_latest_scoreboard(run_root) or {}
    issue_ledger = _load_issue_ledger(run_root)
    converged_feedback = _load_converged_feedback(run_root)
    stagnation = _load_stagnation_summary(run_root)
    context = {
        "round_index": round_index,
        "final_goal": spec["final_goal"],
        "prior_status": status,
        "prior_agreed_fail_tags": list(latest.get("agreed_fail_tags", [])),
        "prior_agreed_pass_tags": list(latest.get("agreed_pass_tags", [])),
        "prior_issue_ledger": list(issue_ledger.get("issues", [])),
        "disagreement_hints": latest.get("disagreements", {}),
        "last_feedback_verdict": latest.get("merged_verdict", status.get("last_feedback_verdict")),
        "consecutive_no_progress": int(status.get("consecutive_no_progress", 0)),
        "reset_count": int(status.get("reset_count", 0)),
        "solver_reset_pending": bool(status.get("solver_reset_pending", False)),
        "active_surface_level": int(status.get("active_surface_level", 0)),
        "surface_name": str(status.get("surface_name") or "default"),
        "surface_escalation_count": int(status.get("surface_escalation_count", 0)),
        "best_checkpoint": _load_best_checkpoint_summary(run_root),
    }
    manager_directive = _build_manager_directive(spec, run_root, round_index=round_index, status=status)
    round_history_payload = {"history": _build_round_history(run_root)}
    _write_json(workspace / "loop_input" / "round_context.json", context)
    _write_json(workspace / "loop_input" / "issue_ledger.json", issue_ledger)
    _write_json(workspace / "loop_input" / "stagnation.json", stagnation)
    _write_json(workspace / "loop_input" / "round_history.json", round_history_payload)
    _write_json(workspace / "loop_input" / "manager_directive.json", manager_directive)
    _write_text(workspace / "loop_input" / "round_context.md", _render_round_context_markdown(context))
    if converged_feedback:
        _write_text(workspace / "loop_input" / "converged_feedback.md", converged_feedback + "\n")
    _write_text(workspace / "loop_input" / "issue_ledger.md", _render_issue_ledger_markdown(issue_ledger))
    _write_text(workspace / "loop_input" / "stagnation.md", _render_stagnation_markdown(stagnation))
    _write_text(workspace / "loop_input" / "round_history.md", _render_round_history_markdown(round_history_payload["history"]))
    _write_text(workspace / "loop_input" / "manager_directive.md", _render_manager_directive_markdown(manager_directive))
    _write_text(
        workspace / "loop_input" / "handoff.md",
        _render_handoff_markdown(
            context,
            issue_ledger,
            stagnation,
            round_history_payload["history"],
            manager_directive,
            converged_feedback=converged_feedback,
        ),
    )
    if append_manager_history:
        _append_jsonl(run_root / "meta" / "manager_history.jsonl", manager_directive)
        _append_orchestrator_memory(
            run_root,
            manager_directive,
            actions=[
                "wrote manager_directive.json/md and solver handoff",
                "appended machine manager_history.jsonl",
            ],
        )


def materialize_round_workspaces(
    spec: dict[str, Any],
    run_root: str | Path,
    round_index: int,
    status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(run_root).resolve()
    current_status = status or _load_status(root)
    round_dir = root / "rounds" / f"round_{round_index:04d}"
    solver_workspace = round_dir / "solver" / "workspace"
    evaluator_workspaces: dict[str, Path] = {}

    if round_dir.exists():
        shutil.rmtree(round_dir)
    solver_workspace.mkdir(parents=True, exist_ok=True)
    _copy_files(spec["references"]["solver_visible"], solver_workspace / "references")
    _seed_solver_state(spec, root, solver_workspace, current_status, round_index=round_index)
    _write_round_context(spec, root, solver_workspace, round_index, current_status, append_manager_history=True)

    for evaluator in spec["evaluators"]:
        workspace = round_dir / evaluator["name"] / "workspace"
        workspace.mkdir(parents=True, exist_ok=True)
        _copy_files(spec["references"]["solver_visible"], workspace / "references")
        _copy_files(spec["references"].get("evaluator_only", []), workspace / "references")
        _copy_last_approved_bundle(root, workspace)
        _write_round_context(spec, root, workspace, round_index, current_status)
        evaluator_workspaces[evaluator["name"]] = workspace

    return {
        "round_dir": round_dir,
        "solver_workspace": solver_workspace,
        "evaluator_workspaces": evaluator_workspaces,
    }


def _terminate_process(process: subprocess.Popen[str], *, grace_seconds: float = 1.0) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        process.terminate()
    else:
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=grace_seconds)
        return
    except subprocess.TimeoutExpired:
        pass
    if process.poll() is not None:
        return
    if os.name == "nt":
        process.kill()
    else:
        os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout=grace_seconds)


def _run_command(
    command: list[str],
    workspace: Path,
    stdout_path: Path,
    stderr_path: Path,
    *,
    completion_paths: list[Path] | None = None,
    completion_probe: Callable[[], bool] | None = None,
    completion_grace_seconds: float = 0.2,
    poll_interval_seconds: float = 0.05,
    termination_grace_seconds: float = 1.0,
    max_runtime_seconds: float | None = None,
    stall_watchdog_seconds: float | None = None,
    progress_paths: list[Path] | None = None,
    progress_probe_interval_seconds: float = 1.0,
) -> dict[str, Any]:
    process = subprocess.Popen(
        command,
        cwd=workspace,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=os.name != "nt",
    )
    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    progress_lock = threading.Lock()
    last_output_at = {"value": time.monotonic()}

    def _drain_stream(stream: Any, chunks: list[str]) -> None:
        try:
            while True:
                chunk = stream.read(8192)
                if not chunk:
                    break
                chunks.append(chunk)
                with progress_lock:
                    last_output_at["value"] = time.monotonic()
        finally:
            stream.close()

    stdout_thread = threading.Thread(target=_drain_stream, args=(process.stdout, stdout_chunks), daemon=True)
    stderr_thread = threading.Thread(target=_drain_stream, args=(process.stderr, stderr_chunks), daemon=True)
    stdout_thread.start()
    stderr_thread.start()

    contract_started_at: float | None = None
    completed_via_contract = False
    started_at = time.monotonic()
    last_progress_at = started_at
    last_progress_probe_at = started_at
    progress_signature = _path_progress_signature(progress_paths)
    timed_out = False
    stalled = False
    try:
        while process.poll() is None:
            now = time.monotonic()
            if max_runtime_seconds is not None and time.monotonic() - started_at >= max_runtime_seconds:
                timed_out = True
                _terminate_process(process, grace_seconds=termination_grace_seconds)
                break
            with progress_lock:
                last_progress_at = max(last_progress_at, last_output_at["value"])
            if (
                stall_watchdog_seconds is not None
                and progress_paths
                and now - last_progress_probe_at >= progress_probe_interval_seconds
            ):
                current_signature = _path_progress_signature(progress_paths)
                if current_signature != progress_signature:
                    progress_signature = current_signature
                    last_progress_at = now
                last_progress_probe_at = now
            if stall_watchdog_seconds is not None and now - last_progress_at >= stall_watchdog_seconds:
                stalled = True
                _terminate_process(process, grace_seconds=termination_grace_seconds)
                break
            completion_ready = False
            if completion_paths and all(path.exists() for path in completion_paths):
                completion_ready = True
            elif completion_probe is not None:
                try:
                    completion_ready = bool(completion_probe())
                except Exception:  # noqa: BLE001
                    completion_ready = False
            if completion_ready:
                if contract_started_at is None:
                    contract_started_at = time.monotonic()
                elif time.monotonic() - contract_started_at >= completion_grace_seconds:
                    completed_via_contract = True
                    _terminate_process(process, grace_seconds=termination_grace_seconds)
                    break
            else:
                contract_started_at = None
            time.sleep(poll_interval_seconds)
        process.wait(timeout=termination_grace_seconds if completed_via_contract else None)
    except subprocess.TimeoutExpired:
        _terminate_process(process, grace_seconds=termination_grace_seconds)
    finally:
        if process.poll() is None:
            _terminate_process(process, grace_seconds=termination_grace_seconds)
        stdout_thread.join(timeout=termination_grace_seconds)
        stderr_thread.join(timeout=termination_grace_seconds)

    stdout_path.write_text("".join(stdout_chunks), encoding="utf-8")
    stderr_path.write_text("".join(stderr_chunks), encoding="utf-8")
    return {
        "returncode": process.returncode,
        "completed_via_contract": completed_via_contract,
        "timed_out": timed_out,
        "stalled": stalled,
    }


def _normalize_evidence_list(evidence: Any) -> list[str]:
    if evidence in (None, [], {}):
        return []
    if isinstance(evidence, str):
        return [evidence]
    if isinstance(evidence, dict):
        return [json.dumps(evidence, ensure_ascii=False, sort_keys=True)]
    if isinstance(evidence, list):
        return [str(item) for item in evidence]
    return [str(evidence)]


def _apply_evidence_gate(results: list[dict[str, Any]], evidence_gate: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not evidence_gate or not evidence_gate.get("enabled"):
        return results

    fail_tag = str(evidence_gate.get("fail_tag") or "EVIDENCE_GATE")
    issue_id = str(evidence_gate.get("issue_id") or "evidence-gate-empty-evidence")
    require_pass_evidence = bool(evidence_gate.get("require_nonempty_evidence_for_pass", True))
    gated: list[dict[str, Any]] = []
    for result in results:
        normalized = dict(result)
        evidence = _normalize_evidence_list(normalized.get("evidence", []))
        normalized["evidence"] = evidence
        verdict = str(normalized.get("verdict") or "")
        claims_terminal = bool(normalized.get("all_pass")) or verdict == "terminate"
        if require_pass_evidence and claims_terminal and not evidence:
            normalized["all_pass"] = False
            if verdict:
                normalized["verdict"] = "no_progress"
            normalized["fail_tags"] = sorted({str(tag) for tag in normalized.get("fail_tags", [])} | {fail_tag})
            issues = list(normalized.get("issues", []))
            issues.append(
                {
                    "issue_id": issue_id,
                    "summary": "terminal pass was claimed without non-empty evaluator evidence",
                    "severity": "high",
                    "focus": "evidence_gate",
                    "evidence": [f"evaluator={normalized.get('evaluator', '(unknown)')}", "top_level_evidence_empty"],
                }
            )
            normalized["issues"] = issues
        gated.append(normalized)
    return gated


def merge_evaluator_results(
    results: list[dict[str, Any]],
    *,
    require_verdict: bool = False,
    evidence_gate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    results = _apply_evidence_gate(results, evidence_gate)
    fail_sets = [set(result.get("fail_tags", [])) for result in results]
    pass_sets = [set(result.get("pass_tags", [])) for result in results]
    agreed_fails = sorted(set.intersection(*fail_sets)) if fail_sets else []
    agreed_passes = sorted(set.intersection(*pass_sets)) if pass_sets else []
    disagreements = {}
    verdicts: dict[str, str] = {}
    for idx, result in enumerate(results):
        evaluator_name = result.get("evaluator", f"eval_{idx}")
        disagreements[evaluator_name] = {
            "fail_tags": sorted(set(result.get("fail_tags", [])) - set(agreed_fails)),
            "pass_tags": sorted(set(result.get("pass_tags", [])) - set(agreed_passes)),
            "verdict": result.get("verdict"),
        }
        if result.get("verdict") is not None:
            verdicts[evaluator_name] = str(result["verdict"])
        elif require_verdict:
            raise ValueError(f"evaluator result missing verdict for {evaluator_name}")

    merged = {
        "all_pass": bool(results) and all(bool(result.get("all_pass")) for result in results),
        "agreed_fail_tags": agreed_fails,
        "agreed_pass_tags": agreed_passes,
        "disagreements": disagreements,
    }
    merged_issues = _merge_issues(results)
    if merged_issues:
        merged["issues"] = merged_issues
        merged["issue_count"] = len(merged_issues)
        severity_counts = {severity: 0 for severity in VALID_ISSUE_SEVERITIES}
        for issue in merged_issues:
            severity_counts[str(issue["severity"])] += 1
        merged["issue_severity_counts"] = {k: v for k, v in severity_counts.items() if v}
        merged["issue_score"] = sum(ISSUE_SEVERITY_SCORE[str(issue["severity"])] for issue in merged_issues)
    if verdicts:
        merged["verdicts"] = verdicts
        merged["merged_verdict"] = _merge_verdicts(list(verdicts.values()))
    return merged


def _normalize_issue_key(issue: dict[str, Any]) -> str:
    issue_id = str(issue.get("issue_id") or "").strip()
    if issue_id:
        return issue_id
    summary = str(issue.get("summary") or "").strip().lower()
    return re.sub(r"[^0-9a-z가-힣]+", "-", summary).strip("-")


def _merge_issues(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for idx, result in enumerate(results):
        evaluator_name = str(result.get("evaluator", f"eval_{idx}"))
        for raw_issue in result.get("issues", []):
            issue = dict(raw_issue)
            key = _normalize_issue_key(issue)
            existing = merged.get(key)
            evidence = sorted({str(item) for item in issue.get("evidence", [])})
            if existing is None:
                merged[key] = {
                    "issue_key": key,
                    "issue_id": issue.get("issue_id"),
                    "summary": str(issue["summary"]),
                    "severity": str(issue["severity"]),
                    "focus": issue.get("focus"),
                    "evidence": evidence,
                    "reported_by": [evaluator_name],
                }
                continue
            if ISSUE_SEVERITY_SCORE[str(issue["severity"])] > ISSUE_SEVERITY_SCORE[str(existing["severity"])]:
                existing["severity"] = str(issue["severity"])
            if not existing.get("focus") and issue.get("focus"):
                existing["focus"] = issue.get("focus")
            existing["evidence"] = sorted(set(existing.get("evidence", [])) | set(evidence))
            existing["reported_by"] = sorted(set(existing.get("reported_by", [])) | {evaluator_name})
    return sorted(
        merged.values(),
        key=lambda item: (-ISSUE_SEVERITY_SCORE[str(item["severity"])], str(item["issue_key"])),
    )


def _merge_verdicts(verdicts: list[str]) -> str:
    if not verdicts:
        raise ValueError("cannot merge empty verdict list")
    unique = set(verdicts)
    if len(unique) == 1:
        return verdicts[0]
    severity = {
        "terminate": 0,
        "progress": 1,
        "no_progress": 2,
        "regress": 3,
    }
    return max(verdicts, key=lambda verdict: severity.get(verdict, -1))


def create_round_summary(
    round_index: int,
    merged: dict[str, Any],
    solver_result: dict[str, Any],
    *,
    solver_memory_files: list[str] | None = None,
    feedback_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    feedback_state = feedback_state or {}
    return {
        "round_index": round_index,
        "all_pass": bool(merged.get("all_pass")),
        "agreed_fail_tags": list(merged.get("agreed_fail_tags", [])),
        "agreed_pass_tags": list(merged.get("agreed_pass_tags", [])),
        "disagreements": dict(merged.get("disagreements", {})),
        "merged_verdict": merged.get("merged_verdict"),
        "verdicts": dict(merged.get("verdicts", {})),
        "agreed_fail_count": len(merged.get("agreed_fail_tags", [])),
        "agreed_pass_count": len(merged.get("agreed_pass_tags", [])),
        "issues": list(merged.get("issues", [])),
        "issue_count": int(merged.get("issue_count", 0)),
        "issue_score": int(merged.get("issue_score", 0)),
        "issue_severity_counts": dict(merged.get("issue_severity_counts", {})),
        "solver_memory_files": list(solver_memory_files or []),
        "consecutive_no_progress": int(feedback_state.get("consecutive_no_progress", 0)),
        "reset_count": int(feedback_state.get("reset_count", 0)),
        "converged_feedback_path": feedback_state.get("converged_feedback_path"),
        "solver_result": solver_result,
    }


def _checkpoint_open_fail_tags(summary: dict[str, Any]) -> set[str]:
    fail_tags = {str(tag) for tag in summary.get("agreed_fail_tags", [])}
    for hint in dict(summary.get("disagreements", {})).values():
        if isinstance(hint, dict):
            fail_tags.update(str(tag) for tag in hint.get("fail_tags", []))
    return fail_tags


def _checkpoint_selected_count(summary: dict[str, Any]) -> int:
    solver_result = summary.get("solver_result", {})
    if not isinstance(solver_result, dict):
        return 1_000_000
    selected_ids = solver_result.get("selected_ids")
    if isinstance(selected_ids, list):
        return len(selected_ids)
    policy = solver_result.get("policy")
    if isinstance(policy, dict):
        top_k = policy.get("top_k")
        if isinstance(top_k, int):
            return top_k
    return 1_000_000


def _checkpoint_verdict_rank(summary: dict[str, Any]) -> int:
    order = {
        "regress": 0,
        "no_progress": 1,
        "progress": 2,
        "terminate": 3,
    }
    return order.get(str(summary.get("merged_verdict") or ""), 0)


def _checkpoint_rank(summary: dict[str, Any]) -> tuple[int, int, int, int, int, int]:
    open_fail_count = len(_checkpoint_open_fail_tags(summary))
    return (
        1 if summary.get("all_pass") else 0,
        -open_fail_count,
        _checkpoint_verdict_rank(summary),
        int(summary.get("agreed_pass_count", 0)),
        -_checkpoint_selected_count(summary),
        -int(summary.get("issue_score", 0)),
    )


def maybe_update_best_checkpoint(
    run_root: str | Path,
    round_summary: dict[str, Any],
    source_dir: str | Path,
    project_dir: str | Path | None = None,
    solver_memory_dir: str | Path | None = None,
) -> bool:
    root = Path(run_root).resolve()
    best_path = root / "meta" / "best_checkpoint.json"
    should_update = True

    if best_path.exists():
        best = _read_json(best_path)
        should_update = _checkpoint_rank(round_summary) > _checkpoint_rank(best)

    if not should_update:
        return False

    checkpoint_dir = root / "checkpoints" / f"checkpoint_{round_summary['round_index']:04d}"
    if checkpoint_dir.exists():
        shutil.rmtree(checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    source = Path(source_dir).resolve()
    if source.is_dir():
        root_name = root.name

        def _ignore(current_dir: str, names: list[str]) -> set[str]:
            current = Path(current_dir).resolve()
            ignored: set[str] = set()
            if current == source and root_name in names:
                ignored.add(root_name)
            return ignored

        shutil.copytree(source, checkpoint_dir / source.name, ignore=_ignore)
    else:
        shutil.copy2(source, checkpoint_dir / source.name)
    if project_dir is not None:
        project_source = Path(project_dir).resolve()
        if project_source.exists():
            _copy_path(project_source, checkpoint_dir / "project")
    if solver_memory_dir is not None:
        memory_source = Path(solver_memory_dir).resolve()
        if memory_source.exists():
            _copy_path(memory_source, checkpoint_dir / memory_source.name)
    best_payload = dict(round_summary)
    best_payload["checkpoint_dir"] = str(checkpoint_dir)
    _write_json(best_path, best_payload)
    return True


def _load_status(run_root: Path) -> dict[str, Any]:
    status_path = run_root / "meta" / "status.json"
    if status_path.exists():
        return _read_json(status_path)
    return {
        "next_round": 1,
        "status": "initialized",
        "active_surface_level": 0,
        "surface_name": "default",
        "surface_escalation_count": 0,
    }


def _save_status(run_root: Path, payload: dict[str, Any]) -> None:
    _write_json(run_root / "meta" / "status.json", payload)


def _extract_first_json_object_from_text(text: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    for idx, char in enumerate(text):
        if char != "{":
            continue
        try:
            payload, _ = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    raise ValueError("no JSON object found in fallback text artifact")


def _load_result_payload_with_fallback(path: Path) -> dict[str, Any]:
    candidate_paths = [path, path.with_suffix(".md"), path.with_suffix(".txt")]
    last_error: Exception | None = None
    for candidate in candidate_paths:
        if not candidate.exists():
            continue
        try:
            if candidate.suffix == ".json":
                return _read_json(candidate)
            return _extract_first_json_object_from_text(candidate.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    if last_error is not None:
        raise ValueError(f"unable to load evaluator result from {path}: {last_error}") from last_error
    raise FileNotFoundError(f"missing evaluator result artifacts near {path}")


def _load_result_json(path: Path, *, require_verdict: bool = False) -> dict[str, Any]:
    payload = _load_result_payload_with_fallback(path)
    missing = [key for key in REQUIRED_RESULT_KEYS if key not in payload]
    if missing:
        raise ValueError(f"evaluator result missing required keys: {', '.join(missing)}")
    if require_verdict and "verdict" not in payload:
        raise ValueError("evaluator result missing required key: verdict")
    verdict = payload.get("verdict")
    if isinstance(verdict, str):
        verdict = VERDICT_ALIASES.get(verdict.strip().lower(), verdict)
        payload["verdict"] = verdict
    if verdict is not None and verdict not in VALID_VERDICTS:
        raise ValueError(f"unsupported evaluator verdict: {verdict}")
    payload["issues"] = _validate_issues(payload.get("issues", []))
    return payload


def _has_usable_solver_artifacts(spec: dict[str, Any], workspace: Path) -> bool:
    for_eval_root = workspace / "for_eval"
    project_root = workspace / "project"
    solver_memory_cfg = spec.get("solver_memory") or {}
    memory_root = workspace / str(solver_memory_cfg.get("root", "solver_memory"))

    if not project_root.exists() or not any(path.is_file() for path in project_root.rglob("*")):
        return False
    if not for_eval_root.exists():
        return False

    has_core_for_eval = any(
        (for_eval_root / relative_path).exists()
        for relative_path in ("evaluation.json", "selected.json", "summary.md", "artifact.json")
    )
    if not has_core_for_eval:
        return False

    if solver_memory_cfg and not memory_root.exists():
        return False
    return True


def _load_selected_ids_from_solver_workspace(workspace: Path) -> list[str]:
    selected_payload = _try_read_json(workspace / "for_eval" / "selected.json") or {}
    evaluation_payload = _try_read_json(workspace / "for_eval" / "evaluation.json") or {}

    for candidate in (
        selected_payload.get("selected_ids"),
        evaluation_payload.get("selected_ids"),
        (evaluation_payload.get("evaluation") or {}).get("selected_ids")
        if isinstance(evaluation_payload.get("evaluation"), dict)
        else None,
    ):
        if isinstance(candidate, list):
            return [str(item) for item in candidate]
    return []


def _load_verification_commands(spec: dict[str, Any], workspace: Path) -> list[list[str]]:
    solver_memory_cfg = spec.get("solver_memory") or {}
    verification_path = workspace / str(solver_memory_cfg.get("root", "solver_memory")) / "verification.json"
    payload = _try_read_json(verification_path) or {}
    raw_commands: list[Any] = []
    if isinstance(payload.get("commands"), list):
        raw_commands.extend(payload["commands"])
    elif payload.get("commands") is not None:
        raw_commands.append(payload["commands"])
    if payload.get("command") is not None:
        raw_commands.append(payload["command"])

    normalized: list[list[str]] = []
    for command in raw_commands:
        if isinstance(command, str) and command.strip():
            normalized.append(["bash", "-lc", command])
        elif isinstance(command, list) and all(isinstance(part, str) for part in command):
            normalized.append([str(part) for part in command])
    return normalized


def _attempt_verification_backfill(
    spec: dict[str, Any],
    workspace: Path,
    round_dir: Path,
) -> dict[str, Any]:
    commands = _load_verification_commands(spec, workspace)
    if not commands:
        return {"attempted": False, "completed": False, "command_count": 0}

    solver_cfg = spec.get("solver") or {}
    max_runtime_seconds_raw = solver_cfg.get("verification_max_runtime_seconds", 60)
    max_runtime_seconds = float(max_runtime_seconds_raw) if max_runtime_seconds_raw is not None else None
    attempted = 0
    completed = False
    for index, command in enumerate(commands, start=1):
        attempted += 1
        _run_command(
            command,
            workspace,
            round_dir / "solver" / f"verification_backfill_{index:02d}.stdout.txt",
            round_dir / "solver" / f"verification_backfill_{index:02d}.stderr.txt",
            max_runtime_seconds=max_runtime_seconds,
        )
        if _has_usable_solver_artifacts(spec, workspace):
            completed = True
            break
    return {"attempted": attempted > 0, "completed": completed, "command_count": attempted}


def _ensure_for_eval_metadata(spec: dict[str, Any], workspace: Path) -> list[str]:
    created: list[str] = []
    for_eval_root = workspace / "for_eval"
    if not for_eval_root.exists():
        return created

    project_policy_path = workspace / "project" / "policy.json"
    if project_policy_path.exists() and not (for_eval_root / "policy.json").exists():
        shutil.copy2(project_policy_path, for_eval_root / "policy.json")
        created.append("for_eval/policy.json")

    created.extend(_copy_declared_replay_artifacts(workspace, for_eval_root))

    if not (for_eval_root / "summary.md").exists():
        changed_files = _relative_file_list(workspace / "project", base=workspace)
        _write_text(
            for_eval_root / "summary.md",
            "\n".join(
                [
                    "# Summary",
                    "",
                    "## change_made",
                    "autogenerated by runner after solver backfill/degraded recovery",
                    "",
                    "## benchmark_effect",
                    "runner recovered missing evaluation artifacts from the recorded verification command",
                    "",
                    "## changed_files",
                    ", ".join(changed_files) or "(none recorded)",
                    "",
                ]
            )
            + "\n",
        )
        created.append("for_eval/summary.md")

    if not (for_eval_root / "replay.md").exists():
        commands = _load_verification_commands(spec, workspace)
        rendered_commands = []
        for command in commands:
            if command[:2] == ["bash", "-lc"] and len(command) >= 3:
                rendered_commands.append(command[2])
            else:
                rendered_commands.append(" ".join(command))
        replay_lines = rendered_commands or ["(no verification commands recorded)"]
        _write_text(for_eval_root / "replay.md", "\n".join(replay_lines) + "\n")
        created.append("for_eval/replay.md")

    return created


def _synthesize_stalled_solver_artifacts(
    spec: dict[str, Any],
    workspace: Path,
    *,
    round_index: int,
    reason: str,
) -> list[str]:
    created: list[str] = []
    for_eval_root = workspace / "for_eval"
    project_root = workspace / "project"
    solver_memory_cfg = spec.get("solver_memory") or {}
    memory_root = workspace / str(solver_memory_cfg.get("root", "solver_memory"))

    for_eval_root.mkdir(parents=True, exist_ok=True)
    if not project_root.exists():
        project_root.mkdir(parents=True, exist_ok=True)
    if not any(project_root.rglob("*")):
        _write_json(project_root / "candidate.json", {"status": "stalled", "round": round_index})
        created.append("project/candidate.json")

    project_policy_path = project_root / "policy.json"
    if project_policy_path.exists() and not (for_eval_root / "policy.json").exists():
        shutil.copy2(project_policy_path, for_eval_root / "policy.json")
        created.append("for_eval/policy.json")

    fallback_files = {
        "for_eval/artifact.json": {
            "quality": "weak",
            "round": round_index,
            "stalled": True,
            "reason": reason,
        },
        "for_eval/evaluation.json": {
            "all_pass": False,
            "selected_ids": [],
            "evaluation": {
                "all_pass": False,
                "selected_ids": [],
                "error": reason,
            },
        },
        "for_eval/selected.json": {"selected_ids": []},
    }
    for relative_path, payload in fallback_files.items():
        path = workspace / relative_path
        if not path.exists():
            _write_json(path, payload)
            created.append(relative_path)

    text_files = {
        "for_eval/summary.md": "\n".join(
            [
                "# Summary",
                "",
                "## change_made",
                "no candidate change was completed because the solver was recovered by the supervisor",
                "",
                "## benchmark_effect",
                "no valid benchmark improvement was produced before supervisor recovery",
                "",
                "## why_this_round",
                reason,
                "",
                "## next_risk",
                "next solver turn must avoid repeating a stalled execution path and must emit artifacts incrementally",
                "",
            ]
        ),
        "for_eval/replay.md": "(no replay command completed before supervisor recovery)\n",
        "for_eval/report.md": f"# Solver stalled\n\nReason: {reason}\n",
    }
    for relative_path, content in text_files.items():
        path = workspace / relative_path
        if not path.exists():
            _write_text(path, content)
            created.append(relative_path)

    if solver_memory_cfg:
        memory_root.mkdir(parents=True, exist_ok=True)
        memory_files = {
            "turn_log.md": f"# Turn {round_index}\n\n- Supervisor recovered a stalled solver: {reason}\n",
            "best_code.json": json.dumps(
                {"round": round_index, "status": "stalled", "policy_path": "project/policy.json"},
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            "verification.json": json.dumps(
                {"round": round_index, "commands": [], "stalled": True, "reason": reason},
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
        }
        for relative_path in solver_memory_cfg.get("required_files", []):
            target = workspace / str(relative_path)
            if target.exists():
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(memory_files.get(Path(str(relative_path)).name, ""), encoding="utf-8")
            created.append(str(relative_path))

    return created


def _build_degraded_solver_result(spec: dict[str, Any], workspace: Path, *, round_index: int) -> dict[str, Any] | None:
    if not _has_usable_solver_artifacts(spec, workspace):
        return None

    solver_memory_cfg = spec.get("solver_memory") or {}
    workspace_base = workspace
    for_eval_root = workspace / "for_eval"
    project_root = workspace / "project"
    memory_root = workspace / str(solver_memory_cfg.get("root", "solver_memory"))

    evaluation_payload = _try_read_json(for_eval_root / "evaluation.json") or {}
    artifact_payload = _try_read_json(for_eval_root / "artifact.json") or {}
    project_policy = _try_read_json(project_root / "policy.json")

    existing_artifacts = sorted(
        set(_relative_file_list(for_eval_root, base=workspace_base))
        | set(_relative_file_list(memory_root, base=workspace_base))
    )
    changed_files = _relative_file_list(project_root, base=workspace_base)

    missing_artifacts = ["solver_result.json"]
    for candidate in ("for_eval/policy.json", "for_eval/summary.md", "for_eval/replay.md"):
        if not (workspace / candidate).exists():
            missing_artifacts.append(candidate)
    for relative_path in solver_memory_cfg.get("required_files", []):
        if not (workspace / str(relative_path)).exists():
            missing_artifacts.append(str(relative_path))

    selected_ids = _load_selected_ids_from_solver_workspace(workspace)
    quality = artifact_payload.get("quality")
    if quality is None and isinstance(evaluation_payload.get("evaluation"), dict):
        quality = "strong" if bool(evaluation_payload["evaluation"].get("all_pass")) else "weak"

    return {
        "round": round_index,
        "status": "degraded_ready",
        "degraded": True,
        "missing_artifacts": missing_artifacts,
        "artifacts": existing_artifacts,
        "changed_files": changed_files,
        "policy": project_policy if isinstance(project_policy, dict) else evaluation_payload.get("policy", {}),
        "policy_path": "project/policy.json" if (project_root / "policy.json").exists() else None,
        "selected_ids": selected_ids,
        "quality": quality,
        "for_eval_dir": str(for_eval_root),
    }


def _validate_issues(issues: Any) -> list[dict[str, Any]]:
    if issues in (None, []):
        return []
    if not isinstance(issues, list):
        raise ValueError("evaluator result issues must be a list")
    normalized: list[dict[str, Any]] = []
    for issue in issues:
        if not isinstance(issue, dict):
            raise ValueError("each issue must be an object")
        summary = str(issue.get("summary") or "").strip()
        severity = str(issue.get("severity") or "").strip().lower()
        severity = ISSUE_SEVERITY_ALIASES.get(severity, severity)
        if not summary:
            raise ValueError("issue.summary is required")
        if severity not in VALID_ISSUE_SEVERITIES:
            raise ValueError("issue.severity must be one of high|medium|low")
        evidence = issue.get("evidence", [])
        if evidence is None:
            evidence = []
        elif isinstance(evidence, str):
            evidence = [evidence]
        elif isinstance(evidence, dict):
            evidence = [json.dumps(evidence, ensure_ascii=False, sort_keys=True)]
        elif not isinstance(evidence, list):
            raise ValueError("issue.evidence must be a string or list when present")
        normalized.append(
            {
                "issue_id": str(issue.get("issue_id") or "").strip() or None,
                "summary": summary,
                "severity": severity,
                "focus": str(issue.get("focus")) if issue.get("focus") is not None else None,
                "evidence": [str(item) for item in evidence],
            }
        )
    return normalized


def _write_issue_ledger(run_root: Path, round_summary: dict[str, Any]) -> None:
    payload = {
        "round_index": int(round_summary["round_index"]),
        "issues": list(round_summary.get("issues", [])),
        "issue_count": int(round_summary.get("issue_count", 0)),
        "issue_score": int(round_summary.get("issue_score", 0)),
        "issue_severity_counts": dict(round_summary.get("issue_severity_counts", {})),
        "merged_verdict": round_summary.get("merged_verdict"),
    }
    _write_json(run_root / "meta" / "issue_ledger.json", payload)


def _collect_solver_memory_files(spec: dict[str, Any], solver_workspace: Path) -> list[str]:
    solver_memory_cfg = spec.get("solver_memory") or {}
    required_files = list(solver_memory_cfg.get("required_files", []))
    if not required_files:
        return []
    missing = [relative for relative in required_files if not (solver_workspace / relative).exists()]
    if missing:
        raise FileNotFoundError(f"solver missing required memory files: {', '.join(missing)}")
    return required_files


def _ensure_solver_memory_files(
    spec: dict[str, Any],
    solver_workspace: Path,
    *,
    round_index: int,
    reason: str,
) -> list[str]:
    solver_memory_cfg = spec.get("solver_memory") or {}
    required_files = list(solver_memory_cfg.get("required_files", []))
    if not required_files:
        return []

    project_root = solver_workspace / "project"
    for_eval_root = solver_workspace / "for_eval"
    existing_result = _try_read_json(solver_workspace / "solver_result.json") or {}
    evaluation = _try_read_json(for_eval_root / "evaluation.json") or {}
    selected = _try_read_json(for_eval_root / "selected.json") or {}
    created: list[str] = []
    changed_files = _relative_file_list(project_root, base=solver_workspace)
    artifact_files = _relative_file_list(for_eval_root, base=solver_workspace)

    fallback_by_name = {
        "MEMORY.md": "\n".join(
            [
                "# Solver Memory",
                "",
                "## FINAL_GOAL",
                f"- {spec.get('final_goal')}",
                "",
                "## CURRENT_FOCUS",
                f"- Runner backfilled missing solver memory after `{reason}`.",
                "- The next solver turn must replace this with a real turn memory if it continues.",
                "",
                "## LAST_RUN",
                f"- round: {round_index}",
                f"- selected_count: {len(selected.get('selected_ids', [])) if isinstance(selected.get('selected_ids'), list) else len(existing_result.get('selected_ids', [])) if isinstance(existing_result.get('selected_ids'), list) else 0}",
                f"- pass_tags: {', '.join(evaluation.get('pass_tags', [])) if isinstance(evaluation.get('pass_tags'), list) else '(unknown)'}",
                f"- fail_tags: {', '.join(evaluation.get('fail_tags', [])) if isinstance(evaluation.get('fail_tags'), list) else '(unknown)'}",
                "",
            ]
        )
        + "\n",
        "turn_log.md": "\n".join(
            [
                f"# Turn {round_index}",
                "",
                f"- Runner backfilled this file because: {reason}.",
                f"- Project files observed: {len(changed_files)}.",
                f"- Evaluation artifacts observed: {len(artifact_files)}.",
                "",
            ]
        )
        + "\n",
        "best_code.json": json.dumps(
            {
                "round": round_index,
                "autogenerated": True,
                "reason": reason,
                "changed_files": changed_files,
                "artifact_files": artifact_files,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        "verification.json": json.dumps(
            {
                "round": round_index,
                "autogenerated": True,
                "reason": reason,
                "commands": ["bash project/run_memory_search_task.sh"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
    }

    for relative in required_files:
        target = solver_workspace / str(relative)
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(fallback_by_name.get(target.name, ""), encoding="utf-8")
        created.append(str(relative))
    return created


def _with_surface_status(spec: dict[str, Any], status: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(status)
    surface_index, surface_level, _ = _get_active_surface(spec, normalized)
    normalized["active_surface_level"] = surface_index
    normalized["surface_name"] = str(surface_level.get("name") or f"surface_{surface_index}")
    normalized["surface_escalation_count"] = int(normalized.get("surface_escalation_count", 0))
    return normalized


def _ordered_evaluators(spec: dict[str, Any]) -> list[dict[str, Any]]:
    evaluators = list(spec["evaluators"])
    evaluation_flow = spec.get("evaluation_flow") or {}
    primary = evaluation_flow.get("primary")
    if not primary:
        return evaluators
    return sorted(evaluators, key=lambda evaluator: 0 if evaluator.get("name") == primary else 1)


def _should_continue_evaluation(spec: dict[str, Any], evaluator_results: list[dict[str, Any]]) -> bool:
    if not evaluator_results:
        return True
    evaluation_flow = spec.get("evaluation_flow") or {}
    mode = evaluation_flow.get("mode", "all")
    if mode == "all":
        return True
    if mode == "primary_then_confirm_terminate":
        return str(evaluator_results[0].get("verdict") or "") == "terminate"
    return True


def _normalize_evaluator_result(
    payload: dict[str, Any],
    *,
    logical_name: str,
    fallback_used: bool,
    fallback_name: str | None = None,
) -> dict[str, Any]:
    normalized = dict(payload)
    normalized["actual_evaluator"] = str(normalized.get("evaluator", logical_name))
    normalized["evaluator"] = logical_name
    normalized["fallback_used"] = fallback_used
    if fallback_name:
        normalized["fallback_name"] = fallback_name
    return normalized


def _run_evaluator_round(
    *,
    spec: dict[str, Any],
    evaluator: dict[str, Any],
    evaluator_workspace: Path,
    round_dir: Path,
    round_index: int,
    run_root: Path,
    require_verdict: bool,
) -> dict[str, Any]:
    command = _render_command(evaluator["command"], evaluator_workspace, round_index, run_root)
    _run_command(
        command,
        evaluator_workspace,
        round_dir / evaluator["name"] / "stdout.txt",
        round_dir / evaluator["name"] / "stderr.txt",
        completion_paths=[evaluator_workspace / "result.json"],
        max_runtime_seconds=(
            float(evaluator["max_runtime_seconds"])
            if evaluator.get("max_runtime_seconds") is not None
            else None
        ),
    )
    result_path = evaluator_workspace / "result.json"
    try:
        payload = _load_result_json(result_path, require_verdict=require_verdict)
        return _normalize_evaluator_result(payload, logical_name=str(evaluator["name"]), fallback_used=False)
    except Exception as primary_exc:  # noqa: BLE001
        fallback = evaluator.get("fallback") or {}
        if not fallback:
            raise
        fallback_name = str(fallback.get("name") or f"{evaluator['name']}_fallback")
        for candidate in (result_path, result_path.with_suffix(".md"), result_path.with_suffix(".txt")):
            if candidate.exists():
                _remove_path(candidate)
        _append_event(
            run_root,
            "evaluator.fallback_started",
            round_index=round_index,
            evaluator=evaluator["name"],
            fallback_name=fallback_name,
            reason=str(primary_exc),
        )
        fallback_command = _render_command(fallback["command"], evaluator_workspace, round_index, run_root)
        _run_command(
            fallback_command,
            evaluator_workspace,
            round_dir / evaluator["name"] / "fallback_stdout.txt",
            round_dir / evaluator["name"] / "fallback_stderr.txt",
            completion_paths=[evaluator_workspace / "result.json"],
            max_runtime_seconds=(
                float(fallback["max_runtime_seconds"])
                if fallback.get("max_runtime_seconds") is not None
                else None
            ),
        )
        payload = _load_result_json(result_path, require_verdict=require_verdict)
        _append_event(
            run_root,
            "evaluator.fallback_completed",
            round_index=round_index,
            evaluator=evaluator["name"],
            fallback_name=fallback_name,
        )
        return _normalize_evaluator_result(
            payload,
            logical_name=str(evaluator["name"]),
            fallback_used=True,
            fallback_name=fallback_name,
        )


def _render_evaluator_result_markdown(result: dict[str, Any]) -> str:
    lines = [
        f"# Critic Feedback: {result.get('evaluator')}",
        "",
        f"- Actual evaluator: {result.get('actual_evaluator', result.get('evaluator'))}",
        f"- Fallback used: {bool(result.get('fallback_used', False))}",
        f"- All pass: {bool(result.get('all_pass', False))}",
        f"- Verdict: {result.get('verdict')}",
        f"- Fail tags: {', '.join(result.get('fail_tags', [])) or '(none)'}",
        f"- Pass tags: {', '.join(result.get('pass_tags', [])) or '(none)'}",
        "",
        "## Issues",
    ]
    issues = result.get("issues", [])
    if issues:
        for issue in issues:
            lines.append(f"- [{issue.get('severity', 'low')}] {issue.get('summary')}")
            evidence = issue.get("evidence", [])
            if evidence:
                lines.append(f"  evidence: {' | '.join(str(item) for item in evidence)}")
    else:
        lines.append("- (none)")
    lines.append("")
    return "\n".join(lines)


def _load_evaluator_feedback_markdown(evaluator_workspace: Path, result: dict[str, Any]) -> str:
    result_md = evaluator_workspace / "result.md"
    if result_md.exists():
        return result_md.read_text(encoding="utf-8").strip() + "\n"
    return _render_evaluator_result_markdown(result)


def _synthesize_converged_feedback(evaluator_results: list[dict[str, Any]]) -> str:
    merged_issues = _merge_issues(evaluator_results)
    critic_count = len(evaluator_results)
    overlapping_issues = [
        issue for issue in merged_issues if len(issue.get("reported_by", [])) >= min(2, critic_count)
    ]
    merged = merge_evaluator_results(evaluator_results, require_verdict=False)
    lines = [
        "# Converged Feedback",
        "",
        "Runner fallback synthesis because the converger did not produce `converged_feedback.md`.",
        "Only critic-overlap signals are included below.",
        "",
        f"- Critics compared: {', '.join(str(result.get('evaluator')) for result in evaluator_results)}",
        f"- Agreed fail tags: {', '.join(merged.get('agreed_fail_tags', [])) or '(none)'}",
        f"- Agreed pass tags to preserve: {', '.join(merged.get('agreed_pass_tags', [])) or '(none)'}",
        "",
        "## Common Issues",
    ]
    if overlapping_issues:
        for issue in overlapping_issues:
            reporters = ", ".join(issue.get("reported_by", []))
            lines.append(f"- [{issue.get('severity')}] {issue.get('summary')} (reported by: {reporters})")
    else:
        lines.append("- no stable overlap beyond the agreed tag verdicts")
    lines.append("")
    return "\n".join(lines)


def _run_converger_round(
    *,
    spec: dict[str, Any],
    paths: dict[str, Any],
    solver_for_eval: Path,
    solver_result_path: Path,
    solver_workspace: Path,
    evaluator_results: list[dict[str, Any]],
    round_dir: Path,
    round_index: int,
    run_root: Path,
) -> dict[str, Any] | None:
    converger = spec.get("converger")
    if not converger or len(evaluator_results) < 2:
        return None

    converger_name = str(converger["name"])
    workspace = round_dir / converger_name / "workspace"
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    _copy_files(spec["references"]["solver_visible"], workspace / "references")
    _copy_files(spec["references"].get("evaluator_only", []), workspace / "references")
    _copy_last_approved_bundle(run_root, workspace)
    _write_round_context(spec, run_root, workspace, round_index, _load_status(run_root))
    _copy_path(solver_for_eval, workspace / "for_eval")
    shutil.copy2(solver_result_path, workspace / "solver_result.json")

    solver_project = solver_workspace / "project"
    if solver_project.exists():
        _copy_path(solver_project, workspace / "project")
    solver_memory_cfg = spec.get("solver_memory") or {}
    solver_memory_root = solver_workspace / str(solver_memory_cfg.get("root", "solver_memory"))
    if solver_memory_root.exists():
        _copy_path(solver_memory_root, workspace / solver_memory_root.name)

    feedback_dir = workspace / "critic_feedback"
    feedback_dir.mkdir(parents=True, exist_ok=True)
    evaluator_workspaces: dict[str, Path] = paths["evaluator_workspaces"]
    index_lines = [
        "# Critic Feedback Inputs",
        "",
        "Read both critic feedback files and produce `converged_feedback.md`.",
        "Include only duplicate/stable failure signals. Do not solve the task here.",
        "",
    ]
    for result in evaluator_results:
        evaluator_name = str(result.get("evaluator"))
        evaluator_workspace = Path(evaluator_workspaces[evaluator_name])
        feedback_text = _load_evaluator_feedback_markdown(evaluator_workspace, result)
        target = feedback_dir / f"{evaluator_name}.md"
        _write_text(
            target,
            "\n".join(
                [
                    f"# Critic Feedback: {evaluator_name}",
                    "",
                    f"- actual_evaluator: {result.get('actual_evaluator', evaluator_name)}",
                    f"- fallback_used: {bool(result.get('fallback_used', False))}",
                    "",
                    feedback_text.strip(),
                    "",
                ]
            ),
        )
        index_lines.append(f"- {target.name}")
    _write_text(feedback_dir / "index.md", "\n".join(index_lines) + "\n")

    command = _render_command(converger["command"], workspace, round_index, run_root)
    command_result = _run_command(
        command,
        workspace,
        round_dir / converger_name / "stdout.txt",
        round_dir / converger_name / "stderr.txt",
        completion_paths=[workspace / "converged_feedback.md"],
        max_runtime_seconds=(
            float(converger["max_runtime_seconds"])
            if converger.get("max_runtime_seconds") is not None
            else None
        ),
    )
    feedback_path = workspace / "converged_feedback.md"
    synthesized = False
    if feedback_path.exists():
        feedback_text = feedback_path.read_text(encoding="utf-8").strip() + "\n"
    else:
        feedback_text = _synthesize_converged_feedback(evaluator_results)
        _write_text(feedback_path, feedback_text)
        synthesized = True

    _write_text(round_dir / "converged_feedback.md", feedback_text)
    _write_text(run_root / "meta" / "converged_feedback.md", feedback_text)
    payload = {
        "round_index": round_index,
        "converger": converger_name,
        "critics": [str(result.get("evaluator")) for result in evaluator_results],
        "synthesized": synthesized,
        "returncode": command_result.get("returncode"),
        "path": str(feedback_path),
    }
    _write_json(run_root / "meta" / "converged_feedback.json", payload)
    _append_event(run_root, "converger.completed", **payload)
    return payload


def run_loop(spec_path: str | Path, run_root: str | Path, max_rounds: int) -> dict[str, Any]:
    spec = load_loop_spec(spec_path)
    init = initialize_run_root(spec, run_root)
    root = Path(init["run_root"])
    status = _with_surface_status(spec, _load_status(root))
    next_round = int(status.get("next_round", 1))
    uncapped = max_rounds <= 0
    feedback_cfg = spec.get("mechanical_feedback") or {}
    require_verdict = bool(feedback_cfg.get("enabled"))
    no_progress_reset_after = int(feedback_cfg.get("no_progress_reset_after", 0) or 0)
    surface_escalation_after = int(feedback_cfg.get("surface_escalation_after", 0) or 0)
    solver_memory_cfg = spec.get("solver_memory") or {}
    solver_cfg = spec.get("solver") or {}
    manager_cfg = spec.get("manager") or {}
    solver_stall_watchdog_raw = solver_cfg.get("stall_watchdog_seconds")
    manager_heartbeat_raw = manager_cfg.get("heartbeat_seconds")
    supervisor_heartbeat_seconds = (
        float(solver_stall_watchdog_raw)
        if solver_stall_watchdog_raw is not None
        else (float(manager_heartbeat_raw) if manager_heartbeat_raw is not None else None)
    )

    round_index = next_round
    while True:
        if not uncapped and round_index > max_rounds:
            break
        current_status = _with_surface_status(spec, _load_status(root))
        paths = materialize_round_workspaces(spec, root, round_index, current_status)
        round_dir = Path(paths["round_dir"])
        solver_workspace = Path(paths["solver_workspace"])
        _append_event(root, "round.started", round_index=round_index, status=current_status)
        solver_command = _render_command(spec["solver"]["command"], solver_workspace, round_index, root)
        solver_run = _run_command(
            solver_command,
            solver_workspace,
            round_dir / "solver" / "stdout.txt",
            round_dir / "solver" / "stderr.txt",
            completion_paths=[solver_workspace / "solver_result.json"],
            completion_probe=lambda: _has_usable_solver_artifacts(spec, solver_workspace),
            max_runtime_seconds=(
                float(solver_cfg["max_runtime_seconds"])
                if solver_cfg.get("max_runtime_seconds") is not None
                else None
            ),
            stall_watchdog_seconds=supervisor_heartbeat_seconds,
            progress_paths=[
                solver_workspace / "project",
                solver_workspace / "for_eval",
                solver_workspace / str(solver_memory_cfg.get("root", "solver_memory")),
                solver_workspace / "solver_result.json",
            ],
            progress_probe_interval_seconds=float(solver_cfg.get("progress_probe_interval_seconds", 1.0)),
        )
        if solver_run.get("timed_out"):
            _append_event(root, "solver.timed_out", round_index=round_index)
        if solver_run.get("stalled"):
            _append_event(
                root,
                "manager.supervisor_intervened",
                round_index=round_index,
                reason="manager_heartbeat_no_progress" if manager_heartbeat_raw is not None else "solver_stalled",
                stall_watchdog_seconds=supervisor_heartbeat_seconds,
                heartbeat_seconds=manager_heartbeat_raw,
            )

        solver_result_path = solver_workspace / "solver_result.json"
        if not solver_result_path.exists() or not _has_usable_solver_artifacts(spec, solver_workspace):
            backfill = _attempt_verification_backfill(spec, solver_workspace, round_dir)
            if backfill["attempted"] and backfill["completed"]:
                _append_event(
                    root,
                    "solver.verification_backfill_completed",
                    round_index=round_index,
                    command_count=backfill["command_count"],
                )
        if solver_run.get("stalled") and not _has_usable_solver_artifacts(spec, solver_workspace):
            synthesized = _synthesize_stalled_solver_artifacts(
                spec,
                solver_workspace,
                round_index=round_index,
                reason="solver_stalled_by_supervisor_watchdog",
            )
            _append_event(
                root,
                "solver.stalled_artifacts_synthesized",
                round_index=round_index,
                created=synthesized,
            )
        created_for_eval_metadata = _ensure_for_eval_metadata(spec, solver_workspace)
        if created_for_eval_metadata:
            _append_event(
                root,
                "solver.for_eval_metadata_backfilled",
                round_index=round_index,
                created=created_for_eval_metadata,
            )
        if not solver_result_path.exists():
            degraded_solver_result = _build_degraded_solver_result(spec, solver_workspace, round_index=round_index)
            if degraded_solver_result is None:
                raise FileNotFoundError(f"solver did not write {solver_result_path}")
            if solver_run.get("stalled"):
                degraded_solver_result["stalled"] = True
                degraded_solver_result["stall_reason"] = "solver_stalled_by_supervisor_watchdog"
            _write_json(solver_result_path, degraded_solver_result)
            solver_result = degraded_solver_result
            _append_event(
                root,
                "solver.degraded_completed",
                round_index=round_index,
                missing_artifacts=list(degraded_solver_result.get("missing_artifacts", [])),
                selected_ids=list(degraded_solver_result.get("selected_ids", [])),
            )
        else:
            solver_result = _read_json(solver_result_path)
            if solver_run.get("stalled"):
                solver_result["stalled"] = True
                solver_result["stall_reason"] = "solver_stalled_by_supervisor_watchdog"
                _write_json(solver_result_path, solver_result)
        _append_event(
            root,
            "solver.completed",
            round_index=round_index,
            solver_result={
                "round": solver_result.get("round"),
                "quality": solver_result.get("quality"),
                "selected_ids": solver_result.get("selected_ids"),
            },
        )
        created_solver_memory = _ensure_solver_memory_files(
            spec,
            solver_workspace,
            round_index=round_index,
            reason="solver_completed_without_required_memory_files",
        )
        if created_solver_memory:
            _append_event(
                root,
                "solver.memory_metadata_backfilled",
                round_index=round_index,
                created=created_solver_memory,
            )
        solver_memory_files = _collect_solver_memory_files(spec, solver_workspace)
        solver_for_eval = solver_workspace / "for_eval"
        if not solver_for_eval.exists():
            raise FileNotFoundError(f"solver did not write {solver_for_eval}")

        evaluator_results: list[dict[str, Any]] = []
        for evaluator_index, evaluator in enumerate(_ordered_evaluators(spec)):
            evaluator_workspace = Path(paths["evaluator_workspaces"][evaluator["name"]])
            _copy_evaluation_code_library_to_workspace(root, evaluator_workspace)
            shutil.copytree(solver_for_eval, evaluator_workspace / "for_eval")
            shutil.copy2(solver_result_path, evaluator_workspace / "solver_result.json")
            solver_project = solver_workspace / "project"
            if solver_project.exists():
                _copy_path(solver_project, evaluator_workspace / "project")
            solver_memory_root = solver_workspace / str(solver_memory_cfg.get("root", "solver_memory"))
            if solver_memory_root.exists():
                _copy_path(solver_memory_root, evaluator_workspace / solver_memory_root.name)
            evaluator_result = _run_evaluator_round(
                spec=spec,
                evaluator=evaluator,
                evaluator_workspace=evaluator_workspace,
                round_dir=round_dir,
                round_index=round_index,
                run_root=root,
                require_verdict=require_verdict,
            )
            evaluator_results.append(evaluator_result)
            promoted_evaluation_code = _collect_evaluation_code_candidates(
                run_root=root,
                round_dir=round_dir,
                evaluator_name=str(evaluator["name"]),
                evaluator_workspace=evaluator_workspace,
                round_index=round_index,
            )
            if promoted_evaluation_code:
                _append_event(
                    root,
                    "evaluation_code_library.promoted",
                    round_index=round_index,
                    evaluator=evaluator["name"],
                    promoted=promoted_evaluation_code,
                )
            _append_event(
                root,
                "evaluator.completed",
                round_index=round_index,
                evaluator=evaluator["name"],
                actual_evaluator=evaluator_result.get("actual_evaluator"),
                fallback_used=bool(evaluator_result.get("fallback_used")),
                verdict=evaluator_result.get("verdict"),
                all_pass=bool(evaluator_result.get("all_pass")),
                fail_tags=list(evaluator_result.get("fail_tags", [])),
                issue_count=len(evaluator_result.get("issues", [])),
            )
            if evaluator_index == 0 and not _should_continue_evaluation(spec, evaluator_results):
                _append_event(
                    root,
                    "evaluation.short_circuit",
                    round_index=round_index,
                    kept_evaluator=evaluator["name"],
                    reason="primary_nonterminal_verdict",
                )
                break

        converged_feedback = _run_converger_round(
            spec=spec,
            paths=paths,
            solver_for_eval=solver_for_eval,
            solver_result_path=solver_result_path,
            solver_workspace=solver_workspace,
            evaluator_results=evaluator_results,
            round_dir=round_dir,
            round_index=round_index,
            run_root=root,
        )
        merged = merge_evaluator_results(
            evaluator_results,
            require_verdict=require_verdict,
            evidence_gate=spec.get("evidence_gate"),
        )
        solver_bundle_dir = round_dir / "for_eval"
        shutil.copytree(solver_for_eval, solver_bundle_dir)
        next_no_progress = 0
        if require_verdict:
            previous_streak = int(current_status.get("consecutive_no_progress", 0))
            next_no_progress = previous_streak + 1 if merged.get("merged_verdict") == "no_progress" else 0
        round_summary = create_round_summary(
            round_index,
            merged,
            solver_result,
            solver_memory_files=solver_memory_files,
            feedback_state={
                "consecutive_no_progress": next_no_progress,
                "reset_count": int(current_status.get("reset_count", 0)),
                "converged_feedback_path": converged_feedback.get("path") if converged_feedback else None,
            },
        )
        _write_json(round_dir / "summary.json", round_summary)
        _append_event(
            root,
            "round.summary_written",
            round_index=round_index,
            merged_verdict=round_summary.get("merged_verdict"),
            all_pass=bool(round_summary.get("all_pass")),
            agreed_fail_count=int(round_summary.get("agreed_fail_count", 0)),
            issue_count=int(round_summary.get("issue_count", 0)),
        )
        if (spec.get("issue_ledger") or {}).get("enabled"):
            _write_issue_ledger(root, round_summary)
        checkpoint_updated = maybe_update_best_checkpoint(
            root,
            round_summary,
            solver_bundle_dir,
            solver_workspace / "project",
            solver_workspace / str(solver_memory_cfg["root"]) if solver_memory_cfg.get("root") else None,
        )
        if checkpoint_updated:
            _append_event(root, "checkpoint.updated", round_index=round_index)
        _write_json(root / "meta" / "scoreboard.json", round_summary)

        success = merged["all_pass"] and not merged["agreed_fail_tags"]
        if require_verdict:
            success = success and merged.get("merged_verdict") == "terminate"

        if success:
            final_status = _with_surface_status(
                spec,
                {
                "next_round": round_index + 1,
                "status": "success",
                "last_feedback_verdict": merged.get("merged_verdict"),
                "consecutive_no_progress": 0,
                "reset_count": int(current_status.get("reset_count", 0)),
                "solver_reset_pending": False,
                "active_surface_level": int(current_status.get("active_surface_level", 0)),
                "surface_escalation_count": int(current_status.get("surface_escalation_count", 0)),
                },
            )
            _save_status(root, final_status)
            _write_stagnation_summary(root, status=final_status)
            _append_event(root, "run.completed", round_index=round_index, status=final_status)
            return final_status

        next_status = _with_surface_status(
            spec,
            {
            "next_round": round_index + 1,
            "status": "running",
            "last_feedback_verdict": merged.get("merged_verdict"),
            "consecutive_no_progress": next_no_progress,
            "reset_count": int(current_status.get("reset_count", 0)),
            "solver_reset_pending": False,
            "active_surface_level": int(current_status.get("active_surface_level", 0)),
            "surface_escalation_count": int(current_status.get("surface_escalation_count", 0)),
            },
        )
        surface_levels = _get_surface_levels(spec)
        if (
            require_verdict
            and surface_escalation_after
            and next_no_progress >= surface_escalation_after
            and int(current_status.get("active_surface_level", 0)) < len(surface_levels) - 1
        ):
            next_status["active_surface_level"] = int(current_status.get("active_surface_level", 0)) + 1
            next_status["surface_escalation_count"] = int(current_status.get("surface_escalation_count", 0)) + 1
            next_status["consecutive_no_progress"] = 0
            next_status = _with_surface_status(spec, next_status)
            _append_event(
                root,
                "surface.escalated",
                round_index=round_index,
                status=next_status,
            )
        if require_verdict and no_progress_reset_after and next_no_progress >= no_progress_reset_after:
            next_status["solver_reset_pending"] = True
            next_status["consecutive_no_progress"] = 0
            next_status["reset_count"] = int(current_status.get("reset_count", 0)) + 1
            _append_event(root, "solver.reset_scheduled", round_index=round_index, status=next_status)
        _save_status(root, next_status)
        _write_stagnation_summary(root, status=next_status)
        round_index += 1

    final_status = _with_surface_status(spec, dict(_load_status(root)))
    final_status["next_round"] = max_rounds + 1
    final_status["status"] = "max_rounds_reached"
    _save_status(root, final_status)
    _write_stagnation_summary(root, status=final_status)
    _append_event(root, "run.completed", status=final_status)
    return final_status


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec-path", required=True)
    parser.add_argument("--run-root", required=True)
    parser.add_argument("--max-rounds", type=int, required=True, help="round cap; use 0 or negative for no cap")
    args = parser.parse_args()

    result = run_loop(args.spec_path, args.run_root, args.max_rounds)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
