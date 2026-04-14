#!/usr/bin/env python3
from __future__ import annotations

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scripts import recall_autoloop as loop


def main() -> int:
    if len(sys.argv) != 4:
        raise SystemExit("usage: resume_round_after_solver.py SPEC_PATH RUN_ROOT ROUND_INDEX")

    spec = loop.load_loop_spec(sys.argv[1])
    root = Path(sys.argv[2]).resolve()
    round_index = int(sys.argv[3])
    current_status = loop._with_surface_status(spec, loop._load_status(root))
    feedback_cfg = spec.get("mechanical_feedback") or {}
    require_verdict = bool(feedback_cfg.get("enabled"))
    no_progress_reset_after = int(feedback_cfg.get("no_progress_reset_after", 0) or 0)
    surface_escalation_after = int(feedback_cfg.get("surface_escalation_after", 0) or 0)
    solver_memory_cfg = spec.get("solver_memory") or {}

    round_dir = root / "rounds" / f"round_{round_index:04d}"
    solver_workspace = round_dir / "solver" / "workspace"
    solver_result_path = solver_workspace / "solver_result.json"
    solver_for_eval = solver_workspace / "for_eval"
    if not solver_result_path.exists():
        raise FileNotFoundError(solver_result_path)
    if not solver_for_eval.exists():
        raise FileNotFoundError(solver_for_eval)
    solver_result = loop._read_json(solver_result_path)

    created_memory = loop._ensure_solver_memory_files(
        spec,
        solver_workspace,
        round_index=round_index,
        reason="resume_after_runner_crash_missing_solver_memory",
    )
    if created_memory:
        loop._append_event(
            root,
            "solver.memory_metadata_backfilled",
            round_index=round_index,
            created=created_memory,
        )
    solver_memory_files = loop._collect_solver_memory_files(spec, solver_workspace)

    evaluator_workspaces: dict[str, Path] = {}
    for evaluator in spec["evaluators"]:
        workspace = round_dir / evaluator["name"] / "workspace"
        workspace.mkdir(parents=True, exist_ok=True)
        if not (workspace / "references").exists():
            loop._copy_files(spec["references"]["solver_visible"], workspace / "references")
            loop._copy_files(spec["references"].get("evaluator_only", []), workspace / "references")
        loop._write_round_context(spec, root, workspace, round_index, current_status)
        evaluator_workspaces[evaluator["name"]] = workspace

    paths = {"evaluator_workspaces": evaluator_workspaces}
    evaluator_results = []
    for evaluator_index, evaluator in enumerate(loop._ordered_evaluators(spec)):
        evaluator_workspace = evaluator_workspaces[evaluator["name"]]
        loop._copy_evaluation_code_library_to_workspace(root, evaluator_workspace)
        for relative in ("for_eval", "project", "solver_memory"):
            target = evaluator_workspace / relative
            if target.exists():
                loop._remove_path(target)
        loop._copy_path(solver_for_eval, evaluator_workspace / "for_eval")
        shutil.copy2(solver_result_path, evaluator_workspace / "solver_result.json")
        solver_project = solver_workspace / "project"
        if solver_project.exists():
            loop._copy_path(solver_project, evaluator_workspace / "project")
        solver_memory_root = solver_workspace / str(solver_memory_cfg.get("root", "solver_memory"))
        if solver_memory_root.exists():
            loop._copy_path(solver_memory_root, evaluator_workspace / solver_memory_root.name)

        evaluator_result = loop._run_evaluator_round(
            spec=spec,
            evaluator=evaluator,
            evaluator_workspace=evaluator_workspace,
            round_dir=round_dir,
            round_index=round_index,
            run_root=root,
            require_verdict=require_verdict,
        )
        evaluator_results.append(evaluator_result)
        promoted = loop._collect_evaluation_code_candidates(
            run_root=root,
            round_dir=round_dir,
            evaluator_name=str(evaluator["name"]),
            evaluator_workspace=evaluator_workspace,
            round_index=round_index,
        )
        if promoted:
            loop._append_event(
                root,
                "evaluation_code_library.promoted",
                round_index=round_index,
                evaluator=evaluator["name"],
                promoted=promoted,
            )
        loop._append_event(
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
        if evaluator_index == 0 and not loop._should_continue_evaluation(spec, evaluator_results):
            loop._append_event(
                root,
                "evaluation.short_circuit",
                round_index=round_index,
                kept_evaluator=evaluator["name"],
                reason="primary_nonterminal_verdict",
            )
            break

    converged_feedback = loop._run_converger_round(
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
    merged = loop.merge_evaluator_results(evaluator_results, require_verdict=require_verdict)
    solver_bundle_dir = round_dir / "for_eval"
    if solver_bundle_dir.exists():
        loop._remove_path(solver_bundle_dir)
    shutil.copytree(solver_for_eval, solver_bundle_dir)

    previous_streak = int(current_status.get("consecutive_no_progress", 0))
    next_no_progress = previous_streak + 1 if require_verdict and merged.get("merged_verdict") == "no_progress" else 0
    round_summary = loop.create_round_summary(
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
    loop._write_json(round_dir / "summary.json", round_summary)
    loop._append_event(
        root,
        "round.summary_written",
        round_index=round_index,
        merged_verdict=round_summary.get("merged_verdict"),
        all_pass=bool(round_summary.get("all_pass")),
        agreed_fail_count=int(round_summary.get("agreed_fail_count", 0)),
        issue_count=int(round_summary.get("issue_count", 0)),
    )
    if (spec.get("issue_ledger") or {}).get("enabled"):
        loop._write_issue_ledger(root, round_summary)
    if loop.maybe_update_best_checkpoint(
        root,
        round_summary,
        solver_bundle_dir,
        solver_workspace / "project",
        solver_workspace / str(solver_memory_cfg["root"]) if solver_memory_cfg.get("root") else None,
    ):
        loop._append_event(root, "checkpoint.updated", round_index=round_index)
    loop._write_json(root / "meta" / "scoreboard.json", round_summary)

    success = merged["all_pass"] and not merged["agreed_fail_tags"]
    if require_verdict:
        success = success and merged.get("merged_verdict") == "terminate"
    if success:
        final_status = loop._with_surface_status(
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
        loop._save_status(root, final_status)
        loop._write_stagnation_summary(root, status=final_status)
        loop._append_event(root, "run.completed", round_index=round_index, status=final_status)
        return 0

    next_status = loop._with_surface_status(
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
    surface_levels = loop._get_surface_levels(spec)
    if (
        require_verdict
        and surface_escalation_after
        and next_no_progress >= surface_escalation_after
        and int(current_status.get("active_surface_level", 0)) < len(surface_levels) - 1
    ):
        next_status["active_surface_level"] = int(current_status.get("active_surface_level", 0)) + 1
        next_status["surface_escalation_count"] = int(current_status.get("surface_escalation_count", 0)) + 1
        next_status["consecutive_no_progress"] = 0
        next_status = loop._with_surface_status(spec, next_status)
        loop._append_event(root, "surface.escalated", round_index=round_index, status=next_status)
    if require_verdict and no_progress_reset_after and next_no_progress >= no_progress_reset_after:
        next_status["solver_reset_pending"] = True
        next_status["consecutive_no_progress"] = 0
        next_status["reset_count"] = int(current_status.get("reset_count", 0)) + 1
        loop._append_event(root, "solver.reset_scheduled", round_index=round_index, status=next_status)
    loop._save_status(root, next_status)
    loop._write_stagnation_summary(root, status=next_status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
