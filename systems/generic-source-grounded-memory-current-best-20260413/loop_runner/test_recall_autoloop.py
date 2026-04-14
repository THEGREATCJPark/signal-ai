from __future__ import annotations

import importlib.util
import json
import time
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().with_name("recall_autoloop.py")
    spec = importlib.util.spec_from_file_location("recall_autoloop", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def _write_spec(
    tmp_path: Path,
    *,
    evaluator_mode: str = "quality",
    evaluator_modes: dict[str, str] | None = None,
    evaluator_fallback_modes: dict[str, str] | None = None,
    mechanical_feedback: bool = False,
    no_progress_reset_after: int = 10,
    include_solver_memory: bool = False,
    issue_ledger: bool = False,
    carry_forward_mode: str = "best_checkpoint",
    solver_script_name: str = "fake_solver.py",
    evaluation_flow_mode: str = "all",
    surface_levels: list[dict[str, object]] | None = None,
    surface_escalation_after: int | None = None,
    solver_timeout_seconds: float | None = None,
    solver_stall_watchdog_seconds: float | None = None,
    progress_probe_interval_seconds: float | None = None,
    manager_heartbeat_seconds: float | None = None,
    include_converger: bool = False,
    evidence_gate: bool = False,
) -> Path:
    fixtures = Path(__file__).resolve().parent / "fixtures" / "recall_autoloop"
    visible = tmp_path / "visible"
    visible.mkdir()
    (visible / "goal.md").write_text("solver visible goal\n", encoding="utf-8")

    hidden = tmp_path / "hidden"
    hidden.mkdir()
    (hidden / "holdout.md").write_text("evaluator only holdout\n", encoding="utf-8")

    editable = tmp_path / "editable"
    editable.mkdir()
    (editable / "state.txt").write_text("seed-0\n", encoding="utf-8")
    (editable / "keywords.txt").write_text("alpha\nbeta\ngamma\n", encoding="utf-8")

    spec_path = tmp_path / "spec.json"
    evaluator_modes = evaluator_modes or {}
    evaluator_fallback_modes = evaluator_fallback_modes or {}

    def _build_evaluator(name: str) -> dict[str, object]:
        mode = evaluator_modes.get(name, evaluator_mode)
        entry: dict[str, object] = {
            "name": name,
            "command": [
                "python3",
                str(fixtures / "fake_eval.py"),
                "--workspace",
                "{workspace}",
                "--round",
                "{round}",
                "--label",
                name,
                "--mode",
                mode,
            ],
        }
        fallback_mode = evaluator_fallback_modes.get(name)
        if fallback_mode:
            fallback_name = "codex_eval_fallback" if name == "claude_eval" else f"{name}_fallback"
            entry["fallback"] = {
                "name": fallback_name,
                "command": [
                    "python3",
                    str(fixtures / "fake_eval.py"),
                    "--workspace",
                    "{workspace}",
                    "--round",
                    "{round}",
                    "--label",
                    fallback_name,
                    "--mode",
                    fallback_mode,
                ],
            }
        return entry

    spec_path.write_text(
        json.dumps(
            {
                "final_goal": "build a generic autonomous loop",
                "solver": {
                    **(
                        {"max_runtime_seconds": solver_timeout_seconds}
                        if solver_timeout_seconds is not None
                        else {}
                    ),
                    **(
                        {"stall_watchdog_seconds": solver_stall_watchdog_seconds}
                        if solver_stall_watchdog_seconds is not None
                        else {}
                    ),
                    **(
                        {"progress_probe_interval_seconds": progress_probe_interval_seconds}
                        if progress_probe_interval_seconds is not None
                        else {}
                    ),
                    "command": [
                        "python3",
                        str(fixtures / solver_script_name),
                        "--workspace",
                        "{workspace}",
                        "--round",
                        "{round}",
                    ],
                },
                **(
                    {"manager": {"heartbeat_seconds": manager_heartbeat_seconds}}
                    if manager_heartbeat_seconds is not None
                    else {}
                ),
                "evaluators": [_build_evaluator("codex_eval"), _build_evaluator("claude_eval")],
                **(
                    {
                        "converger": {
                            "name": "feedback_converger",
                            "command": [
                                "python3",
                                str(fixtures / "fake_converger.py"),
                                "--workspace",
                                "{workspace}",
                                "--round",
                                "{round}",
                            ],
                        }
                    }
                    if include_converger
                    else {}
                ),
                "references": {
                    "solver_visible": [str(visible / "goal.md")],
                    "evaluator_only": [str(hidden / "holdout.md")],
                },
                "workspace_seed": (
                    {
                        "surface_levels": surface_levels,
                        "carry_forward_mode": carry_forward_mode,
                    }
                    if surface_levels is not None
                    else {
                        "solver_editable": [
                            {
                                "source": str(editable / "state.txt"),
                                "target": "project/state.txt",
                            }
                        ],
                        "carry_forward_mode": carry_forward_mode,
                    }
                ),
                "success": {"require_all_evaluators_pass": True},
                "evaluation_flow": {
                    "mode": evaluation_flow_mode,
                    "primary": "codex_eval",
                },
                **(
                    {
                        "mechanical_feedback": {
                            "enabled": True,
                            "no_progress_reset_after": no_progress_reset_after,
                            "reset_policy": "keep_best_checkpoint_code_clear_solver_memory",
                            **(
                                {"surface_escalation_after": surface_escalation_after}
                                if surface_escalation_after is not None
                                else {}
                            ),
                        }
                    }
                    if mechanical_feedback
                    else {}
                ),
                **(
                    {
                        "solver_memory": {
                            "root": "solver_memory",
                            "carry_forward": True,
                            "required_files": [
                                "solver_memory/turn_log.md",
                                "solver_memory/best_code.json",
                                "solver_memory/verification.json",
                            ],
                        }
                    }
                    if include_solver_memory
                    else {}
                ),
                **({"issue_ledger": {"enabled": True}} if issue_ledger else {}),
                **(
                    {
                        "evidence_gate": {
                            "enabled": True,
                            "require_nonempty_evidence_for_pass": True,
                        }
                    }
                    if evidence_gate
                    else {}
                ),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return spec_path


def test_load_loop_spec_rejects_missing_required_sections(tmp_path: Path) -> None:
    autoloop = _load_module()
    bad_spec = tmp_path / "bad_spec.json"
    bad_spec.write_text(json.dumps({"final_goal": "x"}, ensure_ascii=False), encoding="utf-8")

    try:
        autoloop.load_loop_spec(bad_spec)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected ValueError")

    assert "solver" in message
    assert "evaluators" in message
    assert "references" in message


def test_materialize_round_workspaces_keep_evaluator_only_refs_out_of_solver(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=1)

    assert (paths["solver_workspace"] / "references" / "goal.md").exists()
    assert not (paths["solver_workspace"] / "references" / "holdout.md").exists()
    assert (paths["evaluator_workspaces"]["codex_eval"] / "references" / "goal.md").exists()
    assert (paths["evaluator_workspaces"]["codex_eval"] / "references" / "holdout.md").exists()


def test_materialize_round_workspaces_seed_editable_files_and_round_context(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=1)

    seeded = paths["solver_workspace"] / "project" / "state.txt"
    round_context = paths["solver_workspace"] / "loop_input" / "round_context.json"
    assert seeded.read_text(encoding="utf-8") == "seed-0\n"
    context_payload = json.loads(round_context.read_text(encoding="utf-8"))
    assert context_payload["round_index"] == 1
    assert context_payload["prior_agreed_fail_tags"] == []


def test_materialize_round_workspaces_writes_natural_language_briefs(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, mechanical_feedback=True, include_solver_memory=True, issue_ledger=True)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    autoloop._write_json(
        run_root / "meta" / "issue_ledger.json",
        {
            "round_index": 1,
            "issue_count": 1,
            "issue_score": 100,
            "issues": [
                {
                    "issue_key": "missing-smoke",
                    "summary": "missing smoking gun",
                    "severity": "high",
                    "evidence": ["missing=dossier"],
                }
            ],
        },
    )
    autoloop._write_json(
        run_root / "meta" / "status.json",
        {
            "next_round": 2,
            "status": "running",
            "last_feedback_verdict": "no_progress",
            "consecutive_no_progress": 1,
            "reset_count": 0,
            "solver_reset_pending": False,
        },
    )

    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=2)

    round_context_md = (paths["solver_workspace"] / "loop_input" / "round_context.md").read_text(encoding="utf-8")
    issue_ledger_md = (paths["solver_workspace"] / "loop_input" / "issue_ledger.md").read_text(encoding="utf-8")
    manager_directive_md = (paths["solver_workspace"] / "loop_input" / "manager_directive.md").read_text(encoding="utf-8")

    assert "Final goal" in round_context_md
    assert "Prior agreed fail tags" in round_context_md
    assert "missing smoking gun" in issue_ledger_md
    assert "Manager mode" in manager_directive_md
    assert "Instructions" in manager_directive_md


def test_materialize_round_workspaces_writes_single_handoff_markdown(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, mechanical_feedback=True, include_solver_memory=True, issue_ledger=True)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    autoloop._write_json(
        run_root / "meta" / "status.json",
        {
            "next_round": 2,
            "status": "running",
            "last_feedback_verdict": "no_progress",
            "consecutive_no_progress": 1,
            "reset_count": 0,
            "solver_reset_pending": False,
            "active_surface_level": 0,
        },
    )
    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=2)
    handoff = (paths["solver_workspace"] / "loop_input" / "handoff.md").read_text(encoding="utf-8")

    assert "Round Context" in handoff
    assert "Issue Ledger" in handoff
    assert "Manager Directive" in handoff
    assert "Current surface" in handoff


def test_merge_evaluator_results_only_keeps_agreed_fails() -> None:
    autoloop = _load_module()

    merged = autoloop.merge_evaluator_results(
        [
            {
                "evaluator": "codex_eval",
                "all_pass": False,
                "fail_tags": ["AC-1", "AC-3"],
                "pass_tags": ["AC-2"],
                "evidence": [],
            },
            {
                "evaluator": "claude_eval",
                "all_pass": False,
                "fail_tags": ["AC-1", "AC-4"],
                "pass_tags": ["AC-2", "AC-3"],
                "evidence": [],
            },
        ]
    )

    assert merged["agreed_fail_tags"] == ["AC-1"]
    assert merged["agreed_pass_tags"] == ["AC-2"]
    assert merged["all_pass"] is False


def test_merge_evaluator_results_mechanically_merges_verdicts() -> None:
    autoloop = _load_module()

    merged = autoloop.merge_evaluator_results(
        [
            {
                "evaluator": "codex_eval",
                "all_pass": False,
                "fail_tags": ["AC-1"],
                "pass_tags": ["AC-2"],
                "verdict": "progress",
                "evidence": [],
            },
            {
                "evaluator": "claude_eval",
                "all_pass": False,
                "fail_tags": ["AC-1"],
                "pass_tags": ["AC-2"],
                "verdict": "no_progress",
                "evidence": [],
            },
        ],
        require_verdict=True,
    )

    assert merged["merged_verdict"] == "no_progress"
    assert merged["verdicts"] == {"codex_eval": "progress", "claude_eval": "no_progress"}


def test_merge_evaluator_results_evidence_gate_blocks_empty_evidence_terminal_pass() -> None:
    autoloop = _load_module()

    merged = autoloop.merge_evaluator_results(
        [
            {
                "evaluator": "codex_eval",
                "all_pass": True,
                "fail_tags": [],
                "pass_tags": ["QUERY_ANSWERED"],
                "verdict": "terminate",
                "evidence": [],
            },
            {
                "evaluator": "claude_eval",
                "all_pass": True,
                "fail_tags": [],
                "pass_tags": ["QUERY_ANSWERED"],
                "verdict": "terminate",
                "evidence": {},
            },
        ],
        require_verdict=True,
        evidence_gate={"enabled": True},
    )

    assert merged["all_pass"] is False
    assert merged["merged_verdict"] == "no_progress"
    assert merged["agreed_fail_tags"] == ["EVIDENCE_GATE"]
    assert merged["issue_count"] == 1
    assert merged["issues"][0]["issue_id"] == "evidence-gate-empty-evidence"


def test_merge_evaluator_results_merges_issue_ledger_entries() -> None:
    autoloop = _load_module()

    merged = autoloop.merge_evaluator_results(
        [
            {
                "evaluator": "codex_eval",
                "all_pass": False,
                "fail_tags": ["AC-1"],
                "pass_tags": ["AC-2"],
                "evidence": [],
                "issues": [
                    {
                        "issue_id": "missing-recall",
                        "summary": "top60 recall is still too low",
                        "severity": "high",
                        "evidence": ["top60=16/20"],
                    },
                    {
                        "summary": "false positives still include generic military casebooks",
                        "severity": "medium",
                        "evidence": ["fp=군인_징계_정확도순_최근_5년"],
                    },
                ],
            },
            {
                "evaluator": "claude_eval",
                "all_pass": False,
                "fail_tags": ["AC-1"],
                "pass_tags": ["AC-2"],
                "evidence": [],
                "issues": [
                    {
                        "issue_id": "missing-recall",
                        "summary": "top60 recall is still too low",
                        "severity": "medium",
                        "evidence": ["missed=양정.pdf"],
                    },
                    {
                        "summary": "false positives still include generic military casebooks",
                        "severity": "low",
                        "evidence": ["fp=군인_징계_정확도순_최근_5년"],
                    },
                ],
            },
        ]
    )

    assert merged["issue_count"] == 2
    issues = {issue["issue_key"]: issue for issue in merged["issues"]}
    assert issues["missing-recall"]["severity"] == "high"
    assert sorted(issues["missing-recall"]["reported_by"]) == ["claude_eval", "codex_eval"]
    assert "top60=16/20" in issues["missing-recall"]["evidence"]
    assert "missed=양정.pdf" in issues["missing-recall"]["evidence"]


def test_load_result_json_normalizes_string_issue_evidence() -> None:
    autoloop = _load_module()
    result_path = Path(__file__).resolve().parent / "tmp.result.json"
    result_path.write_text(
        json.dumps(
            {
                "all_pass": False,
                "fail_tags": [],
                "pass_tags": ["GENERALITY"],
                "evidence": {},
                "verdict": "progress",
                "issues": [
                    {
                        "summary": "pool size generality not specified",
                        "severity": "medium",
                        "evidence": "top60 appears benchmark-tuned",
                        "focus": "generality",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    try:
        payload = autoloop._load_result_json(result_path, require_verdict=True)
    finally:
        result_path.unlink(missing_ok=True)

    assert payload["issues"][0]["evidence"] == ["top60 appears benchmark-tuned"]


def test_load_result_json_normalizes_live_issue_severity_synonyms() -> None:
    autoloop = _load_module()
    result_path = Path(__file__).resolve().parent / "tmp.severity.result.json"
    result_path.write_text(
        json.dumps(
            {
                "all_pass": False,
                "fail_tags": ["BENCHMARK_PASS"],
                "pass_tags": ["REPLAY"],
                "evidence": {},
                "verdict": "no_progress",
                "issues": [
                    {
                        "summary": "required files still missing",
                        "severity": "critical",
                        "evidence": "missing_required has 4 files",
                    },
                    {
                        "summary": "first round has no prior baseline",
                        "severity": "informational",
                        "evidence": "last_approved absent",
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    try:
        payload = autoloop._load_result_json(result_path, require_verdict=True)
    finally:
        result_path.unlink(missing_ok=True)

    assert payload["issues"][0]["severity"] == "high"
    assert payload["issues"][1]["severity"] == "low"


def test_load_result_json_normalizes_live_verdict_aliases() -> None:
    autoloop = _load_module()
    result_path = Path(__file__).resolve().parent / "tmp.verdict.result.json"
    result_path.write_text(
        json.dumps(
            {
                "all_pass": False,
                "fail_tags": ["BENCHMARK_PASS"],
                "pass_tags": ["REPLAY"],
                "evidence": {},
                "verdict": "not_ready_for_termination",
                "issues": [
                    {
                        "summary": "benchmark all_pass requirement not met",
                        "severity": "high",
                        "evidence": "evaluation.json shows all_pass=false",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    try:
        payload = autoloop._load_result_json(result_path, require_verdict=True)
    finally:
        result_path.unlink(missing_ok=True)

    assert payload["verdict"] == "no_progress"


def test_load_result_json_normalizes_object_issue_evidence() -> None:
    autoloop = _load_module()
    result_path = Path(__file__).resolve().parent / "tmp.object-evidence.result.json"
    result_path.write_text(
        json.dumps(
            {
                "all_pass": False,
                "fail_tags": ["BENCHMARK_PASS"],
                "pass_tags": ["REPLAY"],
                "evidence": {},
                "verdict": "no_progress",
                "issues": [
                    {
                        "summary": "benchmark all_pass requirement not met",
                        "severity": "high",
                        "evidence": {
                            "missing_required": [
                                "dossier_0001_smoking_gun",
                                "dossier_0001_appeal_lower",
                            ]
                        },
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    try:
        payload = autoloop._load_result_json(result_path, require_verdict=True)
    finally:
        result_path.unlink(missing_ok=True)

    assert payload["issues"][0]["evidence"] == [
        json.dumps({"missing_required": ["dossier_0001_smoking_gun", "dossier_0001_appeal_lower"]}, ensure_ascii=False)
    ]


def test_load_result_json_falls_back_to_embedded_json_in_markdown_when_json_missing() -> None:
    autoloop = _load_module()
    result_path = Path(__file__).resolve().parent / "tmp.markdown-fallback.result.json"
    markdown_path = result_path.with_suffix(".md")
    markdown_path.write_text(
        """# Critic Result

Natural language first.

```json
{
  "all_pass": false,
  "fail_tags": ["BENCHMARK_PASS"],
  "pass_tags": ["REPLAY"],
  "evidence": ["fallback=markdown"],
  "verdict": "no_progress",
  "issues": [
    {
      "summary": "markdown fallback worked",
      "severity": "medium",
      "evidence": "result.json missing"
    }
  ]
}
```
""",
        encoding="utf-8",
    )

    try:
        payload = autoloop._load_result_json(result_path, require_verdict=True)
    finally:
        result_path.unlink(missing_ok=True)
        markdown_path.unlink(missing_ok=True)

    assert payload["verdict"] == "no_progress"
    assert payload["evidence"] == ["fallback=markdown"]
    assert payload["issues"][0]["summary"] == "markdown fallback worked"


def test_materialize_round_workspaces_include_prior_disagreement_hints(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    scoreboard = {
        "round_index": 1,
        "all_pass": False,
        "agreed_fail_tags": [],
        "agreed_pass_tags": ["AC-2"],
        "disagreements": {
            "codex_eval": {"fail_tags": ["AC-3"], "pass_tags": []},
            "claude_eval": {"fail_tags": [], "pass_tags": ["AC-3"]},
        },
        "solver_result": {"round": 1},
    }
    (run_root / "meta" / "scoreboard.json").write_text(json.dumps(scoreboard, ensure_ascii=False, indent=2), encoding="utf-8")

    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=2)
    context_payload = json.loads((paths["solver_workspace"] / "loop_input" / "round_context.json").read_text(encoding="utf-8"))

    assert context_payload["disagreement_hints"]["codex_eval"]["fail_tags"] == ["AC-3"]
    assert context_payload["disagreement_hints"]["claude_eval"]["pass_tags"] == ["AC-3"]


def test_checkpoint_updates_only_when_fail_count_improves(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    round1 = autoloop.create_round_summary(
        round_index=1,
        merged={"agreed_fail_tags": ["AC-1", "AC-2"], "agreed_pass_tags": [], "all_pass": False},
        solver_result={"round": 1},
    )
    round2 = autoloop.create_round_summary(
        round_index=2,
        merged={"agreed_fail_tags": ["AC-1"], "agreed_pass_tags": ["AC-2"], "all_pass": False},
        solver_result={"round": 2},
    )

    changed1 = autoloop.maybe_update_best_checkpoint(run_root, round1, source_dir=tmp_path)
    changed2 = autoloop.maybe_update_best_checkpoint(run_root, round2, source_dir=tmp_path)

    assert changed1 is True
    assert changed2 is True

    best = json.loads((run_root / "meta" / "best_checkpoint.json").read_text(encoding="utf-8"))
    assert best["round_index"] == 2
    assert best["agreed_fail_count"] == 1


def test_checkpoint_prefers_tighter_candidate_over_disagreement_artifact(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    older = autoloop.create_round_summary(
        round_index=21,
        merged={
            "agreed_fail_tags": [],
            "agreed_pass_tags": ["BENCHMARK_PASS", "GENERIC_POLICY", "TIGHT_SELECTION"],
            "all_pass": False,
            "disagreements": {
                "codex_eval": {"fail_tags": [], "pass_tags": ["REPLAY"], "verdict": "terminate"},
                "claude_eval": {"fail_tags": ["REPLAY"], "pass_tags": [], "verdict": "progress"},
            },
            "merged_verdict": "progress",
            "issue_score": 10,
        },
        solver_result={
            "round": 21,
            "selected_ids": [f"candidate_{idx}" for idx in range(12)],
            "policy": {"top_k": 15},
        },
    )
    newer = autoloop.create_round_summary(
        round_index=39,
        merged={
            "agreed_fail_tags": ["REPLAY"],
            "agreed_pass_tags": ["BENCHMARK_PASS", "GENERIC_POLICY", "TIGHT_SELECTION"],
            "all_pass": False,
            "disagreements": {},
            "merged_verdict": "progress",
            "issue_score": 210,
        },
        solver_result={
            "round": 39,
            "selected_ids": [f"candidate_{idx}" for idx in range(5)],
            "policy": {"top_k": 8},
        },
    )

    changed1 = autoloop.maybe_update_best_checkpoint(run_root, older, source_dir=tmp_path)
    changed2 = autoloop.maybe_update_best_checkpoint(run_root, newer, source_dir=tmp_path)

    assert changed1 is True
    assert changed2 is True

    best = json.loads((run_root / "meta" / "best_checkpoint.json").read_text(encoding="utf-8"))
    assert best["round_index"] == 39


def test_checkpoint_prefers_terminal_success_over_progress_candidate_tightness(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    progress = autoloop.create_round_summary(
        round_index=2,
        merged={
            "agreed_fail_tags": [],
            "agreed_pass_tags": [
                "INGEST_REAL_TRANSCRIPT",
                "START_PRESERVED",
                "WHOLE_SESSION_COVERAGE",
                "EPISODIC_ORDER",
                "XREF_RESOLVED",
                "LAYERED_MEMORY",
                "REPLAY",
            ],
            "all_pass": True,
            "disagreements": {},
            "merged_verdict": "progress",
            "issue_score": 1,
        },
        solver_result={"round": 2, "selected_ids": []},
    )
    terminal = autoloop.create_round_summary(
        round_index=3,
        merged={
            "agreed_fail_tags": [],
            "agreed_pass_tags": [
                "INGEST_REAL_TRANSCRIPT",
                "START_PRESERVED",
                "WHOLE_SESSION_COVERAGE",
                "EPISODIC_ORDER",
                "XREF_RESOLVED",
                "LAYERED_MEMORY",
                "REPLAY",
            ],
            "all_pass": True,
            "disagreements": {},
            "merged_verdict": "terminate",
            "issue_score": 0,
        },
        solver_result={"round": 3},
    )

    changed1 = autoloop.maybe_update_best_checkpoint(run_root, progress, source_dir=tmp_path)
    changed2 = autoloop.maybe_update_best_checkpoint(run_root, terminal, source_dir=tmp_path)

    assert changed1 is True
    assert changed2 is True

    best = json.loads((run_root / "meta" / "best_checkpoint.json").read_text(encoding="utf-8"))
    assert best["round_index"] == 3
    assert best["merged_verdict"] == "terminate"


def test_run_loop_supports_resume_and_stops_on_real_success(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    run_root = tmp_path / "run"

    first = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)
    resumed = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=3)

    assert first["status"] == "max_rounds_reached"
    assert resumed["status"] == "success"
    state = json.loads((run_root / "meta" / "status.json").read_text(encoding="utf-8"))
    assert state["next_round"] == 3
    assert state["status"] == "success"


def test_run_loop_evidence_gate_prevents_empty_evidence_false_success(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="no_evidence_pass",
        mechanical_feedback=True,
        evidence_gate=True,
    )
    run_root = tmp_path / "run"

    status = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    assert status["status"] == "max_rounds_reached"
    summary = json.loads((run_root / "rounds" / "round_0001" / "summary.json").read_text(encoding="utf-8"))
    assert summary["all_pass"] is False
    assert summary["merged_verdict"] == "no_progress"
    assert summary["agreed_fail_tags"] == ["EVIDENCE_GATE"]


def test_run_loop_carries_forward_best_checkpoint_project_and_feedback(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)
    resumed = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=3)

    assert resumed["status"] == "success"
    artifact = json.loads(
        (run_root / "rounds" / "round_0002" / "solver" / "workspace" / "for_eval" / "artifact.json").read_text(
            encoding="utf-8"
        )
    )
    assert artifact["seed_before"] == "round-1\n"
    assert artifact["prior_fail_tags"] == ["AC-1"]


def test_run_loop_copies_solver_result_into_evaluator_workspaces(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    codex_solver_result = run_root / "rounds" / "round_0001" / "codex_eval" / "workspace" / "solver_result.json"
    claude_solver_result = run_root / "rounds" / "round_0001" / "claude_eval" / "workspace" / "solver_result.json"
    assert json.loads(codex_solver_result.read_text(encoding="utf-8"))["round"] == 1
    assert json.loads(claude_solver_result.read_text(encoding="utf-8"))["round"] == 1


def test_run_loop_copies_last_approved_checkpoint_into_evaluator_workspaces(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)
    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=2)

    approved_artifact = run_root / "rounds" / "round_0002" / "codex_eval" / "workspace" / "last_approved" / "artifact.json"
    assert json.loads(approved_artifact.read_text(encoding="utf-8"))["round"] == 1


def test_run_loop_copies_current_candidate_project_and_solver_memory_into_evaluator_workspaces(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, include_solver_memory=True)
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    evaluator_project = run_root / "rounds" / "round_0001" / "codex_eval" / "workspace" / "project" / "state.txt"
    evaluator_memory = run_root / "rounds" / "round_0001" / "codex_eval" / "workspace" / "solver_memory" / "turn_log.md"
    assert evaluator_project.read_text(encoding="utf-8") == "round-1\n"
    assert "Turn 1" in evaluator_memory.read_text(encoding="utf-8")


def test_run_loop_records_solver_memory_files_and_feedback_state(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="always_no_progress",
        mechanical_feedback=True,
        include_solver_memory=True,
    )
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)
    spec = autoloop.load_loop_spec(spec_path)
    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=2)

    summary = json.loads((run_root / "rounds" / "round_0001" / "summary.json").read_text(encoding="utf-8"))
    context = json.loads((paths["solver_workspace"] / "loop_input" / "round_context.json").read_text(encoding="utf-8"))

    assert summary["merged_verdict"] == "no_progress"
    assert "solver_memory/turn_log.md" in summary["solver_memory_files"]
    assert context["last_feedback_verdict"] == "no_progress"
    assert context["consecutive_no_progress"] == 1


def test_run_loop_resets_solver_memory_after_no_progress_threshold(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="always_no_progress",
        mechanical_feedback=True,
        no_progress_reset_after=2,
        include_solver_memory=True,
    )
    run_root = tmp_path / "run"

    result = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=3)

    assert result["status"] == "max_rounds_reached"
    round3_artifact = json.loads(
        (run_root / "rounds" / "round_0003" / "solver" / "workspace" / "for_eval" / "artifact.json").read_text(
            encoding="utf-8"
        )
    )
    state = json.loads((run_root / "meta" / "status.json").read_text(encoding="utf-8"))

    assert round3_artifact["seed_before"] == "round-1\n"
    assert round3_artifact["memory_before"] is None
    assert state["reset_count"] == 1


def test_run_loop_treats_zero_max_rounds_as_unbounded_until_success(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, mechanical_feedback=True, include_solver_memory=True)
    run_root = tmp_path / "run"

    result = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=0)

    assert result["status"] == "success"
    assert result["next_round"] == 3
    assert (run_root / "rounds" / "round_0001" / "summary.json").exists()
    assert (run_root / "rounds" / "round_0002" / "summary.json").exists()
    assert not (run_root / "rounds" / "round_0003").exists()


def test_run_loop_continues_from_degraded_solver_artifacts_without_solver_result(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        solver_script_name="fake_partial_solver.py",
        mechanical_feedback=True,
        include_solver_memory=True,
    )
    run_root = tmp_path / "run"

    result = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    assert result["status"] == "success"
    summary = json.loads((run_root / "rounds" / "round_0001" / "summary.json").read_text(encoding="utf-8"))
    assert summary["solver_result"]["status"] == "degraded_ready"
    assert "solver_result.json" in summary["solver_result"]["missing_artifacts"]
    events = [json.loads(line) for line in (run_root / "meta" / "events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(event["event_type"] == "solver.degraded_completed" for event in events)


def test_materialize_round_workspaces_exposes_missing_artifacts_after_degraded_round(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        solver_script_name="fake_partial_solver.py",
        evaluator_mode="always_no_progress",
        mechanical_feedback=True,
        include_solver_memory=True,
    )
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)
    spec = autoloop.load_loop_spec(spec_path)
    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=2)

    manager_directive = json.loads(
        (paths["solver_workspace"] / "loop_input" / "manager_directive.json").read_text(encoding="utf-8")
    )
    handoff = (paths["solver_workspace"] / "loop_input" / "handoff.md").read_text(encoding="utf-8")

    assert manager_directive["missing_artifacts_to_backfill"] == ["solver_result.json"]
    assert "Missing artifacts to backfill: solver_result.json" in handoff


def test_run_loop_backfills_missing_for_eval_from_verification_after_solver_timeout(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        solver_script_name="fake_timeout_backfill_solver.py",
        mechanical_feedback=True,
        include_solver_memory=True,
        solver_timeout_seconds=0.2,
    )
    run_root = tmp_path / "run"

    result = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    assert result["status"] == "success"
    summary = json.loads((run_root / "rounds" / "round_0001" / "summary.json").read_text(encoding="utf-8"))
    assert summary["solver_result"]["status"] == "degraded_ready"
    assert summary["solver_result"]["selected_ids"] == ["backfilled-1", "dossier_0001_smoking_gun"]
    assert summary["solver_result"]["missing_artifacts"] == ["solver_result.json"]
    assert (run_root / "rounds" / "round_0001" / "solver" / "workspace" / "for_eval" / "artifact.json").exists()
    assert (run_root / "rounds" / "round_0001" / "solver" / "workspace" / "for_eval" / "policy.json").exists()
    fixed_keywords = run_root / "rounds" / "round_0001" / "solver" / "workspace" / "for_eval" / "fixed_keywords.json"
    assert fixed_keywords.exists()
    assert json.loads(fixed_keywords.read_text(encoding="utf-8"))["keywords"] == [
        "장치 계정 사용 강요",
        "하급심 단계 미주장",
    ]
    assert "for_eval/fixed_keywords.json" in summary["solver_result"]["artifacts"]
    assert (run_root / "rounds" / "round_0001" / "solver" / "workspace" / "for_eval" / "summary.md").exists()
    assert (run_root / "rounds" / "round_0001" / "solver" / "workspace" / "for_eval" / "replay.md").exists()

    events = [json.loads(line) for line in (run_root / "meta" / "events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(event["event_type"] == "solver.timed_out" for event in events)
    assert any(event["event_type"] == "solver.verification_backfill_completed" for event in events)


def test_run_loop_carries_evaluator_code_library_between_critic_rounds(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="write_eval_code",
        mechanical_feedback=True,
        include_solver_memory=True,
    )
    run_root = tmp_path / "run"

    result = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=2)

    assert result["status"] == "success"
    library_probe = run_root / "evaluation_code_library" / "round_0001" / "codex_eval" / "replay_probe" / "check_replay.py"
    assert library_probe.exists()

    round2_result_md = (
        run_root / "rounds" / "round_0002" / "codex_eval" / "workspace" / "result.md"
    ).read_text(encoding="utf-8")
    assert "library_seen: true" in round2_result_md
    round2_handoff = (
        run_root / "rounds" / "round_0002" / "codex_eval" / "workspace" / "loop_input" / "handoff.md"
    ).read_text(encoding="utf-8")
    assert "Evaluation Code Library" in round2_handoff
    assert "available at `evaluation_code_library/`" in round2_handoff

    events = [json.loads(line) for line in (run_root / "meta" / "events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any(
        event["event_type"] == "evaluation_code_library.promoted"
        and event["round_index"] == 1
        and event["evaluator"] == "codex_eval"
        for event in events
    )


def test_run_loop_supervisor_recovers_stalled_solver_without_wall_clock_timeout(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        solver_script_name="fake_timeout_backfill_solver.py",
        mechanical_feedback=True,
        include_solver_memory=True,
        solver_stall_watchdog_seconds=0.3,
        progress_probe_interval_seconds=0.05,
    )
    run_root = tmp_path / "run"

    result = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    assert result["status"] == "success"
    summary = json.loads((run_root / "rounds" / "round_0001" / "summary.json").read_text(encoding="utf-8"))
    assert summary["solver_result"]["status"] == "degraded_ready"
    assert summary["solver_result"]["stalled"] is True
    assert summary["solver_result"]["selected_ids"] == ["backfilled-1", "dossier_0001_smoking_gun"]

    events = [json.loads(line) for line in (run_root / "meta" / "events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    event_types = [event["event_type"] for event in events]
    assert "manager.supervisor_intervened" in event_types
    assert "solver.timed_out" not in event_types
    assert "solver.verification_backfill_completed" in event_types


def test_run_loop_can_carry_forward_last_round_candidate_instead_of_best_checkpoint(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="always_no_progress",
        mechanical_feedback=True,
        include_solver_memory=True,
        carry_forward_mode="last_round_candidate",
    )
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=2)
    spec = autoloop.load_loop_spec(spec_path)
    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=3)

    seeded = (paths["solver_workspace"] / "project" / "state.txt").read_text(encoding="utf-8")
    memory_before = (paths["solver_workspace"] / "solver_memory" / "history.txt").read_text(encoding="utf-8")

    assert seeded == "round-2\n"
    assert memory_before == "round-1\nround-2\n"


def test_run_loop_persists_issue_ledger_to_meta_and_next_round_context(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="issue_ledger",
        mechanical_feedback=True,
        include_solver_memory=True,
        issue_ledger=True,
    )
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)
    spec = autoloop.load_loop_spec(spec_path)
    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=2)

    summary = json.loads((run_root / "rounds" / "round_0001" / "summary.json").read_text(encoding="utf-8"))
    meta_ledger = json.loads((run_root / "meta" / "issue_ledger.json").read_text(encoding="utf-8"))
    next_ledger = json.loads((paths["solver_workspace"] / "loop_input" / "issue_ledger.json").read_text(encoding="utf-8"))
    next_context = json.loads((paths["solver_workspace"] / "loop_input" / "round_context.json").read_text(encoding="utf-8"))

    assert summary["issue_count"] == 2
    assert meta_ledger["issue_count"] == 2
    assert next_ledger["issue_count"] == 2
    assert len(next_context["prior_issue_ledger"]) == 2


def test_run_loop_converges_two_critic_feedback_into_next_handoff(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="issue_ledger",
        mechanical_feedback=True,
        include_solver_memory=True,
        issue_ledger=True,
        include_converger=True,
        evaluation_flow_mode="all",
    )
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)
    spec = autoloop.load_loop_spec(spec_path)
    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=2)

    converger_workspace = run_root / "rounds" / "round_0001" / "feedback_converger" / "workspace"
    meta_feedback = run_root / "meta" / "converged_feedback.md"
    next_handoff = (paths["solver_workspace"] / "loop_input" / "handoff.md").read_text(encoding="utf-8")
    events = [json.loads(line) for line in (run_root / "meta" / "events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]

    assert (converger_workspace / "critic_feedback" / "codex_eval.md").exists()
    assert (converger_workspace / "critic_feedback" / "claude_eval.md").exists()
    assert (converger_workspace / "converged_feedback.md").exists()
    assert meta_feedback.exists()
    assert "top60 recall is still too low" in meta_feedback.read_text(encoding="utf-8")
    assert "Converged Feedback" in next_handoff
    assert "top60 recall is still too low" in next_handoff
    assert any(event["event_type"] == "converger.completed" for event in events)


def test_manager_heartbeat_seconds_drives_supervisor_stall_recovery(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        solver_script_name="fake_timeout_backfill_solver.py",
        mechanical_feedback=True,
        include_solver_memory=True,
        manager_heartbeat_seconds=0.3,
        progress_probe_interval_seconds=0.05,
    )
    run_root = tmp_path / "run"

    result = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    events = [json.loads(line) for line in (run_root / "meta" / "events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    supervisor_events = [event for event in events if event["event_type"] == "manager.supervisor_intervened"]
    event_types = [event["event_type"] for event in events]

    assert result["status"] == "success"
    assert supervisor_events
    assert supervisor_events[0]["heartbeat_seconds"] == 0.3
    assert "solver.timed_out" not in event_types


def test_run_loop_appends_events_jsonl_for_round_lifecycle(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path)
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    events_path = run_root / "meta" / "events.jsonl"
    assert events_path.exists()
    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    event_types = [event["event_type"] for event in events]

    assert event_types[0] == "run.initialized"
    assert "round.started" in event_types
    assert "solver.completed" in event_types
    assert "evaluator.completed" in event_types
    assert "round.summary_written" in event_types


def test_run_loop_skips_redundant_secondary_critic_until_primary_terminate(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="quality",
        evaluation_flow_mode="primary_then_confirm_terminate",
    )
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    events = [json.loads(line) for line in (run_root / "meta" / "events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    evaluator_events = [event for event in events if event["event_type"] == "evaluator.completed"]

    assert len(evaluator_events) == 1
    assert evaluator_events[0]["evaluator"] == "codex_eval"
    assert not (run_root / "rounds" / "round_0001" / "claude_eval" / "workspace" / "result.json").exists()


def test_run_loop_runs_both_evaluators_when_evaluation_flow_all(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="quality",
        evaluation_flow_mode="all",
    )
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    events = [json.loads(line) for line in (run_root / "meta" / "events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    evaluator_events = [event for event in events if event["event_type"] == "evaluator.completed"]

    assert len(evaluator_events) == 2
    assert {event["evaluator"] for event in evaluator_events} == {"codex_eval", "claude_eval"}
    assert (run_root / "rounds" / "round_0001" / "claude_eval" / "workspace" / "result.json").exists()


def test_run_loop_uses_fallback_when_secondary_evaluator_crashes(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="quality",
        evaluator_modes={"claude_eval": "crash"},
        evaluator_fallback_modes={"claude_eval": "quality"},
        evaluation_flow_mode="all",
    )
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)

    events = [json.loads(line) for line in (run_root / "meta" / "events.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    fallback_events = [event for event in events if event["event_type"] == "evaluator.fallback_started"]
    evaluator_events = [event for event in events if event["event_type"] == "evaluator.completed"]
    summary = json.loads((run_root / "rounds" / "round_0001" / "summary.json").read_text(encoding="utf-8"))

    assert len(fallback_events) == 1
    assert fallback_events[0]["evaluator"] == "claude_eval"
    assert fallback_events[0]["fallback_name"] == "codex_eval_fallback"
    assert len(evaluator_events) == 2
    assert any(event["evaluator"] == "claude_eval" and event.get("fallback_used") for event in evaluator_events)
    assert "claude_eval" in summary["verdicts"]


def test_run_loop_escalates_editable_surface_after_stagnation(tmp_path: Path) -> None:
    autoloop = _load_module()
    editable = tmp_path / "editable"
    surface_levels = [
        {
            "name": "policy_only",
            "solver_editable": [
                {
                    "source": str(editable / "state.txt"),
                    "target": "project/state.txt",
                }
            ],
        },
        {
            "name": "policy_plus_keywords",
            "solver_editable": [
                {
                    "source": str(editable / "state.txt"),
                    "target": "project/state.txt",
                },
                {
                    "source": str(editable / "keywords.txt"),
                    "target": "project/keywords.txt",
                },
            ],
        },
    ]
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="always_no_progress",
        mechanical_feedback=True,
        include_solver_memory=True,
        surface_levels=surface_levels,
        surface_escalation_after=2,
    )
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=2)
    status = json.loads((run_root / "meta" / "status.json").read_text(encoding="utf-8"))
    spec = autoloop.load_loop_spec(spec_path)
    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=3, status=status)

    assert status["active_surface_level"] == 1
    assert status["surface_name"] == "policy_plus_keywords"
    assert (paths["solver_workspace"] / "project" / "keywords.txt").exists()
    handoff = (paths["solver_workspace"] / "loop_input" / "handoff.md").read_text(encoding="utf-8")
    assert "policy_plus_keywords" in handoff


def test_run_loop_writes_stagnation_summary_and_exposes_it_to_next_round(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(
        tmp_path,
        evaluator_mode="always_no_progress",
        mechanical_feedback=True,
        include_solver_memory=True,
        no_progress_reset_after=10,
    )
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=2)
    spec = autoloop.load_loop_spec(spec_path)
    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=3)

    stagnation_path = run_root / "meta" / "stagnation.json"
    round_stagnation_path = paths["solver_workspace"] / "loop_input" / "stagnation.json"
    assert stagnation_path.exists()
    assert round_stagnation_path.exists()

    stagnation = json.loads(stagnation_path.read_text(encoding="utf-8"))
    round_stagnation = json.loads(round_stagnation_path.read_text(encoding="utf-8"))

    assert stagnation["repeated_verdict"] == "no_progress"
    assert stagnation["consecutive_no_progress"] == 2
    assert stagnation["verdict_window"] == ["no_progress", "no_progress"]
    assert round_stagnation["consecutive_no_progress"] == 2
    assert round_stagnation["verdict_window"] == ["no_progress", "no_progress"]


def test_materialize_round_workspaces_exposes_round_history_to_next_solver(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, mechanical_feedback=True, include_solver_memory=True)
    run_root = tmp_path / "run"

    autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=2)
    spec = autoloop.load_loop_spec(spec_path)
    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=3)

    round_history = json.loads(
        (paths["solver_workspace"] / "loop_input" / "round_history.json").read_text(encoding="utf-8")
    )["history"]

    assert len(round_history) == 2
    assert round_history[0]["round_index"] == 1
    assert round_history[0]["policy"] == {}
    assert round_history[0]["selected_ids"] == []
    assert round_history[0]["merged_verdict"] == "progress"
    assert round_history[1]["round_index"] == 2
    assert round_history[1]["merged_verdict"] == "terminate"


def test_materialize_round_workspaces_writes_generic_manager_directive_and_history(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, mechanical_feedback=True, include_solver_memory=True)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    autoloop._write_json(
        run_root / "rounds" / "round_0001" / "summary.json",
        {
            "round_index": 1,
            "merged_verdict": "no_progress",
            "agreed_fail_tags": ["BENCHMARK_PASS"],
            "agreed_pass_tags": ["REPLAY"],
            "issue_count": 2,
            "issue_score": 200,
            "solver_result": {
                "policy": {"top_k": 8, "bridge_enabled": True},
                "selected_ids": ["a", "b", "c"],
            },
        },
    )
    autoloop._write_json(
        run_root / "rounds" / "round_0002" / "summary.json",
        {
            "round_index": 2,
            "merged_verdict": "no_progress",
            "agreed_fail_tags": ["BENCHMARK_PASS"],
            "agreed_pass_tags": ["REPLAY"],
            "issue_count": 3,
            "issue_score": 300,
            "solver_result": {
                "policy": {"top_k": 8, "bridge_enabled": False},
                "selected_ids": ["a", "b", "c"],
            },
        },
    )
    autoloop._write_json(
        run_root / "meta" / "status.json",
        {
            "next_round": 3,
            "status": "running",
            "last_feedback_verdict": "no_progress",
            "consecutive_no_progress": 2,
            "reset_count": 0,
            "solver_reset_pending": False,
        },
    )

    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=3)

    manager_directive = json.loads(
        (paths["solver_workspace"] / "loop_input" / "manager_directive.json").read_text(encoding="utf-8")
    )
    history_lines = (
        run_root / "meta" / "manager_history.jsonl"
    ).read_text(encoding="utf-8").splitlines()
    history_payload = json.loads(history_lines[-1])

    assert manager_directive["manager_mode"] == "anti_stagnation"
    assert manager_directive["status"]["consecutive_no_progress"] == 2
    assert any(obs["kind"] == "repeated_selected_signature" for obs in manager_directive["observations"])
    assert any("Do not repeat a candidate direction" in item for item in manager_directive["instructions"])
    assert history_payload["round_index"] == 3
    assert history_payload["manager_mode"] == "anti_stagnation"


def test_materialize_round_workspaces_writes_orchestrator_self_memory(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, mechanical_feedback=True, include_solver_memory=True)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)
    autoloop._write_json(
        run_root / "rounds" / "round_0001" / "summary.json",
        {
            "round_index": 1,
            "merged_verdict": "no_progress",
            "agreed_fail_tags": ["BENCHMARK_PASS"],
            "agreed_pass_tags": ["REPLAY"],
            "issue_count": 2,
            "issue_score": 200,
            "solver_result": {
                "policy": {"top_k": 8, "bridge_enabled": True},
                "selected_ids": ["a", "b", "c"],
            },
        },
    )
    autoloop._write_json(
        run_root / "meta" / "status.json",
        {
            "next_round": 2,
            "status": "running",
            "last_feedback_verdict": "no_progress",
            "consecutive_no_progress": 1,
            "reset_count": 0,
            "solver_reset_pending": False,
        },
    )

    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=2)

    manager_directive = json.loads(
        (paths["solver_workspace"] / "loop_input" / "manager_directive.json").read_text(encoding="utf-8")
    )
    memory_text = (run_root / "meta" / "orchestrator_memory.md").read_text(encoding="utf-8")

    assert manager_directive["orchestrator_memory"]["path"] == "meta/orchestrator_memory.md"
    assert "# Orchestrator Memory" in memory_text
    assert "## Round 0002" in memory_text
    assert "### FINAL_GOAL" in memory_text
    assert "### CURRENT_FOCUS" in memory_text
    assert "### TODO" in memory_text
    assert "### PING_PONG_SUMMARY" in memory_text
    assert "### CHECKPOINT" in memory_text
    assert "### REFERENCES" in memory_text
    assert "- manager_mode: directed_search" in memory_text
    assert "- target fail tags: BENCHMARK_PASS" in memory_text
    assert "- action: wrote manager_directive.json/md and solver handoff" in memory_text
    assert "- action: appended machine manager_history.jsonl" in memory_text


def test_manager_directive_enters_directed_search_after_first_no_progress(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, mechanical_feedback=True, include_solver_memory=True, issue_ledger=True)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    autoloop._write_json(
        run_root / "rounds" / "round_0001" / "summary.json",
        {
            "round_index": 1,
            "merged_verdict": "no_progress",
            "agreed_fail_tags": ["BENCHMARK_PASS"],
            "agreed_pass_tags": ["GENERIC_POLICY", "REPLAY", "TIGHT_SELECTION"],
            "issue_count": 4,
            "issue_score": 310,
            "solver_result": {
                "policy": {"top_k": 8, "bridge_enabled": True},
                "selected_ids": ["a", "b", "c"],
            },
        },
    )
    autoloop._write_json(
        run_root / "meta" / "issue_ledger.json",
        {
            "round_index": 1,
            "issue_count": 4,
            "issue_score": 310,
            "issues": [
                {
                    "issue_key": "missing-smoke",
                    "summary": "missing smoking gun",
                    "severity": "high",
                    "evidence": ["missing=dossier"],
                }
            ],
        },
    )
    autoloop._write_json(
        run_root / "meta" / "status.json",
        {
            "next_round": 2,
            "status": "running",
            "last_feedback_verdict": "no_progress",
            "consecutive_no_progress": 1,
            "reset_count": 0,
            "solver_reset_pending": False,
        },
    )

    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=2)
    manager_directive = json.loads(
        (paths["solver_workspace"] / "loop_input" / "manager_directive.json").read_text(encoding="utf-8")
    )

    assert manager_directive["manager_mode"] == "directed_search"
    assert manager_directive["preserve"]["agreed_pass_tags"] == ["GENERIC_POLICY", "REPLAY", "TIGHT_SELECTION"]
    assert manager_directive["target"]["agreed_fail_tags"] == ["BENCHMARK_PASS"]
    assert manager_directive["target"]["highest_severity_issue_keys"] == ["missing-smoke"]
    assert "preserve_passing_dimensions" in manager_directive["required_move_characteristics"]
    assert "observable_outcome_change" in manager_directive["required_move_characteristics"]
    assert any(obs["kind"] == "protected_pass_tags" for obs in manager_directive["observations"])
    assert any(obs["kind"] == "active_fail_tags" for obs in manager_directive["observations"])


def test_manager_directive_marks_ineffective_capacity_only_expansion(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, mechanical_feedback=True, include_solver_memory=True)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    autoloop._write_json(
        run_root / "rounds" / "round_0001" / "summary.json",
        {
            "round_index": 1,
            "merged_verdict": "no_progress",
            "agreed_fail_tags": ["BENCHMARK_PASS"],
            "agreed_pass_tags": ["GENERIC_POLICY", "REPLAY"],
            "issue_count": 3,
            "issue_score": 300,
            "solver_result": {
                "policy": {"top_k": 8, "bridge_enabled": True, "min_distinct_keywords": 3},
                "selected_ids": ["a", "b", "c"],
            },
        },
    )
    autoloop._write_json(
        run_root / "rounds" / "round_0002" / "summary.json",
        {
            "round_index": 2,
            "merged_verdict": "no_progress",
            "agreed_fail_tags": ["BENCHMARK_PASS"],
            "agreed_pass_tags": ["GENERIC_POLICY", "REPLAY"],
            "issue_count": 3,
            "issue_score": 300,
            "solver_result": {
                "policy": {"top_k": 12, "bridge_enabled": True, "min_distinct_keywords": 3},
                "selected_ids": ["a", "b", "c", "d", "e"],
            },
        },
    )
    autoloop._write_json(
        run_root / "meta" / "status.json",
        {
            "next_round": 3,
            "status": "running",
            "last_feedback_verdict": "no_progress",
            "consecutive_no_progress": 2,
            "reset_count": 0,
            "solver_reset_pending": False,
        },
    )

    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=3)
    manager_directive = json.loads(
        (paths["solver_workspace"] / "loop_input" / "manager_directive.json").read_text(encoding="utf-8")
    )

    assert manager_directive["manager_mode"] == "anti_stagnation"
    assert "orthogonal_to_last_ineffective_delta" in manager_directive["required_move_characteristics"]
    assert "capacity_only_expansion_without_primary_change" in manager_directive["prohibited_patterns"]
    assert any(obs["kind"] == "ineffective_last_delta" for obs in manager_directive["observations"])
    assert any(obs["kind"] == "capacity_only_expansion_without_primary_change" for obs in manager_directive["observations"])


def test_manager_directive_enters_periodic_deep_audit_every_three_completed_rounds(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, mechanical_feedback=True, include_solver_memory=True)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    for idx in range(1, 4):
        autoloop._write_json(
            run_root / "rounds" / f"round_{idx:04d}" / "summary.json",
            {
                "round_index": idx,
                "merged_verdict": "progress" if idx < 3 else "no_progress",
                "agreed_fail_tags": ["BENCHMARK_PASS"],
                "agreed_pass_tags": ["GENERIC_POLICY", "REPLAY"],
                "issue_count": 1,
                "issue_score": 100,
                "solver_result": {
                    "policy": {"top_k": 8, "bridge_enabled": True, "min_distinct_keywords": 4 - idx},
                    "selected_ids": [f"id-{idx}"],
                },
            },
        )
    autoloop._write_json(
        run_root / "meta" / "status.json",
        {
            "next_round": 4,
            "status": "running",
            "last_feedback_verdict": "no_progress",
            "consecutive_no_progress": 1,
            "reset_count": 0,
            "solver_reset_pending": False,
        },
    )

    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=4)
    manager_directive = json.loads(
        (paths["solver_workspace"] / "loop_input" / "manager_directive.json").read_text(encoding="utf-8")
    )

    assert manager_directive["audit"]["level"] == "deep"
    assert "periodic_cadence" in manager_directive["audit"]["reasons"]
    assert "review_recent_search_trajectory" in manager_directive["audit"]["focus_checks"]


def test_manager_directive_escalates_to_deep_audit_on_stagnation_triggers(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, mechanical_feedback=True, include_solver_memory=True)
    spec = autoloop.load_loop_spec(spec_path)
    run_root = tmp_path / "run"
    autoloop.initialize_run_root(spec, run_root)

    autoloop._write_json(
        run_root / "rounds" / "round_0001" / "summary.json",
        {
            "round_index": 1,
            "merged_verdict": "no_progress",
            "agreed_fail_tags": ["BENCHMARK_PASS"],
            "agreed_pass_tags": ["GENERIC_POLICY", "REPLAY"],
            "issue_count": 2,
            "issue_score": 200,
            "solver_result": {
                "policy": {"top_k": 8, "bridge_enabled": True, "min_distinct_keywords": 3},
                "selected_ids": ["a", "b", "c"],
            },
        },
    )
    autoloop._write_json(
        run_root / "rounds" / "round_0002" / "summary.json",
        {
            "round_index": 2,
            "merged_verdict": "no_progress",
            "agreed_fail_tags": ["BENCHMARK_PASS"],
            "agreed_pass_tags": ["GENERIC_POLICY", "REPLAY"],
            "issue_count": 2,
            "issue_score": 200,
            "solver_result": {
                "policy": {"top_k": 12, "bridge_enabled": True, "min_distinct_keywords": 3},
                "selected_ids": ["a", "b", "c", "d"],
            },
        },
    )
    autoloop._write_json(
        run_root / "meta" / "status.json",
        {
            "next_round": 3,
            "status": "running",
            "last_feedback_verdict": "no_progress",
            "consecutive_no_progress": 2,
            "reset_count": 0,
            "solver_reset_pending": False,
        },
    )

    paths = autoloop.materialize_round_workspaces(spec, run_root, round_index=3)
    manager_directive = json.loads(
        (paths["solver_workspace"] / "loop_input" / "manager_directive.json").read_text(encoding="utf-8")
    )

    assert manager_directive["audit"]["level"] == "deep"
    assert "consecutive_no_progress" in manager_directive["audit"]["reasons"]
    assert "repeated_fail_signature" in manager_directive["audit"]["reasons"]
    assert "inspect_stagnation_root_cause" in manager_directive["audit"]["focus_checks"]


def test_run_command_can_finish_from_completion_contract_before_process_exit(tmp_path: Path) -> None:
    autoloop = _load_module()
    script_path = tmp_path / "writes_then_hangs.py"
    done_path = tmp_path / "done.json"
    stdout_path = tmp_path / "stdout.txt"
    stderr_path = tmp_path / "stderr.txt"
    script_path.write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "import json",
                "import sys",
                "import time",
                "from pathlib import Path",
                "",
                "done_path = Path(sys.argv[1])",
                "done_path.write_text(json.dumps({'status': 'ready'}), encoding='utf-8')",
                "time.sleep(4)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    started_at = time.monotonic()
    result = autoloop._run_command(
        ["python3", str(script_path), str(done_path)],
        tmp_path,
        stdout_path,
        stderr_path,
        completion_paths=[done_path],
    )
    elapsed = time.monotonic() - started_at

    assert done_path.exists()
    assert elapsed < 2.5
    assert result["completed_via_contract"] is True


def test_run_command_drains_stdout_while_waiting_for_completion_contract(tmp_path: Path) -> None:
    autoloop = _load_module()
    fixtures = Path(__file__).resolve().parent / "fixtures" / "recall_autoloop"
    done_path = tmp_path / "solver_result.json"
    stdout_path = tmp_path / "stdout.txt"
    stderr_path = tmp_path / "stderr.txt"

    result = autoloop._run_command(
        [
            "python3",
            str(fixtures / "fake_loud_blocking_solver.py"),
            "--workspace",
            str(tmp_path),
            "--round",
            "1",
        ],
        tmp_path,
        stdout_path,
        stderr_path,
        completion_paths=[done_path],
        max_runtime_seconds=1.0,
    )

    assert done_path.exists()
    assert result["timed_out"] is False
    assert result["completed_via_contract"] is True
    assert stdout_path.stat().st_size > 1024 * 1024


def test_run_loop_uses_completion_contract_for_hanging_solver(tmp_path: Path) -> None:
    autoloop = _load_module()
    spec_path = _write_spec(tmp_path, solver_script_name="fake_hanging_solver.py", include_solver_memory=True)
    run_root = tmp_path / "run"

    started_at = time.monotonic()
    result = autoloop.run_loop(spec_path=spec_path, run_root=run_root, max_rounds=1)
    elapsed = time.monotonic() - started_at

    assert result["status"] == "max_rounds_reached"
    assert elapsed < 2.5
    solver_result = json.loads(
        (run_root / "rounds" / "round_0001" / "solver" / "workspace" / "solver_result.json").read_text(encoding="utf-8")
    )
    assert solver_result["round"] == 1
