# Source-Grounded Method Audit - 2026-04-14 Snapshot

This component checks whether a generated answer is supported by method-bound source spans.

It focuses on a narrow but important rule:

- preserve raw records and source locators;
- detect event spans by required role co-occurrence, not loose keyword hits;
- keep direct method-conclusion spans separate from procedural or allegation spans;
- treat audited absence as a bounded negative/unresolved result, never as a positive pass;
- use critic summaries to detect false convergence and stagnation.

## Contents

| Path | Purpose |
| --- | --- |
| `src/source_grounded_method_audit.py` | Generic, config-driven event/conclusion audit scanner for JSONL records. |
| `tests/test_source_grounded_method_audit.py` | Self-contained tests for event detection, allegation rejection, audited absence, and pass-gate behavior. |
| `examples/profile.json` | Minimal generic profile with two method axes. |
| `examples/demo_records.jsonl` | Tiny fixture corpus. |
| `reports/round_audit_summary.json` | Compact round audit summary. |
| `reports/ROUND_AUDIT.md` | Human-readable evaluation of what was useful and what failed. |

## Principle

The previous loop repeatedly failed when it treated "event found" or "audited no direct conclusion found" as equivalent to "answer grounded." This artifact encodes the stricter rule:

```text
required answer pass = event_found AND direct_method_conclusion_found
audited_absence = useful negative evidence, but not a pass for a positive validness/validity question
```

The same rule is domain-agnostic: a source method, research procedure, transcript claim, lab observation, or recommendation candidate must be answered from method-bound source spans, not from generic procedural words or a party's allegation.

## Quick Check

From this folder:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests/test_source_grounded_method_audit.py -v
PYTHONDONTWRITEBYTECODE=1 python3 src/source_grounded_method_audit.py --profile examples/profile.json --records examples/demo_records.jsonl --out /tmp/source_grounded_method_audit_demo
```

Expected demo behavior:

- `method_alpha` has an event and a direct conclusion, so it is grounded.
- `method_beta` has an event only inside an allegation and a procedural dismissal, so it remains unresolved.
