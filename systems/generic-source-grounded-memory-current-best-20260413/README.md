# Generic Source-Grounded Memory System - Current Best Snapshot

Snapshot: 2026-04-13

This folder contains a memory/search system snapshot. The adapter keeps task vocabulary in replaceable profile/configuration layers instead of embedding it into the architecture.

## Status

- The retrieval-variant harness passed its local test suite: `39 passed`.
- The full-corpus memory-search adapter passed its local unit suite: `32 tests OK`.
- The iterative full-corpus loop remains a research prototype. The included reports are bounded summaries, not raw corpus dumps.

## Included Components

| Path | Purpose |
| --- | --- |
| `full_corpus_memory_search/` | Streaming source-grounded memory search adapter with raw source locators, hashes, spans, duplicate tracking, evidence ledger, negative-search reports, and domain verifier hooks. |
| `retrieval_variant_harness/` | Generic retrieval experiment harness comparing summary-only, raw-leaf, compressed projection, graph relation, atomic/KAG-style docs, coverage patch, iterative coverage, and final RRF fusion. |
| `loop_runner/` | Supervisor/evaluator/converger loop runner and shell wrappers used to run iterative solver/critic rounds. |
| `reports/derived300/` | Slim bounded reports where `ultimate_rrf`, `coverage_patch`, and `iterative_coverage_loop` reached full derived-contract coverage in local audits. |

## Core Principles Captured

- Preserve raw source text and content hashes; do not answer from lossy summaries alone.
- Split large records into span-addressable chunks while keeping neighbor links.
- Keep duplicate accounting separate from evidence selection.
- Use multiple retrieval surfaces and reciprocal-rank fusion instead of trusting one filter.
- Use coverage patching to explicitly rerun missing requirements rather than rewriting the whole answer.
- Carry negative-search reports when evidence is absent or insufficient.
- Treat final generation as late as possible and keep it tied to evidence spans.

## What Was Excluded

- Raw corpus data.
- Large candidate frontier dumps.
- Large evidence ledger dumps.
- `__pycache__` and compiled Python artifacts.
- Local user memory/log files.

## Quick Verification

From this folder:

```bash
python3 -m py_compile full_corpus_memory_search/memory_search_system.py full_corpus_memory_search/test_domain_memory_search_system.py
python3 -m py_compile retrieval_variant_harness/evaluate_generic_memory_retrieval_variants.py retrieval_variant_harness/test_generic_memory_retrieval_variants.py
python3 -m py_compile loop_runner/recall_autoloop.py loop_runner/test_recall_autoloop.py
```

The tests are self-contained for this folder; set `PYTHONDONTWRITEBYTECODE=1` if you do not want local `__pycache__` files during verification.
