# Round Audit Summary

This report records the useful part of the audit run without overclaiming success.

## What Worked

- Broad streaming scan can recover a buried factual source-detail when summary-only or keyword-only retrieval fails.
- Role-bound event detection is better than plain keyword hits.
- Negative search becomes useful when it reports audited absence over a broad scan instead of pretending that absence is a positive answer.
- Dual critic summaries are valuable because solver self-evaluation repeatedly overclaimed completion.

## What Failed

- The candidate implementation remained too benchmark-specific.
- It treated direct source-detail evidence and final method-conclusion evidence as if they were interchangeable.
- It repeatedly reused the same selected frontier.
- It occasionally counted allegation/procedural text as if it were a binding direct conclusion.
- It did not become a general relation-preserving memory system.

## Best Reusable Rule

```text
If the user asks whether a method/procedure/result is valid, acceptable, valid, correct, or supported:
  event_found alone is not enough.
  procedural result alone is not enough.
  claimant or requester allegation is not enough.
  audited absence is useful but not a positive pass.
  require a direct method-bound conclusion span, or report the answer as unresolved/negative within the audited scope.
```

## Why This Folder Exists

The broader bundle contains RAG and loop systems. This folder adds a stricter method-audit component that avoids the false-pass pattern observed in iterative retrieval experiments.
