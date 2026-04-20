# Side Ribbon Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move the Daily Don't Die Summary ribbon out of document flow so articles are not pushed down.

**Architecture:** Keep the existing `daily_summary` data and ribbon physics loop, but change the UI from a wide top block to a narrow fixed side ribbon. The summary body opens in a fixed overlay sheet, not an in-flow block.

**Tech Stack:** Static HTML/CSS/vanilla JavaScript, Python `unittest`, agent-browser/CDP mobile verification.

---

### Task 1: Lock the Layout Contract

**Files:**
- Modify: `tests/test_frontend_markup.py`

**Step 1: Write the failing test**

Add a frontend markup test that asserts:
- `.daily-ribbon-stage` is `position: fixed`
- `.daily-ribbon-stage` uses `pointer-events: none`
- `.daily-ribbon-stage` has no `border-bottom`
- `.summary-physics.open` no longer changes `min-height`
- `.summary-sheet` is `position: fixed`
- The pull label is short: `Daily Summary`

**Step 2: Run test to verify it fails**

Run:

```bash
python3 -m unittest tests.test_frontend_markup.FrontendMarkupTest.test_daily_summary_ribbon_does_not_shift_article_flow
```

Expected: FAIL because the current wide ribbon is in normal flow.

### Task 2: Convert the Ribbon

**Files:**
- Modify: `docs/index.html`
- Copy to: `index.html`

**Step 1: Update CSS**

Make `.daily-ribbon-stage` a fixed, narrow right-side rail. Remove flow spacing and large open-state min-height. Make `.summary-sheet` a fixed overlay panel.

**Step 2: Update copy**

Change the pull surface text from a long Korean phrase to `Daily Summary` so the closed ribbon stays thin.

**Step 3: Keep physics**

Reuse pointer capture and the spring loop, but adjust dimensions and open/collapsed Y targets for a vertical side ribbon.

### Task 3: Verify and Publish

**Files:**
- Verify: `docs/index.html`, `index.html`, `tests/test_frontend_markup.py`

**Commands:**

```bash
python3 -m unittest discover -s tests
python3 -m py_compile run_hourly.py discord_export_linux.py discord_export_text_only.py build_gist.py
git diff --check
```

Use local mobile browser verification at 390px and desktop verification at 1280px. Commit only public frontend files and tests, then push `main` and watch the Pages run.
