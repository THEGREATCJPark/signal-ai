# Masthead Ribbon Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Keep the Daily Don't Die Summary affordance out of article flow while visually attaching it to the masthead title plate.

**Architecture:** Keep the existing `daily_summary` data and overlay sheet, but mount `.daily-ribbon-stage` inside `.masthead` as an absolute child. The closed ribbon label is only `Don't Die.`. Horizontal pulls release into a damped pendulum swing, vertical pulls toggle the sheet open/closed. The summary sheet uses the generated `daily_summary.title` instead of a fixed Korean heading.

**Tech Stack:** Static HTML/CSS/vanilla JavaScript, Python `unittest`, agent-browser/CDP mobile verification.

---

### Task 1: Lock the Layout Contract

**Files:**
- Modify: `tests/test_frontend_markup.py`

**Step 1: Write the failing test**

Add a frontend markup test that asserts:
- `.daily-ribbon-stage` lives inside `<header class="masthead">`
- `.masthead` is `position: relative`
- `.daily-ribbon-stage` is `position: absolute`, not `fixed`
- `.daily-ribbon-stage` uses `pointer-events: none`
- `.daily-ribbon-stage` has no `border-bottom`
- `.summary-physics.open` no longer changes `min-height`
- `.summary-sheet` is `position: fixed`
- The pull label is exactly `Don't Die.`
- The ribbon has 3D cloth cues and pendulum state variables

**Step 2: Run test to verify it fails**

Run:

```bash
python3 -m unittest tests.test_frontend_markup.FrontendMarkupTest.test_daily_summary_ribbon_does_not_shift_article_flow
```

Expected: FAIL before implementation if the ribbon is still fixed to the viewport or uses the old label.

### Task 2: Convert the Ribbon

**Files:**
- Modify: `docs/index.html`
- Copy to: `index.html`

**Step 1: Update CSS**

Make `.masthead` relative and `.daily-ribbon-stage` absolute on the masthead right edge. Add perspective, 3D transforms, fold gradients, and wrinkle highlights. Keep `.summary-sheet` a fixed overlay panel.

**Step 2: Update copy**

Change the pull surface text to `Don't Die.` only. Hide generated title on the ribbon surface, but use it inside the opened sheet.

**Step 3: Keep physics**

Reuse pointer capture and the spring loop, but add damped pendulum variables (`swingAngle`, `swingVelocity`). Pulling down past threshold toggles open, and pulling again toggles closed.

### Task 3: Generate Summary Titles

**Files:**
- Modify: `run_hourly.py`
- Modify data: `docs/articles.json`, `articles.json`, `exports/articles/YYYY-MM-DD.json`

Update `prompt_daily_summary()` and parsing so Gemma returns `{"title": "...", "body": "..."}`. Store the generated title in the daily summary payload and use a safe fallback only if generation fails.

### Task 4: Verify and Publish

**Files:**
- Verify: `docs/index.html`, `index.html`, `tests/test_frontend_markup.py`

**Commands:**

```bash
python3 -m unittest discover -s tests
python3 -m py_compile run_hourly.py discord_export_linux.py discord_export_text_only.py build_gist.py
git diff --check
```

Use local mobile browser verification at 390px and desktop verification at 1280px. Confirm the ribbon scrolls away with the masthead, opens/closes, and no old `Daily Summary`/`updates` label remains. Commit only public frontend/pipeline files and tests, then push `main` and watch the Pages run.
