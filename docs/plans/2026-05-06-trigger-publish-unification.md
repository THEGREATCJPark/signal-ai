# Trigger Publish Unification Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make trigger and daily X publishing use the same content-rich article format, and auto-publish official high-signal trigger posts while still creating review issues.

**Architecture:** Add compact plain-text rendering in `bot/x_poster.py`, reuse `post_article()` for daily X publishing in `bot/scheduler.py`, and add auto-publish classification plus scan-time side effects in `scripts/x_trigger_scan.py`. Keep Telegram rendering unchanged, fed by the same article title/summary/url fields.

**Tech Stack:** Python 3.11, unittest, GitHub Actions, Supabase-backed publish state, Telegram Bot API, X API OAuth 1.0a.

---

### Task 1: Compact Trigger X Text

**Files:**
- Modify: `bot/x_poster.py`
- Test: `tests/test_publish_format.py`

**Steps:**
1. Write failing tests proving trigger X posts include title, short summary content, and URL, capped at five lines.
2. Implement a helper that builds compact article text from title, summary, and URL.
3. Route `build_trigger_post_text()` through that helper.
4. Run `python -m unittest tests.test_publish_format`.

### Task 2: Daily X Uses Per-Article Path

**Files:**
- Modify: `bot/scheduler.py`
- Test: `tests/test_publish_format.py`

**Steps:**
1. Write a failing test proving daily X publish calls `post_article()` once per selected unpublished article.
2. Replace `post_daily_summary()` usage in scheduler X branch with per-article `post_article()` calls.
3. Keep marking only successfully posted articles as published.
4. Run `python -m unittest tests.test_publish_format`.

### Task 3: Auto-Publish Official High-Signal Triggers

**Files:**
- Modify: `scripts/x_trigger_scan.py`
- Test: `tests/test_x_trigger_pipeline.py`

**Steps:**
1. Write failing tests for `should_auto_publish_candidate()` with official GPT rollout and non-official rumor cases.
2. Write a failing scan test showing issue creation still happens and auto-publish is called for official high-signal candidates.
3. Add `trigger-auto-published` label support.
4. Import and call `publish_trigger_candidate()` only after issue creation.
5. Add issue comments for auto-publish success/failure.
6. Run `python -m unittest tests.test_x_trigger_pipeline`.

### Task 4: Verification

**Files:**
- All touched files

**Steps:**
1. Run `python -m unittest discover -s tests`.
2. Run `python -m compileall -q bot crawlers db scripts tests run_hourly.py discord_export_linux.py`.
3. Run `git diff --check`.
