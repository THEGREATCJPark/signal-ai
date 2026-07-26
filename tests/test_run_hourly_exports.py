import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import run_hourly

KST = timezone(timedelta(hours=9))


class DailyExportsTest(unittest.TestCase):
    def test_general_pipeline_uses_gemini_35_flash_lite(self):
        self.assertEqual(run_hourly.MODEL, "gemini-3.5-flash-lite")

    def test_latest_gemini_call_omits_deprecated_sampling_and_uses_model_defaults(self):
        class FakeScheduler:
            def acquire(self):
                return "key"

        class FakeResponse:
            status_code = 200
            ok = True
            text = ""

            @staticmethod
            def json():
                return {"candidates": [{"content": {"parts": [{"text": "ok"}]}}]}

        with patch.object(run_hourly.requests, "post", return_value=FakeResponse()) as post:
            result = run_hourly.call_gemma(
                "prompt",
                FakeScheduler(),
                model="gemini-3.6-flash",
                temp=0.25,
                max_attempts=1,
            )

        self.assertEqual(result, "ok")
        url = post.call_args.args[0]
        body = post.call_args.kwargs["json"]
        self.assertIn("models/gemini-3.6-flash:generateContent", url)
        self.assertNotIn("temperature", body["generationConfig"])
        self.assertEqual(body["generationConfig"]["thinkingConfig"]["thinkingLevel"], "medium")

    def test_resolve_run_at_uses_explicit_as_of_boundary(self):
        self.assertEqual(
            datetime(2026, 7, 24, 8, 0, tzinfo=KST),
            run_hourly.resolve_run_at("2026-07-24T08:00:00+09:00"),
        )

    def test_load_keys_uses_config_env(self):
        with tempfile.TemporaryDirectory() as td:
            key_file = Path(td) / "keys.env"
            key_file.write_text("# local runtime keys\nGEMINI_API_KEYS=key-a,key-b\n", encoding="utf-8")

            with patch.dict(os.environ, {"GEMINI_KEYS_CONFIG": str(key_file)}):
                self.assertEqual(run_hourly.load_keys(), ["key-a", "key-b"])

    def test_call_gemma_raises_immediately_on_input_token_limit(self):
        class FakeScheduler:
            def acquire(self):
                return "key"

        class FakeResponse:
            status_code = 400
            ok = False
            text = '{"error":{"message":"The input token count exceeds the maximum number of tokens allowed 262144."}}'

        with (
            patch.object(run_hourly.requests, "post", return_value=FakeResponse()) as post,
            patch.object(run_hourly.time, "sleep") as sleep,
        ):
            with self.assertRaisesRegex(RuntimeError, "input token count exceeds"):
                run_hourly.call_gemma("too large", FakeScheduler(), max_attempts=3)

        self.assertEqual(post.call_count, 1)
        sleep.assert_not_called()

    def test_discord_export_uses_current_python_interpreter(self):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout=b"final_file=/tmp/export.txt\n", stderr=b"")

        with patch.object(run_hourly.subprocess, "run", side_effect=fake_run):
            result = run_hourly.discord_export("2026-06-21T08:00:02+09:00")

        self.assertEqual(result, Path("/tmp/export.txt"))
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], sys.executable)
        self.assertIn("discord_export_linux.py", calls[0][1])

    def test_model_focus_uses_provided_chat_without_exporting_again(self):
        now = datetime(2026, 6, 22, 17, 30, tzinfo=KST)
        expected = {"id": "model-focus"}
        sched = object()

        with (
            patch.object(run_hourly, "discord_export", side_effect=AssertionError("should not export")),
            patch.object(run_hourly, "generate_model_focus_article", return_value=expected) as generate,
        ):
            article = run_hourly.generate_devmode_model_focus_article(now, sched, source_text="existing chat text")

        self.assertEqual(article, expected)
        generate.assert_called_once_with("existing chat text", now, sched)

    def test_model_focus_reuses_primary_chat_when_it_covers_daily_window(self):
        now = datetime(2026, 6, 23, 8, 0, tzinfo=KST)
        source = run_hourly.model_focus_source_from_primary_chat(
            "existing chat",
            "2026-06-22T08:00:02+09:00",
            now,
            explicit_chat_file=False,
        )
        self.assertEqual(source, "existing chat")

    def test_model_focus_does_not_reuse_short_catchup_chat(self):
        now = datetime(2026, 6, 23, 0, 40, tzinfo=KST)
        source = run_hourly.model_focus_source_from_primary_chat(
            "short catchup chat",
            "2026-06-22T17:27:34+09:00",
            now,
            explicit_chat_file=False,
        )
        self.assertIsNone(source)

    def test_model_focus_reuses_explicit_chat_file_even_when_shorter_than_daily_window(self):
        now = datetime(2026, 6, 23, 0, 40, tzinfo=KST)
        source = run_hourly.model_focus_source_from_primary_chat(
            "manual chat",
            "2026-06-22T17:27:34+09:00",
            now,
            explicit_chat_file=True,
        )
        self.assertEqual(source, "manual chat")

    def test_single_export_since_extends_to_model_focus_window(self):
        now = datetime(2026, 6, 24, 9, 22, 53, tzinfo=KST)

        since = run_hourly.single_export_since_for_run(
            "2026-06-23T11:41:08+09:00",
            now,
        )

        self.assertEqual(since, "2026-06-23T09:22:53+09:00")

    def test_single_export_since_keeps_older_scan_since(self):
        now = datetime(2026, 6, 24, 9, 22, 53, tzinfo=KST)

        since = run_hourly.single_export_since_for_run(
            "2026-06-22T08:00:00+09:00",
            now,
        )

        self.assertEqual(since, "2026-06-22T08:00:00+09:00")

    def test_filter_chat_since_trims_widened_export_for_scan(self):
        chat = (
            "[2026. 6. 23. 오전 9:24] old\n"
            "older model chatter\n\n"
            "[2026. 6. 23. 오전 11:41] user\n"
            "first eligible message\n\n"
            "[2026. 6. 23. 오후 12:00] user\n"
            "later message\n"
        )

        filtered = run_hourly.filter_chat_since(chat, "2026-06-23T11:41:08+09:00")

        self.assertNotIn("오전 9:24", filtered)
        self.assertIn("오전 11:41", filtered)
        self.assertIn("오후 12:00", filtered)

    def test_main_uses_one_widened_export_for_scan_and_model_focus(self):
        run_at = datetime(2026, 6, 24, 9, 22, 53, tzinfo=KST)

        class FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return run_at.astimezone(tz) if tz else run_at.replace(tzinfo=None)

            @classmethod
            def fromisoformat(cls, value):
                return datetime.fromisoformat(value)

        export_chat = (
            "[2026. 6. 23. 오전 9:24] old\n"
            "model focus only\n\n"
            "[2026. 6. 23. 오전 11:41] user\n"
            "scan eligible\n\n"
            "[2026. 6. 24. 오전 1:00] user\n"
            "daily nuggets recent\n"
        )
        state = {
            "schema_version": 2,
            "last_run_at": "2026-06-23T11:41:08+09:00",
            "generated_at": "2026-06-23T11:41:08+09:00",
            "journal": "First Light AI",
            "model": run_hourly.MODEL,
            "articles": [],
            "decision_log": [],
        }
        captured = {}

        def fake_scan(chunks, titles, now, sched, min_chars=run_hourly.MIN_SCAN_SPLIT_CHARS):
            captured["scan_text"] = "\n".join(chunks)
            return []

        model_article = {
            "id": "model-focus-20260624",
            "headline": run_hourly.MODEL_FOCUS_HEADLINE,
            "body": "모델 위주 본문",
            "category": "news",
            "trust": "high",
            "created_at": run_at.isoformat(),
            "placement": None,
            "placed_at": run_at.isoformat(),
            "kind": run_hourly.MODEL_FOCUS_KIND,
        }

        with tempfile.TemporaryDirectory() as td:
            with (
                patch.object(sys, "argv", ["run_hourly.py"]),
                patch.object(run_hourly, "ROOT", Path(td)),
                patch.object(run_hourly, "datetime", FixedDatetime),
                patch.object(run_hourly, "load_keys", return_value=["key"]),
                patch.object(run_hourly, "load_state", return_value=state),
                patch.object(run_hourly, "discord_export", return_value=Path("/tmp/one-export.txt")) as export,
                patch.object(run_hourly, "read_chat_text", return_value=export_chat),
                patch.object(run_hourly, "scan_chunks_for_articles", side_effect=fake_scan),
                patch.object(run_hourly, "generate_model_focus_article", return_value=model_article) as model_focus,
                patch.object(run_hourly, "_classify_and_save") as classify,
            ):
                run_hourly.main()

        export.assert_called_once_with("2026-06-23T09:22:53+09:00")
        self.assertNotIn("오전 9:24", captured["scan_text"])
        self.assertIn("오전 11:41", captured["scan_text"])
        self.assertNotIn("model focus only", model_focus.call_args.args[0])
        self.assertIn("daily nuggets recent", model_focus.call_args.args[0])
        classify.assert_called_once()
        self.assertEqual(classify.call_args.args[1][0]["id"], "model-focus-20260624")

    def test_default_publish_branch_uses_current_branch(self):
        def fake_run(cmd, **kwargs):
            if cmd[:3] == ["git", "branch", "--show-current"]:
                return subprocess.CompletedProcess(cmd, 0, stdout="dev\n", stderr="")
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="unexpected")

        with (
            patch.dict(os.environ, {
                key: value
                for key, value in os.environ.items()
                if key not in ("FIRST_LIGHT_PUBLISH_BRANCH", "GITHUB_REF_NAME")
            }, clear=True),
            patch.object(run_hourly.subprocess, "run", side_effect=fake_run),
        ):
            self.assertEqual(run_hourly.default_publish_branch(), "dev")

    def test_build_daily_summary_payload_uses_generated_title_and_metadata(self):
        run_at = datetime(2026, 4, 20, 8, 0, tzinfo=KST)
        articles = [{"id": "a1"}, {"id": "a2"}]
        payload = run_hourly.build_daily_summary_payload(
            "오늘의 AI 흐름입니다.",
            articles,
            run_at,
            title="보안과 모델 경쟁이 겹친 하루",
        )
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["title"], "보안과 모델 경쟁이 겹친 하루")
        self.assertEqual(payload["date"], "2026-04-20")
        self.assertEqual(payload["generated_at"], run_at.isoformat())
        self.assertEqual(payload["article_count"], 2)
        self.assertEqual(payload["body"], "오늘의 AI 흐름입니다.")

    def test_prompt_daily_summary_uses_full_article_bodies_for_detailed_summary(self):
        long_body = "앞부분 " + ("세부정보 " * 140) + "끝부분-반드시-포함"
        prompt = run_hourly.prompt_daily_summary([
            {
                "headline": "긴 본문 기사",
                "body": long_body,
                "category": "news",
                "trust": "high",
            }
        ])

        self.assertIn("1200~2200자", prompt)
        self.assertIn('"title":"오늘 흐름을 대표하는 한국어 제목"', prompt)
        self.assertIn('"body":"요약 본문"', prompt)
        self.assertIn(long_body, prompt)
        self.assertIn("끝부분-반드시-포함", prompt)

    def test_parse_daily_summary_response_reads_title_and_body(self):
        parsed = run_hourly.parse_daily_summary_response(
            '{"title":"모델 경쟁과 보안 경보가 겹친 하루","body":"오늘은 보안 이슈와 모델 루머가 함께 움직였습니다."}'
        )
        self.assertEqual(parsed["title"], "모델 경쟁과 보안 경보가 겹친 하루")
        self.assertEqual(parsed["body"], "오늘은 보안 이슈와 모델 루머가 함께 움직였습니다.")

    def test_parse_chunk_articles_downgrades_unsourced_model_launch_claim(self):
        raw = json.dumps({
            "articles": [
                {
                    "headline": "Google, 차세대 Gemini 3.1 및 3.5 모델 공개 소식",
                    "body": "Google이 자사의 가장 진보된 AI 모델인 Gemini 3.1과 3.5를 출시했다는 소식이 전해졌습니다. 이번 업데이트는 사용자 경험을 혁신할 것으로 보입니다.",
                    "category": "news",
                    "trust": "high",
                }
            ]
        }, ensure_ascii=False)

        parsed = run_hourly.parse_chunk_articles(raw)

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["category"], "rumor")
        self.assertEqual(parsed[0]["trust"], "low")
        self.assertTrue(parsed[0]["headline"].startswith("미확인: "))
        self.assertIn("공식 출처가 확인되지 않은", parsed[0]["body"])

    def test_parse_chunk_articles_keeps_official_model_launch_claim_high_trust(self):
        raw = json.dumps({
            "articles": [
                {
                    "headline": "Google, Gemini 3 Pro 공식 블로그 공개",
                    "body": "Google 공식 블로그(blog.google)에 따르면 Gemini 3 Pro Preview가 AI Studio와 Vertex AI에 공개됐다는 소식입니다. 공식 모델 카드와 API 문서도 함께 안내됐습니다.",
                    "category": "news",
                    "trust": "high",
                }
            ]
        }, ensure_ascii=False)

        parsed = run_hourly.parse_chunk_articles(raw)

        self.assertEqual(parsed[0]["category"], "news")
        self.assertEqual(parsed[0]["trust"], "high")
        self.assertFalse(parsed[0]["headline"].startswith("미확인: "))

    def test_story_guard_drops_repeated_gpt_image_2_release_claim(self):
        existing = [
            {
                "id": "old-release",
                "headline": "OpenAI, 차세대 이미지 생성 모델 'GPT Image 2' 순차적 배포 시작",
                "body": "OpenAI 개발자 문서와 시스템 카드에 GPT Image 2가 올라오며 순차적 배포가 시작됐다는 소식입니다.",
                "category": "news",
                "trust": "high",
            }
        ]
        new = [
            {
                "id": "new-release",
                "headline": "OpenAI, 이미지 생성 모델 GPT Image 2 출시",
                "body": "OpenAI가 GPT Image 2를 출시했다는 소식입니다. 이미지 생성과 편집 품질이 개선됐다고 알려졌습니다.",
                "category": "news",
                "trust": "high",
            }
        ]

        kept, dropped = run_hourly.apply_product_story_guard(new, existing)

        self.assertEqual(kept, [])
        self.assertEqual(dropped, ["new-release"])

    def test_story_guard_keeps_gpt_image_2_followup_but_labels_it(self):
        existing = [
            {
                "id": "old-release",
                "headline": "OpenAI, 차세대 이미지 생성 모델 'GPT Image 2' 순차적 배포 시작",
                "body": "OpenAI 개발자 문서와 시스템 카드에 GPT Image 2가 올라오며 순차적 배포가 시작됐다는 소식입니다.",
                "category": "news",
                "trust": "high",
            }
        ]
        new = [
            {
                "id": "new-benchmark",
                "headline": "GPT Image 2, 이미지 생성 리더보드 1위 등극",
                "body": "GPT Image 2가 Artificial Analysis 텍스트-이미지 리더보드에서 1위를 기록했다는 후속 소식입니다.",
                "category": "news",
                "trust": "high",
            }
        ]

        kept, dropped = run_hourly.apply_product_story_guard(new, existing)

        self.assertEqual(dropped, [])
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["id"], "new-benchmark")
        self.assertTrue(kept[0]["headline"].startswith("후속: "))

    def test_dedup_cluster_keeps_all_candidates_when_gemma_is_unavailable(self):
        candidates = [
            {"id": "art-202606110800-01", "headline": "첫 기사", "body": "본문 A"},
            {"id": "art-202606110800-02", "headline": "둘째 기사", "body": "본문 B"},
        ]

        with patch.object(run_hourly, "call_gemma", side_effect=RuntimeError("API failed after 20 attempts")):
            kept, dropped = run_hourly.dedup_cluster(candidates, sched=object())

        self.assertEqual(kept, candidates)
        self.assertEqual(dropped, [])

    def test_cross_existing_dedup_batches_large_active_archive(self):
        existing = [
            {
                "id": f"existing-{idx}",
                "headline": f"기존 기사 {idx}",
                "body": "기존 기사 본문 " * 30,
            }
            for idx in range(151)
        ]
        new = [{"id": "new-1", "headline": "새 기사", "body": "새 기사 본문"}]

        with patch.object(
            run_hourly,
            "call_gemma",
            return_value='{"drop_new":[]}',
        ) as call_gemma:
            kept, dropped = run_hourly.cross_existing_dedup(new, existing, sched=object())

        self.assertEqual(kept, new)
        self.assertEqual(dropped, [])
        self.assertEqual(call_gemma.call_count, 3)
        self.assertTrue(all(len(call.args[0]) < 40_000 for call in call_gemma.call_args_list))

    def test_classification_pool_keeps_front_page_and_caps_archive(self):
        active = [
            {
                "id": f"article-{idx}",
                "placement": "top" if idx == 0 else ("main" if idx < 7 else "side"),
                "created_at": f"2026-07-{(idx % 17) + 1:02d}T08:00:00+09:00",
            }
            for idx in range(1_124)
        ]

        selected = run_hourly.select_active_for_classification(active)

        self.assertEqual(len(selected), 75)
        self.assertTrue({f"article-{idx}" for idx in range(7)}.issubset({a["id"] for a in selected}))

    def test_scan_chunks_continues_after_one_chunk_gemma_exhaustion(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)

        with patch.object(
            run_hourly,
            "call_gemma",
            side_effect=[
                RuntimeError("API failed after 20 attempts"),
                json.dumps({
                    "articles": [
                        {
                            "headline": "살아남은 청크 기사",
                            "body": "두 번째 청크에서 뽑은 기사입니다. API 장애가 있어도 이 기사 후보는 보존되어야 합니다.",
                            "category": "news",
                            "trust": "high",
                        }
                    ]
                }, ensure_ascii=False),
            ],
        ):
            articles = run_hourly.scan_chunks_for_articles(
                ["첫 청크", "둘째 청크"],
                titles=[],
                now=run_at,
                sched=object(),
            )

        self.assertEqual(len(articles), 1)
        self.assertEqual(articles[0]["id"], "art-202606110800-01")
        self.assertEqual(articles[0]["headline"], "살아남은 청크 기사")
        self.assertEqual(articles[0]["placement"], None)

    def test_scan_chunks_splits_failed_chunk_and_keeps_successful_halves(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        chunk = (
            "[2026. 6. 11. 오전 8:00] user\n"
            + ("Gemini 성능 소식 " * 800)
            + "\n\n\n[2026. 6. 11. 오전 8:10] user\n"
            + ("Claude 출시일 소식 " * 800)
        )
        first_half = {
            "articles": [{
                "headline": "Gemini 성능 개선 소식",
                "body": "첫 번째 반쪽 청크에서 나온 성능 관련 소식입니다. 충분히 긴 본문으로 기사 후보가 유지됩니다.",
                "category": "news",
                "trust": "high",
            }]
        }
        second_half = {
            "articles": [{
                "headline": "Claude 출시일 관측",
                "body": "두 번째 반쪽 청크에서 나온 출시일 관련 관측입니다. 충분히 긴 본문으로 기사 후보가 유지됩니다.",
                "category": "rumor",
                "trust": "low",
            }]
        }

        with patch.object(
            run_hourly,
            "call_gemma",
            side_effect=[
                RuntimeError("API failed after 20 attempts"),
                json.dumps(first_half, ensure_ascii=False),
                json.dumps(second_half, ensure_ascii=False),
            ],
        ) as call_gemma:
            articles = run_hourly.scan_chunks_for_articles(
                [chunk],
                titles=[],
                now=run_at,
                sched=object(),
                min_chars=10_000,
            )

        self.assertEqual(call_gemma.call_count, 3)
        self.assertEqual([a["id"] for a in articles], ["art-202606110800-01", "art-202606110800-02"])
        self.assertEqual(articles[0]["headline"], "Gemini 성능 개선 소식")
        self.assertEqual(articles[1]["headline"], "미확인: Claude 출시일 관측")

    def test_scan_chunks_drops_poison_chunk_after_minimum_split_size(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        chunk = (
            "[2026. 6. 11. 오전 8:00] user\n"
            + ("검열될 수 있는 이상한 청크 " * 700)
            + "\n\n\n[2026. 6. 11. 오전 8:10] user\n"
            + ("계속 실패하는 이상한 청크 " * 700)
        )

        with patch.object(
            run_hourly,
            "call_gemma",
            side_effect=RuntimeError("API failed after 20 attempts"),
        ) as call_gemma:
            articles = run_hourly.scan_chunks_for_articles(
                [chunk],
                titles=[],
                now=run_at,
                sched=object(),
                min_chars=10_000,
            )

        self.assertEqual(articles, [])
        self.assertEqual(call_gemma.call_count, 7)

    def test_scan_chunks_uses_ten_attempt_budget_before_splitting(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        chunk = (
            "[2026. 6. 11. 오전 8:00] user\n"
            + ("Gemma API 쿨다운 확인 " * 650)
            + "\n\n\n[2026. 6. 11. 오전 8:10] user\n"
            + ("청크 분할 확인 " * 650)
        )

        with patch.object(
            run_hourly,
            "call_gemma",
            side_effect=[
                RuntimeError("API failed after 10 attempts"),
                '{"articles":[]}',
                '{"articles":[]}',
            ],
        ) as call_gemma:
            articles = run_hourly.scan_chunks_for_articles(
                [chunk],
                titles=[],
                now=run_at,
                sched=object(),
                min_chars=10_000,
            )

        self.assertEqual(articles, [])
        self.assertTrue(call_gemma.call_args_list)
        self.assertTrue(
            all(call.kwargs["max_attempts"] == 10 for call in call_gemma.call_args_list)
        )

    def test_default_scan_chunks_stay_within_direct_api_budget(self):
        chat = "".join(
            f"[2026. 7. 18. 오전 8:{minute % 60:02d}] user\n" + ("AI 소식 " * 120) + "\n\n\n"
            for minute in range(100)
        )

        chunks = run_hourly.chunk_by_messages(chat)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= 40_000 for chunk in chunks))

    def test_generate_daily_nuggets_uses_exact_short_prompt_and_primary_model(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        chat = (
            "[2026. 6. 10. 오후 9:00] user\n"
            "Gemini 3.5가 곧 나올 수 있다는 루머와 벤치마크 이야기가 나왔다.\n\n\n"
            "[2026. 6. 10. 오후 9:05] user\n"
            "잡담은 포함하지 않는다."
        )
        response = "이것만 봐도 배부른 알짜배기만 모았습니다. Gemini 3.5 루머와 벤치마크 관측이 중심이었다."

        with patch.object(run_hourly, "call_gemma", return_value=response) as call_gemma:
            article = run_hourly.generate_model_focus_article(chat, run_at, sched=object())

        self.assertIsNotNone(article)
        self.assertEqual(article["id"], "model-focus-20260611")
        self.assertEqual(article["headline"], "일일 알짜배기")
        self.assertEqual(article["kind"], "model_focus")
        self.assertIn("Gemini 3.5", article["body"])
        prompt = call_gemma.call_args.args[0]
        self.assertTrue(prompt.startswith(run_hourly.MODEL_FOCUS_INSTRUCTION))
        self.assertIn("자세히 정리좀, 찌라시 빠짐없이", prompt)
        self.assertIn("디스코드 대화내용', 'devmode' 이런 용어는 쓰지말고", prompt)
        self.assertIn("이것만 봐도 배부른 알짜배기만\n모았습니다라고 시작해줘.", prompt)
        self.assertIn("Gemini 3.5", prompt)
        self.assertIn("잡담은 포함하지 않는다", prompt)
        self.assertEqual(call_gemma.call_args.kwargs["model"], "gemini-3.6-flash")
        self.assertFalse(call_gemma.call_args.kwargs["json_mode"])

    def test_daily_nuggets_derives_latest_12h_from_reused_24h_source(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        chat = (
            "[2026. 6. 10. 오후 7:59] old\n"
            "12시간 경계보다 오래된 내용\n\n"
            "[2026. 6. 10. 오후 8:00] kept\n"
            "정확한 12시간 경계 내용\n\n"
            "[2026. 6. 11. 오전 7:59] kept\n"
            "최신 내용\n"
        )
        expected = {"id": "daily-nuggets"}

        with patch.object(run_hourly, "generate_model_focus_article", return_value=expected) as generate:
            article = run_hourly.generate_devmode_model_focus_article(
                run_at,
                sched=object(),
                source_text=chat,
            )

        self.assertEqual(article, expected)
        derived = generate.call_args.args[0]
        self.assertNotIn("오후 7:59", derived)
        self.assertIn("오후 8:00", derived)
        self.assertIn("오전 7:59", derived)

    def test_daily_nuggets_falls_back_from_36_to_35_lite_before_splitting(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        chat = "[2026. 6. 11. 오전 7:00] user\n새 소식"

        with patch.object(
            run_hourly,
            "call_gemma",
            side_effect=[
                RuntimeError("primary unavailable"),
                "이것만 봐도 배부른 알짜배기만 모았습니다. 대체 모델이 충분히 자세한 결과를 반환했습니다.",
            ],
        ) as call_gemma:
            article = run_hourly.generate_model_focus_article(chat, run_at, sched=object())

        self.assertIsNotNone(article)
        self.assertEqual(
            [call.kwargs["model"] for call in call_gemma.call_args_list],
            ["gemini-3.6-flash", "gemini-3.5-flash-lite"],
        )

    def test_generate_daily_nuggets_splits_only_after_both_full_context_models_fail(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        chat = (
            "[2026. 6. 10. 오전 8:00] user\n"
            + ("Gemini 3.5 성능 루머 " * 700)
            + "\n\n\n[2026. 6. 10. 오후 8:00] user\n"
            + ("Claude Opus 출시일 관측 " * 700)
        )
        left_summary = "Gemini 3.5 성능 루머와 벤치마크 관측 정리"
        right_summary = "Claude Opus 출시일 관측과 미확인 일정 정리"
        merged_summary = "이것만 봐도 배부른 알짜배기만 모았습니다. 두 부분을 누락 없이 병합했습니다."

        with patch.object(
            run_hourly,
            "call_gemma",
            side_effect=[
                RuntimeError("primary full-context failure"),
                RuntimeError("fallback full-context failure"),
                left_summary,
                right_summary,
                merged_summary,
            ],
        ) as call_gemma:
            article = run_hourly.generate_model_focus_article(chat, run_at, sched=object(), min_chars=10_000)

        self.assertIsNotNone(article)
        self.assertIn("누락 없이 병합", article["body"])
        prompts = [call.args[0] for call in call_gemma.call_args_list]
        self.assertIn("Gemini 3.5 성능 루머", prompts[0])
        self.assertIn("Claude Opus 출시일 관측", prompts[0])
        self.assertIn("Gemini 3.5 성능 루머와 벤치마크 관측 정리", prompts[-1])
        self.assertIn("Claude Opus 출시일 관측과 미확인 일정 정리", prompts[-1])
        self.assertIn("자세히 정리좀, 찌라시 빠짐없이", prompts[-1])
        self.assertEqual(
            [call.kwargs["model"] for call in call_gemma.call_args_list],
            [
                "gemini-3.6-flash",
                "gemini-3.5-flash-lite",
                "gemini-3.6-flash",
                "gemini-3.6-flash",
                "gemini-3.6-flash",
            ],
        )

    def test_daily_nuggets_does_not_publish_a_partial_summary_when_a_leaf_fails(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        chat = "왼쪽 자료 " * 2_000 + "\n\n\n" + "오른쪽 자료 " * 2_000

        with patch.object(
            run_hourly,
            "call_gemma",
            side_effect=RuntimeError("all models unavailable"),
        ):
            article = run_hourly.generate_model_focus_article(
                chat,
                run_at,
                sched=object(),
                min_chars=10_000,
            )

        self.assertIsNone(article)

    def test_model_focus_pre_splits_oversized_source_before_api_call(self):
        source = ("Gemini 성능 소식 " * 120) + "\n\n\n" + ("Claude 출시 소식 " * 120)
        left_summary = "Gemini 성능 소식을 정리한 부분 요약입니다."
        right_summary = "Claude 출시 소식을 정리한 부분 요약입니다."
        merged_summary = "Gemini 성능과 Claude 출시 소식을 병합한 전체 요약이며 세부 내용을 함께 담았습니다."

        with patch.object(
            run_hourly,
            "call_gemma",
            side_effect=[left_summary, right_summary, merged_summary],
        ) as call_gemma:
            body = run_hourly.summarize_model_focus_source(
                source,
                sched=object(),
                min_chars=100,
                direct_max_chars=len(source) // 2 + 20,
            )

        self.assertIn("병합한", body)
        self.assertEqual(call_gemma.call_count, 3)
        self.assertTrue(all(len(call.args[0]) < len(source) for call in call_gemma.call_args_list[:2]))

    def test_model_focus_article_updates_ribbon_summary_payload(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        state = {"articles": []}
        article = {
            "id": "model-focus-20260611",
            "headline": "일일 알짜배기",
            "body": "이것만 봐도 배부른 알짜배기만 모았습니다. 오늘의 전체 요약입니다.",
            "category": "news",
            "trust": "high",
            "kind": "model_focus",
            "created_at": run_at.isoformat(),
            "placement": None,
            "placed_at": run_at.isoformat(),
        }

        new_articles = run_hourly.upsert_model_focus_article(state, [], article)

        self.assertEqual(new_articles, [article])
        self.assertEqual(state["model_focus_summary"]["title"], "일일 알짜배기")
        self.assertEqual(
            state["model_focus_summary"]["body"],
            "이것만 봐도 배부른 알짜배기만 모았습니다. 오늘의 전체 요약입니다.",
        )
        self.assertEqual(state["model_focus_summary"]["date"], "2026-06-11")

    def test_model_focus_article_is_forced_to_first_main_slot(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        model_focus = {
            "id": "model-focus-20260611",
            "headline": "일일 알짜배기",
            "body": "이것만 봐도 배부른 알짜배기만 모았습니다. 오늘의 분석입니다.",
            "category": "news",
            "trust": "high",
            "kind": "model_focus",
            "created_at": run_at.isoformat(),
            "placement": None,
            "placed_at": run_at.isoformat(),
        }
        new_articles = [model_focus] + [
            {
                "id": f"new-{i}",
                "headline": f"신규 기사 {i}",
                "body": "새로 들어온 기사 본문",
                "category": "news",
                "trust": "high",
                "created_at": run_at.isoformat(),
                "placement": None,
                "placed_at": run_at.isoformat(),
            }
            for i in range(1, 8)
        ]
        placement_map, ordered = run_hourly.prioritize_new_articles_for_front_page(
            new_articles,
            new_articles,
            {a["id"]: "side" for a in new_articles},
        )

        self.assertEqual(placement_map["new-1"], "top")
        self.assertEqual(placement_map["model-focus-20260611"], "main")
        self.assertEqual([a["id"] for a in ordered[:3]], ["new-1", "model-focus-20260611", "new-2"])
        self.assertEqual(sum(1 for p in placement_map.values() if p == "main"), 6)

    def test_classify_save_publishes_with_fallback_when_gemma_is_unavailable(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        state = {
            "schema_version": 2,
            "journal": "First Light AI",
            "articles": [],
            "decision_log": [],
        }
        new_articles = [
            {
                "id": "art-202606110800-01",
                "headline": "새 기사",
                "body": "Gemma API가 장애여도 이 후보는 버리지 않고 공개 상태에 남아야 합니다.",
                "category": "news",
                "trust": "high",
                "created_at": run_at.isoformat(),
                "placement": None,
                "placed_at": run_at.isoformat(),
            }
        ]

        with tempfile.TemporaryDirectory() as td:
            docs_path = Path(td) / "docs" / "articles.json"
            pages_path = Path(td) / "articles.json"
            exports_dir = Path(td) / "exports" / "articles"
            docs_path.parent.mkdir(parents=True)
            with (
                patch.object(run_hourly, "ARTICLES_PATH", docs_path),
                patch.object(run_hourly, "PAGES_ARTICLES_PATH", pages_path),
                patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", exports_dir),
                patch.object(run_hourly, "call_gemma", side_effect=RuntimeError("API failed after 20 attempts")),
                patch.object(run_hourly.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="gist ok\n", stderr="")),
                patch.object(run_hourly, "publish_after_run") as publish_after_run,
            ):
                run_hourly._classify_and_save(state, new_articles, run_at, sched=object())

            saved = json.loads(docs_path.read_text(encoding="utf-8"))

        self.assertEqual(saved["generated_at"], run_at.isoformat())
        self.assertEqual(saved["articles"][0]["id"], "art-202606110800-01")
        self.assertIn(saved["articles"][0]["placement"], {"top", "main", "side"})
        publish_after_run.assert_called_once()

    def test_validate_placement_discards_unknown_ids_and_fills_missing(self):
        placement = {
            "top": ["999"],
            "main": ["2", "1000"],
            "side": ["1", "2", "1001"],
        }

        err = run_hourly.validate_placement(placement, {"1", "2", "3"})

        self.assertIsNone(err)
        self.assertEqual(placement["top"], [])
        self.assertEqual(placement["main"], ["2"])
        self.assertEqual(placement["side"], ["1", "3"])

    def test_classify_uses_bounded_gemini_retries_before_fail_open(self):
        run_at = datetime(2026, 7, 1, 8, 0, tzinfo=KST)
        state = {
            "schema_version": 2,
            "journal": "First Light AI",
            "articles": [],
            "decision_log": [],
        }
        new_articles = [
            {
                "id": "art-202607010800-01",
                "headline": "새 기사",
                "body": "classify API가 오래 붙잡혀도 publish는 fail-open 해야 합니다.",
                "category": "news",
                "trust": "high",
                "created_at": run_at.isoformat(),
                "placement": None,
                "placed_at": run_at.isoformat(),
            }
        ]
        calls = []

        def fake_call_gemma(*args, **kwargs):
            calls.append(kwargs)
            raise RuntimeError("API failed after bounded attempts")

        with tempfile.TemporaryDirectory() as td:
            docs_path = Path(td) / "docs" / "articles.json"
            pages_path = Path(td) / "articles.json"
            exports_dir = Path(td) / "exports" / "articles"
            docs_path.parent.mkdir(parents=True)
            with (
                patch.object(run_hourly, "ARTICLES_PATH", docs_path),
                patch.object(run_hourly, "PAGES_ARTICLES_PATH", pages_path),
                patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", exports_dir),
                patch.object(run_hourly, "call_gemma", side_effect=fake_call_gemma),
                patch.object(run_hourly.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="gist ok\n", stderr="")),
                patch.object(run_hourly, "publish_after_run"),
            ):
                run_hourly._classify_and_save(state, new_articles, run_at, sched=object())

        self.assertEqual(calls[0]["max_attempts"], 2)

    def test_write_daily_new_articles_export_uses_date_folder_and_metadata(self):
        run_at = datetime(2026, 4, 20, 12, 0, tzinfo=KST)
        articles = [
            {
                "id": "art-202604201200-01",
                "headline": "새 기사",
                "body": "본문",
                "category": "news",
                "trust": "high",
                "created_at": run_at.isoformat(),
                "placement": "side",
            }
        ]
        daily_summary = run_hourly.build_daily_summary_payload("하루 요약", articles, run_at, title="하루 제목")
        with tempfile.TemporaryDirectory() as td:
            with patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)):
                out = run_hourly.write_daily_new_articles_export(articles, run_at, daily_summary)
                self.assertEqual(out, Path(td) / "2026-04-20.json")
                payload = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["journal"], "First Light AI")
        self.assertEqual(payload["date"], "2026-04-20")
        self.assertEqual(payload["generated_at"], run_at.isoformat())
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["articles"][0]["headline"], "새 기사")
        self.assertEqual(payload["daily_summary"]["title"], "하루 제목")
        self.assertEqual(payload["daily_summary"]["body"], "하루 요약")

    def test_write_daily_new_articles_export_writes_empty_run(self):
        run_at = datetime(2026, 4, 20, 12, 0, tzinfo=KST)
        with tempfile.TemporaryDirectory() as td:
            with patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)):
                out = run_hourly.write_daily_new_articles_export([], run_at)
                payload = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["articles"], [])

    def test_save_state_mirrors_pages_articles_json(self):
        state = {
            "schema_version": 2,
            "journal": "First Light AI",
            "articles": [{"id": "a1", "headline": "새 기사", "body": "본문"}],
        }
        with tempfile.TemporaryDirectory() as td:
            docs_path = Path(td) / "docs" / "articles.json"
            pages_path = Path(td) / "articles.json"
            docs_path.parent.mkdir()
            with (
                patch.object(run_hourly, "ARTICLES_PATH", docs_path),
                patch.object(run_hourly, "PAGES_ARTICLES_PATH", pages_path),
            ):
                run_hourly.save_state(state)

            self.assertEqual(json.loads(docs_path.read_text(encoding="utf-8")), state)
            self.assertEqual(json.loads(pages_path.read_text(encoding="utf-8")), state)

    def test_publish_public_artifacts_commits_and_pushes_pages_files(self):
        run_at = datetime(2026, 4, 20, 8, 0, tzinfo=KST)
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[:4] == ["git", "diff", "--cached", "--quiet"]:
                return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        with (
            patch.object(run_hourly, "PUBLISH_BRANCH", "dev"),
            patch.object(run_hourly.subprocess, "run", side_effect=fake_run),
        ):
            published = run_hourly.publish_public_artifacts(
                [
                    run_hourly.ARTICLES_PATH,
                    run_hourly.PAGES_ARTICLES_PATH,
                    run_hourly.EXPORTS_ARTICLES_DIR / "2026-04-20.json",
                ],
                run_at,
            )

        self.assertTrue(published)
        self.assertEqual(
            calls[0],
            [
                "git",
                "add",
                "--",
                "docs/articles.json",
                "articles.json",
                "exports/articles/2026-04-20.json",
            ],
        )
        self.assertEqual(
            calls[2],
            [
                "git",
                "-c",
                "user.name=pineapplesour",
                "-c",
                "user.email=59020461+pineapplesour@users.noreply.github.com",
                "commit",
                "-m",
                "chore: publish First Light AI 2026-04-20",
                "--",
                "docs/articles.json",
                "articles.json",
                "exports/articles/2026-04-20.json",
            ],
        )
        self.assertEqual(calls[3], ["git", "push", "origin", "HEAD:dev"])

    def test_publish_retries_from_origin_worktree_after_non_fast_forward(self):
        run_at = datetime(2026, 7, 26, 8, 0, tzinfo=KST)

        def fake_run(cmd, **kwargs):
            if cmd[:4] == ["git", "diff", "--cached", "--quiet"]:
                return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="")
            if cmd[:2] == ["git", "push"]:
                return subprocess.CompletedProcess(
                    cmd,
                    1,
                    stdout="",
                    stderr="! [rejected] HEAD -> dev (non-fast-forward)",
                )
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        with (
            patch.object(run_hourly, "PUBLISH_BRANCH", "dev"),
            patch.object(run_hourly.subprocess, "run", side_effect=fake_run),
            patch.object(
                run_hourly,
                "publish_public_artifacts_from_remote_branch",
                return_value=True,
            ) as isolated_publish,
        ):
            published = run_hourly.publish_public_artifacts(
                [run_hourly.ARTICLES_PATH, run_hourly.PAGES_ARTICLES_PATH],
                run_at,
            )

        self.assertTrue(published)
        isolated_publish.assert_called_once_with(
            ["docs/articles.json", "articles.json"],
            "2026-07-26",
        )

    def test_publish_public_artifacts_skips_when_no_changes(self):
        run_at = datetime(2026, 4, 20, 8, 0, tzinfo=KST)
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        with patch.object(run_hourly.subprocess, "run", side_effect=fake_run):
            published = run_hourly.publish_public_artifacts([run_hourly.PAGES_ARTICLES_PATH], run_at)

        self.assertFalse(published)
        self.assertEqual(len(calls), 2)

    def test_classify_and_save_keeps_existing_articles_without_expiry(self):
        run_at = datetime(2026, 4, 20, 12, 0, tzinfo=KST)
        old_created_at = (run_at - timedelta(days=4)).isoformat()
        state = {
            "schema_version": 2,
            "last_run_at": old_created_at,
            "generated_at": old_created_at,
            "journal": "First Light AI",
            "model": run_hourly.MODEL,
            "articles": [
                {
                    "id": "old-mythos",
                    "headline": "Claude Mythos 후폭풍",
                    "body": "오래된 기사도 아카이브에 남아야 한다.",
                    "created_at": old_created_at,
                    "placement": "side",
                    "placed_at": old_created_at,
                }
            ],
            "decision_log": [],
        }
        with tempfile.TemporaryDirectory() as td:
            with (
                patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)),
                patch.object(run_hourly, "save_state"),
                patch.object(run_hourly.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="gist ok\n", stderr="")),
            ):
                run_hourly._classify_and_save(state, [], run_at, sched=None)

        self.assertEqual(len(state["articles"]), 1)
        self.assertEqual(state["articles"][0]["id"], "old-mythos")

    def test_classify_and_save_stores_daily_summary(self):
        run_at = datetime(2026, 4, 20, 8, 0, tzinfo=KST)
        state = {
            "schema_version": 2,
            "last_run_at": None,
            "generated_at": run_at.isoformat(),
            "journal": "First Light AI",
            "model": run_hourly.MODEL,
            "articles": [],
            "decision_log": [],
        }
        new_articles = [
            {
                "id": "new-1",
                "headline": "새 모델 공개",
                "body": "새 모델이 공개됐다는 소식입니다.",
                "category": "news",
                "trust": "high",
                "created_at": run_at.isoformat(),
                "placement": "side",
                "placed_at": run_at.isoformat(),
            }
        ]
        summary = run_hourly.build_daily_summary_payload(
            "오늘은 새 모델 공개가 중심입니다.",
            new_articles,
            run_at,
            title="새 모델 공개가 중심이 된 하루",
        )
        with tempfile.TemporaryDirectory() as td:
            with (
                patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)),
                patch.object(run_hourly, "save_state"),
                patch.object(run_hourly, "generate_daily_summary", return_value=summary),
                patch.object(run_hourly, "call_gemma", return_value='{"top": null, "main": [], "side": ["1"]}'),
                patch.object(run_hourly.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="gist ok\n", stderr="")),
            ):
                run_hourly._classify_and_save(state, new_articles, run_at, sched=None)

        self.assertEqual(state["daily_summary"]["title"], "새 모델 공개가 중심이 된 하루")
        self.assertEqual(state["daily_summary"]["body"], "오늘은 새 모델 공개가 중심입니다.")

    def test_classify_and_save_reuses_current_daily_nuggets_for_ribbon(self):
        run_at = datetime(2026, 7, 26, 8, 0, tzinfo=KST)
        nugget_body = "이것만 봐도 배부른 알짜배기만 모았습니다. 오늘의 12시간 요약입니다."
        state = {
            "schema_version": 2,
            "last_run_at": None,
            "generated_at": run_at.isoformat(),
            "journal": "First Light AI",
            "model": "gemma-4-26b-a4b-it",
            "articles": [],
            "decision_log": [],
            "model_focus_summary": {
                "schema_version": 1,
                "title": "일일 알짜배기",
                "date": "2026-07-26",
                "generated_at": run_at.isoformat(),
                "body": nugget_body,
            },
        }

        with tempfile.TemporaryDirectory() as td:
            with (
                patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)),
                patch.object(run_hourly, "save_state"),
                patch.object(run_hourly, "generate_daily_summary", side_effect=AssertionError("must reuse nuggets")),
                patch.object(run_hourly.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="gist ok\n", stderr="")),
            ):
                run_hourly._classify_and_save(state, [], run_at, sched=object())

        self.assertEqual(state["daily_summary"]["title"], "일일 알짜배기")
        self.assertEqual(state["daily_summary"]["body"], nugget_body)
        self.assertEqual(state["model"], "gemini-3.5-flash-lite")

    def test_classify_and_save_promotes_new_articles_to_front_page_first(self):
        run_at = datetime(2026, 4, 24, 8, 0, tzinfo=KST)
        old_articles = [
            {
                "id": f"old-{i}",
                "headline": f"과거 중요 기사 {i}",
                "body": "과거 기사 본문",
                "created_at": (run_at - timedelta(days=i)).isoformat(),
                "placement": "top" if i == 1 else "main",
                "placed_at": (run_at - timedelta(days=i)).isoformat(),
            }
            for i in range(1, 8)
        ]
        new_articles = [
            {
                "id": f"new-{i}",
                "headline": f"신규 기사 {i}",
                "body": "새로 들어온 기사 본문",
                "category": "news",
                "trust": "high",
                "created_at": run_at.isoformat(),
                "placement": "side",
                "placed_at": run_at.isoformat(),
            }
            for i in range(1, 4)
        ]
        state = {
            "schema_version": 2,
            "last_run_at": (run_at - timedelta(days=1)).isoformat(),
            "generated_at": (run_at - timedelta(days=1)).isoformat(),
            "journal": "First Light AI",
            "model": run_hourly.MODEL,
            "articles": old_articles,
            "decision_log": [],
        }
        summary = run_hourly.build_daily_summary_payload("신규 요약", new_articles, run_at, title="신규 중심")

        # The LLM tries to preserve yesterday's front page and sends new articles to side.
        llm_keeps_old_front = '{"top":"1","main":["2","3","4","5","6","7"],"side":["8","9","10"]}'

        with tempfile.TemporaryDirectory() as td:
            with (
                patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)),
                patch.object(run_hourly, "save_state"),
                patch.object(run_hourly, "generate_daily_summary", return_value=summary),
                patch.object(run_hourly, "call_gemma", return_value=llm_keeps_old_front),
                patch.object(run_hourly.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="gist ok\n", stderr="")),
            ):
                run_hourly._classify_and_save(state, new_articles, run_at, sched=None)

        front = [a for a in state["articles"] if a["placement"] in ("top", "main")]
        by_id = {a["id"]: a for a in state["articles"]}
        self.assertEqual([a["id"] for a in front[:3]], ["new-1", "new-2", "new-3"])
        self.assertEqual(by_id["new-1"]["placement"], "top")
        self.assertEqual(by_id["new-2"]["placement"], "main")
        self.assertEqual(by_id["new-3"]["placement"], "main")
        self.assertEqual(sum(1 for a in state["articles"] if a["placement"] == "top"), 1)
        self.assertEqual(sum(1 for a in state["articles"] if a["placement"] == "main"), 6)

    def test_classify_and_save_keeps_low_trust_rumor_behind_credible_front_candidates(self):
        run_at = datetime(2026, 4, 24, 8, 0, tzinfo=KST)
        old_articles = [
            {
                "id": f"old-{i}",
                "headline": f"기존 신뢰 기사 {i}",
                "body": "공식 출처로 확인된 기존 기사 본문",
                "category": "news",
                "trust": "high",
                "created_at": (run_at - timedelta(days=i)).isoformat(),
                "placement": "top" if i == 1 else "main",
                "placed_at": (run_at - timedelta(days=i)).isoformat(),
            }
            for i in range(1, 8)
        ]
        new_articles = [
            {
                "id": "new-rumor",
                "headline": "미확인: Gemini 3.5 출시 주장",
                "body": "공식 출처가 확인되지 않은 채팅 기반 주장입니다.",
                "category": "rumor",
                "trust": "low",
                "created_at": run_at.isoformat(),
                "placement": "side",
                "placed_at": run_at.isoformat(),
            }
        ]
        state = {
            "schema_version": 2,
            "last_run_at": (run_at - timedelta(days=1)).isoformat(),
            "generated_at": (run_at - timedelta(days=1)).isoformat(),
            "journal": "First Light AI",
            "model": run_hourly.MODEL,
            "articles": old_articles,
            "decision_log": [],
        }
        llm_promotes_rumor = json.dumps({"top": "8", "main": ["1", "2", "3", "4", "5", "6"], "side": ["7"]})
        summary = run_hourly.build_daily_summary_payload("루머 요약", new_articles, run_at, title="루머 중심")

        with tempfile.TemporaryDirectory() as td:
            with (
                patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)),
                patch.object(run_hourly, "save_state"),
                patch.object(run_hourly, "generate_daily_summary", return_value=summary),
                patch.object(run_hourly, "call_gemma", return_value=llm_promotes_rumor),
                patch.object(run_hourly.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="gist ok\n", stderr="")),
            ):
                run_hourly._classify_and_save(state, new_articles, run_at, sched=None)

        by_id = {a["id"]: a for a in state["articles"]}
        self.assertEqual(by_id["new-rumor"]["placement"], "side")
        self.assertEqual([a["id"] for a in state["articles"][:7]], [f"old-{i}" for i in range(1, 8)])

    def test_classify_and_save_caps_front_page_at_seven_new_articles(self):
        run_at = datetime(2026, 4, 24, 8, 0, tzinfo=KST)
        state = {
            "schema_version": 2,
            "last_run_at": (run_at - timedelta(days=1)).isoformat(),
            "generated_at": (run_at - timedelta(days=1)).isoformat(),
            "journal": "First Light AI",
            "model": run_hourly.MODEL,
            "articles": [],
            "decision_log": [],
        }
        new_articles = [
            {
                "id": f"new-{i}",
                "headline": f"신규 기사 {i}",
                "body": "새로 들어온 기사 본문",
                "category": "news",
                "trust": "high",
                "created_at": run_at.isoformat(),
                "placement": None,
                "placed_at": run_at.isoformat(),
            }
            for i in range(1, 10)
        ]
        side_ids = [str(i) for i in range(1, 10)]
        llm_sends_all_to_side = json.dumps({"top": None, "main": [], "side": side_ids})
        summary = run_hourly.build_daily_summary_payload("신규 요약", new_articles, run_at, title="신규 중심")

        with tempfile.TemporaryDirectory() as td:
            with (
                patch.object(run_hourly, "EXPORTS_ARTICLES_DIR", Path(td)),
                patch.object(run_hourly, "save_state"),
                patch.object(run_hourly, "generate_daily_summary", return_value=summary),
                patch.object(run_hourly, "call_gemma", return_value=llm_sends_all_to_side),
                patch.object(run_hourly.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout="gist ok\n", stderr="")),
            ):
                run_hourly._classify_and_save(state, new_articles, run_at, sched=None)

        front_ids = [a["id"] for a in state["articles"] if a["placement"] in ("top", "main")]
        self.assertEqual(front_ids, [f"new-{i}" for i in range(1, 8)])
        self.assertEqual([a["placement"] for a in state["articles"][:9]], ["top", "main", "main", "main", "main", "main", "main", "side", "side"])


if __name__ == "__main__":
    unittest.main()
