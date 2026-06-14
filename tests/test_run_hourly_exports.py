import json
import os
import subprocess
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import run_hourly

KST = timezone(timedelta(hours=9))


class DailyExportsTest(unittest.TestCase):
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

    def test_generate_model_focus_article_uses_full_devmode_24h_prompt(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        chat = (
            "[2026. 6. 10. 오후 9:00] user\n"
            "Gemini 3.5가 곧 나올 수 있다는 루머와 벤치마크 이야기가 나왔다.\n\n\n"
            "[2026. 6. 10. 오후 9:05] user\n"
            "잡담은 포함하지 않는다."
        )
        response = json.dumps({
            "body": "Dev Mode 24시간 채팅에서는 Gemini 3.5 루머와 벤치마크 관측이 중심이었다. 출시일은 확정되지 않았고, 성능 이야기는 흥미로운 찌라시로 분류해야 한다.",
        }, ensure_ascii=False)

        with patch.object(run_hourly, "call_gemma", return_value=response) as call_gemma:
            article = run_hourly.generate_model_focus_article(chat, run_at, sched=object())

        self.assertIsNotNone(article)
        self.assertEqual(article["id"], "model-focus-20260611")
        self.assertEqual(article["headline"], "최신 모델 위주 정보")
        self.assertEqual(article["kind"], "model_focus")
        self.assertIn("Gemini 3.5", article["body"])
        prompt = call_gemma.call_args.args[0]
        self.assertIn("이 내용을 자세히 분석해서 최신 또는 곧 나올 모델의 성능", prompt)
        self.assertIn("Gemini 3.5", prompt)
        self.assertIn("잡담은 포함하지 않는다", prompt)

    def test_generate_model_focus_article_splits_and_merges_when_full_24h_fails(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        chat = (
            "[2026. 6. 10. 오전 8:00] user\n"
            + ("Gemini 3.5 성능 루머 " * 700)
            + "\n\n\n[2026. 6. 10. 오후 8:00] user\n"
            + ("Claude Opus 출시일 관측 " * 700)
        )
        left_summary = json.dumps({"body": "Gemini 3.5 성능 루머와 벤치마크 관측 정리"}, ensure_ascii=False)
        right_summary = json.dumps({"body": "Claude Opus 출시일 관측과 미확인 일정 정리"}, ensure_ascii=False)
        merged_summary = json.dumps({
            "body": "Gemini 3.5 성능 루머와 Claude Opus 출시일 관측을 누락 없이 병합한 24시간 요약입니다."
        }, ensure_ascii=False)

        with patch.object(
            run_hourly,
            "call_gemma",
            side_effect=[
                RuntimeError("API failed after 10 attempts"),
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
        self.assertIn("누락 없이", prompts[-1])

    def test_model_focus_article_updates_ribbon_summary_payload(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        state = {"articles": []}
        article = {
            "id": "model-focus-20260611",
            "headline": "최신 모델 위주 정보",
            "body": "Dev Mode 24시간 전체 요약입니다.",
            "category": "news",
            "trust": "high",
            "kind": "model_focus",
            "created_at": run_at.isoformat(),
            "placement": None,
            "placed_at": run_at.isoformat(),
        }

        new_articles = run_hourly.upsert_model_focus_article(state, [], article)

        self.assertEqual(new_articles, [article])
        self.assertEqual(state["model_focus_summary"]["title"], "최신 모델 위주 정보")
        self.assertEqual(state["model_focus_summary"]["body"], "Dev Mode 24시간 전체 요약입니다.")
        self.assertEqual(state["model_focus_summary"]["date"], "2026-06-11")

    def test_model_focus_article_is_forced_to_first_main_slot(self):
        run_at = datetime(2026, 6, 11, 8, 0, tzinfo=KST)
        model_focus = {
            "id": "model-focus-20260611",
            "headline": "최신 모델 위주 정보",
            "body": "Dev Mode 24시간 모델 관련 분석입니다.",
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
