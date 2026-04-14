import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import memory_search_system as m  # noqa: E402


class MemorySearchSystemTests(unittest.TestCase):
    def test_json_loader_preserves_item_locator_and_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "06_source_api_api" / "page_1.json"
            source.parent.mkdir(parents=True)
            payload = [
                {
                    "id": "case-1",
                    "record_id": "record-2024-0001",
                    "사건명": "자료확보",
                    "기록내용": "검토기관이 장치 인증토큰을 확인하였다.",
                }
            ]
            source.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

            records = list(m.iter_json_records(root, source, source.relative_to(root), start_ordinal=7))

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record.source_id, "06_source_api_api/page_1.json::item:0")
        self.assertEqual(record.ordinal, 7)
        self.assertEqual(record.metadata["id"], "case-1")
        self.assertIn("[기록내용] 검토기관이 장치 인증토큰을 확인하였다.", record.raw_text)
        self.assertEqual(record.content_hash, hashlib.sha256(record.raw_text.encode("utf-8")).hexdigest())

    def test_split_record_preserves_identity_spans_and_neighbors(self):
        text = "0123456789" * 12
        record = m.SourceRecord(
            source_id="sample.txt::file",
            path="/tmp/sample.txt",
            source_type="text",
            ordinal=3,
            raw_text=text,
            metadata={"title": "sample"},
            content_hash=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        )

        chunks = list(m.split_record(record, chunk_chars=50, overlap_chars=10))

        self.assertGreater(len(chunks), 2)
        self.assertEqual(chunks[0].source_id, record.source_id)
        self.assertEqual(chunks[0].content_hash, record.content_hash)
        self.assertEqual(chunks[1].char_start, 40)
        self.assertEqual(chunks[1].text, text[40:90])
        self.assertIsNone(chunks[0].prev_chunk_id)
        self.assertEqual(chunks[0].next_chunk_id, chunks[1].chunk_id)
        self.assertEqual(chunks[1].prev_chunk_id, chunks[0].chunk_id)

    def test_extract_evidence_spans_are_grounded_in_record_offsets(self):
        raw = "검토기관은 확보한 장치에서 인증토큰을 빼서 메신저에 로그인하였다. 판정기관은 권한범위 범위 내 분석이라 위법하지 않다고 보았다."
        record = m.SourceRecord(
            source_id="case.json::item:0",
            path="/tmp/case.json",
            source_type="json",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=200, overlap_chars=0)))
        profile = m.build_query_profile("인증토큰 메신저 로그인 반복시도 패스워드 합법")

        spans = m.extract_evidence_spans(chunk, profile)
        quotes = {span.quote for span in spans}

        self.assertIn("인증토큰", quotes)
        self.assertIn("메신저", quotes)
        self.assertTrue(any(span.category == "domain_conclusion" for span in spans))
        for span in spans:
            self.assertEqual(raw[span.char_start:span.char_end], span.quote)
            self.assertEqual(span.source_id, record.source_id)
            self.assertEqual(span.content_hash, record.content_hash)

    def test_duplicate_groups_keep_all_original_locators(self):
        text = "같은 판결문"
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        records = [
            m.SourceRecord("a::row:1", "/a", "parquet", 1, text, {}, digest),
            m.SourceRecord("b::row:2", "/b", "parquet", 2, text, {}, digest),
            m.SourceRecord("c::row:3", "/c", "parquet", 3, "다른 판결문", {}, hashlib.sha256("다른 판결문".encode("utf-8")).hexdigest()),
        ]

        groups = m.group_duplicate_records(records)

        self.assertEqual(groups[digest]["count"], 2)
        self.assertEqual(groups[digest]["source_ids"], ["a::row:1", "b::row:2"])

    def test_duplicate_index_tracks_counts_without_retaining_raw_records(self):
        text = "대용량 원문" * 1000
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        record = m.SourceRecord("a::row:1", "/a", "parquet", 1, text, {}, digest)
        index = {}

        m.record_duplicate_seen(index, record, max_source_ids=1)
        m.record_duplicate_seen(index, m.SourceRecord("b::row:2", "/b", "parquet", 2, text, {}, digest), max_source_ids=1)

        self.assertEqual(index[digest]["count"], 2)
        self.assertEqual(index[digest]["source_ids"], ["a::row:1"])
        self.assertNotIn("raw_text", index[digest])

    def test_query_surface_terms_do_not_dominate_specific_aspects(self):
        profile = m.build_query_profile("검토기관 장치 인증토큰 메신저 로그인 패스워드 반복시도")

        broad_score, _, broad_aspects = m.score_text("장치 케이스를 샀다.", profile)
        specific_score, _, specific_aspects = m.score_text("인증토큰을 빼서 메신저에 로그인하고 비밀번호를 반복 입력하여 잠금 해제하였다.", profile)

        self.assertNotIn("query_surface_terms", broad_aspects)
        self.assertLess(broad_score, 10)
        self.assertIn("token_messenger_access", specific_aspects)
        self.assertGreater(specific_score, broad_score)

    def test_repeated_attempt_verification_rejects_voluntary_password_entry_keyword_match(self):
        raw = (
            "접근권한 집행 중 관련자가 대리인에게 전화하려고 장치 비밀번호를 직접 입력하여 "
            "잠금 해제를 하자 검토기관이 이를 제지하였다. 이후 디지털 기술분석은 진행되었지만 "
            "검토기관이 비밀번호를 반복 대입하거나 추측하여 잠금을 푼 사실은 없다."
        )
        record = m.SourceRecord(
            source_id="case.txt::file",
            path="/tmp/case.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=500, overlap_chars=0)))
        profile = m.build_query_profile("장치 비밀번호 반복시도 반복 입력 잠금 해제")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        self.assertIn("phone_password_repeated_attempt", candidate.matched_aspects)
        self.assertNotIn("phone_password_repeated_attempt", candidate.verified_aspects)
        self.assertEqual(
            candidate.aspect_verifications["phone_password_repeated_attempt"]["status"],
            "candidate_only_rejected",
        )

    def test_repeated_attempt_verification_rejects_portal_account_password_automation(self):
        raw = (
            "대상자들은 장치 인증토큰과 포털 아이디를 수집하고 포털 사이트 계정 비밀번호를 "
            "저장해 둔 뒤 프로그램으로 뉴스 댓글을 반복 입력하였다. 검토기관은 확보한 자료를 분석하여 "
            "메신저서비스 계정 접속 내역과 비밀번호 목록을 확인하였다."
        )
        record = m.SourceRecord(
            source_id="domain_record_1.parquet::row:10887",
            path="/tmp/domain_record_1.parquet",
            source_type="parquet",
            ordinal=10887,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=500, overlap_chars=0)))
        profile = m.build_query_profile("장치 비밀번호 반복시도 반복 입력 잠금 해제")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        self.assertIn("phone_password_repeated_attempt", candidate.matched_aspects)
        self.assertNotIn("phone_password_repeated_attempt", candidate.verified_aspects)
        self.assertEqual(
            candidate.aspect_verifications["phone_password_repeated_attempt"]["method_label"],
            "account_password_not_phone_passcode",
        )

    def test_repeated_attempt_verification_rejects_technical_audit_form_template_passcode_value(self):
        raw = (
            "현장조직 범죄검토규칙 별지 서식 디지털기술분석 현장 조사 확인서. "
            "▣ 확보 대상 장치 정보: 삼성 스마트폰. "
            "▣ 화면 잠금해제 입력값 / 비밀번호 : 1234. "
            "▣ 백업 비밀번호 : 123456. "
            "▣ 디지털 기술분석 분석 의뢰 및 확보물 처리."
        )
        record = m.SourceRecord(
            source_id="03_distressed_korean_domain/6.parquet::row:16596",
            path="/tmp/6.parquet",
            source_type="parquet",
            ordinal=16596,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=600, overlap_chars=0)))
        profile = m.build_query_profile("장치 비밀번호 반복시도 반복 입력 잠금 해제")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        self.assertIn("phone_password_repeated_attempt", candidate.matched_aspects)
        self.assertNotIn("phone_password_repeated_attempt", candidate.verified_aspects)
        self.assertEqual(
            candidate.aspect_verifications["phone_password_repeated_attempt"]["method_label"],
            "technical_audit_form_template_or_disclosed_passcode_value",
        )

    def test_token_verification_rejects_sim_box_or_messenger_fraud_without_seized_login(self):
        raw = (
            "대상자들은 해외에서 보이스피싱에 사용할 SIM-BOX와 인증토큰을 개통하고 메신저 메신저로 "
            "공범에게 인증번호와 계정 정보를 전달하였다. 검토기관은 사후에 자료확보으로 장비를 "
            "확인했지만, 확보한 인증토큰을 빼서 메신저에 로그인한 검토기법은 기재되어 있지 않다."
        )
        record = m.SourceRecord(
            source_id="domain_record_1.parquet::row:30410",
            path="/tmp/domain_record_1.parquet",
            source_type="parquet",
            ordinal=30410,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=500, overlap_chars=0)))
        profile = m.build_query_profile("검토기관 인증토큰 메신저 로그인")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        self.assertIn("token_messenger_access", candidate.matched_aspects)
        self.assertNotIn("token_messenger_access", candidate.verified_aspects)
        self.assertEqual(
            candidate.aspect_verifications["token_messenger_access"]["method_label"],
            "not_operator_acquired_token_access",
        )

    def test_token_verification_rejects_debt_collection_evidence_list_false_positive(self):
        raw = (
            "증거의 요지에는 메신저 대화내용, 메신저서비스 회신 자료, "
            "디지털 증거 분석 결과보고서, 대상자 B이 구매하여 사용한 인증토큰과 "
            "장치 사진 확인 자료가 기재되어 있다. 이 목록은 인증토큰을 별도 장치에 "
            "장착해 메신저에 접속한 실행 사실을 설명하지 않는다."
        )
        record = m.SourceRecord(
            source_id="08_local_record_archive_local/군/군 기록/전주record_org-record-0004_extracted.txt::file",
            path="/tmp/전주record_org-record-0004_extracted.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=500, overlap_chars=0)))
        profile = m.build_query_profile("검토기관 인증토큰 메신저 로그인")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        self.assertIn("token_messenger_access", candidate.matched_aspects)
        self.assertNotIn("token_messenger_access", candidate.verified_aspects)
        self.assertEqual(
            candidate.aspect_verifications["token_messenger_access"]["method_label"],
            "not_operator_acquired_token_access",
        )

    def test_token_verification_accepts_investigator_seized_token_messenger_access(self):
        raw = (
            "검토팀은 접근권한에 따라 관련자의 장치 인증토큰을 확보한 후 별도의 공기계에 "
            "꽂고, 그 공기계를 통해 텔레그램 및 메신저 PC 버전에 접속하여 대화내용을 확인하였다."
        )
        record = m.SourceRecord(
            source_id="record_org_a_record_0001.txt::file",
            path="/tmp/record_org_a_record_0001.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=500, overlap_chars=0)))
        profile = m.build_query_profile("검토기관 인증토큰 메신저 로그인")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        self.assertIn("token_messenger_access", candidate.verified_aspects)
        self.assertEqual(
            candidate.aspect_verifications["token_messenger_access"]["method_label"],
            "operator_acquired_token_messenger_access",
        )

    def test_token_verification_rejects_plan_only_access_prediction(self):
        raw = (
            "검토팀은 인증토큰을 확보하면 별도 공기계에 꽂아 메신저에 접속하여 "
            "대화내용을 확인할 수 있을 것이라고 판단하였다. 이후 실제 집행 여부는 기록에 없다."
        )
        record = m.SourceRecord(
            source_id="plan_only.txt::file",
            path="/tmp/plan_only.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=500, overlap_chars=0)))
        profile = m.build_query_profile("검토기관 인증토큰 메신저 로그인")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        self.assertIn("token_messenger_access", candidate.matched_aspects)
        self.assertNotIn("token_messenger_access", candidate.verified_aspects)
        verification = candidate.aspect_verifications["token_messenger_access"]
        self.assertEqual(verification["status"], "candidate_only_rejected")
        self.assertEqual(verification["method_label"], "planned_or_predicted_token_access_not_actual_execution")

    def test_token_verification_support_window_uses_decisive_execution_span(self):
        raw = (
            "검토팀은 인증토큰을 확보하면 메신저 및 텔레그램 통신 내용을 확인할 수 있을 것이라고 생각하였다. "
            + ("배경 설명 " * 180)
            + "관련자 측은 절차에 동의하지 않고 퇴실하였다. "
            "검토팀은 관련자가 퇴실한 후 참여인을 두고 관련자 장치의 인증토큰을 공기계에 삽입하여 "
            "텔레그램 및 메신저 대화내용을 확인하는 방식으로 접근권한을 진행하였다. "
            "텔레그램은 2단계 비밀번호를 풀지 못하여 확인하지 못하였고, 메신저은 인증을 통해 성공하였으나 "
            "권한범위에 기재된 범위 내의 대화내용을 발견하지 못하였다."
        )
        record = m.SourceRecord(
            source_id="record_org_a_record_0001.txt::file",
            path="/tmp/record_org_a_record_0001.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=5000, overlap_chars=0)))
        profile = m.build_query_profile("검토기관 인증토큰 메신저 로그인")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        verification = candidate.aspect_verifications["token_messenger_access"]
        self.assertEqual(verification["status"], "verified_answer_evidence")
        self.assertIn("메신저은 인증을 통해 성공", verification["supporting_text"])
        self.assertGreater(verification["supporting_span"]["char_start"], raw.index("검토팀은 관련자가 퇴실한 후") - 50)

    def test_technical_audit_passcode_attempt_without_method_is_negative_frontier_not_verified(self):
        raw = (
            "검토팀은 확보한 관련자의 장치에 대해 디지털 기술분석을 시도하였으나 장치의 "
            "비밀번호를 알아내지 못하여 저장된 정보를 확인하지 못하였다. 관련자에게 장치와 "
            "비밀번호를 알려 줄 것을 요청하였으나 관련자는 이를 거부하였다."
        )
        record = m.SourceRecord(
            source_id="record_org_a_record_0001.txt::file",
            path="/tmp/record_org_a_record_0001.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=500, overlap_chars=0)))
        profile = m.build_query_profile("장치 비밀번호 기술분석 반복시도 반복 대입")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        self.assertIn("phone_password_repeated_attempt", candidate.matched_aspects)
        self.assertNotIn("phone_password_repeated_attempt", candidate.verified_aspects)
        self.assertEqual(
            candidate.aspect_verifications["phone_password_repeated_attempt"]["method_label"],
            "technical_audit_attempt_without_cracking_or_guessing_method",
        )

    def test_repeated_attempt_verification_accepts_investigator_repeated_phone_passcode_attempt(self):
        raw = (
            "검토자은 확보한 장치의 비밀번호를 알 수 없자 0000, 1234 등 짧은 암호를 여러 차례 "
            "대입하는 방식으로 잠금 해제를 시도한 뒤 디지털 기술분석으로 대화내용을 추출하였다."
        )
        record = m.SourceRecord(
            source_id="case.txt::file",
            path="/tmp/case.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=500, overlap_chars=0)))
        profile = m.build_query_profile("장치 비밀번호 반복시도 반복 입력 잠금 해제")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        self.assertIn("phone_password_repeated_attempt", candidate.verified_aspects)
        verification = candidate.aspect_verifications["phone_password_repeated_attempt"]
        self.assertEqual(verification["status"], "verified_answer_evidence")
        self.assertIn("여러 차례", verification["supporting_text"])

    def test_repeated_attempt_verification_rejects_suspect_random_entry_during_warrant(self):
        raw = (
            "검토기관은 대상자의 장치(아이폰 12폰)에 대한 접근권한을 집행하였다. "
            "담당 검토자이 대상자에게 장치 비밀번호를 알려달라고 요청하였으나 대상자은 "
            "비밀번호가 기억나지 않는다고 하였다. 대상자이 직접 비밀번호를 해제하겠다고 하면서 "
            "장치을 건네받은 후 장치을 탁자 아래로 내려 보이지 않게 숨긴 뒤 비밀번호를 푸는 척하며 "
            "불특정 숫자를 2번을 번갈아 가며 수십 회 반복하였다. 이후 기술분석 과정에서도 3차례나 "
            "6자리 번호를 눌렀지만 모두 비밀번호 오류로 장치이 잠겼다."
        )
        record = m.SourceRecord(
            source_id="record_org_b_record_0002.txt::file",
            path="/tmp/record_org_b_record_0002.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=1000, overlap_chars=0)))
        profile = m.build_query_profile("검토기관 장치 비밀번호 반복시도 반복 대입")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)

        self.assertIn("phone_password_repeated_attempt", candidate.matched_aspects)
        self.assertNotIn("phone_password_repeated_attempt", candidate.verified_aspects)
        verification = candidate.aspect_verifications["phone_password_repeated_attempt"]
        self.assertEqual(verification["status"], "candidate_only_rejected")
        self.assertEqual(verification["method_label"], "suspect_or_user_random_entry_not_investigator_repeated_attempt")

    def test_coverage_uses_verified_evidence_before_answering_repeated_attempt(self):
        raw = (
            "접근권한 집행 중 관련자가 장치 비밀번호를 직접 입력하여 잠금 해제하였다. "
            "검토팀은 메신저 대화내용을 확인하려 하였고 적법 절차를 주장하였다."
        )
        record = m.SourceRecord(
            source_id="case.txt::file",
            path="/tmp/case.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=500, overlap_chars=0)))
        profile = m.build_query_profile("장치 비밀번호 반복 입력 잠금 해제")
        score, terms, aspects = m.score_text(chunk.text, profile)
        candidate = m.build_candidate(chunk, profile, score, terms, aspects)
        manifest = {
            "records_seen": 1,
            "chunks_scored": 1,
            "candidate_count_seen": 1,
            "source_type_counts": {"text": 1},
            "source_type_candidate_counts": {"text": 1},
        }

        coverage = m.build_coverage([candidate], manifest, profile)

        status = coverage["aspect_status"]["phone_password_repeated_attempt"]
        self.assertEqual(status["status"], "candidate_frontier_only_unverified")
        self.assertEqual(status["verified_answer_evidence_count"], 0)
        self.assertIn("phone_password_repeated_attempt", coverage["negative_search_report"])
        reviews = coverage["negative_search_report"]["phone_password_repeated_attempt"]["rejected_candidate_reviews"]
        self.assertEqual(reviews[0]["source_id"], "case.txt::file")
        self.assertIn("method_label", reviews[0])
        self.assertIn("why_insufficient", reviews[0])
        self.assertIn("closest_grounded_window", reviews[0])

    def test_select_unique_candidates_keeps_highest_scoring_candidate_per_source(self):
        def candidate(source_id, chunk_id, score):
            return m.Candidate(
                score=score,
                candidate_id=f"cand-{chunk_id}",
                source_id=source_id,
                chunk_id=chunk_id,
                path=f"/tmp/{source_id}",
                source_type="text",
                ordinal=1,
                chunk_span={"char_start": 0, "char_end": 1, "chunk_index": 0},
                metadata={},
                content_hash=chunk_id,
                matched_terms=[],
                matched_aspects=[],
                snippets={},
                evidence_spans=[],
                relation_evidence=[],
                conclusion_labels=[],
            )

        selected = m.select_unique_source_candidates(
            [
                candidate("same::file", "low", 1.0),
                candidate("other::file", "other", 2.0),
                candidate("same::file", "high", 3.0),
            ],
            top_k=2,
        )

        self.assertEqual([item.chunk_id for item in selected], ["high", "other"])
        self.assertEqual(len({item.source_id for item in selected}), len(selected))

    def test_select_unique_candidates_prefers_actual_execution_over_plan_only_same_source(self):
        def candidate(chunk_id, score, actual_execution):
            return m.Candidate(
                score=score,
                candidate_id=f"cand-{chunk_id}",
                source_id="same::file",
                chunk_id=chunk_id,
                path="/tmp/same.txt",
                source_type="text",
                ordinal=1,
                chunk_span={"char_start": 0, "char_end": 100, "chunk_index": 0 if chunk_id == "plan" else 1},
                metadata={},
                content_hash="same",
                matched_terms=[],
                matched_aspects=["token_messenger_access"],
                snippets={},
                evidence_spans=[],
                relation_evidence=[],
                conclusion_labels=[],
                verified_aspects=["token_messenger_access"],
                aspect_verifications={
                    "token_messenger_access": {
                        "status": "verified_answer_evidence",
                        "actual_execution": actual_execution,
                    }
                },
            )

        selected = m.select_unique_source_candidates(
            [candidate("plan", 200.0, False), candidate("actual", 50.0, True)],
            top_k=1,
        )

        self.assertEqual(selected[0].chunk_id, "actual")

    def test_select_diverse_candidates_preserves_low_score_aspect_frontier(self):
        def candidate(name, score, aspects):
            return m.Candidate(
                score=score,
                candidate_id=f"cand-{name}",
                source_id=f"{name}::file",
                chunk_id=f"{name}::file::span:0-100",
                path=f"/tmp/{name}.txt",
                source_type="text",
                ordinal=1,
                chunk_span={"char_start": 0, "char_end": 100, "chunk_index": 0},
                metadata={},
                content_hash=hashlib.sha256(name.encode("utf-8")).hexdigest(),
                matched_terms=[],
                matched_aspects=aspects,
                snippets={},
                evidence_spans=[],
                relation_evidence=[],
                conclusion_labels=[],
            )

        selected = m.select_diverse_candidates(
            [
                candidate("token_high_1", 100.0, ["token_messenger_access"]),
                candidate("token_high_2", 95.0, ["token_messenger_access"]),
                candidate("brute_low", 10.0, ["phone_password_repeated_attempt"]),
            ],
            top_k=2,
            aspects=["token_messenger_access", "phone_password_repeated_attempt"],
            min_per_aspect=1,
        )

        self.assertEqual({item.source_id for item in selected}, {"token_high_1::file", "brute_low::file"})

    def test_negative_report_closest_ids_are_ranked_subset_not_frontier_copy(self):
        def candidate(source_id, score):
            return m.Candidate(
                score=score,
                candidate_id=f"cand-{source_id}",
                source_id=f"{source_id}::file",
                chunk_id=f"{source_id}::file::span:0-100",
                path=f"/tmp/{source_id}.txt",
                source_type="text",
                ordinal=int(source_id.replace("case", "")),
                chunk_span={"char_start": 0, "char_end": 100, "chunk_index": 0},
                metadata={},
                content_hash=hashlib.sha256(source_id.encode("utf-8")).hexdigest(),
                matched_terms=["장치", "비밀번호", "4자리"],
                matched_aspects=["phone_model_password_conditions"],
                snippets={"phone_model_password_conditions": "장치 비밀번호 4자리 조건만 언급"},
                evidence_spans=[],
                relation_evidence=[],
                conclusion_labels=[],
                aspect_verifications={},
            )

        profile = m.build_query_profile(
            "장치 비밀번호 반복시도",
            {
                "query_adapters": {
                    "aspects": {
                        "phone_model_password_conditions": [
                            ["구형", "오래된", "최신", "4자리", "네 자리", "짧은", "비밀번호", "패턴", "잠금"],
                            ["장치", "장치", "스마트폰", "장치"],
                        ]
                    }
                }
            },
        )
        candidates = [candidate(f"case{idx}", 100 - idx) for idx in range(1, 8)]
        manifest = {
            "records_seen": 7,
            "chunks_scored": 7,
            "candidate_count_seen": 7,
            "source_type_counts": {"text": 7},
            "source_type_candidate_counts": {"text": 7},
        }

        coverage = m.build_coverage(candidates, manifest, profile)

        report = coverage["negative_search_report"]["phone_model_password_conditions"]
        self.assertNotEqual(report["closest_source_ids"], report["candidate_frontier_source_ids"])
        self.assertLess(len(report["closest_source_ids"]), len(report["candidate_frontier_source_ids"]))
        self.assertIn("closest_selection_policy", report)

    def test_evaluation_guard_does_not_pass_unexplained_validness_conflict(self):
        coverage = {
            "schema": "coverage_report_v2",
            "aspect_status": {
                "token_messenger_access": {
                    "status": "verified_answer_evidence_found",
                    "verified_answer_evidence_count": 1,
                },
                "phone_password_repeated_attempt": {
                    "status": "candidate_frontier_only_unverified",
                    "verified_answer_evidence_count": 0,
                },
            },
            "negative_search_report": {
                "phone_password_repeated_attempt": {
                    "rejected_candidate_reviews": [{"source_id": "x", "why_insufficient": "not a phone passcode method"}],
                }
            },
            "contradiction_checks": {
                "valid_unvalid_conflict": "both_valid_and_unvalid_language_present_in_frontier_review_required",
            },
        }

        evaluation = m.build_evaluation_payload(["a::file"], {}, coverage)

        self.assertFalse(evaluation["all_pass"])
        self.assertIn("CORE_CONCLUSION_GROUNDED", evaluation["fail_tags"])
        self.assertNotIn("DOMAIN_CONCLUSION_CONFLICT_NOT_EXPLAINED", evaluation["fail_tags"])
        self.assertIn("DOMAIN_CONCLUSION_CONFLICT_NOT_EXPLAINED", {detail["code"] for detail in evaluation["failure_details"]})
        self.assertNotIn("ERROR_PROPAGATION_GUARD", evaluation["pass_tags"])
        self.assertNotIn("CORE_CONCLUSION_GROUNDED", evaluation["pass_tags"])

    def test_evaluation_keeps_bounded_negative_repeated_attempt_query_open(self):
        coverage = {
            "schema": "coverage_report_v2",
            "aspect_status": {
                "token_messenger_access": {
                    "status": "verified_answer_evidence_found",
                    "verified_answer_evidence_count": 1,
                },
                "phone_password_repeated_attempt": {
                    "status": "candidate_frontier_only_unverified",
                    "verified_answer_evidence_count": 0,
                    "strict_repeated_attempt_or_guessing_count": 0,
                },
            },
            "negative_search_report": {
                "phone_password_repeated_attempt": {
                    "scan_scope": {"bounded": True, "bounded_limits": {"max_json_records": 60000}},
                    "closest_source_ids": ["case.txt::file"],
                    "rejected_candidate_reviews": [
                        {
                            "source_id": "case.txt::file",
                            "why_insufficient": "suspect entered the passcode, not investigators",
                            "closest_grounded_window": "대상자이 직접 비밀번호를 반복 입력",
                        }
                    ],
                },
                "token_messenger_direct_validness": {
                    "verified_technique_source_ids": ["token-case.txt::file"],
                    "rejected_candidate_reviews": [
                        {
                            "source_id": "token-case.txt::file",
                            "why_insufficient": "domain conclusion is not bound to the TOKEN method",
                            "closest_grounded_window": "인증토큰 메신저",
                        }
                    ],
                },
            },
            "contradiction_checks": {
                "valid_unvalid_conflict": "both_valid_and_unvalid_language_present_in_frontier_review_required",
            },
        }

        evaluation = m.build_evaluation_payload(["a::file"], {}, coverage)

        self.assertFalse(evaluation["all_pass"])
        self.assertIn("QUERY_ANSWERED", evaluation["fail_tags"])
        self.assertIn("BOUNDED_NEGATIVE_SEARCH_NOT_CORPUS_WIDE", {detail["code"] for detail in evaluation["failure_details"]})

    def test_evaluation_guard_rejects_generic_negative_search_reviews(self):
        coverage = {
            "schema": "coverage_report_v2",
            "aspect_status": {
                "token_messenger_access": {
                    "status": "verified_answer_evidence_found",
                    "verified_answer_evidence_count": 1,
                },
                "phone_password_repeated_attempt": {
                    "status": "candidate_frontier_only_unverified",
                    "verified_answer_evidence_count": 0,
                },
            },
            "negative_search_report": {
                "phone_password_repeated_attempt": {
                    "rejected_candidate_reviews": [
                        {
                            "source_id": "case.txt::file",
                            "why_insufficient": "The source is a close candidate, but no aspect verifier promoted it to source-grounded answer evidence.",
                            "closest_grounded_window": "비밀번호",
                        }
                    ],
                },
                "token_messenger_direct_validness": {
                    "rejected_candidate_reviews": [
                        {
                            "source_id": "case2.txt::file",
                            "why_insufficient": "domain terms concern a different act",
                            "closest_grounded_window": "인증토큰 메신저",
                        }
                    ],
                },
            },
            "contradiction_checks": {
                "valid_unvalid_conflict": "both_valid_and_unvalid_language_present_in_frontier_review_required",
            },
        }

        evaluation = m.build_evaluation_payload(["a::file"], {}, coverage)

        self.assertFalse(evaluation["all_pass"])
        self.assertIn("ERROR_PROPAGATION_GUARD", evaluation["fail_tags"])
        self.assertNotIn("NEGATIVE_SEARCH_REVIEW_GENERIC:phone_password_repeated_attempt", evaluation["fail_tags"])
        self.assertIn("NEGATIVE_SEARCH_REVIEW_GENERIC", {detail["code"] for detail in evaluation["failure_details"]})
        self.assertNotIn("ERROR_PROPAGATION_GUARD", evaluation["pass_tags"])

    def test_json_loader_raw_prefilter_skips_irrelevant_file_before_parsing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "07_source_api_fulltext" / "case.json"
            source.parent.mkdir(parents=True)
            source.write_text('{"기록내용": "노동 사건 기록"}', encoding="utf-8")

            records = list(m.iter_json_records(root, source, source.relative_to(root), raw_prefilter_terms=["인증토큰"]))

        self.assertEqual(records, [])

    def test_json_loader_raw_prefilter_filters_list_items_after_page_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "06_source_api_api" / "page_1.json"
            source.parent.mkdir(parents=True)
            source.write_text(
                json.dumps(
                    [
                        {"id": "1", "사건명": "노동 사건"},
                        {"id": "2", "사건명": "장치 비밀번호 사건"},
                    ],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            records = list(m.iter_json_records(root, source, source.relative_to(root), raw_prefilter_terms=["비밀번호"]))

        self.assertEqual([record.metadata["id"] for record in records], ["2"])

    def test_json_raw_prefilter_terms_can_be_limited_to_high_signal_aspects(self):
        profile = m.build_query_profile("검토기관 장치 인증토큰 메신저 로그인 패스워드 반복시도 합법")

        terms = m.json_raw_prefilter_terms(
            profile,
            {
                "json_raw_prefilter": True,
                "json_raw_prefilter_aspects": ["token_messenger_access", "phone_password_repeated_attempt"],
            },
        )

        self.assertIn("인증토큰", terms)
        self.assertIn("비밀번호", terms)
        self.assertNotIn("참여", terms)
        self.assertNotIn("범위", terms)
        self.assertNotIn("합법", terms)

    def test_targeted_json_probe_recovers_candidate_beyond_broad_json_cap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            api = root / "06_source_api_api"
            api.mkdir(parents=True)
            (api / "page_1.json").write_text(
                json.dumps({"id": "broad-cap", "기록내용": "인증토큰 메신저 로그인 일반 후보"}, ensure_ascii=False),
                encoding="utf-8",
            )
            (api / "page_2.json").write_text(
                json.dumps(
                    {
                        "id": "strict-passcode",
                        "기록내용": (
                            "검토자은 확보한 장치의 비밀번호를 알 수 없자 0000과 1234를 "
                            "여러 차례 대입하는 방식으로 잠금 해제를 시도하였다."
                        ),
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            policy = {
                "scan_parquet": False,
                "scan_text": False,
                "scan_json": True,
                "json_source_dirs": ["06_source_api_api"],
                "max_json_records": 1,
                "min_record_score": 0,
                "min_chunk_score": 0,
                "top_k": 5,
                "candidate_heap_k": 5,
                "targeted_probes": [
                    {
                        "name": "strict_phone_passcode_probe",
                        "aspect": "phone_password_repeated_attempt",
                        "source_types": ["json"],
                        "json_source_dirs": ["06_source_api_api"],
                        "term_groups": [["장치"], ["비밀번호"], ["대입"]],
                        "regexes": ["장치.{0,160}비밀번호.{0,160}대입"],
                        "max_records": 10,
                        "candidate_boost": 30.0,
                    }
                ],
            }

            candidates, manifest, _ = m.run_search(root, "장치 비밀번호 반복 대입", policy, root / "out")

        strict = [
            candidate
            for candidate in candidates
            if candidate.source_id == "06_source_api_api/page_2.json::record"
            and "phone_password_repeated_attempt" in candidate.verified_aspects
        ]
        self.assertEqual(len(strict), 1)
        self.assertGreaterEqual(manifest["targeted_probe_summary"]["records_matched"], 1)
        self.assertGreaterEqual(manifest["targeted_probe_summary"]["candidates_added"], 1)

    def test_passcode_unlock_validness_is_verified_separately_from_repeated_attempt(self):
        raw = (
            "검토기관은 긴급체포 과정에서 대상자 D에게 장치 비밀번호의 해제를 요구하였고, "
            "대상자 D은 검토기관의 요구에 따라 어쩔 수 없이 비밀번호를 해제한 것으로 보일 뿐 "
            "자발적인 의사에 따라 해제하였다고 평가할 수 없다. 이후 장치 전자정보를 "
            "취득하는 과정은 위법하다고 보아 증거능력이 없다고 판단하였다."
        )
        record = m.SourceRecord(
            source_id="record_org_c_record_0003.txt::file",
            path="/tmp/record_org_c_record_0003.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=1000, overlap_chars=0)))
        profile = m.build_query_profile(
            "장치 비밀번호 해제 위법 증거능력",
            {
                "query_adapters": {
                    "aspects": {
                        "device_passcode_unlock_validity": [
                            ["장치", "장치", "스마트폰", "아이폰"],
                            ["비밀번호", "암호", "잠금", "패턴"],
                            ["해제", "입력", "요구", "풀"],
                            ["위법", "증거능력", "자발", "강압", "어쩔 수 없이"],
                        ]
                    }
                }
            },
        )
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)
        coverage = m.build_coverage(
            [candidate],
            {
                "records_seen": 1,
                "chunks_scored": 1,
                "candidate_count_seen": 1,
                "source_type_counts": {"text": 1},
                "source_type_candidate_counts": {"text": 1},
            },
            profile,
        )

        self.assertIn("device_passcode_unlock_validity", candidate.verified_aspects)
        verification = candidate.aspect_verifications["device_passcode_unlock_validity"]
        self.assertEqual(verification["status"], "verified_answer_evidence")
        self.assertEqual(verification["method_label"], "compelled_or_requested_device_passcode_unlock_validity")
        self.assertNotIn("phone_password_repeated_attempt", candidate.verified_aspects)
        self.assertEqual(
            coverage["aspect_status"]["device_passcode_unlock_validity"]["verified_answer_evidence_count"],
            1,
        )
        self.assertEqual(
            coverage["aspect_status"]["phone_password_repeated_attempt"]["strict_repeated_attempt_or_guessing_count"],
            0,
        )

    def test_token_validness_context_is_bound_to_verified_execution_source(self):
        raw = (
            "검토팀은 접근권한에 따라 관련자의 장치 인증토큰을 확보한 후 별도의 공기계에 "
            "꽂고, 그 공기계를 통해 텔레그램 및 메신저 PC 버전에 접속하여 대화내용을 확인하였다. "
            "판정기관은 위 접근권한에 기재된 방법과 범위 안에서 진행된 분석이어서 위법하지 않다고 판단하였다."
        )
        record = m.SourceRecord(
            source_id="direct-token-validness.txt::file",
            path="/tmp/direct-token-validness.txt",
            source_type="text",
            ordinal=1,
            raw_text=raw,
            metadata={},
            content_hash=hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        )
        chunk = next(iter(m.split_record(record, chunk_chars=1000, overlap_chars=0)))
        profile = m.build_query_profile("검토기관 인증토큰 메신저 로그인 접근권한 위법")
        score, terms, aspects = m.score_text(chunk.text, profile)

        candidate = m.build_candidate(chunk, profile, score, terms, aspects)
        coverage = m.build_coverage(
            [candidate],
            {
                "records_seen": 1,
                "chunks_scored": 1,
                "candidate_count_seen": 1,
                "source_type_counts": {"text": 1},
                "source_type_candidate_counts": {"text": 1},
            },
            profile,
        )

        self.assertIn("token_messenger_access", candidate.verified_aspects)
        self.assertIn("validity_conditions", candidate.verified_aspects)
        self.assertEqual(
            candidate.aspect_verifications["validity_conditions"]["method_label"],
            "same_source_token_messenger_warrant_validness_context",
        )
        self.assertEqual(
            coverage["contradiction_checks"]["direct_token_validness_source_ids"],
            ["direct-token-validness.txt::file"],
        )

    def test_default_policy_keeps_broad_negative_search_unbounded(self):
        policy = json.loads((Path(__file__).resolve().parent / "policy.json").read_text(encoding="utf-8"))

        self.assertEqual(policy.get("max_parquet_records", 0), 0)
        self.assertEqual(policy.get("max_json_records", 0), 0)
        self.assertEqual(policy.get("max_text_records", 0), 0)


if __name__ == "__main__":
    unittest.main()
