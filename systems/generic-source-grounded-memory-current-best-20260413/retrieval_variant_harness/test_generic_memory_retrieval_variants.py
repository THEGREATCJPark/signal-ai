import json
import importlib.util
from pathlib import Path

import pytest


def load_module():
    script_path = Path(__file__).resolve().with_name("evaluate_generic_memory_retrieval_variants.py")
    spec = importlib.util.spec_from_file_location("evaluate_generic_memory_retrieval_variants", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def synthetic_pack():
    return {
        "sources": [
            {
                "source_id": "domain_001",
                "domain": "domain",
                "kind": "record",
                "title": "장치 기술분석 기록",
                "summary": "자료확보과 디지털 증거의 증거능력에 관한 일반 법리",
                "raw_text": "검토기관은 확보한 장치에서 인증토큰을 분리한 뒤 별도 단말기에 장착하여 메신저에 로그인하고 대화 내용을 확인하였다. 대상자은 이 절차가 권한범위 범위를 벗어난 위법한 검토라고 다투었다.",
                "raw_ref": "domain/case_001.txt",
            },
            {
                "source_id": "domain_002",
                "domain": "domain",
                "kind": "record",
                "title": "구형 장치 비밀번호 해제 기록",
                "summary": "디지털 기술분석 과정에서 확보물 분석 절차의 적법성이 문제된 사안",
                "raw_text": "감정인은 최신 기기가 아닌 구형 장치에 대하여 네 자리 비밀번호 후보를 제한된 범위에서 순차 대입하는 방식으로 잠금을 해제하였다. 판정기관은 권한범위에 기재된 확보물 분석 범위와 절차 통제 여부를 보아 판단하였다.",
                "raw_ref": "domain/case_002.txt",
            },
            {
                "source_id": "course_syllabus",
                "domain": "course",
                "kind": "syllabus",
                "title": "회로이론 강의계획서",
                "summary": "회로이론 평가 방식 안내",
                "raw_text": "중간고사는 2026년 4월 22일 수요일에 실시한다. 시험범위는 1주차부터 6주차 테브난 등가회로, 중첩정리, 정상상태 AC 회로 전까지이다.",
                "raw_ref": "course/syllabus.md",
            },
            {
                "source_id": "course_week6_video",
                "domain": "course",
                "kind": "video_transcript",
                "title": "회로이론 6주차 강의영상",
                "summary": "테브난 등가회로와 RTH 풀이",
                "raw_text": "문제 A: VOC와 RTH를 구하라. 교수 설명: 먼저 독립전원을 끄고 입력 단자에서 본 등가저항을 구합니다. 이 부분은 시험에 그대로 응용해서 낼 수 있으니 반드시 표시해 두세요. 교수의 출제 성향은 단순 암기보다 회로를 변형해 등가회로를 만드는 과정을 본다는 점입니다.",
                "raw_ref": "course/week6/transcript.txt",
            },
            {
                "source_id": "course_week6_recording",
                "domain": "course",
                "kind": "audio_transcript",
                "title": "GDrive 6주차 회로이론 녹음본",
                "summary": "6주차 보조 녹음",
                "raw_text": "아까 영상에서 본 그 회로를 다시 보면 VOC를 먼저 잡고 RTH는 입력 단자 기준으로 봅니다. 녹음본은 course_week6_video 자료를 보고 말한 것입니다.",
                "raw_ref": "G:/소통용/6주차/recording.m4a.txt",
            },
        ],
        "relations": [
            {
                "relation_id": "rel_recording_week6",
                "source_id": "course_week6_recording",
                "target_id": "course_week6_video",
                "kind": "recording_supports_video",
                "text": "GDrive 6주차 녹음본은 회로이론 6주차 강의영상 자료를 보고 말한 보조 녹음이다.",
            }
        ],
        "problems": [
            {
                "problem_id": "prob_voc_rth",
                "source_id": "course_week6_video",
                "question_text": "문제 A: VOC와 RTH를 구하라.",
                "official_explanation": "먼저 독립전원을 끄고 입력 단자에서 본 등가저항을 구합니다.",
                "local_meta": ["시험에 그대로 응용해서 낼 수 있으니 반드시 표시해 두세요."],
            }
        ],
        "visual_atoms": [
            {
                "visual_atom_id": "vis_voc_rth",
                "source_id": "course_week6_video",
                "kind": "circuit_image",
                "image_path": "course/week6/images/voc_rth_crop.png",
                "text_hint": "VOC와 RTH를 구하는 테브난 등가회로 회로 사진",
            }
        ],
    }


def test_summary_only_misses_buried_domain_fact_but_raw_and_fusion_find_it():
    module = load_module()
    pack = synthetic_pack()
    contracts = module.default_synthetic_contracts()

    summary = module.evaluate_variant(pack, "summary_only", contracts)["contracts"]["domain_messenger"]
    raw = module.evaluate_variant(pack, "raw_leaf", contracts)["contracts"]["domain_messenger"]
    fusion = module.evaluate_variant(pack, "ultimate_rrf", contracts)["contracts"]["domain_messenger"]

    assert summary["passed"] is False
    assert raw["passed"] is True
    assert fusion["passed"] is True
    assert any("인증토큰" in hit["text"] and "메신저" in hit["text"] for hit in fusion["hits"])


def test_raw_leaf_misses_cross_source_relation_but_graph_and_fusion_find_it():
    module = load_module()
    pack = synthetic_pack()
    contracts = module.default_synthetic_contracts()

    raw = module.evaluate_variant(pack, "raw_leaf", contracts)["contracts"]["course_recording_relation"]
    graph = module.evaluate_variant(pack, "graph_relation", contracts)["contracts"]["course_recording_relation"]
    fusion = module.evaluate_variant(pack, "ultimate_rrf", contracts)["contracts"]["course_recording_relation"]

    assert raw["passed"] is False
    assert graph["passed"] is True
    assert fusion["passed"] is True
    assert fusion["hits"][0]["kind"] in {"relation", "rrf_fused"}


def test_ultimate_rrf_satisfies_course_and_domain_query_contracts(tmp_path):
    module = load_module()
    report = module.run_fixture_experiment(synthetic_pack(), out_dir=tmp_path)

    assert report["selected_variant"] == "ultimate_rrf"
    assert report["variants"]["summary_only"]["passed_count"] < report["variants"]["ultimate_rrf"]["passed_count"]
    assert report["variants"]["raw_leaf"]["contracts"]["course_recording_relation"]["passed"] is False
    assert "iterative_coverage_loop" in report["variants"]
    assert report["variants"]["iterative_coverage_loop"]["contracts"]["course_recording_relation"]["passed"] is True

    selected_contracts = report["variants"]["ultimate_rrf"]["contracts"]
    for contract_id in [
        "domain_messenger",
        "domain_passcode_repeated_attempt",
        "course_all_problems",
        "course_problem_explanation",
        "course_exam_scope",
        "course_exam_date",
        "course_circuit_image",
        "course_professor_important",
        "course_problem_tendency",
        "course_recording_relation",
    ]:
        assert selected_contracts[contract_id]["passed"] is True, contract_id

    assert (tmp_path / "retrieval_variant_report.json").exists()
    slim_report = json.loads((tmp_path / "retrieval_variant_report.slim.json").read_text(encoding="utf-8"))
    for variant_result in slim_report["variants"].values():
        for contract_result in variant_result["contracts"].values():
            for hit in contract_result["hits"]:
                assert "text" not in hit
    assert "ultimate_rrf" in (tmp_path / "RETRIEVAL_VARIANT_REPORT.md").read_text(encoding="utf-8")


def test_coverage_patch_reruns_only_missing_contracts():
    module = load_module()
    pack = synthetic_pack()
    contracts = module.default_synthetic_contracts()

    result = module.run_coverage_patch(pack, base_variant="raw_leaf", contracts=contracts)

    assert "course_recording_relation" in result["rerun_contract_ids"]
    assert "domain_messenger" not in result["rerun_contract_ids"]
    assert result["contracts"]["course_recording_relation"]["passed"] is True
    assert result["contracts"]["domain_messenger"]["passed"] is True


def test_coverage_patch_expands_rerun_query_with_missing_required_anchors():
    module = load_module()
    pack = synthetic_pack()
    contracts = [
        {
            "contract_id": "domain_messenger",
            "query": "검토기관 장치 사례",
            "required": ["인증토큰", "메신저"],
            "top_k": 1,
            "coverage_top_k": 8,
        }
    ]

    result = module.run_coverage_patch(pack, base_variant="summary_only", contracts=contracts)

    patched = result["contracts"]["domain_messenger"]
    assert patched["patched_from"] == "summary_only"
    assert "인증토큰" in patched["patch_query"]
    assert "메신저" in patched["patch_query"]


def test_iterative_coverage_loop_reduces_only_still_missing_contracts_with_anchor_expansion():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "plain_note",
                "domain": "generic",
                "kind": "note",
                "title": "평문 메모",
                "summary": "",
                "raw_text": "초기 결론 plain: 이미 바로 찾을 수 있는 사실입니다.",
                "raw_ref": "generic/plain.md",
            },
            {
                "source_id": "bridge_table",
                "domain": "generic",
                "kind": "table_note",
                "title": "브릿지 표",
                "summary": "다른 문서를 가리키는 표",
                "raw_text": "브릿지 표는 detail_doc_77을 참고합니다. 자세한 숨은 결론은 그 문서에 있습니다.",
                "raw_ref": "generic/bridge.md",
            },
            {
                "source_id": "detail_doc_77",
                "domain": "generic",
                "kind": "detail_note",
                "title": "상세 문서 77",
                "summary": "",
                "raw_text": "detail_doc_77: 은닉 사실은 인증토큰 분리 후 메신저 로그인 절차입니다.",
                "raw_ref": "generic/detail_77.md",
            },
        ]
    }
    contracts = [
        {
            "contract_id": "already_answered",
            "query": "초기 결론 plain",
            "required": ["초기 결론", "plain"],
        },
        {
            "contract_id": "vague_bridge_detail",
            "query": "브릿지 표가 가리키는 숨은 결론",
            "required": ["detail_doc_77", "인증토큰", "메신저"],
            "same_hit": True,
        },
    ]

    result = module.run_iterative_coverage_loop(
        pack,
        base_variant="raw_leaf",
        contracts=contracts,
        max_rounds=3,
    )

    assert result["initial_missing_contract_ids"] == ["vague_bridge_detail"]
    assert result["rounds"][0]["input_contract_ids"] == ["vague_bridge_detail"]
    assert result["rounds"][0]["newly_passed_contract_ids"] == ["vague_bridge_detail"]
    assert result["remaining_contract_ids"] == []
    assert result["contracts"]["already_answered"]["passed"] is True
    assert result["contracts"]["vague_bridge_detail"]["passed"] is True
    assert "already_answered" not in result["rerun_contract_ids"]
    assert result["rerun_contract_ids"] == ["vague_bridge_detail"]


def test_select_variant_uses_report_key_not_internal_patch_variant_name():
    module = load_module()
    results = {
        "ultimate_rrf": {
            "variant": "ultimate_rrf",
            "passed_count": 112,
            "total_count": 113,
        },
        "coverage_patch": {
            "variant": "coverage_patch_from_raw_leaf",
            "passed_count": 112,
            "total_count": 113,
        },
        "iterative_coverage_loop": {
            "variant": "iterative_coverage_loop_from_raw_leaf",
            "passed_count": 113,
            "total_count": 113,
        },
    }

    assert module.select_variant_key(results) == "iterative_coverage_loop"


def adversarial_pack():
    long_prefix = "배경 설명 " * 1200
    long_suffix = " 부가 설명" * 1200
    return {
        "sources": [
            {
                "source_id": "course_week1_scope",
                "domain": "course",
                "kind": "video_transcript",
                "title": "회로이론 1주차 강의영상",
                "week": 1,
                "summary": "초기 시험범위 안내",
                "raw_text": "중간고사 시험범위는 1주차부터 4주차까지라고 일단 생각하세요. 이후 변경될 수 있습니다.",
                "raw_ref": "course/week1/video.txt",
            },
            {
                "source_id": "course_week6_scope_correction",
                "domain": "course",
                "kind": "audio_transcript",
                "title": "GDrive 6주차 회로이론 녹음본",
                "week": 6,
                "summary": "중간고사 직전 보충 녹음",
                "raw_text": "정정합니다. 중간고사 시험범위는 1주차부터 6주차 테브난 등가회로까지입니다. 앞서 4주차까지라고 말한 것은 취소합니다.",
                "raw_ref": "G:/소통용/6주차/scope_correction.m4a.txt",
            },
            {
                "source_id": "course_week6_video",
                "domain": "course",
                "kind": "video_transcript",
                "title": "회로이론 6주차 강의영상",
                "week": 6,
                "summary": "테브난 등가회로 강의",
                "raw_text": "테브난 등가회로에서 VOC와 RTH를 구합니다. 중첩정리는 여기서 한 번 언급됩니다.",
                "raw_ref": "course/week6/video.txt",
            },
            {
                "source_id": "course_week6_recording",
                "domain": "course",
                "kind": "audio_transcript",
                "title": "GDrive 6주차 회로이론 녹음본 후보",
                "week": 6,
                "summary": "6주차 보조 녹음",
                "raw_text": "아까 본 영상 자료의 그 회로를 다시 설명합니다. VOC와 RTH를 다시 잡으세요.",
                "raw_ref": "G:/소통용/6주차/recording.m4a.txt",
            },
            {
                "source_id": "long_domain_case",
                "domain": "domain",
                "kind": "record",
                "title": "긴 기록 원문",
                "summary": "자료확보 일반론만 요약되어 있음",
                "raw_text": f"{long_prefix}숨은 사실: 검토기관은 인증토큰을 다른 단말기에 장착하여 메신저에 로그인하였다.{long_suffix}",
                "raw_ref": "domain/long_case.txt",
            },
            {
                "source_id": "course_dup_1",
                "domain": "course",
                "kind": "note",
                "title": "중첩정리 첫 언급",
                "week": 2,
                "summary": "",
                "raw_text": "중첩정리는 예제에서 한 번 언급됩니다.",
                "raw_ref": "course/dup1.txt",
            },
            {
                "source_id": "course_dup_2",
                "domain": "course",
                "kind": "note",
                "title": "중첩정리 둘째 언급",
                "week": 3,
                "summary": "",
                "raw_text": "중첩정리는 과제 해설에서 다시 언급됩니다.",
                "raw_ref": "course/dup2.txt",
            },
        ]
    }


def test_adversarial_contracts_force_chunking_relation_temporal_and_counting():
    module = load_module()
    pack = adversarial_pack()
    contracts = [
        {
            "contract_id": "long_case_chunked_buried_fact",
            "query": "긴 기록에서 인증토큰 메신저 로그인 숨은 사실",
            "required": ["숨은 사실", "인증토큰", "메신저", "로그인"],
        },
        {
            "contract_id": "auto_recording_video_relation",
            "query": "6주차 녹음본은 어떤 영상 자료를 보고 말한 것인지",
            "required": ["GDrive 6주차 회로이론 녹음본 후보", "회로이론 6주차 강의영상", "inferred_same_week_audio_video"],
            "same_hit": True,
        },
        {
            "contract_id": "latest_corrected_exam_scope",
            "query": "최신 정정된 중간고사 시험범위",
            "required": ["1주차부터 6주차", "4주차까지라고 말한 것은 취소"],
            "forbidden": ["1주차부터 4주차까지라고 일단"],
            "top_hit": True,
        },
        {
            "contract_id": "duplicate_count_superposition",
            "query": "중첩정리 몇 번 언급됐는지 중복 집계",
            "required": ["중첩정리", "mention_count=3"],
        },
    ]

    report = module.run_fixture_experiment(pack, contracts=contracts)

    assert report["selected_variant"] == "ultimate_rrf"
    for contract_id, result in report["variants"]["ultimate_rrf"]["contracts"].items():
        assert result["passed"] is True, contract_id
    assert report["variants"]["summary_only"]["passed_count"] < report["variants"]["ultimate_rrf"]["passed_count"]


def test_user_style_alias_queries_still_retrieve_raw_and_relation_facts():
    module = load_module()
    pack = synthetic_pack()
    contracts = [
        {
            "contract_id": "messenger_alias",
            "query": "카톡 접속 사례",
            "required": ["메신저", "로그인"],
        },
        {
            "contract_id": "phone_alias",
            "query": "장치 암호 순서대로 넣어서 푼 구형 사례",
            "required": ["구형 장치", "네 자리 비밀번호", "순차 대입"],
        },
        {
            "contract_id": "audio_alias_relation",
            "query": "6주차 음성 파일은 어느 원본 강의와 이어짐",
            "required": ["GDrive 6주차 녹음본", "회로이론 6주차 강의영상", "보조 녹음"],
            "same_hit": True,
        },
    ]

    report = module.run_fixture_experiment(pack, contracts=contracts)

    assert report["selected_variant"] == "ultimate_rrf"
    for contract_id, result in report["variants"]["ultimate_rrf"]["contracts"].items():
        assert result["passed"] is True, contract_id
    assert report["variants"]["summary_only"]["passed_count"] < report["variants"]["ultimate_rrf"]["passed_count"]


def generic_relation_pack():
    return {
        "sources": [
            {
                "source_id": "meeting_table_source",
                "domain": "generic",
                "kind": "slide_text",
                "title": "프로젝트 3주차 표 B",
                "week": 3,
                "summary": "비용 정확도 관계 그래프 실험표",
                "raw_text": "표 B: 비용, 정확도, 맥락 관계성, 그래프 연결 점수를 비교한 실험 결과표입니다.",
                "raw_ref": "project/week3/table_b.md",
            },
            {
                "source_id": "meeting_followup_note",
                "domain": "generic",
                "kind": "note",
                "title": "프로젝트 3주차 후속 메모",
                "week": 3,
                "summary": "표 B를 다시 언급한 메모",
                "raw_text": "아까 본 표 B를 기준으로 비용과 정확도를 다시 판단합니다. 이 자료는 앞서 본 프로젝트 3주차 표를 가리킵니다.",
                "raw_ref": "project/week3/followup.md",
            },
        ]
    }


def test_generic_relation_graph_infers_deictic_cross_source_edges_without_audio_video_labels():
    module = load_module()
    pack = generic_relation_pack()
    docs = module.normalize_pack(pack)["graph_relation"]

    assert any(
        doc["metadata"].get("kind") == "generic_candidate_refers_to"
        and doc["metadata"].get("source_id") == "meeting_followup_note"
        and doc["metadata"].get("target_id") == "meeting_table_source"
        for doc in docs
    )

    contracts = [
        {
            "contract_id": "generic_deictic_relation",
            "query": "후속 메모의 아까 본 표 B는 어떤 자료를 가리키는지",
            "required": ["generic_candidate_refers_to", "meeting_followup_note", "meeting_table_source", "표 B"],
            "same_hit": True,
        }
    ]
    report = module.run_fixture_experiment(pack, contracts=contracts)

    assert report["variants"]["graph_relation"]["contracts"]["generic_deictic_relation"]["passed"] is True
    assert report["variants"]["ultimate_rrf"]["contracts"]["generic_deictic_relation"]["passed"] is True


def test_generic_relation_graph_does_not_link_same_folder_sources_without_reference_signal():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "unrelated_week3_a",
                "domain": "generic",
                "kind": "note",
                "title": "프로젝트 3주차 실험 A",
                "week": 3,
                "summary": "비용 정확도 실험",
                "raw_text": "비용과 정확도 수치를 독립적으로 정리한 메모입니다.",
                "raw_ref": "project/week3/a.md",
            },
            {
                "source_id": "unrelated_week3_b",
                "domain": "generic",
                "kind": "note",
                "title": "프로젝트 3주차 실험 B",
                "week": 3,
                "summary": "비용 정확도 실험",
                "raw_text": "비용과 정확도 수치를 별도 조건에서 정리한 메모입니다.",
                "raw_ref": "project/week3/b.md",
            },
        ]
    }

    docs = module.normalize_pack(pack)["graph_relation"]

    assert [
        doc
        for doc in docs
        if doc["metadata"].get("kind") == "generic_candidate_refers_to"
    ] == []


def test_single_character_deictic_words_do_not_create_broad_relation_signal():
    module = load_module()

    assert module.has_reference_signal("위법성 판단과 증거능력 일반론을 설명한 문장입니다.") is False
    assert module.has_reference_signal("관련 법령의 참조조문과 지난 기록의 법리를 설명한 문장입니다.") is False
    assert module.has_reference_signal("전화연락으로 연결되어 조사를 받았다는 일반 사실입니다.") is False
    assert module.has_reference_signal('{"source_id": "external/file.txt::file", "quote": "증거"}') is False
    assert module.has_reference_signal("위 자료를 참고하여 다음 표의 값을 해석합니다.") is True
    assert module.has_reference_signal("앞서 본 표를 참고하여 다음 값을 해석합니다.") is True
    assert module.has_reference_signal("이 녹음본은 위 자료와 연결됩니다.") is True


def test_relation_candidate_prefilter_requires_specific_target_or_proximity():
    module = load_module()
    source = {
        "source_id": "source_a",
        "title": "후속 메모",
        "raw_text": "위 자료를 참고합니다.",
        "raw_ref": "a/followup.md",
    }
    unrelated = {
        "source_id": "unrelated_b",
        "title": "완전 무관 문서",
        "raw_text": "다른 주제입니다.",
        "raw_ref": "b/other.md",
    }
    explicit = {
        "source_id": "target_c",
        "title": "명시 대상",
        "raw_text": "명시 대상 원문입니다.",
        "raw_ref": "c/target.md",
    }
    same_week = {
        "source_id": "target_d",
        "title": "같은 주차 자료",
        "week": 3,
        "raw_text": "같은 주차 원문입니다.",
        "raw_ref": "d/week3.md",
    }

    assert module.should_score_relation_candidate(source, unrelated, module.relation_source_text(source)) is False
    source["raw_text"] = "target_c 자료를 참고합니다."
    assert module.should_score_relation_candidate(source, explicit, module.relation_source_text(source)) is True
    source["raw_text"] = "위 자료를 참고합니다."
    source["week"] = 3
    assert module.should_score_relation_candidate(source, same_week, module.relation_source_text(source)) is True


def test_same_container_artifact_path_is_not_relation_path_proximity():
    module = load_module()
    source = {"source_id": "record_a", "title": "candidate_records.json", "raw_ref": "for_eval/candidate_records.json"}
    target = {"source_id": "record_b", "title": "candidate_records.json", "raw_ref": "for_eval/candidate_records.json"}
    ledger_target = {"source_id": "record_c", "title": "evidence_ledger.json", "raw_ref": "for_eval/evidence_ledger.json"}

    assert module.has_path_family_proximity(source, target) is False
    assert module.has_path_family_proximity(source, ledger_target) is False
    assert module.should_score_relation_candidate(source, target, module.relation_source_text(source)) is False
    assert module.has_relation_candidate_signal(source, module.relation_source_text(source), [source, target]) is False


def test_source_id_relation_matching_requires_identifier_boundaries():
    module = load_module()
    source = {
        "source_id": "domain_actual_candidate_records.json_13",
        "title": "candidate_records.json",
        "raw_ref": "for_eval/candidate_records.json",
    }
    target_prefix = {
        "source_id": "domain_actual_candidate_records.json_1",
        "title": "candidate_records.json",
        "raw_ref": "for_eval/candidate_records.json",
    }
    target_explicit = {
        "source_id": "domain_actual_candidate_records.json_1",
        "title": "candidate_records.json",
        "raw_ref": "for_eval/candidate_records.json",
    }

    assert module.should_score_relation_candidate(source, target_prefix, module.relation_source_text(source)) is False
    source["raw_text"] = "실제 참조: domain_actual_candidate_records.json_1 자료를 참고합니다."
    assert module.should_score_relation_candidate(source, target_explicit, module.relation_source_text(source)) is True


def test_search_index_precomputes_token_counts_for_repeated_queries():
    module = load_module()
    index = module.build_search_index(
        [
            module.make_doc("doc1", "generic", "raw", "메신저 로그인 메신저"),
            module.make_doc("doc2", "generic", "raw", "비밀번호 순차 대입"),
        ]
    )

    assert index["counts"][0]["메신저"] == 2
    assert index["length_norms"][0] > 0
    assert index["postings"]["메신저"] == [0]
    assert index["postings"]["비밀번호"] == [1]
    assert module.score_index(index, "메신저 로그인", top_k=1)[0]["doc_id"] == "doc1"

    inflected_index = module.build_search_index(
        [
            module.make_doc("doc1", "generic", "raw", "메신저에 로그인하고 절차를 확인했다."),
            module.make_doc("doc2", "generic", "raw", "비밀번호 순차 대입"),
        ]
    )
    assert inflected_index["postings"]["메신저"] == [0]
    assert inflected_index["postings"]["로그인"] == [0]
    assert module.score_index(inflected_index, "메신저 로그인", top_k=1)[0]["doc_id"] == "doc1"


def test_build_indexes_does_not_materialize_unused_ultimate_rrf_index():
    module = load_module()
    indexes = module.build_indexes(synthetic_pack())

    assert "ultimate_rrf" not in indexes
    assert module.search_indexes(indexes, "ultimate_rrf", "메신저 로그인", top_k=1)


def test_final_fusion_boost_prefers_hits_with_broader_query_term_coverage():
    module = load_module()
    query = "검토기관이 장치 인증토큰을 빼서 메신저 로그인한 사례"
    full = {"kind": "raw_chunk", "text": "검토기관은 장치에서 인증토큰을 빼서 메신저 로그인 절차를 확인하였다."}
    partial = {"kind": "generic_memory_atom", "text": "장치 인증토큰 빼서 잠시 교부해 달라."}

    assert module.final_fusion_boost(full, query) > module.final_fusion_boost(partial, query)


def test_ultimate_rrf_overfetches_and_prefers_complete_rare_query_coverage():
    module = load_module()
    distractors = [
        {
            "source_id": f"generic_distractor_{index}",
            "domain": "domain",
            "kind": "record",
            "title": f"장치 검토 일반 사례 {index}",
            "summary": "장치 검토기관 사례",
            "raw_text": " ".join(["검토기관 장치 사례"] * 30),
            "raw_ref": f"domain/distractor_{index}.txt",
        }
        for index in range(12)
    ]
    pack = {
        "sources": [
            *distractors,
            {
                "source_id": "complete_token_messenger_case",
                "domain": "domain",
                "kind": "record",
                "title": "인증토큰 메신저 로그인 검토 사례",
                "summary": "요약에는 장치 검토 일반론만 있다.",
                "raw_text": "검토기관은 장치에서 인증토큰을 빼서 공기계에 장착한 뒤 메신저 로그인 절차를 확인하였다.",
                "raw_ref": "domain/complete_token_messenger.txt",
            },
        ]
    }
    query = "검토기관 장치 인증토큰 메신저 로그인 사례"

    hits = module.search_variant(pack, "ultimate_rrf", query, top_k=3)

    assert hits[0]["source_id"] == "complete_token_messenger_case"


def test_final_fusion_boost_preserves_relation_queries_over_surface_keyword_overlap():
    module = load_module()
    query = "구글드라이브 녹음본 어떤 영상 자료와 매칭"
    relation = {
        "kind": "relation",
        "text": "recording_supports_video GDrive 6주차 녹음본은 회로이론 6주차 강의영상 자료를 보고 말한 보조 녹음이다.",
    }
    surface_overlap = {
        "kind": "atomic_visual",
        "text": "구글드라이브 녹음본 어떤 영상 자료와 매칭되는지 표시한 화면 캡처",
    }

    assert module.final_fusion_boost(relation, query) > module.final_fusion_boost(surface_overlap, query)


def test_final_fusion_boost_preserves_explicit_meta_queries():
    module = load_module()
    query = "global_meta 메타 정보 (a) AC(Alternating Current): 교류"
    meta = {
        "kind": "atomic_global_meta",
        "text": "global_meta 메타 정보 (a) AC(Alternating Current): 교류",
    }
    raw = {
        "kind": "raw_chunk",
        "text": "global_meta 메타 정보 (a) AC(Alternating Current): 교류",
    }

    assert module.final_fusion_boost(meta, query) > module.final_fusion_boost(raw, query)


def test_query_term_coverage_boost_is_case_insensitive_for_technical_tokens():
    module = load_module()
    query = "global_meta 메타 정보 (a) AC(Alternating Current): 교류"
    uppercase = {"kind": "atomic_global_meta", "text": "global_meta | (a) AC(Alternating Current): 교류"}
    lowercase = {"kind": "atomic_global_meta", "text": "global_meta | (a) ac alternating current: 교류"}

    assert module.query_term_coverage_boost(uppercase, query) == module.query_term_coverage_boost(lowercase, query)


def test_rrf_fuse_preserves_component_kinds_for_final_rerank():
    module = load_module()
    relation_query = "구글드라이브 녹음본 어떤 영상 자료와 매칭"
    relation = module.make_doc(
        "relation::r1",
        "course",
        "relation",
        "recording_supports_video GDrive 녹음본은 회로이론 6주차 강의영상 자료를 보고 말한 보조 녹음이다.",
        metadata={"kind": "recording_supports_video"},
    )
    relation["score"] = 10
    fused_relation = module.rrf_fuse([[relation], [relation]], top_k=1)[0]

    assert fused_relation["kind"] == "rrf_fused"
    assert "relation" in fused_relation["metadata"]["rrf_kinds"]
    assert module.final_fusion_boost(fused_relation, relation_query) > 0.5

    meta_query = "global_meta 메타 정보 (a) AC(Alternating Current): 교류"
    meta = module.make_doc(
        "atomic_global_meta::m1",
        "course",
        "atomic_global_meta",
        "global_meta 메타 정보 (a) AC(Alternating Current): 교류",
    )
    meta["score"] = 10
    fused_meta = module.rrf_fuse([[meta], [meta]], top_k=1)[0]

    assert "atomic_global_meta" in fused_meta["metadata"]["rrf_kinds"]
    assert module.final_fusion_boost(fused_meta, meta_query) > module.final_fusion_boost(
        {"kind": "raw_chunk", "text": meta["text"]},
        meta_query,
    )


def test_generic_memory_atoms_preserve_offsets_anchors_and_source_provenance():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "generic_source_1",
                "domain": "generic",
                "kind": "transcript",
                "title": "범용 기억 원자화 샘플",
                "summary": "",
                "raw_text": "도입 문장입니다. 핵심 사실: 앞서 본 파일3의 회로 표를 근거로 시험범위를 정정했습니다. 마무리 문장입니다.",
                "raw_ref": "generic/source_1.txt",
            }
        ]
    }

    docs = module.normalize_pack(pack)["atomic_kag"]
    atom = next(
        doc
        for doc in docs
        if doc["kind"] == "generic_memory_atom" and "핵심 사실" in doc["text"]
    )

    assert atom["metadata"]["source_id"] == "generic_source_1"
    assert atom["metadata"]["raw_ref"] == "generic/source_1.txt"
    assert atom["metadata"]["span_start"] >= 0
    assert atom["metadata"]["span_end"] > atom["metadata"]["span_start"]
    assert "시험범위" in atom["metadata"]["anchor_terms"]
    assert "앞서" in atom["metadata"]["deictic_anchors"]
    assert "raw_ref=generic/source_1.txt" in atom["text"]
    assert "span_start=" in atom["text"]


def test_generic_memory_atom_overflow_is_explicit_when_projection_is_capped():
    module = load_module()
    raw_text = " ".join(f"핵심사실{index} 시험범위 정정 문장입니다." for index in range(85))
    docs = module.build_generic_memory_atoms(
        [
            {
                "source_id": "long_source",
                "domain": "generic",
                "kind": "transcript",
                "title": "긴 소스",
                "raw_text": raw_text,
                "raw_ref": "generic/long.txt",
            }
        ],
        max_atoms_per_source=3,
    )

    overflow = next(doc for doc in docs if doc["kind"] == "generic_memory_atom_overflow")

    assert overflow["metadata"]["source_id"] == "long_source"
    assert overflow["metadata"]["raw_ref"] == "generic/long.txt"
    assert overflow["metadata"]["kept_atom_count"] == 3
    assert overflow["metadata"]["omitted_atom_count"] > 0
    assert "projection capped" in overflow["text"]
    assert "raw_ref=generic/long.txt" in overflow["text"]


def test_generic_relation_graph_builds_two_hop_edges_from_explicit_mentions():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "raw_dataset_c",
                "domain": "generic",
                "kind": "dataset_note",
                "title": "원본 데이터 C",
                "week": 4,
                "summary": "측정 원본",
                "raw_text": "원본 데이터 C: 비용 10, 정확도 98, 맥락 관계성 0.91을 기록했습니다.",
                "raw_ref": "project/week4/raw_dataset_c.md",
            },
            {
                "source_id": "table_b",
                "domain": "generic",
                "kind": "table_note",
                "title": "프로젝트 표 B",
                "week": 4,
                "summary": "원본 데이터 C를 요약한 표",
                "raw_text": "프로젝트 표 B는 원본 데이터 C를 요약한 표입니다.",
                "raw_ref": "project/week4/table_b.md",
            },
            {
                "source_id": "followup_a",
                "domain": "generic",
                "kind": "memo",
                "title": "후속 메모 A",
                "week": 4,
                "summary": "표 B를 보고 작성한 메모",
                "raw_text": "아까 본 프로젝트 표 B를 기준으로 최종 판단을 정리합니다.",
                "raw_ref": "project/week4/followup_a.md",
            },
        ]
    }

    docs = module.normalize_pack(pack)["graph_relation"]
    assert any(
        doc["metadata"].get("kind") == "generic_multi_hop_refers_to"
        and doc["metadata"].get("source_id") == "followup_a"
        and doc["metadata"].get("via_id") == "table_b"
        and doc["metadata"].get("target_id") == "raw_dataset_c"
        for doc in docs
    )

    contracts = [
        {
            "contract_id": "generic_two_hop_relation",
            "query": "후속 메모 A가 최종적으로 근거로 삼은 원본 데이터",
            "required": ["generic_multi_hop_refers_to", "followup_a", "table_b", "raw_dataset_c", "원본 데이터 C"],
            "same_hit": True,
        }
    ]
    report = module.run_fixture_experiment(pack, contracts=contracts)

    assert report["variants"]["graph_relation"]["contracts"]["generic_two_hop_relation"]["passed"] is True
    assert report["variants"]["ultimate_rrf"]["contracts"]["generic_two_hop_relation"]["passed"] is True


def test_graph_relation_carries_target_raw_excerpt_for_multi_hop_fact_questions():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "raw_dataset_c",
                "domain": "generic",
                "kind": "dataset_note",
                "title": "원본 데이터 C",
                "week": 4,
                "summary": "측정 원본",
                "raw_text": "원본 데이터 C: 비용 10, 정확도 98, 맥락 관계성 0.91을 기록했습니다.",
                "raw_ref": "project/week4/raw_dataset_c.md",
            },
            {
                "source_id": "table_b",
                "domain": "generic",
                "kind": "table_note",
                "title": "프로젝트 표 B",
                "week": 4,
                "summary": "원본 데이터 C를 요약한 표",
                "raw_text": "프로젝트 표 B는 원본 데이터 C를 요약한 표입니다.",
                "raw_ref": "project/week4/table_b.md",
            },
            {
                "source_id": "followup_a",
                "domain": "generic",
                "kind": "memo",
                "title": "후속 메모 A",
                "week": 4,
                "summary": "표 B를 보고 작성한 메모",
                "raw_text": "아까 본 프로젝트 표 B를 기준으로 최종 판단을 정리합니다.",
                "raw_ref": "project/week4/followup_a.md",
            },
        ]
    }
    contracts = [
        {
            "contract_id": "generic_two_hop_target_fact",
            "query": "후속 메모 A가 근거로 삼은 원본 데이터의 정확도 값",
            "required": ["generic_multi_hop_refers_to", "raw_dataset_c", "정확도 98"],
            "same_hit": True,
        }
    ]

    report = module.run_fixture_experiment(pack, contracts=contracts)

    assert report["variants"]["graph_relation"]["contracts"]["generic_two_hop_target_fact"]["passed"] is True
    assert report["variants"]["ultimate_rrf"]["contracts"]["generic_two_hop_target_fact"]["passed"] is True


def test_derived_audit_contracts_expand_beyond_handwritten_contracts():
    module = load_module()
    pack = synthetic_pack()

    contracts = module.derive_audit_contracts(pack, max_per_kind=3)
    contract_ids = {contract["contract_id"] for contract in contracts}

    assert "derived_atom::domain_001" in contract_ids
    assert "derived_problem::prob_voc_rth" in contract_ids
    assert "derived_visual::vis_voc_rth" in contract_ids
    assert "derived_relation::rel_recording_week6" in contract_ids
    assert any(contract_id.startswith("derived_source::") for contract_id in contract_ids)

    report = module.run_fixture_experiment(pack, contracts=contracts)

    assert report["selected_variant"] == "ultimate_rrf"
    for contract_id, result in report["variants"]["ultimate_rrf"]["contracts"].items():
        assert result["passed"] is True, contract_id
    assert report["variants"]["summary_only"]["passed_count"] < report["variants"]["ultimate_rrf"]["passed_count"]


def test_visual_audit_contracts_use_visual_id_to_disambiguate_repeated_hints():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "week6_video",
                "domain": "course",
                "kind": "video",
                "title": "6주차 강의",
                "summary": "",
                "raw_text": "시각 자료를 설명합니다.",
                "raw_ref": "course/week6/video.md",
            }
        ],
        "visual_atoms": [
            {
                "visual_atom_id": "vis_circuit_1",
                "source_id": "week6_video",
                "kind": "circuit_image",
                "image_path": "course/week6/images/circuit_1.png",
                "text_hint": "Circuit 도선에 전류가 흐릅니다.",
            },
            {
                "visual_atom_id": "vis_circuit_2",
                "source_id": "week6_video",
                "kind": "circuit_image",
                "image_path": "course/week6/images/circuit_2.png",
                "text_hint": "Circuit 도선에 전류가 흐릅니다.",
            },
        ],
    }

    contracts = [
        contract
        for contract in module.derive_audit_contracts(pack, max_per_kind=10)
        if contract["contract_id"].startswith("derived_visual::")
    ]

    assert {
        tuple(contract["required"])
        for contract in contracts
    } == {
        ("vis_circuit_1", "circuit_1.png", "Circuit"),
        ("vis_circuit_2", "circuit_2.png", "Circuit"),
    }
    assert all(contract.get("same_hit") is True for contract in contracts)

    report = module.run_fixture_experiment(pack, contracts=contracts)

    assert report["variants"]["ultimate_rrf"]["contracts"]["derived_visual::vis_circuit_1"]["passed"] is True
    assert report["variants"]["ultimate_rrf"]["contracts"]["derived_visual::vis_circuit_2"]["passed"] is True


def test_visual_audit_exact_id_survives_many_repeated_fig_distractors():
    module = load_module()
    distractors = [
        {
            "visual_atom_id": f"vis_distractor_{index}",
            "source_id": "week1_notes",
            "kind": "page_image",
            "image_path": f"course/week1/images/page_image_{index}.png",
            "text_hint": "**Fig 1.2**",
            "confidence": "needs_model_verification",
        }
        for index in range(90)
    ]
    pack = {
        "sources": [
            {
                "source_id": "week1_notes",
                "domain": "course",
                "kind": "file",
                "title": "1주차 강의록",
                "summary": "",
                "raw_text": "시각 자료를 설명합니다.",
                "raw_ref": "course/week1/notes.pdf",
            }
        ],
        "visual_atoms": [
            *distractors,
            {
                "visual_atom_id": "vis_target_repeated_fig",
                "source_id": "week1_notes",
                "kind": "text_visual_hint",
                "image_path": "",
                "text_hint": "**Fig 1.2**",
                "confidence": "text_hint_only",
            },
        ],
    }

    contracts = [
        contract
        for contract in module.derive_audit_contracts(pack, max_per_kind=100)
        if contract["contract_id"] == "derived_visual::vis_target_repeated_fig"
    ]

    assert contracts == [
        {
            "contract_id": "derived_visual::vis_target_repeated_fig",
            "query": "시각 증거 이미지 회로 사진 vis_target_repeated_fig **Fig 1.2** ",
            "required": ["vis_target_repeated_fig", "Fig"],
            "same_hit": True,
        }
    ]

    report = module.run_fixture_experiment(pack, contracts=contracts)

    assert report["variants"]["ultimate_rrf"]["contracts"]["derived_visual::vis_target_repeated_fig"]["passed"] is True


def test_derived_source_and_meta_contracts_survive_noisy_same_title_meta():
    module = load_module()
    pack = synthetic_pack()
    noisy_meta = [
        {
            "meta_id": f"gmeta_noise_{index}",
            "source_id": "course_week6_video",
            "text": "회로이론 6주차 강의영상 source provenance 잡음 메타 VOC RTH",
        }
        for index in range(20)
    ]
    pack["global_meta"] = noisy_meta
    pack["local_meta"] = [
        {
            "meta_id": "lmeta_directional_circuit",
            "source_id": "course_week6_video",
            "text": "* (a) Circuit 1: 도선에 오른쪽 방향으로 I_1 = 2 A 전류가 흐릅니다.",
        }
    ]
    contracts = [
        contract
        for contract in module.derive_audit_contracts(pack, max_per_kind=5)
        if contract["contract_id"] in {"derived_source::course_week6_video", "derived_local_meta::lmeta_directional_circuit"}
    ]

    assert {contract["contract_id"] for contract in contracts} == {
        "derived_source::course_week6_video",
        "derived_local_meta::lmeta_directional_circuit",
    }

    report = module.run_fixture_experiment(pack, contracts=contracts)

    assert report["selected_variant"] == "ultimate_rrf"
    for contract_id, result in report["variants"]["ultimate_rrf"]["contracts"].items():
        assert result["passed"] is True, contract_id


def test_derived_audit_contracts_include_contrastive_latest_correction_contracts():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "week1_scope",
                "domain": "course",
                "kind": "video_transcript",
                "title": "1주차 시험범위 안내",
                "week": 1,
                "summary": "",
                "raw_text": "중간고사 시험범위는 1주차부터 4주차까지라고 일단 생각하세요.",
                "raw_ref": "course/week1.txt",
            },
            {
                "source_id": "week6_scope_correction",
                "domain": "course",
                "kind": "audio_transcript",
                "title": "6주차 시험범위 정정",
                "week": 6,
                "summary": "",
                "raw_text": "정정합니다. 중간고사 시험범위는 1주차부터 6주차 테브난 등가회로까지입니다. 앞서 4주차까지라고 말한 것은 취소합니다.",
                "raw_ref": "course/week6.txt",
            },
        ]
    }

    contracts = module.derive_audit_contracts(pack, max_per_kind=5)
    correction_contract = next(
        contract
        for contract in contracts
        if contract["contract_id"] == "derived_correction::week6_scope_correction"
    )

    assert correction_contract["top_hit"] is True
    assert any("6주차" in anchor or "테브난" in anchor for anchor in correction_contract["required"])
    assert any("4주차" in anchor for anchor in correction_contract["forbidden"])

    report = module.run_fixture_experiment(pack, contracts=[correction_contract])

    assert report["variants"]["ultimate_rrf"]["contracts"][correction_contract["contract_id"]]["passed"] is True


def test_correction_audit_ignores_domain_case_type_cancellation_words():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "domain_disposition_cancel_case",
                "domain": "domain",
                "kind": "record",
                "title": "징계처분취소 사건",
                "summary": "처분취소 소송의 일반 법리",
                "raw_text": "이 사건은 징계처분취소 청구에 관한 기록로서, 취소소송의 요건과 재량권 일탈 남용을 설명한다.",
                "raw_ref": "domain/cancel_case.txt",
            }
        ]
    }

    assert module.has_correction_signal("징계처분취소 청구와 취소소송 요건") is False
    assert module.has_correction_signal("앞서 4주차까지라고 말한 것은 취소합니다.") is True
    assert [
        contract
        for contract in module.derive_audit_contracts(pack, max_per_kind=5)
        if contract["contract_id"].startswith("derived_correction::")
    ] == []


def test_correction_audit_skips_aggregate_artifact_records():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "domain_actual_candidate_records.json_5",
                "domain": "domain",
                "kind": "record",
                "title": "candidate_records.json",
                "summary": "",
                "raw_text": "검토보고 수정 보고와 쟁점사실 변경 내용이 포함된 집계 산출물입니다.",
                "raw_ref": "for_eval/candidate_records.json",
            },
            {
                "source_id": "course_scope_correction",
                "domain": "course",
                "kind": "transcript",
                "title": "시험범위 정정",
                "summary": "",
                "raw_text": "정정합니다. 시험범위는 6주차까지입니다. 앞서 4주차까지라고 말한 것은 취소합니다.",
                "raw_ref": "course/week6/transcript.txt",
            },
        ]
    }

    contract_ids = {
        contract["contract_id"]
        for contract in module.derive_audit_contracts(pack, max_per_kind=5)
        if contract["contract_id"].startswith("derived_correction::")
    }

    assert "derived_correction::domain_actual_candidate_records.json_5" not in contract_ids
    assert "derived_correction::course_scope_correction" in contract_ids


def test_derived_audit_skips_aggregate_artifact_source_and_atom_contracts():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "aggregate_candidate_records",
                "domain": "generic",
                "kind": "system_artifact",
                "title": "candidate_records.json",
                "summary": "",
                "raw_text": "generic_memory_atom 검토자 검토기관 pin sim 계정 기기",
                "raw_ref": "for_eval/candidate_records.json",
            },
            {
                "source_id": "original_transcript",
                "domain": "generic",
                "kind": "transcript",
                "title": "원본 강의록",
                "summary": "",
                "raw_text": "원본 강의록에는 시험범위와 교수 설명이 들어 있습니다.",
                "raw_ref": "course/week6/transcript.txt",
            },
        ]
    }

    contract_ids = {
        contract["contract_id"]
        for contract in module.derive_audit_contracts(pack, max_per_kind=5)
        if contract["contract_id"].startswith(("derived_atom::", "derived_source::"))
    }

    assert "derived_atom::aggregate_candidate_records" not in contract_ids
    assert "derived_source::aggregate_candidate_records" not in contract_ids
    assert "derived_atom::original_transcript" in contract_ids
    assert "derived_source::original_transcript" in contract_ids


def test_derived_audit_aggregate_artifacts_do_not_consume_source_budget():
    module = load_module()
    pack = {
        "sources": [
            {
                "source_id": "aggregate_candidate_records",
                "domain": "generic",
                "kind": "system_artifact",
                "title": "candidate_records.json",
                "summary": "",
                "raw_text": "검토자 검토기관 pin sim 계정 기기",
                "raw_ref": "for_eval/candidate_records.json",
            },
            {
                "source_id": "original_transcript",
                "domain": "generic",
                "kind": "transcript",
                "title": "원본 강의록",
                "summary": "",
                "raw_text": "원본 강의록에는 시험범위와 교수 설명이 들어 있습니다.",
                "raw_ref": "course/week6/transcript.txt",
            },
        ]
    }

    contract_ids = {
        contract["contract_id"]
        for contract in module.derive_audit_contracts(pack, max_per_kind=1)
        if contract["contract_id"].startswith(("derived_atom::", "derived_source::"))
    }

    assert "derived_atom::original_transcript" in contract_ids
    assert "derived_source::original_transcript" in contract_ids


def test_slim_report_removes_nested_raw_payloads():
    module = load_module()
    report = {
        "selected_variant": "ultimate_rrf",
        "contracts": ["c1"],
        "variants": {
            "ultimate_rrf": {
                "variant": "ultimate_rrf",
                "passed_count": 1,
                "total_count": 1,
                "contracts": {
                    "c1": {
                        "passed": True,
                        "query": "q",
                        "hits": [
                            {
                                "doc_id": "d1",
                                "text": "RAW HIT TEXT",
                                "metadata": {
                                    "safe": "keep",
                                    "text": "NESTED RAW TEXT",
                                    "raw_text": "NESTED RAW SOURCE",
                                    "items": [{"body": "NESTED BODY", "safe": "also_keep"}],
                                },
                            }
                        ],
                    }
                },
            }
        },
    }

    slim = module.make_slim_report(report)
    hit = slim["variants"]["ultimate_rrf"]["contracts"]["c1"]["hits"][0]

    assert "text" not in hit
    assert hit["metadata"] == {"safe": "keep", "items": [{"safe": "also_keep"}]}
    assert hit["text_chars"] == len("RAW HIT TEXT")


def test_slim_only_report_skips_full_json_for_large_audits(tmp_path):
    module = load_module()
    report = {
        "selected_variant": "raw_leaf",
        "contracts": [],
        "variants": {
            "raw_leaf": {
                "variant": "raw_leaf",
                "passed_count": 0,
                "total_count": 0,
                "contracts": {},
            }
        },
    }

    module.write_report(report, tmp_path, slim_only=True)

    assert not (tmp_path / "retrieval_variant_report.json").exists()
    assert (tmp_path / "retrieval_variant_report.slim.json").exists()
    assert (tmp_path / "RETRIEVAL_VARIANT_REPORT.md").exists()


def test_empty_domain_for_eval_is_preflight_error(tmp_path):
    module = load_module()
    empty_for_eval = tmp_path / "for_eval"
    empty_for_eval.mkdir()

    with pytest.raises(ValueError, match="no usable domain for_eval artifacts"):
        module.run_actual_experiment(None, str(empty_for_eval), tmp_path / "out")
