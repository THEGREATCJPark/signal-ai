# 기억시스템 코드 모음

대량 자료에서 질문별로 필요한 정보를 찾아오고, 근거 위치와 반복 검증 상태를 보존하는 기억시스템 구현을 구성 단위별로 정리한 코드 모음이다.

## 원칙

- 실행 가능한 코드와 검증용 fixture/test를 우선한다.
- 원문 자료를 직접 넣지 않고, 근거 위치와 검증 절차를 재현 가능한 형태로 둔다.
- 하나의 실행 흐름으로 쓰인 구성은 같은 폴더에 보관한다.
- 대량 데이터, 사용자 원문 로그, 코퍼스 파일은 포함하지 않는다.

## 시스템별 폴더

| 폴더 | 구성 단위 | 포함 코드 | 핵심 원리 |
| --- | --- | --- | --- |
| `systems/generic-memory-builder/` | 대화 기억 빌더 실험 | 최종 `memory_builder.py`, 실행 래퍼, cycle 1~3 코드 | 대화/로그 전체를 raw-preserving 산출물로 바꾸는 실험이다. 단, `final/`은 hand-tuned anchor regex와 평가 입력 의존 규칙이 많아 범용 기억시스템의 최종해로 보지 않는다. |
| `systems/record-ledger-rag/` | 대형 근거자료 질문응답 시스템 | `memory_evidence_rag.py` | 후보 선별, chunk 분석, 주장·근거 ledger, 본문 커버리지 패치, 메타 누출 방어를 한 파이프라인으로 묶는다. |
| `systems/coverage-bundle-maker/` | 커버리지형 guide maker/renderer 시스템 | `guide_maker.html`, `renderer.html` | 생성 → 검증 → 미충족만 패치 → 렌더링의 로컬 번들형 기억시스템이다. |
| `systems/runtime-artifact-memory/` | 서비스형 작업/후속질문 런타임 | `backend/jobs.py`, `backend/search.py`, 관련 frontend state helpers | 긴 작업 상태 복구, 사용자별 기록, 후속질문 산출물 라우팅, 후보 백필을 담당한다. |
| `systems/supervisor-recall-loop/` | 감독자형 반복 회상 루프 | `recall_autoloop.py` | solver/evaluator/converger/manager, issue ledger, checkpoint, no-progress 대응으로 긴 기억 구축을 반복 개선한다. |
| `systems/deterministic-parser/` | 결정적 자료 파서 | `deterministic_parser.py` | 모델 없이 파일/상태/명시 문구를 파싱해 근거 위치와 claim span을 남긴다. |
| `systems/generic-source-grounded-memory-current-best-20260413/` | 현재 가장 나은 source-grounded 범용 기억검색 스냅샷 | full-corpus adapter, retrieval variant harness, supervisor loop runner, slim reports | 원문 보존 + 다중 검색면 + RRF + coverage patch + negative-search + evaluator loop를 한 독립 산출물로 묶는다. |
| `systems/source-grounded-method-audit-20260414/` | method-audit 컴포넌트 | config-driven source-grounded scanner, demo profile/records, tests, round audit report | 사건/절차/방법이 실제로 발견된 것과 직접 결론 근거가 있는 것을 분리하고, 감사된 부재를 양성 통과로 오판하지 않도록 강제한다. |

## 현재 우선 참조

범용 검색 파이프라인 전체를 볼 때의 우선 검토 대상은 `systems/generic-source-grounded-memory-current-best-20260413/`이다. 이 폴더는 최종 성공 주장 없이, 원문 보존, 다중 검색면, RRF, coverage patch, negative-search, supervisor loop runner, 테스트 결과를 함께 둔 최신 스냅샷이다.

방법 검증 규칙만 볼 때의 우선 검토 대상은 `systems/source-grounded-method-audit-20260414/`이다. 이 폴더는 "사건 발견"과 "직접 결론 근거"를 구분해 false-pass를 막는 컴포넌트다.

`systems/generic-memory-builder/final/`은 보존 가치가 있는 레거시 실험이지만, `ANCHOR_RULES`가 특정 입력 문구에 강하게 맞춰져 있으므로 "범용 최종 통과본"으로 인용하지 않는다.

## 읽는 순서

1. `systems/source-grounded-method-audit-20260414/README.md`
2. `systems/source-grounded-method-audit-20260414/reports/ROUND_AUDIT.md`
3. `systems/source-grounded-method-audit-20260414/src/source_grounded_method_audit.py`
4. `systems/generic-source-grounded-memory-current-best-20260413/README.md`
5. `systems/generic-source-grounded-memory-current-best-20260413/retrieval_variant_harness/evaluate_generic_memory_retrieval_variants.py`
6. `systems/generic-source-grounded-memory-current-best-20260413/full_corpus_memory_search/memory_search_system.py`
7. `systems/generic-source-grounded-memory-current-best-20260413/loop_runner/recall_autoloop.py`
8. `systems/supervisor-recall-loop/README.md`
9. `systems/record-ledger-rag/README.md`
10. `systems/generic-memory-builder/README.md`
