# Evidence Ledger RAG

## 포함 파일

- `memory_evidence_rag.py`: 대형 근거자료 질문응답 파이프라인의 실제 코드.

## 원리

자료군에서 후보 레코드를 고르고, 레코드를 chunk로 분석한 뒤, 주장 축과 근거 span을 ledger로 병합한다. 최종 답변은 ledger를 기반으로 생성하고, 이후 커버리지 검증자가 빠진 주장·근거·레코드를 찾아 국소 패치한다.

## 핵심 메커니즘

- 후보 레코드 선별과 본문 추출
- chunk 분석과 ledger 병합
- 질문별 답변 설계
- 의미 기반 누락 검증
- 전체 재작성 금지형 local patch
- 메타 누출과 마크다운 손상 방어

## 범용화 포인트

특정 분야의 “기억 레코드”가 아니라 어떤 근거자료든 `memory record`로 보면 된다. 레거시 코드 내부에는 분야별 이름이 남아 있을 수 있지만, 시스템 원리는 `record -> claim ledger -> answer -> coverage patch` 구조다.
