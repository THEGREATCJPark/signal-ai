# Runtime Memory and Follow-Up Router

## 포함 파일

- `backend/jobs.py`: 작업 상태, 후속질문 라우터, 문서 preflight, 복구 로직.
- `backend/search.py`: 후보 검색·선별·백필 로직.
- `lib/session-history.ts`: 사용자별 브라우저 기록 관리.
- `lib/conversation-pending.ts`: 전송 직후 pending turn과 loader 상태 관리.

## 원리

긴 작업을 서버 메모리에만 두지 않고 job directory와 runtime status에 남긴다. 후속질문이 오면 기존 최종답, 답변 설계, ledger, 청크 분석, 선택 레코드 본문을 먼저 pack해서 답변 가능성을 판단하고, 부족할 때만 새 검색으로 넘어간다.

## 핵심 메커니즘

- 디스크 기반 job recovery
- 최종 산출물 존재 시 stale interrupted error 정규화
- client id 기반 사용자별 기록 분리
- follow-up artifact packing
- answerable 판단 후 기존 산출물 답변 또는 재검색 분기
- 후보 overfetch/rerank/backfill
- 전송 즉시 user bubble과 pending loader 표시

## 범용화 포인트

특정 앱 UI가 아니라 “긴 기억 검색 작업을 서비스로 제공할 때 필요한 런타임 계층”이다.
