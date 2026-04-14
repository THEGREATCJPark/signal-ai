# Coverage Bundle Maker

## 포함 파일

- `guide_maker.html`: 자료를 읽고 guide/problem/coverage 산출물을 만드는 단일 파일 도구.
- `renderer.html`: 생성된 기억 번들을 렌더링하고 상호작용하는 도구.

## 원리

이 시스템은 로컬 브라우저 기반으로 자료를 번들화하고, 생성 결과가 입력 항목을 충분히 덮었는지 검증한 뒤 빠진 항목만 패치한다. 핵심은 “생성 → 검증 → 미충족만 재시도/패치 → 렌더링”이다.

## 핵심 메커니즘

- file-first chunking
- 유형 생성, 병합, 분류, 미분류 재시도
- coverage verifier의 `missing_only` 모드
- 요청당 토큰 예산과 자동 분할
- 최종 검증 모드
- renderer와 builder 분리

## 범용화 포인트

특정 학습 자료가 아니라 어떤 대량 자료라도 `guide bundle`로 만들 수 있다. 렌더러는 원본 자료를 다시 파싱하지 않고 생성된 기억 번들을 읽는다.
