# Supervisor Recall Loop

## 포함 파일

- `recall_autoloop.py`: solver/evaluator/converger/manager 기반 반복 회상 루프 코드.

## 원리

복잡한 기억시스템 구축은 한 번의 생성으로 끝나지 않는다. 실행자, 평가자, 통합자, 감독자를 분리하고, 각 라운드에서 issue ledger, manager directive, solver memory, orchestrator memory, best checkpoint를 갱신한다.

## 핵심 메커니즘

- round workspace materialization
- solver result와 evaluator result 분리
- converged feedback 생성
- manager directive와 no-progress 대응
- best checkpoint ranking
- replay artifact preservation
- evaluator code library 분리
- heartbeat/stall 감지

## 범용화 포인트

특정 benchmark 풀이가 아니라, 장시간 반복 개선이 필요한 기억 구축 작업을 관리하는 오케스트레이터다.
