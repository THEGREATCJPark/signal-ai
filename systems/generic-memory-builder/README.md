# Generic Memory Builder

## 포함 파일

- `final/memory_builder.py`: 레거시 평가 통과 checkpoint의 기억 빌더 코드.
- `final/run_memory_task.sh`: 레거시 실행 래퍼.
- `cycles/cycle-01/memory_builder.py`: 1차 solver 사이클 코드.
- `cycles/cycle-02/memory_builder.py`: 2차 solver 사이클 코드.
- `cycles/cycle-03/memory_builder.py`: 3차 solver 사이클 코드.

## 원리

이 시스템은 대화나 로그 전체를 단일 요약으로 압축하지 않고, 계층형 기억으로 만드는 실험이다. 입력 메시지를 보존하고, 시작 앵커, 에피소드 로그, cross-reference 해소, 메모리 인덱스, replay manifest를 함께 만든다.

## 한계

`final/memory_builder.py`는 특정 평가 입력 문구에 맞춘 `ANCHOR_RULES` 정규식과 hand-tuned 규칙이 많다. 따라서 "범용 최종해"나 자연어 이해 기반 기억시스템으로 인용하면 안 된다. 이 폴더는 raw-preserving memory artifact 형식과 replay pointer 설계를 참고하기 위한 레거시 실험으로 보존한다.

## cycle별 의미

- cycle 1: 실제 transcript를 ingest하고 기본 기억 산출물을 만든다.
- cycle 2: 평가에서 드러난 cross-reference inspectability 문제를 보완한다.
- cycle 3: 평가 checkpoint. 전체 메시지 보존, anchor, xref, replay 조건을 통과했지만 hand-tuned 규칙 의존성이 남아 있다.

## 범용화 포인트

입력은 특정 도메인 문서가 아니라 순서가 있는 메시지/기록 묶음이면 된다. 다만 현재 코드는 범용 anchor discovery가 아니라 평가 문구 기반 anchor matching에 가깝다. 핵심 참고점은 “질문에 답할 때 다시 찾아갈 수 있는 raw pointer와 replay manifest를 남기는 산출물 형식”이다.
