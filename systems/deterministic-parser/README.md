# Deterministic Parser

## 포함 파일

- `deterministic_parser.py`: 모델 없이 로컬 파일과 상태를 읽어 명시 claim과 근거 span을 추출하는 코드.

## 원리

LLM 추론 전에 명시 문구와 구조화 상태를 결정적으로 파싱한다. 직접 근거가 있는 claim과 약한 파생 후보를 섞지 않고, 각 claim의 대상 텍스트와 원문 위치를 저장한다.

## 핵심 메커니즘

- 로컬 파일/상태 스캔
- 명시 포함/제외 claim 추출
- target text span 계산
- source path, line, char offset 기록
- 직접 근거와 후보 범위 분리

## 범용화 포인트

특정 시험범위 파서가 아니라, “자료 안에 명시적으로 A는 포함/제외된다고 적힌 경우 그 대상 span을 정확히 잡는 결정적 기억 파서”다.
