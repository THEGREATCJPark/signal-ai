# Signal AI — 구구 브리핑

매일 아침, AI 최전방 소식을 자동 수집 → LLM 요약/분류/점수화 → 텔레그램 & X에 발행하는 서비스.

- **텔레그램:** [t.me/gugubrief](https://t.me/gugubrief)
- **X (트위터):** [@gugubrief1961](https://x.com/gugubrief1961)

---

## 팀

| 역할 | 담당 | 범위 |
|------|------|------|
| **제품 오너** | CJ | 텔레그램 봇, 메시지 포맷, 채널 운영, 발행 파이프라인 |
| **시스템 오너** | HB | 크롤링, LLM 요약/분류/점수화, 중복 제거, 자동화 |

---

## 아키텍처

```
크롤러 (HN, Reddit, GitHub, HuggingFace, ...)
    ↓
LLM 파이프라인 (요약 · 태깅 · 점수화)
    ↓
docs/articles.json  ← 정규화된 기사 JSON
    ↓
run_publish.py  ← 발행 스크립트
    ↓
┌────────────┐    ┌────────────┐
│  Telegram  │    │   X (트위터) │
│ @gugubrief │    │ @gugubrief1961│
└────────────┘    └────────────┘
```

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 크롤링 | Python (requests, BeautifulSoup, feedparser) |
| LLM | GPT-4.1 mini / Gemini Flash |
| 봇 | Telegram Bot API (requests 직접 호출) |
| X 포스팅 | X API v2 (OAuth 2.0 + Refresh Token) |
| 스케줄링 | GitHub Actions cron (매일 08:00 KST) |
| 상태 관리 | `data/published.json` (중복 발행 방지) |

---

## 프로젝트 구조

```
signal-ai/
├── bot/                    # 발행 모듈
│   ├── telegram_bot.py     # 텔레그램 발행
│   ├── x_poster.py         # X 발행 (OAuth 2.0)
│   ├── formatter.py        # 메시지 포맷팅 (HTML)
│   ├── scheduler.py        # 발행 오케스트레이션
│   └── test_publish.py     # 테스트 유틸
│
├── publisher/              # 상태 관리
│   └── state.py            # 발행 이력 추적 (published.json)
│
├── scripts/                # CLI 엔트리포인트
│   ├── run_publish.py      # 메인 발행 스크립트
│   └── validate_articles.py # 기사 JSON 검증
│
├── docs/
│   └── articles.json       # 발행할 기사 데이터
│
├── data/
│   └── published.json      # 발행 상태 (자동 생성)
│
├── .github/workflows/
│   └── daily_publish.yml   # GitHub Actions 자동 발행
│
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## 실행 방법

### 로컬 실행

```bash
# 의존성 설치
pip install -r requirements.txt

# .env 설정
cp .env.example .env
# .env 파일에 실제 키 입력

# 검증
python scripts/validate_articles.py

# Dry-run (미리보기, API 호출 없음)
python scripts/run_publish.py --dry-run

# 텔레그램만 발행
python scripts/run_publish.py --platform telegram

# X만 발행
python scripts/run_publish.py --platform x

# 전체 발행 (Telegram + X)
python scripts/run_publish.py --platform both

# 기사 수 제한 (크레딧 절약)
python scripts/run_publish.py --platform x --limit 1

# 강제 재발행 (이미 발행된 기사 포함)
python scripts/run_publish.py --force
```

### GitHub Actions 수동 실행

**Actions → Daily Signal AI Publish → Run workflow** 에서 옵션 선택:

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `dry_run` | 미리보기만 (실제 발행 X) | false |
| `platform` | 발행 플랫폼 (telegram / x / both) | both |
| `force` | 이미 발행된 기사도 재발행 | false |
| `limit` | 발행 기사 수 제한 (0=무제한) | 0 |

---

## 환경 변수 / GitHub Secrets

### Telegram

| 변수 | 설명 |
|------|------|
| `TELEGRAM_BOT_TOKEN` | BotFather에서 발급한 봇 토큰 |
| `TELEGRAM_CHANNEL_ID` | 채널 ID (예: `@gugubrief`) |

### X (Twitter) — OAuth 2.0

| 변수 | 설명 |
|------|------|
| `X_CLIENT_ID` | OAuth 2.0 Client ID |
| `X_CLIENT_SECRET` | OAuth 2.0 Client Secret |
| `X_REFRESH_TOKEN` | OAuth 2.0 Refresh Token (자동 갱신) |

> X는 OAuth 2.0 PKCE 인증을 사용합니다. Access Token은 2시간마다 만료되며, Refresh Token으로 자동 갱신됩니다.

---

## 기사 JSON 포맷

`docs/articles.json` 입력 형식:

```json
[
  {
    "id": "unique-id",
    "source": "hackernews",
    "title": "기사 제목",
    "url": "https://example.com/article",
    "score": 342,
    "comments": 128,
    "timestamp": "2026-04-09T08:00:00Z",
    "summary": "기사 요약 텍스트",
    "media": []
  }
]
```

Discord 다이제스트 형식 (`{ "articles": [...] }`)도 자동 정규화됩니다.

---

## 크롤링 소스

### 1차 (MVP)

| 소스 | 수집 방식 | 비용 |
|------|-----------|------|
| Hacker News | Algolia API | 무료 |
| Reddit (r/MachineLearning, r/LocalLLaMA) | RSS | 무료 |
| GitHub Trending | HTML 스크래핑 | 무료 |
| HuggingFace Trending | HF API | 무료 |

### 2차 (런칭 후 추가)

| 소스 | 수집 방식 | 비용 |
|------|-----------|------|
| arXiv (cs.AI, cs.CL, cs.LG) | RSS / API | 무료 |
| Anthropic Blog | RSS | 무료 |
| OpenAI Blog | RSS | 무료 |
| Google AI Blog | RSS | 무료 |
| GeekNews (긱뉴스) | RSS | 무료 |

---

## Git 협업 룰

### 브랜치 전략

```
main        ← 항상 동작하는 상태. 직접 push 금지.
  dev       ← 개발 통합 브랜치. 기능 완성되면 여기로 PR.
    feat/*  ← 새 기능
    fix/*   ← 버그 수정
```

### 커밋 메시지

```
feat: 텔레그램 봇 자동 발행 기능 추가
fix: HN 크롤러 URL 파싱 오류 수정
docs: README 업데이트
chore: 의존성 추가
```

---

## 라이선스

Private repo.
