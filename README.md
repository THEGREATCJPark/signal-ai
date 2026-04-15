
## 프로젝트 개요

매일 아침, AI 최전방 소식을 자동 수집 -> LLM 요약/분류/점수화 -> 텔레그램 채널로 발행하는 서비스.

- **타깃 유저:** 개인 개발자, 바이브코더, AI에 관심 있는 엔지니어
- **핵심 가치:** "이게 왜 중요한지, 오늘 뭘 해볼 수 있는지"까지 알려주는 것
- **매체:** 텔레그램 채널 (1차) -> 웹 대시보드/뉴스레터 (2차)

---

## 팀

| 역할 | 담당 | 범위 |
|------|------|------|
| **제품 오너** | CJ (박찬준) | 텔레그램 봇, 메시지 포맷, 채널 운영, 콘텐츠 톤앤매너, UX, 마케팅 |
| **시스템 오너** | HB (박형빈) | 크롤링 파이프라인, LLM 요약/분류/점수화, 중복 제거, 자동화, 품질 검증 |

---

## 기술 스택 (초안, 논의 후 변경 가능)

- **크롤링:** Python (requests, BeautifulSoup, feedparser)
- **LLM:** GPT-4.1 mini 또는 Gemini Flash (비용 최적화)
- **봇:** Telegram Bot API (requests 직접 호출)
- **스케줄링:** GitHub Actions cron 또는 Railway
- **DB:** Supabase (무료 티어) 또는 SQLite
- **언어:** Python 기본, 필요시 논의

---

## 크롤링 소스

### 1차 (MVP, Day 2~5에 구현)

| 소스 | URL | 수집 방식 | 설명 | 비용 |
|------|-----|-----------|------|------|
| **Hacker News** | https://news.ycombinator.com | RSS / Algolia API (https://hn.algolia.com/api) | AI 키워드 필터링. 개발자 커뮤니티 반응(점수/댓글수)이 곧 중요도 지표 | 무료 |
| **Reddit** | r/MachineLearning, r/LocalLLaMA, r/singularity | RSS (.rss 접미사) | 최전방 논문/모델 소식 + 로컬 LLM 실전 후기 + AI 전망 토론 | 무료 |
| **GitHub Trending** | https://github.com/trending | HTML 스크래핑 | 오늘 뜨는 AI 오픈소스 프로젝트. 언어별/기간별 필터 가능 | 무료 |
| **HuggingFace Trending** | https://huggingface.co/models?sort=trending | HF API 또는 스크래핑 | 새 모델 출시, 인기 모델 변동, Spaces 트렌딩 | 무료 |

### 2차 (런칭 후 1~2주 내 추가)

| 소스 | URL | 수집 방식 | 설명 | 비용 |
|------|-----|-----------|------|------|
| **arXiv** | https://arxiv.org/list/cs.AI/recent | RSS / arXiv API | cs.AI, cs.CL, cs.LG 카테고리. 제목+초록만 가져와서 LLM으로 실전 영향도 판별 | 무료 |
| **Anthropic Blog** | https://www.anthropic.com/news | RSS / 스크래핑 | Claude 업데이트, 안전 연구, 정책 변경 | 무료 |
| **OpenAI Blog** | https://openai.com/blog | RSS / 스크래핑 | GPT/API 업데이트, 가격 변경, 신기능 | 무료 |
| **Google AI Blog** | https://blog.google/technology/ai/ | RSS | Gemini, DeepMind 소식 | 무료 |
| **GeekNews (긱뉴스)** | https://news.hada.io | RSS (https://news.hada.io/rss) | 한국어 HN 격. 한국 개발자 관심사 반영 | 무료 |
| **Meta AI Blog** | https://ai.meta.com/blog/ | RSS / 스크래핑 | Llama, 오픈소스 모델 업데이트 | 무료 |

### 3차 (구독자 1,000+ 이후 검토)

| 소스 | URL | 수집 방식 | 설명 | 비용 |
|------|-----|-----------|------|------|
| **X (트위터)** | 주요 계정 리스트 | API v2 Basic | @karpathy, @swyx, @ylecun, @sama 등 최전방 인물 실시간 | $100/월 |
| **Product Hunt** | https://www.producthunt.com | API | AI 신규 제품/도구 출시 | 무료 |
| **특이점 갤러리** | https://gall.dcinside.com/mgallery/board/list/?id=thesingularity | 스크래핑 | 한국 AI 커뮤니티. 우리 타깃 유저의 관심사 직접 반영 | 무료 |
| **SemiAnalysis** | https://semianalysis.com | RSS | 반도체/인프라 심층 분석 | 무료~유료 |
| **AI News (swyx)** | https://buttondown.com/ainews | RSS / 이메일 파싱 | Karpathy 추천 뉴스레터. 영어권 최전방 요약 | 무료 |
| **Import AI (Jack Clark)** | https://importai.substack.com | RSS | Anthropic 공동창업자 운영. 연구+정책 깊이 있음 | 무료 |

### 소스 관리 원칙

1. 새 소스 추가 시 `crawler/` 폴더에 독립 파일로 작성 (예: `arxiv.py`)
2. 모든 크롤러는 동일한 JSON 포맷으로 출력
3. 소스별 수집 주기, 마지막 수집 시각, 에러 횟수를 로깅
4. 소스가 깨지거나 구조가 바뀌면 텔레그램으로 알림

### 크롤러 공통 출력 포맷 (JSON)

```json
{
  "source": "hackernews",
  "title": "Claude 4.6 Released with Extended Context",
  "url": "https://example.com/article",
  "score": 342,
  "comments": 128,
  "timestamp": "2026-04-09T08:00:00Z",
  "raw_text": "원문 텍스트 또는 본문 일부 (LLM 입력용)"
}
```

---

## 2주 MVP 플랜

### Week 1: 만들기

| Day | CJ (제품) | HB (시스템) | 공동 |
|-----|-----------|-------------|------|
| **1** | - | - | 킥오프 미팅 (1h): 서비스 정의, 역할 확인, 채널명 확정, 레포 세팅 |
| **2** | 텔레그램 채널+봇 개설, 프로필/소개글 세팅 | 크롤러 뼈대 (HN, Reddit) | 매일 저녁 싱크 시작 |
| **3** | 메시지 포맷 디자인, 예시 포스트 수동 제작 3개 | 크롤러 확장 (GitHub Trending, HuggingFace) | - |
| **4** | 봇 자동 발행 로직 구현 | LLM 파이프라인 (요약+태깅+점수) | - |
| **5** | 봇 입력 포맷 맞추기 | 중복 제거, JSON 출력 통일 | - |
| **6** | - | - | 파이프라인 -> 봇 연결, 비공개 테스트 채널에서 첫 자동 발행 |
| **7** | - | - | 결과물 리뷰, 피드백 목록 작성 |

### Week 2: 다듬기 + 런칭

| Day | CJ (제품) | HB (시스템) | 공동 |
|-----|-----------|-------------|------|
| **8** | "오늘의 핵심 3개" 상단 요약 포맷 추가 | 프롬프트 튜닝 (요약 품질, 점수 기준) | - |
| **9** | 카테고리 이모지 통일, 수동 코멘트 방식 테스트 | "바이브코더 실전 적용도" 필터 추가 | - |
| **10** | 3일치 결과 콘텐츠 리뷰 | 에러 핸들링 (소스 깨짐, API 실패 알림) | - |
| **11** | 런칭 소개글 작성 (특갤용, 단톡방용) | 크론잡 최종 세팅 (매일 08:00 KST) | - |
| **12** | - | - | 공개 채널 전환, 첫 공개 포스트 발행 |
| **13** | 특갤 홍보, 지인 공유 | 발행 후 모니터링 | - |
| **14** | - | - | **회고 미팅 (30min)** |

### Day 14 회고 체크리스트

- [ ] 협업 속도가 맞았는가?
- [ ] 역할 분담이 적절했는가?
- [ ] 소통 빈도/방식이 괜찮았는가?
- [ ] 서로 기다린 시간이 많았는가?
- [ ] 이 프로젝트를 계속 할 것인가?
- [ ] 다음 2주 목표는?

---

## Git 협업 룰

### 브랜치 전략

```
main           <- 항상 동작하는 상태. 직접 push 금지.
  dev          <- 개발 통합 브랜치. 기능 완성되면 여기로 PR.
  feat/봇-세팅           <- CJ 작업 예시
  feat/크롤러-hn         <- HB 작업 예시
  feat/llm-파이프라인    <- HB 작업 예시
  fix/점수-버그          <- 버그 수정 예시
```

**규칙:**
1. `main`에 직접 push 절대 금지
2. 모든 작업은 `feat/`, `fix/`, `hotfix/` 브랜치에서 진행
3. 작업 끝나면 `dev`로 PR(Pull Request) 생성
4. 상대방이 간단히 리뷰 후 머지 (꼼꼼한 코드리뷰 X, "돌아가는지" 확인 수준)
5. `dev`에서 안정적이면 `main`으로 머지

### 브랜치 이름 규칙

```
feat/기능설명    -> 새 기능 (feat/telegram-bot, feat/crawler-reddit)
fix/버그설명     -> 버그 수정 (fix/score-calculation)
hotfix/설명      -> 긴급 수정
docs/설명        -> 문서 수정
```

### 커밋 메시지 규칙

형식: `타입: 한국어로 뭘 했는지 한 줄`

```
feat: 텔레그램 봇 자동 발행 기능 추가
fix: HN 크롤러 URL 파싱 오류 수정
docs: README 기술스택 업데이트
refactor: LLM 프롬프트 구조 정리
chore: .env.example 추가
```

| 타입 | 용도 |
|------|------|
| feat | 새 기능 |
| fix | 버그 수정 |
| docs | 문서 |
| refactor | 기능 변화 없는 코드 개선 |
| chore | 설정, 의존성, 기타 잡일 |
| test | 테스트 추가/수정 |

### PR (Pull Request) 규칙

PR 본문 예시:

```
## 뭘 했는지
- Reddit 크롤러 추가 (r/MachineLearning, r/LocalLLaMA)

## 확인해볼 것
- python crawler/reddit.py 실행하면 JSON 나오는지

## 참고
- Reddit API 안 쓰고 RSS로 처리함
```

- PR은 간결하게. 3줄이면 충분
- 상대방은 1일 이내에 확인 (안 되면 카톡으로 알림)
- "Approve" 누르면 본인이 직접 머지
- 충돌(conflict) 나면 브랜치 만든 사람이 해결

### 파일/폴더 구조

```
ai-trend-radar/
  README.md
  .gitignore
  .env.example
  requirements.txt

  crawler/              <- HB 주 담당
    hn.py
    reddit.py
    github_trending.py
    huggingface.py
    dedup.py

  pipeline/             <- HB 주 담당
    summarizer.py
    tagger.py
    scorer.py
    filter.py

  bot/                  <- CJ 주 담당
    telegram_bot.py
    formatter.py
    scheduler.py

  data/                 <- 크롤링 결과 임시 저장 (.gitignore 대상)
    .gitkeep

  prompts/              <- LLM 프롬프트 버전 관리
    summarize_v1.txt
    tag_v1.txt

  scripts/
    run_daily.py        <- 전체 파이프라인 실행
```

---

## 환경 설정

### .env.example

```env
# 텔레그램
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHANNEL_ID=@your_channel_id

# LLM API
OPENAI_API_KEY=your_key_here
# 또는
GOOGLE_API_KEY=your_key_here

# (선택) Supabase
SUPABASE_URL=your_url
SUPABASE_KEY=your_key
```

### 로컬 세팅 순서

```bash
# 1. 레포 클론
git clone git@github.com:yourname/ai-trend-radar.git
cd ai-trend-radar

# 2. 가상환경
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. 의존성 설치
pip install -r requirements.txt

# 4. 환경변수 설정
cp .env.example .env
# .env 파일 열어서 실제 키 입력

# 5. 테스트 실행
python scripts/run_daily.py
```

---

## 절대 하지 말 것

1. **.env 파일 커밋 금지** - API 키, 봇 토큰 등 민감 정보. .gitignore에 반드시 포함.
2. **main 직접 push 금지** - 항상 브랜치 -> PR -> 머지.
3. **상대방 브랜치에서 작업 금지** - 내 브랜치는 내가 관리. 상대 코드 수정 필요하면 말하고 PR로.
4. **큰 파일 커밋 금지** - 데이터셋, 모델 파일, 로그 등은 .gitignore에 추가.
5. **2일 넘게 연락 없이 사라지지 않기** - 바쁘면 바쁘다고 한 줄이라도.

---

## .gitignore

```
# 환경변수
.env

# 가상환경
venv/
.venv/

# Python
__pycache__/
*.pyc
*.pyo
.pytest_cache/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# 데이터/로그
data/*.json
data/*.csv
logs/
*.log

# 기타
node_modules/
dist/
build/
```

---

## 소통 규칙

| 항목 | 방식 |
|------|------|
| **일일 싱크** | 매일 저녁, 카톡으로 3줄: 오늘 한 것 / 막힌 것 / 내일 할 것 |
| **급한 것** | 카톡 (답장 기대: 수 시간 이내) |
| **코드 논의** | GitHub PR 코멘트 또는 Issue |
| **큰 방향 논의** | 통화 또는 디코 (주 1회 이상) |
| **의사결정 교착** | 제품 관련 -> CJ 결정권, 시스템 관련 -> HB 결정권 |

---

## 비용 예산

| 항목 | 월 예상 비용 | 비고 |
|------|-------------|------|
| 크롤링 | $0 | 공개 API/RSS |
| LLM API | ~$3 | GPT-4.1 mini 기준 |
| 서버/크론 | $0 | GitHub Actions 무료 티어 |
| DB | $0 | Supabase 무료 티어 |
| 텔레그램 봇 | $0 | 무료 |
| **합계** | **~$3/월** | X API 추가 시 +$100 |

---

## 진행 현황

### Day 2 (2026-04-09) - CJ

- [x] 텔레그램 채널 개설 완료 (구구 브리핑: https://t.me/gugubrief)
- [x] 텔레그램 봇 생성 및 채널 관리자 등록 완료
- [x] X (트위터) 계정 개설 예정
- [x] 프로젝트 초기 설정 파일 추가 (`.gitignore`, `.env.example`, `requirements.txt`)
- [x] `bot/` 기본 구조 세팅 (`telegram_bot.py`, `formatter.py`, `scheduler.py`)
- [ ] 로컬에서 봇 테스트 메시지 발송 확인 (`python -m bot.telegram_bot`)

### Day 2 - HB

- [ ] 크롤러 뼈대 (HN, Reddit)

---

## 런칭 후 로드맵

- **v1.0** (Day 14): 텔레그램 자동 발행 MVP
- **v1.1** (Week 3-4): 프롬프트 고도화, 2차 소스 추가, 구독자 피드백 반영
- **v1.2** (Month 2): 웹 대시보드 (검색/필터/북마크)
- **v1.3** (Month 3): 이메일 뉴스레터, 유료 멤버십 검토
- **v2.0** (미정): 멀티 LLM 검증, 개인화 추천

---

## 발행 MVP (현재 상태)

### 범위

기존 JSON 결과물(`docs/articles.json`)을 읽어서 Telegram/X 채널에 발행하는 MVP.
수집/요약 파이프라인은 별도 관리되며, 이 MVP는 **"JSON → 발행" 연결만** 담당합니다.

### 입력

- `docs/articles.json` — Discord 다이제스트 또는 크롤러 출력 JSON

### 실행 방법

```bash
# 검증만
python scripts/validate_articles.py

# Dry-run (미리보기, API 호출 없음)
python scripts/run_publish.py --dry-run

# 텔레그램만 발행
python scripts/run_publish.py --platform telegram

# 전체 발행 (Telegram + X)
python scripts/run_publish.py --platform both

# 강제 재발행 (이미 발행된 기사 포함)
python scripts/run_publish.py --force

# 커스텀 입력 파일
python scripts/run_publish.py --input data/latest.json --dry-run
```

### GitHub Actions 수동 실행

Actions → "Daily Signal AI Publish" → Run workflow에서 dry_run, platform, force 옵션을 선택할 수 있습니다.

### 필요한 Secrets

| Secret | 용도 |
|--------|------|
| `TELEGRAM_BOT_TOKEN` | 텔레그램 봇 토큰 |
| `TELEGRAM_CHANNEL_ID` | 텔레그램 채널 ID |
| `X_API_KEY` | X API 키 |
| `X_API_SECRET` | X API 시크릿 |
| `X_ACCESS_TOKEN` | X 액세스 토큰 |
| `X_ACCESS_TOKEN_SECRET` | X 액세스 토큰 시크릿 |

### 협업 원칙

- **수집/요약/소스 확장:** HB (박형빈) 담당 영역
- **발행/포맷/채널 운영:** CJ (박찬준) 담당 영역
- 크롤러, 파이프라인 코드는 이 MVP에서 건드리지 않음

---

## 라이선스

Private repo. 추후 논의.
