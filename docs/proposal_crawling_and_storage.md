# 크롤링 소스 + 저장 방식 제안

작성: 2026-04-15
작성자: HB
관련 브랜치: `docs/memory-systems-20260414` (기억시스템 개념)

## 1. 장기 방향 (공유 이해)

```
데이터 수집(크롤링) → 저장(DB, raw 보존) → 검색(RAG) → Agent 판단 → 결과 제공
```

- LLM 호출 최소화, 원문 직접 인용 기반 응답
- 스코어링으로 독자가 원하는 방향 수렴 (사용 로그 축적 필요 → DB)
- 기억시스템은 `docs/memory-systems-20260414` 브랜치의 `generic-source-grounded-memory-current-best-20260413` 참조 방향

## 2. 데이터 소스 후보 (지속 수집 가능한 것 위주)

### Tier 1 — 즉시 구현, 안정적

| 소스 | 접근 | 한도 | 신선도 | 포맷 |
|---|---|---|---|---|
| **Hacker News** | Algolia API (`hn.algolia.com/api/v1/search_by_date`) | 무료, 실질 무제한 | 분 단위 | JSON (스레드 + 점수 + 댓글 수) |
| **Reddit** | RSS (`.rss` 접미사), /r/LocalLLaMA /r/MachineLearning /r/singularity | RSS 무한, PRAW는 60req/min | 수 분 | XML (→ JSON 변환) |
| **arXiv** | RSS (`arxiv.org/rss/cs.AI` 등) | 무제한 | 매일 21:00 UTC | XML |
| **HuggingFace** | 공개 API (`huggingface.co/api/models?sort=trending`) | 완화된 rate limit | 실시간 | JSON |
| **GeekNews (news.hada.io)** | RSS (`news.hada.io/rss`) | 무제한 | 일간 | XML, 한국어 100% |

### Tier 2 — 추가 가치, 조금 복잡

| 소스 | 접근 | 한도 | 비고 |
|---|---|---|---|
| **LessWrong** | RSS + GraphQL | 무제한 | AI 안전/align 커뮤니티 |
| **Papers with Code** | REST API | 무제한 | 논문 + 깃허브 연결 |
| **GitHub Trending** | HTML 스크래핑 (공식 API 없음) | 차단 위험 | 일간 |
| **Product Hunt** | GraphQL (무료 token) | 무제한 | AI 신제품 |
| **Substacks** (Import AI, AI News) | RSS | 무제한 | 주간 영어 뉴스레터 |

### Tier 3 — 비용/위험 → 보류 또는 신중

| 소스 | 이슈 |
|---|---|
| **X/Twitter** | 무료 API 폐지. Nitter 공개 인스턴스 대부분 CF 차단. twikit/twscrape는 계정 세션 필요, 유지보수 비용↑ |
| **특이점 갤러리 (DCInside)** | 매크로 감지 공격적, IP 차단 위험 |
| **Discord** | 이미 구현됨 (`discord_export_text_only.py`). 특정 서버만 |

## 3. 저장 방식 제안

### 통합 스키마 (source-agnostic)

```sql
CREATE TABLE posts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  -- 식별
  source VARCHAR(20) NOT NULL,              -- 'hn' | 'reddit' | 'discord' | 'arxiv' | ...
  source_id VARCHAR(200) NOT NULL,          -- 소스 측 고유 ID
  source_url TEXT,

  -- 내용 (raw 보존)
  author VARCHAR(100),
  content TEXT NOT NULL,                    -- 원문 전체, 요약 X
  timestamp TIMESTAMPTZ NOT NULL,           -- 원 게시 시각
  fetched_at TIMESTAMPTZ DEFAULT now(),

  -- 관계
  parent_id UUID REFERENCES posts(id),      -- 댓글/스레드

  -- 확장
  metadata JSONB DEFAULT '{}',              -- 소스별 필드 (subreddit, points, guild 등)
  score_fields JSONB DEFAULT '{}',          -- 정규화된 스코어 (engagement, recency, weight)
  embedding vector(1536),                   -- 벡터 검색용 (pgvector)

  UNIQUE (source, source_id)
);

CREATE INDEX idx_posts_ts       ON posts (timestamp DESC);
CREATE INDEX idx_posts_source   ON posts (source);
CREATE INDEX idx_posts_meta     ON posts USING gin (metadata);
CREATE INDEX idx_posts_embed    ON posts USING ivfflat (embedding vector_cosine_ops);
CREATE INDEX idx_posts_content  ON posts USING gin (to_tsvector('simple', content));
```

### 저장소 비교

| 옵션 | 적합도 | 설정 | 벡터검색 | 비용 |
|---|---|---|---|---|
| **PostgreSQL + pgvector** | ⭐⭐⭐⭐⭐ | 중 | ✓ | 자체호스팅 무료 |
| **Supabase 무료** | ⭐⭐⭐⭐⭐ | 낮 | ✓ | 500MB/월 무료, 관리형 Postgres |
| **SQLite + sqlite-vss** | ⭐⭐⭐⭐ | 낮 | △ (플러그인) | 무료, 파일 하나 |
| ChromaDB/Qdrant | ⭐⭐ | 중 | ✓ | 벡터 전용 → 원문 저장 따로 필요 |
| MongoDB | ⭐⭐ | 중 | ✗ | 스키마 유연하나 풀텍스트 약함 |

**추천:**
- 초기 개발: **SQLite + jsonb** (파일 하나, 로컬 테스트 쉬움)
- 프로덕션/공유: **Supabase 무료 티어** (PostgreSQL + pgvector + 관리형)
- 자체호스팅 원하면: 로컬 PostgreSQL + pgvector

### 크롤링 결과 JSON 샘플 (저장 전 중간 포맷)

```json
{
  "source": "hackernews",
  "source_id": "42125",
  "source_url": "https://news.ycombinator.com/item?id=42125",
  "author": "dang",
  "content": "Claude 4.6 Released with 1M Context\n\nSome details...",
  "timestamp": "2026-04-15T08:30:00+00:00",
  "parent_id": null,
  "metadata": {
    "title": "Claude 4.6 Released with 1M Context",
    "points": 342,
    "num_comments": 128,
    "tags": ["story", "front_page"]
  }
}
```

```json
{
  "source": "reddit",
  "source_id": "t3_1xyz89",
  "source_url": "https://reddit.com/r/LocalLLaMA/comments/1xyz89/",
  "author": "u/localllamauser",
  "content": "Tested GPT Image 2 vs Nano Banana Pro...",
  "timestamp": "2026-04-15T04:22:11+00:00",
  "parent_id": null,
  "metadata": {
    "subreddit": "LocalLLaMA",
    "upvotes": 892,
    "num_comments": 156,
    "flair": "Discussion"
  }
}
```

## 4. 다음 단계 제안

### HB가 담당 (크롤링 쪽)
- [ ] `crawlers/` 디렉토리 구조 (`hn.py`, `reddit_rss.py`, `arxiv.py`, `hf_trending.py`)
- [ ] 공통 출력 스키마 준수 (위 JSON 포맷)
- [ ] `run_daily.sh`에 통합 (병렬 실행)
- [ ] 중복 제거 로직 (`(source, source_id)` 유니크)

### CJ가 담당 (DB/계정)
- [ ] Supabase vs 로컬 PostgreSQL 선택
- [ ] 프로젝트 생성, 스키마 적용
- [ ] API 키 안전 저장 방식 (환경변수 / .env.local)

### 공동 논의
- [ ] Gemma 4 유지 vs Gemini 3 Flash 교체
  - Gemma 4: 무료 한도 높음, 구조화 출력 약함
  - Gemini 3 Flash: 유료 but 저렴, JSON mode 지원, 품질↑
  - 추천: 프로덕션은 Flash, 실험은 Gemma 4
- [ ] Agent 레이어 (스코어링 + 사용자 피드백 반영)
- [ ] RAG 검색 인터페이스 설계 (기억시스템 통합 시점)

## 5. 단기 작업 (1주)

1. HN + Reddit + arXiv 크롤러 3개 구현 → JSON 파일로 저장
2. SQLite 스키마 적용 + 인제스트 스크립트
3. 기존 Discord 수집도 이 스키마로 통합
4. V1 원샷 기사 생성 파이프라인에 여러 소스 붙이기 → 통합 다이제스트

---

## 참고

- 메모리시스템 설계: `docs/memory-systems-20260414` 브랜치
- 현재 Discord 파이프라인: `feat/discord-digest-demo` 브랜치
- Gemma 4 구조화 출력 제한: 공식적으로 미지원 (Gemini만 JSON mode)
