"""
RAG 기반 질의응답 챗봇.

크롤링된 뉴스 데이터를 기반으로 사용자 질문에 답변한다.
LLM의 일반 지식이 아닌, 실제 수집된 기사 데이터에서 답변을 생성.

사용법:
    from rag.chatbot import ask

    answer = ask("현재 SOTA 모델이 뭐야?")
    print(answer["answer"])
    print(answer["sources"])
"""

import json
import os

import requests
from dotenv import load_dotenv

from rag.search import search

load_dotenv()

# LLM 설정 — Gemini Flash (무료) 또는 GPT-4.1 mini
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini")  # "gemini" 또는 "openai"

SYSTEM_PROMPT = """당신은 AI 뉴스 전문가 챗봇입니다.
아래 제공된 최근 AI 뉴스 기사들만을 참고하여 사용자의 질문에 답변하세요.

규칙:
1. 제공된 기사에 없는 정보는 "수집된 뉴스에서는 관련 정보를 찾지 못했습니다"라고 답하세요.
2. 답변에 출처(기사 제목, URL)를 반드시 포함하세요.
3. 한국어로 답변하되, 기술 용어는 영어 원문을 병기하세요.
4. 간결하고 실용적으로 답변하세요.
"""


def _build_context(articles: list[dict]) -> str:
    """검색된 기사들을 LLM 컨텍스트 문자열로 변환."""
    parts = []
    for i, a in enumerate(articles, 1):
        text = a.get("body") or a.get("summary") or ""
        parts.append(
            f"[기사 {i}]\n"
            f"제목: {a.get('title', '제목 없음')}\n"
            f"출처: {a.get('source', '알 수 없음')}\n"
            f"URL: {a.get('url', '')}\n"
            f"날짜: {a.get('crawled_at', '')}\n"
            f"내용: {text[:1000]}\n"
        )
    return "\n---\n".join(parts)


def _call_gemini(prompt: str, context: str) -> str:
    """Google Gemini API 호출."""
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY 환경변수가 설정되지 않았습니다")

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
    payload = {
        "contents": [
            {
                "parts": [
                    {"text": f"{SYSTEM_PROMPT}\n\n## 참고 기사\n{context}\n\n## 사용자 질문\n{prompt}"}
                ]
            }
        ],
    }
    resp = requests.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]


def _call_openai(prompt: str, context: str) -> str:
    """OpenAI API 호출."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY 환경변수가 설정되지 않았습니다")

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}"}
    payload = {
        "model": "gpt-4.1-mini",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"## 참고 기사\n{context}\n\n## 질문\n{prompt}"},
        ],
        "max_tokens": 1024,
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data["choices"][0]["message"]["content"]


def ask(question: str, top_k: int = 5) -> dict:
    """사용자 질문에 RAG 기반으로 답변.

    Args:
        question: 사용자 질문 (자연어)
        top_k: 참고할 최대 기사 수

    Returns:
        {
            "answer": str,       # LLM 답변
            "sources": list,     # 참고한 기사 목록 [{title, url, similarity}]
            "question": str,     # 원본 질문
        }
    """
    # 1. 관련 기사 검색
    articles = search(question, top_k=top_k)

    if not articles:
        return {
            "answer": "수집된 뉴스에서 관련 정보를 찾지 못했습니다. 다른 키워드로 질문해 주세요.",
            "sources": [],
            "question": question,
        }

    # 2. 컨텍스트 구성
    context = _build_context(articles)

    # 3. LLM 호출
    if LLM_PROVIDER == "openai":
        answer = _call_openai(question, context)
    else:
        answer = _call_gemini(question, context)

    # 4. 결과 반환
    sources = [
        {
            "title": a.get("title", ""),
            "url": a.get("url", ""),
            "similarity": a.get("similarity", 0),
        }
        for a in articles
    ]

    return {
        "answer": answer,
        "sources": sources,
        "question": question,
    }
