"""
임베딩 생성 모듈 — sentence-transformers (multilingual-e5-small).

사용법:
    from rag.embedder import embed, embed_batch

    vec = embed("오늘 발표된 SOTA 모델")
    vecs = embed_batch(["텍스트1", "텍스트2"])
"""

from sentence_transformers import SentenceTransformer

_MODEL_NAME = "intfloat/multilingual-e5-small"
_model: SentenceTransformer | None = None


def _get_model() -> SentenceTransformer:
    """모델 싱글턴 로딩 (최초 호출 시 다운로드 ~470MB)."""
    global _model
    if _model is None:
        _model = SentenceTransformer(_MODEL_NAME)
    return _model


def _prepare_text(text: str, prefix: str = "query: ") -> str:
    """E5 모델 형식에 맞게 prefix 추가.

    E5 모델은 입력에 prefix가 필요:
      - 검색 쿼리: "query: ..."
      - 문서/기사: "passage: ..."
    """
    return f"{prefix}{text}"


def embed(text: str, prefix: str = "query: ") -> list[float]:
    """단일 텍스트를 384차원 벡터로 변환."""
    model = _get_model()
    prepared = _prepare_text(text, prefix)
    vec = model.encode(prepared, normalize_embeddings=True)
    return vec.tolist()


def embed_batch(texts: list[str], prefix: str = "passage: ", batch_size: int = 32) -> list[list[float]]:
    """여러 텍스트를 일괄 임베딩."""
    model = _get_model()
    prepared = [_prepare_text(t, prefix) for t in texts]
    vecs = model.encode(prepared, normalize_embeddings=True, batch_size=batch_size)
    return vecs.tolist()


def embed_article(article: dict) -> list[float]:
    """기사 dict에서 텍스트를 추출하여 임베딩 생성.

    title + summary를 결합하여 임베딩.
    body가 있으면 앞 500자까지 추가.
    """
    parts = [article.get("title", "")]
    summary = article.get("summary", "")
    if summary:
        parts.append(summary)
    body = article.get("body", "")
    if body:
        parts.append(body[:500])
    text = " ".join(parts)
    return embed(text, prefix="passage: ")
