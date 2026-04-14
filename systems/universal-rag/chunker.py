"""
Universal document chunker.

Strategy: overlapping fixed-size windows with paragraph-boundary snapping.
Each chunk carries (char_start, char_end) offsets into the original body
so the raw source is always recoverable.

- target_size: ~800 chars (fits well in embedding models)
- overlap: 200 chars
- boundary snapping: prefers splitting at paragraph breaks (\n\n) or
  line breaks (\n) within a tolerance window
"""
from __future__ import annotations

import re


PARA_BREAK = re.compile(r"\n\s*\n")
LINE_BREAK = re.compile(r"\n")


def chunk_text(
    text: str,
    target_size: int = 800,
    overlap: int = 200,
    min_chunk: int = 100,
) -> list[dict]:
    """Split text into overlapping chunks with boundary snapping.

    Returns [{'text': str, 'char_start': int, 'char_end': int}].
    """
    if len(text) <= target_size:
        return [{"text": text, "char_start": 0, "char_end": len(text)}]

    chunks = []
    pos = 0
    while pos < len(text):
        end = min(pos + target_size, len(text))
        if end < len(text):
            # Try to snap to paragraph break within tolerance
            search_start = max(pos + target_size - 200, pos + min_chunk)
            search_end = min(pos + target_size + 100, len(text))
            window = text[search_start:search_end]
            # Prefer paragraph break
            m = PARA_BREAK.search(window)
            if m:
                end = search_start + m.end()
            else:
                # Fall back to line break
                m = LINE_BREAK.search(window)
                if m:
                    end = search_start + m.end()

        chunk_text_val = text[pos:end].strip()
        if len(chunk_text_val) >= min_chunk:
            chunks.append({
                "text": chunk_text_val,
                "char_start": pos,
                "char_end": end,
            })
        # Advance with overlap
        pos = max(pos + 1, end - overlap)
        if pos >= len(text):
            break

    return chunks
