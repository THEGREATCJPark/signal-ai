#!/usr/bin/env python3
"""
Minimal OpenAI-compatible proxy over gemini-webapi.

  POST /v1/chat/completions  →  Gemini 2.5 Pro via browser session
  GET  /v1/models            →  list available models

Usage:
  python3 gemini_proxy.py &
  export OPENAI_API_KEY=dummy
  export OPENAI_BASE_URL=http://127.0.0.1:8321/v1
  # now any OpenAI client/SDK uses Gemini for free
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import time
import uuid
from pathlib import Path

from aiohttp import web

COOKIE_DB = "/tmp/ff_cookies.sqlite"
PORT = 8321
_client = None


def load_cookies():
    con = sqlite3.connect(COOKIE_DB)
    cur = con.cursor()
    cur.execute(
        "SELECT name, value FROM moz_cookies WHERE host=? AND name IN (?, ?)",
        (".google.com", "__Secure-1PSID", "__Secure-1PSIDTS"),
    )
    return dict(cur.fetchall())


async def get_client():
    global _client
    if _client is None:
        from gemini_webapi import GeminiClient

        cookies = load_cookies()
        _client = GeminiClient(
            cookies["__Secure-1PSID"],
            cookies["__Secure-1PSIDTS"],
            proxy=None,
        )
        await _client.init(timeout=30)
    return _client


async def handle_completions(request: web.Request):
    body = await request.json()
    messages = body.get("messages", [])
    prompt_parts = []
    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        # Merge all roles into a single user prompt (Gemini web doesn't
        # support separate system/assistant roles cleanly).
        prompt_parts.append(content)
    prompt = "\n\n".join(prompt_parts)

    client = await get_client()
    t0 = time.time()
    try:
        resp = await client.generate_content(prompt)
        text = resp.text or ""
    except Exception as e:
        text = f"Error: {e}"
    elapsed = time.time() - t0

    result = {
        "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": "gemini-2.5-pro-via-proxy",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": len(prompt) // 4,
            "completion_tokens": len(text) // 4,
            "total_tokens": (len(prompt) + len(text)) // 4,
        },
    }
    return web.json_response(result)


async def handle_models(request: web.Request):
    return web.json_response(
        {
            "object": "list",
            "data": [
                {
                    "id": "gemini-2.5-pro-via-proxy",
                    "object": "model",
                    "owned_by": "google-via-proxy",
                }
            ],
        }
    )


app = web.Application()
app.router.add_post("/v1/chat/completions", handle_completions)
app.router.add_get("/v1/models", handle_models)

if __name__ == "__main__":
    print(f"Starting Gemini proxy on http://127.0.0.1:{PORT}/v1")
    web.run_app(app, host="127.0.0.1", port=PORT)
