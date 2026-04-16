"""
Supabase 클라이언트 초기화.

환경변수:
  SUPABASE_URL  — Supabase 프로젝트 URL
  SUPABASE_KEY  — Supabase anon/service-role 키
"""

import os

from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

_client: Client | None = None


def get_client() -> Client:
    """싱글턴 Supabase 클라이언트 반환."""
    global _client
    if _client is None:
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_KEY"]
        _client = create_client(url, key)
    return _client
