"""Supabase client factory.

Reads use the anon key. Writes and private pipeline state use the service role
key and must only run on the local automation machine.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dotenv is optional at import time.
    load_dotenv = None

ROOT = Path(__file__).resolve().parents[1]
_CLIENTS: dict[str, Any] = {}


def _load_env() -> None:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env", override=False)


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required for Supabase access")
    return value


def get_client(service: bool = False):
    """Return a cached Supabase client.

    Args:
        service: False for anon/read client, True for service_role/write client.
    """
    _load_env()
    cache_key = "service" if service else "anon"
    if cache_key in _CLIENTS:
        return _CLIENTS[cache_key]

    url = _required_env("SUPABASE_URL")
    key_name = "SUPABASE_SERVICE_ROLE_KEY" if service else "SUPABASE_ANON_KEY"
    key = _required_env(key_name)

    try:
        from supabase import create_client
    except Exception as exc:  # pragma: no cover - depends on local install.
        raise RuntimeError("supabase package is required. Install requirements.txt.") from exc

    client = create_client(url, key)
    _CLIENTS[cache_key] = client
    return client


def clear_client_cache() -> None:
    _CLIENTS.clear()
