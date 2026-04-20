"""Published state access.

The previous JSON/GitHub-committed state file path is intentionally gone.
Supabase is the single source for published article state.
"""
from __future__ import annotations

from db.publish_log import DBPublishedState


def get_state(platform: str = "github_pages") -> DBPublishedState:
    return DBPublishedState(platform=platform)
