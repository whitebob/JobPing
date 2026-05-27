"""JobPing id generation."""

from __future__ import annotations

from uuid import uuid4


def create_job_id() -> str:
    return str(uuid4())


def create_peer_id() -> str:
    return f"peer-{uuid4()}"
