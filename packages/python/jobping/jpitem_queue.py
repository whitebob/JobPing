"""JPItemQueue interface and an in-memory implementation.

This module provides:
- JPItemQueue: abstract base class describing the queue surface used by
  endpoint proxies and wrappers.
- InMemoryJPItemQueue: a simple in-process implementation suitable for
  examples, tests, and lightweight deployments. It mirrors the behaviour
  of the existing sandbox MockJPItemQueue but lives in the public package.

The implementation intentionally depends only on an envelope endpoint
with send(envelope) and recv(job_id=..., type=..., timeout=...) methods
so it can be used with the existing MockEnvelopeEndpoint in sandbox or
with a production envelope endpoint later.
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal

from jobping.envelope import JOBPING_RESULT, box_result, unbox_result

JPItemRole = Literal["producer", "consumer"]
JPItemStatus = Literal["created", "waiting", "queued", "completed", "destroyed"]

# Status constants for tests and external usage
JPITEM_CREATED = "created"
JPITEM_WAITING = "waiting"
JPITEM_QUEUED = "queued"
JPITEM_COMPLETED = "completed"
JPITEM_DESTROYED = "destroyed"


@dataclass
class JPItem:
    job_id: str
    role: JPItemRole
    status: JPItemStatus
    result: Any = None


class JPItemQueue(ABC):
    """Abstract JPItem queue surface used by EndpointProxy and wrappers."""

    @abstractmethod
    def offer(self, job_id: str) -> JPItem:
        raise NotImplementedError

    @abstractmethod
    def defer(self, item_or_job_id: JPItem | str) -> JPItem:
        raise NotImplementedError

    @abstractmethod
    def fulfill(self, job_id: str, result: Any) -> JPItem:
        raise NotImplementedError

    @abstractmethod
    async def await_result(self, job_id: str, *, timeout: float = 1.0) -> JPItem:
        raise NotImplementedError

    @abstractmethod
    def accept(self, job_id: str) -> JPItem:
        raise NotImplementedError

    @abstractmethod
    def release(self, job_id: str) -> JPItem:
        raise NotImplementedError

    @abstractmethod
    def get(self, job_id: str) -> JPItem | None:
        raise NotImplementedError

    @abstractmethod
    def snapshot(self) -> dict[str, Any]:
        raise NotImplementedError


def _assert_valid_job_id(job_id: str) -> None:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")


# Concrete implementations live in the imp subpackage. Import and re-export
# the in-memory implementation for backward compatibility.
from importlib import import_module

try:
    _imp = import_module("jobping.imp.jpitem_queue_inmemory")
    JPItemQueueInMemory = getattr(_imp, "JPItemQueueInMemory")
except Exception:
    # If the imp module is unavailable, leave the name undefined to surface
    # import-time errors where the implementation is required.
    JPItemQueueInMemory = None
