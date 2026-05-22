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


class JPItemQueueInMemory(JPItemQueue):
    """In-process JPItem queue implementation (JPItemQueueInMemory).

    Designed to match the behaviour of sandbox MockJPItemQueue so it can be
    used as a drop-in replacement for examples and most tests.
    """

    def __init__(self, envelope_endpoint: Any) -> None:
        # Envelope endpoint must implement send(envelope) and recv(job_id=..., type=..., timeout=...)
        self.envelope_endpoint = envelope_endpoint
        self._items: dict[str, JPItem] = {}

    def offer(self, job_id: str) -> JPItem:
        _assert_valid_job_id(job_id)
        if job_id in self._items:
            raise ValueError(f"JPItem already exists: {job_id}")

        item = JPItem(job_id=job_id, role="producer", status="created")
        self._items[job_id] = item
        return item

    def defer(self, item_or_job_id: JPItem | str) -> JPItem:
        item = self._resolve_item(item_or_job_id)
        if item.role != "producer":
            raise ValueError("Only offered JPItems can be deferred")

        item.status = "queued"
        return item

    def fulfill(self, job_id: str, result: Any) -> JPItem:
        item = self._resolve_item(job_id)
        if item.role != "producer":
            raise ValueError("Only offered JPItems can be fulfilled")

        item.status = "completed"
        item.result = result
        # send boxed result via envelope endpoint
        self.envelope_endpoint.send(box_result(job_id, result))
        return item

    async def await_result(self, job_id: str, *, timeout: float = 1.0) -> JPItem:
        item = self._resolve_item(job_id)
        if item.role != "consumer":
            raise ValueError("Only accepted JPItems can await results")

        item.status = "waiting"
        envelope = await self.envelope_endpoint.recv(job_id=job_id, type=JOBPING_RESULT, timeout=timeout)
        item.result = unbox_result(envelope, expected_job_id=job_id)
        item.status = "completed"
        return item

    def accept(self, job_id: str) -> JPItem:
        _assert_valid_job_id(job_id)
        if job_id in self._items:
            raise ValueError(f"JPItem already exists: {job_id}")

        item = JPItem(job_id=job_id, role="consumer", status="waiting")
        self._items[job_id] = item
        return item

    def release(self, job_id: str) -> JPItem:
        item = self._resolve_item(job_id)
        item.status = "destroyed"
        del self._items[job_id]
        return item

    def get(self, job_id: str) -> JPItem | None:
        _assert_valid_job_id(job_id)
        return self._items.get(job_id)

    def snapshot(self) -> dict[str, Any]:
        statuses: dict[str, int] = {}
        for item in self._items.values():
            statuses[item.status] = statuses.get(item.status, 0) + 1

        return {"items": len(self._items), "statuses": statuses, "envelopes": self.envelope_endpoint.size()}

    def _resolve_item(self, item_or_job_id: JPItem | str) -> JPItem:
        job_id = item_or_job_id if isinstance(item_or_job_id, str) else item_or_job_id.job_id
        _assert_valid_job_id(job_id)

        item = self._items.get(job_id)
        if item is None:
            raise ValueError(f"Unknown JPItem: {job_id}")

        return item
