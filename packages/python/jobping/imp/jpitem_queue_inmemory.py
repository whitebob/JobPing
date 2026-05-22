from __future__ import annotations

from typing import Any
from dataclasses import dataclass

from jobping.envelope import JOBPING_RESULT, box_result, unbox_result

JPITEM_CREATED = "created"
JPITEM_WAITING = "waiting"
JPITEM_QUEUED = "queued"
JPITEM_COMPLETED = "completed"
JPITEM_DESTROYED = "destroyed"


def _assert_valid_job_id(job_id: str) -> None:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")


@dataclass
class JPItem:
    job_id: str
    role: str
    status: str
    result: Any = None


class JPItemQueueInMemory:
    """In-memory JPItemQueue implementation (placed under imp).

    This implementation intentionally avoids importing package-level types to
    prevent circular imports; it mirrors the behaviour expected by the public
    JPItemQueue interface.
    """

    def __init__(self, envelope_endpoint: Any) -> None:
        if not hasattr(envelope_endpoint, "send") or not hasattr(envelope_endpoint, "recv"):
            raise ValueError("envelope_endpoint must implement send() and recv()")
        self.envelope_endpoint = envelope_endpoint
        self._items: dict[str, JPItem] = {}

    def offer(self, job_id: str):
        _assert_valid_job_id(job_id)
        if job_id in self._items:
            raise ValueError(f"JPItem already exists: {job_id}")

        item = JPItem(job_id=job_id, role="producer", status=JPITEM_CREATED, result=None)
        self._items[job_id] = item
        return item

    def defer(self, item_or_job_id: Any):
        item = self._resolve_item(item_or_job_id)
        if item.role != "producer":
            raise ValueError("Only offered JPItems can be deferred")
        item.status = JPITEM_QUEUED
        return item

    def fulfill(self, job_id: str, result: Any):
        item = self._resolve_item(job_id)
        if item.role != "producer":
            raise ValueError("Only offered JPItems can be fulfilled")
        item.status = JPITEM_COMPLETED
        item.result = result
        self.envelope_endpoint.send(box_result(job_id, result))
        return item

    async def await_result(self, job_id: str, *, timeout: float = 1.0):
        item = self._resolve_item(job_id)
        if item.role != "consumer":
            raise ValueError("Only accepted JPItems can await results")

        item.status = JPITEM_WAITING
        envelope = await self.envelope_endpoint.recv(job_id=job_id, type=JOBPING_RESULT, timeout=timeout)
        item.result = unbox_result(envelope, expected_job_id=job_id)
        item.status = JPITEM_COMPLETED
        return item

    def accept(self, job_id: str):
        _assert_valid_job_id(job_id)
        if job_id in self._items:
            raise ValueError(f"JPItem already exists: {job_id}")
        item = JPItem(job_id=job_id, role="consumer", status=JPITEM_WAITING, result=None)
        self._items[job_id] = item
        return item

    def release(self, job_id: str):
        item = self._resolve_item(job_id)
        item.status = JPITEM_DESTROYED
        del self._items[job_id]
        return item

    def get(self, job_id: str):
        _assert_valid_job_id(job_id)
        return self._items.get(job_id)

    def snapshot(self) -> dict[str, Any]:
        statuses: dict[str, int] = {}
        for item in self._items.values():
            statuses[item.status] = statuses.get(item.status, 0) + 1
        return {"items": len(self._items), "statuses": statuses, "envelopes": self.envelope_endpoint.size()}

    def _resolve_item(self, item_or_job_id: Any):
        if isinstance(item_or_job_id, str):
            job_id = item_or_job_id
        else:
            # support either mapping-like inputs or JPItem instances
            job_id = getattr(item_or_job_id, "job_id", None) or (item_or_job_id.get("job_id") if hasattr(item_or_job_id, "get") else None)
        _assert_valid_job_id(job_id)
        item = self._items.get(job_id)
        if item is None:
            raise ValueError(f"Unknown JPItem: {job_id}")
        return item
