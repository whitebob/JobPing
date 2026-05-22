"""Endpoint-local JPItem queue mock."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from examples.experiment_group.jobping_envelope_mock import (
    JOBPING_RESULT,
    MockEnvelopeEndpoint,
    box_result,
    unbox_result,
)


JPITEM_CREATED = "created"
JPITEM_WAITING = "waiting"
JPITEM_QUEUED = "queued"
JPITEM_COMPLETED = "completed"
JPITEM_DESTROYED = "destroyed"

JPItemRole = Literal["producer", "consumer"]
JPItemStatus = Literal["created", "waiting", "queued", "completed", "destroyed"]


@dataclass
class MockJPItem:
    job_id: str
    role: JPItemRole
    status: JPItemStatus
    result: Any = None


def _assert_valid_job_id(job_id: str) -> None:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")


class MockJPItemQueue:
    """Small queue surface for endpoint-side JPItem lifecycle tests."""

    def __init__(self, envelope_endpoint: MockEnvelopeEndpoint) -> None:
        self.envelope_endpoint = envelope_endpoint
        self._items: dict[str, MockJPItem] = {}

    def accept(self, job_id: str) -> MockJPItem:
        _assert_valid_job_id(job_id)
        self._assert_missing(job_id)

        item = MockJPItem(
            job_id=job_id,
            role="consumer",
            status=JPITEM_WAITING,
        )
        self._items[job_id] = item
        return item

    def offer(self, job_id: str) -> MockJPItem:
        _assert_valid_job_id(job_id)
        self._assert_missing(job_id)

        item = MockJPItem(
            job_id=job_id,
            role="producer",
            status=JPITEM_CREATED,
        )
        self._items[job_id] = item
        return item

    def defer(self, item_or_job_id: MockJPItem | str) -> MockJPItem:
        item = self._resolve_item(item_or_job_id)
        if item.role != "producer":
            raise ValueError("Only offered JPItems can be deferred")

        item.status = JPITEM_QUEUED
        return item

    def fulfill(self, job_id: str, result: Any) -> MockJPItem:
        item = self._resolve_item(job_id)
        if item.role != "producer":
            raise ValueError("Only offered JPItems can be fulfilled")

        item.status = JPITEM_COMPLETED
        item.result = result
        self.envelope_endpoint.send(box_result(job_id, result))
        return item

    async def await_result(
        self,
        job_id: str,
        *,
        timeout: float = 1.0,
    ) -> MockJPItem:
        item = self._resolve_item(job_id)
        if item.role != "consumer":
            raise ValueError("Only accepted JPItems can await results")

        item.status = JPITEM_WAITING
        envelope = await self.envelope_endpoint.recv(
            job_id=job_id,
            type=JOBPING_RESULT,
            timeout=timeout,
        )
        item.result = unbox_result(envelope, expected_job_id=job_id)
        item.status = JPITEM_COMPLETED
        return item

    def release(self, job_id: str) -> MockJPItem:
        item = self._resolve_item(job_id)
        item.status = JPITEM_DESTROYED
        del self._items[job_id]
        return item

    def get(self, job_id: str) -> MockJPItem | None:
        _assert_valid_job_id(job_id)
        return self._items.get(job_id)

    def snapshot(self) -> dict[str, Any]:
        statuses: dict[str, int] = {}
        for item in self._items.values():
            statuses[item.status] = statuses.get(item.status, 0) + 1

        return {
            "items": len(self._items),
            "statuses": statuses,
            "envelopes": self.envelope_endpoint.size(),
        }

    def _assert_missing(self, job_id: str) -> None:
        if job_id in self._items:
            raise ValueError(f"JPItem already exists: {job_id}")

    def _resolve_item(self, item_or_job_id: MockJPItem | str) -> MockJPItem:
        job_id = item_or_job_id if isinstance(item_or_job_id, str) else item_or_job_id.job_id
        _assert_valid_job_id(job_id)

        item = self._items.get(job_id)
        if item is None:
            raise ValueError(f"Unknown JPItem: {job_id}")

        return item


# Future producer endpoint pseudocode:
# jp_item = queue.offer(job_id)
# queue.defer(jp_item)
# endpoint_proxy.fulfill_later(
#     task=lambda: wrapped_callable(*args, **kwargs),
#     on_done=lambda result: queue.fulfill(job_id, result),
# )
#
# Future consumer endpoint pseudocode:
# queue.accept(job_id)
# completed_item = await queue.await_result(job_id)
# return completed_item.result
