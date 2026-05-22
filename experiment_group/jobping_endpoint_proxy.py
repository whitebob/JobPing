"""Endpoint-level JobPing composition root."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from experiment_group.jobping_id import create_job_id as default_create_job_id
from experiment_group.jobping_jpitem_queue_mock import (
    JPITEM_COMPLETED,
    MockJPItem,
    MockJPItemQueue,
)
from experiment_group.jobping_result_handoff import ResultHandoff
from experiment_group.jobping_state_sync import StateSync


JOBPING_JOB_REF_KIND = "jobping.job_ref.v1"


def _assert_valid_job_id(job_id: str) -> None:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")


class EndpointProxy:
    def __init__(
        self,
        *,
        state_sync: StateSync,
        result_handoff: ResultHandoff,
        queue: MockJPItemQueue,
        create_job_id: Callable[[], str] = default_create_job_id,
    ) -> None:
        self.state_sync = state_sync
        self.result_handoff = result_handoff
        self.queue = queue
        self._create_job_id = create_job_id

    def create_job_id(self) -> str:
        return self._create_job_id()

    def make_job_ref(self, job_id: str) -> dict[str, str]:
        _assert_valid_job_id(job_id)
        return {
            "jobping": JOBPING_JOB_REF_KIND,
            "type": "job_ref",
            "job_id": job_id,
        }

    def is_job_ref(self, value: Any) -> bool:
        return (
            isinstance(value, dict)
            and value.get("jobping") == JOBPING_JOB_REF_KIND
            and value.get("type") == "job_ref"
            and isinstance(value.get("job_id"), str)
            and len(value["job_id"]) > 0
        )

    def offer(self, job_id: str | None = None) -> MockJPItem:
        return self.queue.offer(job_id or self.create_job_id())

    def accept(self, job_id: str) -> MockJPItem:
        _assert_valid_job_id(job_id)
        return self.queue.accept(job_id)

    def defer(self, item_or_job_id: MockJPItem | str) -> MockJPItem:
        return self.queue.defer(item_or_job_id)

    def publish_state(
        self,
        job_id: str,
        status: str,
        state_context: Any = None,
    ) -> None:
        self.state_sync.publish(job_id, status, state_context)

    async def wait_for_state(
        self,
        job_id: str,
        *,
        status: str | None = None,
        timeout: float = 1.0,
    ) -> Any:
        return await self.state_sync.wait_for(
            job_id,
            status=status,
            timeout=timeout,
        )

    def fulfill(self, job_id: str, result: Any) -> MockJPItem:
        _assert_valid_job_id(job_id)
        item = self.queue.get(job_id)
        if item is None or item.role != "producer":
            raise ValueError("Only offered JPItems can be fulfilled")

        item.status = JPITEM_COMPLETED
        item.result = result
        self.result_handoff.fulfill(job_id, result)
        return item

    async def fulfill_later(
        self,
        job_id: str,
        task: Callable[[], Awaitable[Any]],
    ) -> Any:
        _assert_valid_job_id(job_id)
        result = await task()
        self.fulfill(job_id, result)
        return result

    async def await_result(
        self,
        job_id: str,
        *,
        timeout: float = 1.0,
    ) -> MockJPItem:
        _assert_valid_job_id(job_id)
        item = self.queue.get(job_id)
        if item is None or item.role != "consumer":
            raise ValueError("Only accepted JPItems can await results")

        result = await self.result_handoff.await_result(job_id, timeout=timeout)
        item.status = JPITEM_COMPLETED
        item.result = result
        return item

    def release(self, job_id: str) -> MockJPItem:
        return self.queue.release(job_id)
