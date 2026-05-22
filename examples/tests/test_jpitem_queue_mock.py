from __future__ import annotations

import asyncio

import pytest

from examples.experiment_group.jobping_envelope_mock import MockEnvelopeEndpoint
from examples.experiment_group.jobping_jpitem_queue_mock import (
    JPITEM_COMPLETED,
    JPITEM_CREATED,
    JPITEM_QUEUED,
    JPITEM_WAITING,
    MockJPItemQueue,
)


def test_jpitem_queue_lifecycle_and_envelope_handoff() -> None:
    async def run() -> None:
        endpoint = MockEnvelopeEndpoint()
        client_queue = MockJPItemQueue(endpoint)
        server_queue = MockJPItemQueue(endpoint)

        accepted_item = client_queue.accept("job-1")
        assert accepted_item.status == JPITEM_WAITING

        offered_item = server_queue.offer("job-1")
        assert offered_item.status == JPITEM_CREATED
        assert server_queue.defer(offered_item).status == JPITEM_QUEUED

        waiting = asyncio.create_task(client_queue.await_result("job-1"))
        await asyncio.sleep(0)
        assert client_queue.snapshot() == {
            "items": 1,
            "statuses": {JPITEM_WAITING: 1},
            "envelopes": {"pending": 0, "waiters": 1},
        }

        payload = {"status": "OK", "value": 42}
        server_queue.fulfill("job-1", payload)

        completed_item = await waiting
        assert completed_item.status == JPITEM_COMPLETED
        assert completed_item.result is payload
        assert server_queue.get("job-1") is not None
        assert server_queue.get("job-1").status == JPITEM_COMPLETED  # type: ignore[union-attr]
        assert server_queue.get("job-1").result is payload  # type: ignore[union-attr]
        assert endpoint.size() == {"pending": 0, "waiters": 0}

        client_queue.release("job-1")
        server_queue.release("job-1")
        assert client_queue.snapshot() == {
            "items": 0,
            "statuses": {},
            "envelopes": {"pending": 0, "waiters": 0},
        }
        assert server_queue.snapshot() == {
            "items": 0,
            "statuses": {},
            "envelopes": {"pending": 0, "waiters": 0},
        }

    asyncio.run(run())


def test_jpitem_queue_rejects_invalid_role_operations() -> None:
    async def run() -> None:
        endpoint = MockEnvelopeEndpoint()
        client_queue = MockJPItemQueue(endpoint)
        server_queue = MockJPItemQueue(endpoint)

        client_queue.accept("job-1")
        server_queue.offer("job-1")

        with pytest.raises(ValueError, match="JPItem already exists"):
            client_queue.accept("job-1")

        with pytest.raises(ValueError, match="Only offered JPItems can be fulfilled"):
            client_queue.fulfill("job-1", {"status": "OK"})

        with pytest.raises(ValueError, match="Only accepted JPItems can await results"):
            await server_queue.await_result("job-1", timeout=0.001)

        with pytest.raises(ValueError, match="Unknown JPItem"):
            server_queue.fulfill("missing", {"status": "OK"})

    asyncio.run(run())
