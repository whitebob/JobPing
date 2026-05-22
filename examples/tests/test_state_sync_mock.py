from __future__ import annotations

import asyncio

import pytest

from examples.experiment_group.jobping_id import create_job_id
from examples.experiment_group.jobping_state_sync import StateSync
from examples.experiment_group.jobping_transport_layer import TransportLayerMock


def test_state_sync_publish_and_wait_for_state_context() -> None:
    async def run() -> None:
        transport = TransportLayerMock()
        state_sync = StateSync(transport)
        job_id = create_job_id()

        waiting = asyncio.create_task(state_sync.wait_for(job_id, status="running"))
        await asyncio.sleep(0)
        assert transport.size() == {"messages": 0, "waiters": 1}

        state_sync.publish(job_id, "queued", {"path": ["created", "queued"]})
        state_sync.publish(job_id, "running", {"path": ["created", "queued", "running"]})

        assert await waiting == {
            "status": "running",
            "state_context": {
                "path": ["created", "queued", "running"],
            },
        }
        assert transport.size() == {"messages": 0, "waiters": 0}

    asyncio.run(run())


def test_state_sync_validates_job_id_and_status() -> None:
    state_sync = StateSync(TransportLayerMock())

    with pytest.raises(ValueError, match="job_id must be a non-empty string"):
        state_sync.publish("", "running")

    with pytest.raises(ValueError, match="status must be a non-empty string"):
        state_sync.publish(create_job_id(), "")
