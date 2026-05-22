from __future__ import annotations

import asyncio

import pytest

from experiment_group.jobping_endpoint_proxy import EndpointProxy
from experiment_group.jobping_envelope_mock import MockEnvelopeEndpoint
from experiment_group.jobping_jpitem_queue_mock import (
    JPITEM_COMPLETED,
    JPITEM_QUEUED,
    MockJPItemQueue,
)
from experiment_group.jobping_result_handoff import ResultHandoff
from experiment_group.jobping_state_sync import StateSync
from experiment_group.jobping_transport_layer import TransportLayerMock


def test_endpoint_proxy_composes_state_sync_result_handoff_and_queue() -> None:
    async def run() -> None:
        state_transport = TransportLayerMock()
        result_transport = TransportLayerMock()
        state_sync = StateSync(state_transport)
        result_handoff = ResultHandoff(result_transport)

        producer = EndpointProxy(
            state_sync=state_sync,
            result_handoff=result_handoff,
            queue=MockJPItemQueue(MockEnvelopeEndpoint()),
        )
        consumer = EndpointProxy(
            state_sync=state_sync,
            result_handoff=result_handoff,
            queue=MockJPItemQueue(MockEnvelopeEndpoint()),
        )

        job_id = producer.create_job_id()

        offered = producer.offer(job_id)
        assert offered.job_id == job_id
        assert producer.defer(job_id).status == JPITEM_QUEUED

        state_wait = asyncio.create_task(
            consumer.wait_for_state(job_id, status="running"),
        )
        await asyncio.sleep(0)
        producer.publish_state(
            job_id,
            "running",
            {"path": ["created", "queued", "running"]},
        )
        assert await state_wait == {
            "status": "running",
            "state_context": {
                "path": ["created", "queued", "running"],
            },
        }

        accepted = consumer.accept(job_id)
        assert accepted.job_id == job_id

        result_wait = asyncio.create_task(consumer.await_result(job_id))
        await asyncio.sleep(0)
        result = {"status": "OK", "rows": [1, 2, 3]}
        assert await producer.fulfill_later(job_id, lambda: async_result(result)) is result

        completed = await result_wait
        assert completed.status == JPITEM_COMPLETED
        assert completed.result is result
        assert producer.queue.get(job_id) is not None
        assert producer.queue.get(job_id).status == JPITEM_COMPLETED  # type: ignore[union-attr]
        assert producer.queue.get(job_id).result is result  # type: ignore[union-attr]

        consumer.release(job_id)
        producer.release(job_id)
        assert consumer.queue.get(job_id) is None
        assert producer.queue.get(job_id) is None

    asyncio.run(run())


async def async_result(result: object) -> object:
    return result


def test_endpoint_proxy_rejects_fulfill_without_offer() -> None:
    state_sync = StateSync(TransportLayerMock())
    result_handoff = ResultHandoff(TransportLayerMock())
    proxy = EndpointProxy(
        state_sync=state_sync,
        result_handoff=result_handoff,
        queue=MockJPItemQueue(MockEnvelopeEndpoint()),
    )

    job_id = proxy.create_job_id()
    proxy.accept(job_id)

    with pytest.raises(ValueError, match="Only offered JPItems can be fulfilled"):
        proxy.fulfill(job_id, {"status": "OK"})
