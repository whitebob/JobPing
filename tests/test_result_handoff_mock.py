from __future__ import annotations

import asyncio

import pytest

from experiment_group.jobping_id import create_job_id
from experiment_group.jobping_result_handoff import ResultHandoff
from experiment_group.jobping_transport_mock import MockTransportAdapter


def test_result_handoff_fulfills_and_awaits_result() -> None:
    async def run() -> None:
        transport = MockTransportAdapter()
        result_handoff = ResultHandoff(transport)
        job_id = create_job_id()

        waiting = asyncio.create_task(result_handoff.await_result(job_id))
        await asyncio.sleep(0)
        assert transport.size() == {"messages": 0, "waiters": 1}

        result = {"status": "OK", "rows": [1, 2, 3]}
        result_handoff.fulfill(job_id, result)

        assert await waiting is result
        assert transport.size() == {"messages": 0, "waiters": 0}

    asyncio.run(run())


def test_result_handoff_validates_job_id() -> None:
    result_handoff = ResultHandoff(MockTransportAdapter())

    with pytest.raises(ValueError, match="job_id must be a non-empty string"):
        result_handoff.fulfill("", {"status": "OK"})
