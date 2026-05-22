from __future__ import annotations

import asyncio

import pytest

from experiment_group.jobping_envelope_mock import (
    JOBPING_JOB_REF,
    JOBPING_RESULT,
    MockEnvelopeEndpoint,
    box_job_ref,
    box_result,
    is_envelope,
    is_job_ref_envelope,
    is_result_envelope,
    unbox_result,
)


def test_boxing_and_unboxing_result_envelopes() -> None:
    job_ref = box_job_ref("job-1")
    assert job_ref == {
        "jobping": "jobping.envelope.v1",
        "type": JOBPING_JOB_REF,
        "job_id": "job-1",
    }
    assert is_envelope(job_ref)
    assert is_job_ref_envelope(job_ref)
    assert not is_result_envelope(job_ref)

    payload = {"status": "OK", "value": 42}
    result = box_result("job-1", payload)
    assert is_envelope(result)
    assert is_result_envelope(result)
    assert unbox_result(result, expected_job_id="job-1") is payload


def test_envelope_validation_rejects_wrong_shape() -> None:
    with pytest.raises(ValueError, match="job_id must be a non-empty string"):
        box_job_ref("")

    with pytest.raises(ValueError, match="Expected JobPing result envelope"):
        unbox_result(box_job_ref("job-1"))

    with pytest.raises(ValueError, match="Unexpected JobPing result job_id"):
        unbox_result(box_result("job-1", {"status": "OK"}), expected_job_id="job-2")


def test_mock_endpoint_send_and_recv_pending_envelope() -> None:
    async def run() -> None:
        endpoint = MockEnvelopeEndpoint()
        result = box_result("job-1", {"status": "DONE"})

        endpoint.send(result)
        assert endpoint.size() == {"pending": 1, "waiters": 0}

        received = await endpoint.recv(job_id="job-1", type=JOBPING_RESULT)
        assert received == result
        assert endpoint.size() == {"pending": 0, "waiters": 0}

    asyncio.run(run())


def test_mock_endpoint_send_to_waiting_receiver() -> None:
    async def run() -> None:
        endpoint = MockEnvelopeEndpoint()

        waiting = asyncio.create_task(
            endpoint.recv(job_id="job-2", type=JOBPING_RESULT),
        )
        await asyncio.sleep(0)
        assert endpoint.size() == {"pending": 0, "waiters": 1}

        result = box_result("job-2", {"status": "DONE"})
        endpoint.send(result)

        assert await waiting == result
        assert endpoint.size() == {"pending": 0, "waiters": 0}

    asyncio.run(run())


def test_mock_endpoint_timeout_removes_waiter() -> None:
    async def run() -> None:
        endpoint = MockEnvelopeEndpoint()

        with pytest.raises(TimeoutError):
            await endpoint.recv(job_id="missing", timeout=0.001)

        assert endpoint.size() == {"pending": 0, "waiters": 0}

    asyncio.run(run())
