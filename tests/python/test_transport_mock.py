from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

from jobping.envelope import JOBPING_RESULT, box_result
from jobping.id import create_job_id
from jobping.transport_layer import (
    JOBPING_JOB_ID_HEADER,
    TransportLayer,
)
from jobping_sandbox.envelope_endpoint_mock import MockEnvelopeEndpoint
from jobping_sandbox.transport_layer_mock import TransportLayerMock


def test_job_id_generation_uses_uuid() -> None:
    job_id = create_job_id()

    assert UUID(job_id).version == 4
    assert create_job_id() != job_id


def test_transport_layer_is_abstract() -> None:
    with pytest.raises(TypeError):
        TransportLayer()


def test_transport_adapter_attaches_and_extracts_job_id() -> None:
    adapter = TransportLayerMock()
    original_carrier = {"headers": {"x-existing": "yes"}}
    job_id = create_job_id()

    carrier = adapter.attach_job_id(original_carrier, job_id)

    assert JOBPING_JOB_ID_HEADER not in original_carrier["headers"]
    assert carrier["headers"]["x-existing"] == "yes"
    assert carrier["headers"][JOBPING_JOB_ID_HEADER] == job_id
    assert adapter.extract_job_id({"headers": {"X-JobPing-Job-Id": job_id}}) == job_id
    assert adapter.extract_job_id({"headers": {}}) is None


def test_transport_adapter_attaches_and_extracts_envelope() -> None:
    adapter = TransportLayerMock()
    job_id = create_job_id()
    envelope = box_result(job_id, {"status": "OK"})

    carrier = adapter.attach_envelope({}, envelope)

    assert adapter.extract_envelope(carrier) == envelope
    with pytest.raises(ValueError, match="Can only attach JobPing envelopes"):
        adapter.attach_envelope({}, {"bad": "shape"})  # type: ignore[arg-type]


def test_transport_adapter_can_delegate_envelope_send_recv() -> None:
    async def run() -> None:
        endpoint = MockEnvelopeEndpoint()
        adapter = TransportLayerMock(envelope_endpoint=endpoint)
        job_id = create_job_id()
        envelope = box_result(job_id, {"status": "OK"})

        adapter.send_envelope(envelope)
        assert endpoint.size() == {"pending": 1, "waiters": 0}
        assert await adapter.recv_envelope(job_id=job_id, type=JOBPING_RESULT) == envelope
        assert endpoint.size() == {"pending": 0, "waiters": 0}

    asyncio.run(run())


def test_transport_adapter_requires_endpoint_for_send_recv() -> None:
    async def run() -> None:
        adapter = TransportLayerMock()
        envelope = box_result(create_job_id(), {"status": "OK"})

        with pytest.raises(RuntimeError, match="No envelope endpoint configured"):
            adapter.send_envelope(envelope)

        with pytest.raises(RuntimeError, match="No envelope endpoint configured"):
            await adapter.recv_envelope()

    asyncio.run(run())
