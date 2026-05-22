from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from importlib import import_module
from typing import Any

import pytest
from fastapi.testclient import TestClient

from examples.experiment_group.jobping_endpoint_proxy import EndpointProxy
from examples.experiment_group.jobping_envelope_mock import MockEnvelopeEndpoint
from examples.experiment_group.jobping_jpitem_queue_mock import MockJPItemQueue
from examples.experiment_group.jobping_result_handoff import ResultHandoff
from examples.experiment_group.jobping_server_mock import JobPing, is_jobping_disabled, jobping
from examples.experiment_group.jobping_state_sync import StateSync
from examples.experiment_group.jobping_transport_layer import TransportLayerMock


@pytest.mark.parametrize(
    ("module_name", "server_label"),
    [
        ("examples.control_group.server", "server without JP"),
        ("examples.experiment_group.server", "server with JP mock"),
    ],
)
def test_work_response_shape_and_request_counter_cleanup(
    module_name: str,
    server_label: str,
) -> None:
    module = import_module(module_name)
    client = TestClient(module.app)

    reset_response = client.post("/reset")
    assert reset_response.status_code == 200

    work_response = client.get(
        "/work",
        params={"request_id": 42, "sleep_seconds": 0},
    )
    assert work_response.status_code == 200

    output = work_response.json()
    assert sorted(output) == [
        "elapsed_seconds",
        "request_id",
        "sleep_seconds",
        "status",
    ], server_label
    assert output["request_id"] == 42
    assert output["status"] == "OK"

    metrics_response = client.get("/metrics")
    assert metrics_response.status_code == 200
    assert metrics_response.json() == {
        "active_requests": 0,
        "max_active_requests": 1,
        "completed_requests": 1,
    }


def test_server_mock_without_jobping_context_returns_opaque_output(capsys: pytest.CaptureFixture[str]) -> None:
    async def wrapped_callable(value: int) -> dict[str, int | str]:
        return {"value": value, "status": "OK"}

    wrapped = jobping.wrap()(wrapped_callable)
    output = asyncio.run(wrapped(7))

    assert output == {"value": 7, "status": "OK"}

    logs = capsys.readouterr().out
    assert "doing server_proxy.inspect_transport_context" in logs
    assert "doing server_proxy.no_jobping_context_call_wrapped_callable" in logs
    assert "doing server_proxy.capture_call_output" in logs


def test_server_mock_preserves_wrapped_callable_metadata() -> None:
    async def wrapped_callable() -> str:
        """Business callable docstring."""

        return "OK"

    wrapped: Callable[..., Awaitable[Any]] = jobping.wrap()(wrapped_callable)

    assert wrapped.__name__ == "wrapped_callable"
    assert wrapped.__doc__ == "Business callable docstring."


def test_server_mock_unload_switch_preserves_original_call_path(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("JOBPING_DISABLED", "1")

    async def wrapped_callable(value: int) -> dict[str, int | str]:
        return {"value": value, "status": "OK"}

    wrapped = jobping.wrap()(wrapped_callable)
    output = asyncio.run(wrapped(7))

    assert is_jobping_disabled()
    assert output == {"value": 7, "status": "OK"}
    assert capsys.readouterr().out == ""


def test_server_mock_with_job_context_returns_job_ref_and_fulfills_result() -> None:
    async def run() -> None:
        state_sync = StateSync(TransportLayerMock())
        result_handoff = ResultHandoff(TransportLayerMock())
        producer_proxy = EndpointProxy(
            state_sync=state_sync,
            result_handoff=result_handoff,
            queue=MockJPItemQueue(MockEnvelopeEndpoint()),
        )
        consumer_proxy = EndpointProxy(
            state_sync=state_sync,
            result_handoff=result_handoff,
            queue=MockJPItemQueue(MockEnvelopeEndpoint()),
        )
        job_id = producer_proxy.create_job_id()
        server_jobping = JobPing(
            endpoint_proxy=producer_proxy,
            job_context_provider=lambda *args, **kwargs: job_id,
        )

        async def wrapped_callable(value: int) -> dict[str, int | str]:
            return {"value": value, "status": "OK"}

        wrapped = server_jobping.wrap()(wrapped_callable)
        job_ref = await wrapped(7)

        assert producer_proxy.is_job_ref(job_ref)
        assert job_ref["job_id"] == job_id

        consumer_proxy.accept(job_id)
        completed = await consumer_proxy.await_result(job_id)

        assert completed.result == {"value": 7, "status": "OK"}

    asyncio.run(run())
