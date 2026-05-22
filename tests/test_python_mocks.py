from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from importlib import import_module
from typing import Any

import pytest
from fastapi.testclient import TestClient

from experiment_group.jobping_server_mock import is_jobping_disabled, jobping


@pytest.mark.parametrize(
    ("module_name", "server_label"),
    [
        ("control_group.server", "server without JP"),
        ("experiment_group.server", "server with JP mock"),
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
