"""State synchronization semantic mock."""

from __future__ import annotations

from typing import Any, TypedDict

from experiment_group.jobping_transport_mock import MockTransportAdapter


JOBPING_STATE_UPDATE = "jobping.state_update.v1"


class StateUpdate(TypedDict):
    status: str
    state_context: Any


def _assert_valid_job_id(job_id: str) -> None:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")


def _assert_valid_status(status: str) -> None:
    if not isinstance(status, str) or not status:
        raise ValueError("status must be a non-empty string")


class MockStateSync:
    def __init__(self, transport_layer: MockTransportAdapter) -> None:
        self.transport_layer = transport_layer

    def publish(
        self,
        job_id: str,
        status: str,
        state_context: Any = None,
    ) -> None:
        _assert_valid_job_id(job_id)
        _assert_valid_status(status)

        self.transport_layer.send_message(
            {
                "kind": JOBPING_STATE_UPDATE,
                "job_id": job_id,
                "data": {
                    "status": status,
                    "state_context": state_context if state_context is not None else {},
                },
            },
        )

    async def wait_for(
        self,
        job_id: str,
        *,
        status: str | None = None,
        timeout: float = 1.0,
    ) -> StateUpdate:
        _assert_valid_job_id(job_id)

        while True:
            message = await self.transport_layer.recv_message(
                kind=JOBPING_STATE_UPDATE,
                job_id=job_id,
                timeout=timeout,
            )
            state = message["data"]

            if status is None or state["status"] == status:
                return state
