"""Result handoff semantic service."""

from __future__ import annotations

from typing import Any, Protocol

from jobping.envelope import box_result, unbox_result
from jobping.transport_layer import TransportMessage


JOBPING_RESULT_HANDOFF = "jobping.result_handoff.v1"


class ResultHandoffTransport(Protocol):
    def send_message(self, message: TransportMessage) -> None: ...

    async def recv_message(
        self,
        *,
        kind: str | None = None,
        job_id: str | None = None,
        timeout: float = 1.0,
    ) -> TransportMessage: ...


def _assert_valid_job_id(job_id: str) -> None:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")


class ResultHandoff:
    def __init__(self, transport_layer: ResultHandoffTransport) -> None:
        self.transport_layer = transport_layer

    def fulfill(self, job_id: str, result: Any, *, trace: dict | None = None) -> None:
        _assert_valid_job_id(job_id)

        msg: dict = {
            "kind": JOBPING_RESULT_HANDOFF,
            "job_id": job_id,
            "data": box_result(job_id, result),
        }
        if trace is not None:
            msg["_trace"] = trace

        self.transport_layer.send_message(msg)

    async def await_result(
        self,
        job_id: str,
        *,
        timeout: float = 1.0,
    ) -> Any:
        _assert_valid_job_id(job_id)

        message = await self.transport_layer.recv_message(
            kind=JOBPING_RESULT_HANDOFF,
            job_id=job_id,
            timeout=timeout,
        )

        return unbox_result(message["data"], expected_job_id=job_id)
