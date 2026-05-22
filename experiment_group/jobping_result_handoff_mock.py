"""Result handoff semantic mock."""

from __future__ import annotations

from typing import Any

from experiment_group.jobping_envelope_mock import box_result, unbox_result
from experiment_group.jobping_transport_mock import MockTransportAdapter


JOBPING_RESULT_HANDOFF = "jobping.result_handoff.v1"


def _assert_valid_job_id(job_id: str) -> None:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")


class MockResultHandoff:
    def __init__(self, transport_layer: MockTransportAdapter) -> None:
        self.transport_layer = transport_layer

    def fulfill(self, job_id: str, result: Any) -> None:
        _assert_valid_job_id(job_id)

        self.transport_layer.send_message(
            {
                "kind": JOBPING_RESULT_HANDOFF,
                "job_id": job_id,
                "data": box_result(job_id, result),
            },
        )

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
