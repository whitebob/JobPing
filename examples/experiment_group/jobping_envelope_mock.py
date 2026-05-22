"""Transport-neutral JobPing envelope mock."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Literal, TypedDict


JOBPING_ENVELOPE_KIND = "jobping.envelope.v1"
JOBPING_JOB_REF = "job_ref"
JOBPING_RESULT = "result"

EnvelopeType = Literal["job_ref", "result"]


class JobRefEnvelope(TypedDict):
    jobping: str
    type: Literal["job_ref"]
    job_id: str


class ResultEnvelope(TypedDict):
    jobping: str
    type: Literal["result"]
    job_id: str
    payload: Any


JobPingEnvelope = JobRefEnvelope | ResultEnvelope


def _assert_valid_job_id(job_id: str) -> None:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")


def box_job_ref(job_id: str) -> JobRefEnvelope:
    _assert_valid_job_id(job_id)

    return {
        "jobping": JOBPING_ENVELOPE_KIND,
        "type": JOBPING_JOB_REF,
        "job_id": job_id,
    }


def box_result(job_id: str, payload: Any) -> ResultEnvelope:
    _assert_valid_job_id(job_id)

    return {
        "jobping": JOBPING_ENVELOPE_KIND,
        "type": JOBPING_RESULT,
        "job_id": job_id,
        "payload": payload,
    }


def is_envelope(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and value.get("jobping") == JOBPING_ENVELOPE_KIND
        and isinstance(value.get("type"), str)
        and isinstance(value.get("job_id"), str)
        and len(value["job_id"]) > 0
    )


def is_job_ref_envelope(value: Any) -> bool:
    return is_envelope(value) and value["type"] == JOBPING_JOB_REF


def is_result_envelope(value: Any) -> bool:
    return is_envelope(value) and value["type"] == JOBPING_RESULT and "payload" in value


def unbox_result(envelope: Any, expected_job_id: str | None = None) -> Any:
    if not is_result_envelope(envelope):
        raise ValueError("Expected JobPing result envelope")

    if expected_job_id is not None and envelope["job_id"] != expected_job_id:
        raise ValueError("Unexpected JobPing result job_id")

    return envelope["payload"]


@dataclass
class _Waiter:
    job_id: str | None
    type: EnvelopeType | None
    future: asyncio.Future[JobPingEnvelope]


class MockEnvelopeEndpoint:
    """In-memory endpoint for envelope send/recv tests."""

    def __init__(self) -> None:
        self._pending: list[JobPingEnvelope] = []
        self._waiters: list[_Waiter] = []

    def send(self, envelope: JobPingEnvelope) -> None:
        if not is_envelope(envelope):
            raise ValueError("Can only send JobPing envelopes")

        for index, waiter in enumerate(self._waiters):
            if self._matches(envelope, waiter.job_id, waiter.type):
                self._waiters.pop(index)
                waiter.future.set_result(envelope)
                return

        self._pending.append(envelope)

    async def recv(
        self,
        *,
        job_id: str | None = None,
        type: EnvelopeType | None = None,
        timeout: float = 1.0,
    ) -> JobPingEnvelope:
        for index, envelope in enumerate(self._pending):
            if self._matches(envelope, job_id, type):
                return self._pending.pop(index)

        loop = asyncio.get_running_loop()
        waiter = _Waiter(
            job_id=job_id,
            type=type,
            future=loop.create_future(),
        )
        self._waiters.append(waiter)

        try:
            return await asyncio.wait_for(waiter.future, timeout=timeout)
        except TimeoutError:
            if waiter in self._waiters:
                self._waiters.remove(waiter)
            raise

    def size(self) -> dict[str, int]:
        return {
            "pending": len(self._pending),
            "waiters": len(self._waiters),
        }

    def _matches(
        self,
        envelope: JobPingEnvelope,
        job_id: str | None,
        type: EnvelopeType | None,
    ) -> bool:
        return (
            is_envelope(envelope)
            and (job_id is None or envelope["job_id"] == job_id)
            and (type is None or envelope["type"] == type)
        )
