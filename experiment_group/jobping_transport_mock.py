"""Transport-adapter mock for JobPing metadata carriers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Literal, NotRequired, TypedDict

from experiment_group.jobping_envelope_mock import (
    EnvelopeType,
    JobPingEnvelope,
    MockEnvelopeEndpoint,
    is_envelope,
)


JOBPING_JOB_ID_HEADER = "x-jobping-job-id"


class MockCarrier(TypedDict):
    headers: NotRequired[dict[str, str]]
    envelope: NotRequired[JobPingEnvelope]


class TransportMessage(TypedDict):
    kind: str
    job_id: str
    data: NotRequired[Any]


@dataclass
class _MessageWaiter:
    kind: str | None
    job_id: str | None
    future: asyncio.Future[TransportMessage]


def _assert_valid_job_id(job_id: str) -> None:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")


def _find_header(headers: dict[str, str] | None, name: str) -> str | None:
    if headers is None:
        return None

    target = name.lower()
    for key, value in headers.items():
        if key.lower() == target:
            return value

    return None


class MockTransportAdapter:
    """Small carrier adapter that can later be replaced by HTTP or websocket transport."""

    def __init__(self, envelope_endpoint: MockEnvelopeEndpoint | None = None) -> None:
        self.envelope_endpoint = envelope_endpoint
        self._messages: list[TransportMessage] = []
        self._waiters: list[_MessageWaiter] = []

    def attach_job_id(self, carrier: MockCarrier | None, job_id: str) -> MockCarrier:
        _assert_valid_job_id(job_id)

        current = carrier or {}
        headers = dict(current.get("headers", {}))
        headers[JOBPING_JOB_ID_HEADER] = job_id

        return {
            **current,
            "headers": headers,
        }

    def extract_job_id(self, carrier: MockCarrier | None) -> str | None:
        if carrier is None:
            return None

        value = _find_header(carrier.get("headers"), JOBPING_JOB_ID_HEADER)
        return value if isinstance(value, str) and len(value) > 0 else None

    def attach_envelope(self, carrier: MockCarrier | None, envelope: JobPingEnvelope) -> MockCarrier:
        if not is_envelope(envelope):
            raise ValueError("Can only attach JobPing envelopes")

        return {
            **(carrier or {}),
            "envelope": envelope,
        }

    def extract_envelope(self, carrier: MockCarrier | None) -> JobPingEnvelope | None:
        if carrier is None:
            return None

        envelope = carrier.get("envelope")
        return envelope if is_envelope(envelope) else None

    def send_envelope(self, envelope: JobPingEnvelope) -> None:
        if self.envelope_endpoint is None:
            raise RuntimeError("No envelope endpoint configured")

        self.envelope_endpoint.send(envelope)

    async def recv_envelope(
        self,
        *,
        job_id: str | None = None,
        type: EnvelopeType | None = None,
        timeout: float = 1.0,
    ) -> JobPingEnvelope:
        if self.envelope_endpoint is None:
            raise RuntimeError("No envelope endpoint configured")

        return await self.envelope_endpoint.recv(
            job_id=job_id,
            type=type,
            timeout=timeout,
        )

    def send_message(self, message: TransportMessage) -> None:
        for index, waiter in enumerate(self._waiters):
            if self._matches_message(message, waiter.kind, waiter.job_id):
                self._waiters.pop(index)
                waiter.future.set_result(message)
                return

        self._messages.append(message)

    async def recv_message(
        self,
        *,
        kind: str | None = None,
        job_id: str | None = None,
        timeout: float = 1.0,
    ) -> TransportMessage:
        for index, message in enumerate(self._messages):
            if self._matches_message(message, kind, job_id):
                return self._messages.pop(index)

        loop = asyncio.get_running_loop()
        waiter = _MessageWaiter(
            kind=kind,
            job_id=job_id,
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
            "messages": len(self._messages),
            "waiters": len(self._waiters),
        }

    def _matches_message(
        self,
        message: TransportMessage,
        kind: str | None,
        job_id: str | None,
    ) -> bool:
        return (
            isinstance(message, dict)
            and (kind is None or message.get("kind") == kind)
            and (job_id is None or message.get("job_id") == job_id)
        )
