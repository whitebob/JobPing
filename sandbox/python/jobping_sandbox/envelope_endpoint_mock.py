from __future__ import annotations

import asyncio
from dataclasses import dataclass

from jobping.envelope import EnvelopeType, JobPingEnvelope, is_envelope


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
