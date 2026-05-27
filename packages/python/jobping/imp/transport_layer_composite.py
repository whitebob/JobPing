"""CompositeTransportLayer — merges multiple TransportLayer instances into one.

Enabled by the uniform embedded-broker model where a peer connects to its
own local broker (fast path) and optionally to remote peers' brokers.
"""

from __future__ import annotations

import asyncio
from typing import Any

from jobping.envelope import EnvelopeType, JobPingEnvelope, is_envelope
from jobping.transport_layer import (
    JOBPING_JOB_ID_HEADER,
    TransportCarrier,
    TransportLayer,
    TransportMessage,
)


class CompositeTransportLayer(TransportLayer):
    """Merges 1 LocalTransportLayer + N TransportLayerWS into a single TransportLayer.

    ``send_*`` broadcasts to every sub-transport.
    ``recv_*`` races all sub-transports and returns the first match.
    """

    def __init__(self, transports: list[TransportLayer]) -> None:
        if len(transports) < 2:
            raise ValueError("CompositeTransportLayer requires at least 2 transports")
        self._transports = list(transports)

    @property
    def transports(self) -> list[TransportLayer]:
        return list(self._transports)

    # -- carrier metadata (delegates to primary transport) ------------------

    def attach_job_id(self, carrier: TransportCarrier | None, job_id: str) -> TransportCarrier:
        return self._transports[0].attach_job_id(carrier, job_id)

    def extract_job_id(self, carrier: TransportCarrier | None) -> str | None:
        return self._transports[0].extract_job_id(carrier)

    def attach_envelope(self, carrier: TransportCarrier | None, envelope: JobPingEnvelope) -> TransportCarrier:
        return self._transports[0].attach_envelope(carrier, envelope)

    def extract_envelope(self, carrier: TransportCarrier | None) -> JobPingEnvelope | None:
        return self._transports[0].extract_envelope(carrier)

    # -- message I/O --------------------------------------------------------

    def send_message(self, message: TransportMessage) -> None:
        for t in self._transports:
            t.send_message(message)

    async def recv_message(self, *, kind: str | None = None, job_id: str | None = None, timeout: float = 1.0) -> TransportMessage:
        async def _recv_one(t: TransportLayer) -> TransportMessage:
            return await t.recv_message(kind=kind, job_id=job_id, timeout=timeout)

        tasks = [asyncio.create_task(_recv_one(t)) for t in self._transports]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            result = next(iter(done)).result()
            return result
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

    # -- envelope I/O -------------------------------------------------------

    def send_envelope(self, envelope: JobPingEnvelope) -> None:
        for t in self._transports:
            t.send_envelope(envelope)

    async def recv_envelope(self, *, job_id: str | None = None, type: EnvelopeType | None = None, timeout: float = 1.0) -> JobPingEnvelope:
        async def _recv_one(t: TransportLayer) -> JobPingEnvelope:
            return await t.recv_envelope(job_id=job_id, type=type, timeout=timeout)

        tasks = [asyncio.create_task(_recv_one(t)) for t in self._transports]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            result = next(iter(done)).result()
            return result
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
