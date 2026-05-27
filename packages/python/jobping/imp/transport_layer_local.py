"""In-process TransportLayer — connects directly to the local EmbeddedBroker.

No network, no serialization, no idle timeout. Uses the broker's local queues
for send and registers callbacks for recv.
"""

from __future__ import annotations

import asyncio
from typing import Any

from jobping.envelope import EnvelopeType, JobPingEnvelope
from jobping.transport_layer import (
    JOBPING_JOB_ID_HEADER,
    TransportCarrier,
    TransportLayer,
    TransportMessage,
)


class _Mailbox:
    """Waiter/message matchmaker — identical pattern to TransportLayerWS._Mailbox."""

    def __init__(self) -> None:
        self._messages: list[dict] = []
        self._waiters: list[list] = []

    def put(self, data: Any) -> None:
        for entry in self._waiters:
            if entry[0](data):
                entry[2] = data
                entry[1].set()
                self._waiters.remove(entry)
                return
        self._messages.append(data)

    async def get(self, matches: callable, timeout: float) -> Any:
        for i, msg in enumerate(self._messages):
            if matches(msg):
                return self._messages.pop(i)

        event = asyncio.Event()
        entry = [matches, event, None]
        self._waiters.append(entry)
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return entry[2]
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Timed out waiting for message") from exc
        finally:
            try:
                self._waiters.remove(entry)
            except ValueError:
                pass


class LocalTransportLayer(TransportLayer):
    """TransportLayer that connects directly to the local EmbeddedBroker.

    send_* pushes into the broker's in-process queues.  recv_* drains from
    private Mailboxes that the broker feeds via callbacks.
    """

    def __init__(self, broker) -> None:
        # broker is EmbeddedBroker instance (lazy import to avoid circular deps)
        self._broker = broker
        self._message_mailbox = _Mailbox()
        self._envelope_mailbox = _Mailbox()

        # Wire broker callbacks so it can deliver to us.
        broker._on_local_message = self._message_mailbox.put
        broker._on_local_envelope = self._envelope_mailbox.put

    # -- carrier metadata (same pattern as other transports) -----------------

    def attach_job_id(self, carrier: TransportCarrier | None, job_id: str) -> TransportCarrier:
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("job_id must be a non-empty string")
        current = carrier or {}
        headers = dict(current.get("headers", {}))
        headers[JOBPING_JOB_ID_HEADER] = job_id
        return {**current, "headers": headers}

    def extract_job_id(self, carrier: TransportCarrier | None) -> str | None:
        if carrier is None:
            return None
        headers = carrier.get("headers") or {}
        for k, v in headers.items():
            if k.lower() == JOBPING_JOB_ID_HEADER.lower():
                return v
        return None

    def attach_envelope(self, carrier: TransportCarrier | None, envelope: JobPingEnvelope) -> TransportCarrier:
        from jobping.envelope import is_envelope
        if not is_envelope(envelope):
            raise ValueError("Can only attach JobPing envelopes")
        return {**(carrier or {}), "envelope": envelope}

    def extract_envelope(self, carrier: TransportCarrier | None) -> JobPingEnvelope | None:
        if carrier is None:
            return None
        from jobping.envelope import is_envelope
        env = carrier.get("envelope")
        return env if is_envelope(env) else None

    # -- message I/O --------------------------------------------------------

    def send_message(self, message: TransportMessage) -> None:
        self._broker.local_send_message(message)

    async def recv_message(self, *, kind: str | None = None, job_id: str | None = None, timeout: float = 1.0) -> TransportMessage:
        def matches(msg: Any) -> bool:
            if kind is not None and msg.get("kind") != kind:
                return False
            if job_id is not None and msg.get("job_id") != job_id:
                return False
            return True
        return await self._message_mailbox.get(matches, timeout)

    # -- envelope I/O -------------------------------------------------------

    def send_envelope(self, envelope: JobPingEnvelope) -> None:
        self._broker.local_send_envelope(envelope)

    async def recv_envelope(self, *, job_id: str | None = None, type: EnvelopeType | None = None, timeout: float = 1.0) -> JobPingEnvelope:
        def matches(env: Any) -> bool:
            if job_id is not None and getattr(env, "job_id", env.get("job_id")) != job_id:
                return False
            if type is not None and getattr(env, "type", env.get("type")) != type:
                return False
            return True
        return await self._envelope_mailbox.get(matches, timeout)
