"""WebSocket TransportLayer using python-socketio (implementation under imp).

This module mirrors the TransportLayerWS implementation but lives under
jobping.imp to separate concrete implementations from public ABCs.
"""
from __future__ import annotations

import asyncio
from typing import Any

from jobping.envelope import is_envelope, JobPingEnvelope, EnvelopeType
from jobping.transport_layer import (
    JOBPING_JOB_ID_HEADER,
    TransportCarrier,
    TransportLayer,
    TransportMessage,
)


class _Mailbox:
    """Waiter/message matchmaker — no messages lost to wrong consumer.

    When a message arrives: route to a matching waiter; if none, store.
    When a consumer waits: check stored messages; if none match, register waiter.
    """

    def __init__(self) -> None:
        self._messages: list[dict] = []
        self._waiters: list[list] = []  # [matches_callable, event, data_or_None]

    def put(self, data: Any) -> None:
        # Try to match a waiting consumer
        for entry in self._waiters:
            if entry[0](data):
                entry[2] = data  # mutate in-place so get() sees the value
                entry[1].set()
                self._waiters.remove(entry)
                return
        # No matching waiter — store
        self._messages.append(data)

    async def get(self, matches: callable, timeout: float) -> Any:
        # Check stored messages first
        for i, msg in enumerate(self._messages):
            if matches(msg):
                return self._messages.pop(i)

        # Register waiter
        event = asyncio.Event()
        entry = [matches, event, None]  # list so we can mutate result
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
                pass  # already removed by put()


class TransportLayerWS(TransportLayer):
    """Async Socket.IO-backed TransportLayer.

    - Emits/receives `jobping:envelope` events for envelopes
    - Emits/receives `jobping:message` events for transport messages

    This adapter uses an AsyncClient and connects on-demand when the
    first async operation is requested.
    """

    def __init__(self, url: str, namespace: str = "/", **client_kwargs: Any) -> None:
        self.url = url
        self.namespace = namespace
        try:
            import socketio  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "python-socketio is required for TransportLayerWS: install 'python-socketio[asyncio_client]'"
            ) from exc

        self._sio: "socketio.AsyncClient" = socketio.AsyncClient(**client_kwargs)
        self._connected = False
        self._connect_lock = asyncio.Lock()
        self._message_mailbox = _Mailbox()
        self._envelope_mailbox = _Mailbox()

        @self._sio.on("jobping:envelope", namespace=self.namespace)
        async def _on_envelope(data: dict) -> None:
            try:
                envelope = JobPingEnvelope(**data)
            except Exception:
                envelope = data
            self._envelope_mailbox.put(envelope)

        @self._sio.on("jobping:message", namespace=self.namespace)
        async def _on_message(data: dict) -> None:
            self._message_mailbox.put(data)

    async def _ensure_connected(self) -> None:
        if self._connected and getattr(self._sio, "connected", False):
            return
        async with self._connect_lock:
            if self._connected and getattr(self._sio, "connected", False):
                return
            await self._sio.connect(self.url, namespaces=[self.namespace])
            self._connected = True

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
        if not is_envelope(envelope):
            raise ValueError("Can only attach JobPing envelopes")
        return {**(carrier or {}), "envelope": envelope}

    def extract_envelope(self, carrier: TransportCarrier | None) -> JobPingEnvelope | None:
        if carrier is None:
            return None
        envelope = carrier.get("envelope")
        return envelope if is_envelope(envelope) else None

    def send_envelope(self, envelope: JobPingEnvelope) -> None:
        async def _emit():
            try:
                await self._ensure_connected()
                await self._sio.emit("jobping:envelope", envelope, namespace=self.namespace)
            except Exception:
                import sys
                import traceback
                print("TransportLayerWS send_envelope._emit failed:", file=sys.stderr)
                traceback.print_exc()

        asyncio.create_task(_emit())

    async def recv_envelope(self, *, job_id: str | None = None, type: EnvelopeType | None = None, timeout: float = 1.0) -> JobPingEnvelope:
        await self._ensure_connected()

        def matches(envelope: Any) -> bool:
            if job_id is not None and getattr(envelope, "job_id", None) != job_id:
                return False
            if type is not None and getattr(envelope, "type", None) != type:
                return False
            return True

        return await self._envelope_mailbox.get(matches, timeout)

    def send_message(self, message: TransportMessage) -> None:
        async def _emit():
            try:
                await self._ensure_connected()
                await self._sio.emit("jobping:message", message, namespace=self.namespace)
            except Exception:
                import sys
                import traceback
                print("TransportLayerWS send_message._emit failed:", file=sys.stderr)
                traceback.print_exc()

        asyncio.create_task(_emit())

    async def recv_message(self, *, kind: str | None = None, job_id: str | None = None, timeout: float = 1.0) -> TransportMessage:
        await self._ensure_connected()

        def matches(message: Any) -> bool:
            if kind is not None and message.get("kind") != kind:
                return False
            if job_id is not None and message.get("job_id") != job_id:
                return False
            return True

        return await self._message_mailbox.get(matches, timeout)
