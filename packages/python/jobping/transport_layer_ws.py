"""WebSocket TransportLayer using python-socketio.

This is a lightweight adapter implementing the TransportLayer interface
backed by a Socket.IO websocket connection (python-socketio AsyncClient).

Note: importing python-socketio is optional; a helpful error is raised
if the package is not installed.
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
        self._envelope_queue: asyncio.Queue[JobPingEnvelope] = asyncio.Queue()
        self._message_queue: asyncio.Queue[TransportMessage] = asyncio.Queue()

        # Register handlers
        @self._sio.on("jobping:envelope", namespace=self.namespace)
        async def _on_envelope(data: dict) -> None:  # noqa: E305 - explicit handler
            # Assume data is already a JobPingEnvelope-compatible mapping
            try:
                self._envelope_queue.put_nowait(JobPingEnvelope(**data))
            except Exception:
                # Fallback: put raw data
                self._envelope_queue.put_nowait(data)  # type: ignore

        @self._sio.on("jobping:message", namespace=self.namespace)
        async def _on_message(data: dict) -> None:  # noqa: E305 - explicit handler
            self._message_queue.put_nowait(data)

    async def _ensure_connected(self) -> None:
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
        # Schedule async emit in background
        async def _emit():
            await self._ensure_connected()
            await self._sio.emit("jobping:envelope", envelope, namespace=self.namespace)

        asyncio.create_task(_emit())

    async def recv_envelope(self, *, job_id: str | None = None, type: EnvelopeType | None = None, timeout: float = 1.0) -> JobPingEnvelope:
        try:
            while True:
                envelope = await asyncio.wait_for(self._envelope_queue.get(), timeout=timeout)
                # Basic filtering
                if job_id is not None and getattr(envelope, "job_id", None) != job_id:
                    continue
                if type is not None and getattr(envelope, "type", None) != type:
                    continue
                return envelope
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Timed out waiting for envelope") from exc

    def send_message(self, message: TransportMessage) -> None:
        async def _emit():
            await self._ensure_connected()
            await self._sio.emit("jobping:message", message, namespace=self.namespace)

        asyncio.create_task(_emit())

    async def recv_message(self, *, kind: str | None = None, job_id: str | None = None, timeout: float = 1.0) -> TransportMessage:
        try:
            while True:
                message = await asyncio.wait_for(self._message_queue.get(), timeout=timeout)
                if kind is not None and message.get("kind") != kind:
                    continue
                if job_id is not None and message.get("job_id") != job_id:
                    continue
                return message
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Timed out waiting for transport message") from exc
