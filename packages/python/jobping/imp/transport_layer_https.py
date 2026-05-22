"""HTTP(S) TransportLayer implementation (under imp).

This TransportLayer uses HTTP endpoints to POST envelopes/messages and
poll for new envelopes/messages. The implementation is intentionally
lightweight and uses aiohttp as an optional dependency; a helpful error
is raised if aiohttp is missing when methods are used.

Endpoints (convention):
- POST {base_url}/envelope  -> accepts envelope JSON
- GET  {base_url}/envelope?job_id=...&type=... -> returns next envelope JSON
- POST {base_url}/message   -> accepts message JSON
- GET  {base_url}/message?kind=...&job_id=... -> returns next message JSON

This simple design is sufficient for examples/tests and can be adapted to
use signed URLs / object storage for large boxed results in production.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Optional

from jobping.envelope import is_envelope, JobPingEnvelope, EnvelopeType
from jobping.transport_layer import (
    JOBPING_JOB_ID_HEADER,
    TransportCarrier,
    TransportLayer,
    TransportMessage,
)


class TransportLayerHTTPS(TransportLayer):
    def __init__(self, base_url: str, *, session_kwargs: Optional[dict] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = None
        self._session_kwargs = session_kwargs or {}

    def _ensure_session(self):
        if self._session is None:
            try:
                import aiohttp
            except Exception as exc:  # pragma: no cover - optional dependency
                raise RuntimeError("aiohttp is required for TransportLayerHTTPS") from exc
            self._session = aiohttp.ClientSession(**self._session_kwargs)
        return self._session

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
        async def _post():
            session = self._ensure_session()
            url = f"{self.base_url}/envelope"
            payload = envelope if isinstance(envelope, dict) else getattr(envelope, "__dict__", envelope)
            await session.post(url, json=payload)

        asyncio.create_task(_post())

    async def recv_envelope(self, *, job_id: str | None = None, type: EnvelopeType | None = None, timeout: float = 1.0) -> JobPingEnvelope:
        session = self._ensure_session()
        url = f"{self.base_url}/envelope"
        params = {}
        if job_id is not None:
            params["job_id"] = job_id
        if type is not None:
            params["type"] = type

        try:
            # simple polling loop until timeout
            deadline = asyncio.get_event_loop().time() + timeout
            while True:
                if asyncio.get_event_loop().time() > deadline:
                    raise asyncio.TimeoutError()
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # try to construct JobPingEnvelope if possible
                        try:
                            return JobPingEnvelope(**data)  # type: ignore
                        except Exception:
                            return data  # type: ignore
                await asyncio.sleep(0.1)
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Timed out waiting for envelope") from exc

    def send_message(self, message: TransportMessage) -> None:
        async def _post():
            session = self._ensure_session()
            url = f"{self.base_url}/message"
            await session.post(url, json=message)

        asyncio.create_task(_post())

    async def recv_message(self, *, kind: str | None = None, job_id: str | None = None, timeout: float = 1.0) -> TransportMessage:
        session = self._ensure_session()
        url = f"{self.base_url}/message"
        params = {}
        if kind is not None:
            params["kind"] = kind
        if job_id is not None:
            params["job_id"] = job_id

        try:
            deadline = asyncio.get_event_loop().time() + timeout
            while True:
                if asyncio.get_event_loop().time() > deadline:
                    raise asyncio.TimeoutError()
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data  # type: ignore
                await asyncio.sleep(0.1)
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Timed out waiting for transport message") from exc

    def size(self) -> dict[str, int]:
        # HTTP transport does not maintain local queues
        return {"messages": 0, "waiters": 0}
