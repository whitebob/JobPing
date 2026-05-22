"""EnvelopeEndpoint abstraction and in-memory implementation.

Provides:
- EnvelopeEndpoint (ABC): abstract surface for sending/receiving JobPing envelopes
- EnvelopeEndpointInMemory: simple in-process implementation suitable for examples/tests

This mirrors the existing sandbox MockEnvelopeEndpoint behaviour but lives in the
public package.
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass

from jobping.envelope import EnvelopeType, JobPingEnvelope, is_envelope
from typing import Any


class EnvelopeEndpoint(ABC):
    """Abstract envelope endpoint surface.

    Concrete implementations must provide send(envelope) to deliver an envelope
    and recv(job_id=..., type=..., timeout=...) to receive a matching envelope.
    """

    @abstractmethod
    def send(self, envelope: JobPingEnvelope) -> None:
        raise NotImplementedError

    @abstractmethod
    async def recv(self, *, job_id: str | None = None, type: EnvelopeType | None = None, timeout: float = 1.0) -> JobPingEnvelope:
        raise NotImplementedError

    @abstractmethod
    def size(self) -> dict[str, int]:
        raise NotImplementedError


# Concrete implementations live under jobping.imp — import and re-export the
# in-memory implementation for backward compatibility.
from importlib import import_module

try:
    _imp = import_module("jobping.imp.envelope_endpoint_inmemory")
    EnvelopeEndpointInMemory = getattr(_imp, "EnvelopeEndpointInMemory")
except Exception:
    EnvelopeEndpointInMemory = None
