"""TransportLayer abstractions for JobPing metadata and semantic messages."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Literal, NotRequired, TypedDict

from jobping.envelope import (
    EnvelopeType,
    JobPingEnvelope,
)


JOBPING_JOB_ID_HEADER = "x-jobping-job-id"


class TransportCarrier(TypedDict):
    headers: NotRequired[dict[str, str]]
    envelope: NotRequired[JobPingEnvelope]


MockCarrier = TransportCarrier


class TransportMessage(TypedDict):
    kind: str
    job_id: str
    data: NotRequired[Any]


class TransportLayer(ABC):
    @abstractmethod
    def attach_job_id(self, carrier: TransportCarrier | None, job_id: str) -> TransportCarrier:
        raise NotImplementedError

    @abstractmethod
    def extract_job_id(self, carrier: TransportCarrier | None) -> str | None:
        raise NotImplementedError

    @abstractmethod
    def attach_envelope(
        self,
        carrier: TransportCarrier | None,
        envelope: JobPingEnvelope,
    ) -> TransportCarrier:
        raise NotImplementedError

    @abstractmethod
    def extract_envelope(self, carrier: TransportCarrier | None) -> JobPingEnvelope | None:
        raise NotImplementedError

    @abstractmethod
    def send_envelope(self, envelope: JobPingEnvelope) -> None:
        raise NotImplementedError

    @abstractmethod
    async def recv_envelope(
        self,
        *,
        job_id: str | None = None,
        type: EnvelopeType | None = None,
        timeout: float = 1.0,
    ) -> JobPingEnvelope:
        raise NotImplementedError

    @abstractmethod
    def send_message(self, message: TransportMessage) -> None:
        raise NotImplementedError

    @abstractmethod
    async def recv_message(
        self,
        *,
        kind: str | None = None,
        job_id: str | None = None,
        timeout: float = 1.0,
    ) -> TransportMessage:
        raise NotImplementedError
