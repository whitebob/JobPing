"""JobPing wrapper facade."""

from __future__ import annotations

import asyncio
import contextvars
import os
import time
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar

from jobping.endpoint_proxy import EndpointProxy
from jobping.result_handoff import ResultHandoff
from jobping.state_sync import StateSync
from jobping.transport_layer import TransportLayer
from jobping.imp.transport_layer_ws import TransportLayerWS
from jobping.imp.transport_layer_local import LocalTransportLayer
from jobping.imp.transport_layer_composite import CompositeTransportLayer
from jobping.imp.broker import EmbeddedBroker
from jobping.id import create_peer_id


Result = TypeVar("Result")
JobRef = dict[str, str]

# Per-job trace flag — set by wrap_trace() and inherited by nested calls via
# the x-jobping-trace-enabled header.  ContextVar defaults to False so the
# normal path has zero overhead beyond a single boolean read.
_trace_enabled: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "jobping_trace_enabled", default=False
)


def is_jobping_disabled() -> bool:
    return os.environ.get("JOBPING_DISABLED", "").lower() in {"1", "true", "yes", "on"}


def _check_trace_header(*args: Any, **kwargs: Any) -> bool:
    """Inspect the incoming request for an x-jobping-trace-enabled header."""
    candidate = kwargs.get("request") if "request" in kwargs else (args[0] if args else None)
    if candidate is None:
        return False

    headers = None
    try:
        hdrs = getattr(candidate, "headers", None)
        if hdrs is not None:
            headers = dict(hdrs)
    except Exception:
        pass

    if headers is None:
        try:
            scope = getattr(candidate, "scope", None)
            if isinstance(scope, dict) and "headers" in scope:
                raw = scope["headers"]
                headers = {k.decode(): v.decode() for k, v in raw}
        except Exception:
            pass

    if headers is None and isinstance(candidate, dict):
        try:
            headers = {
                k[5:].replace("_", "-").lower(): v
                for k, v in candidate.items()
                if k.startswith("HTTP_")
            }
        except Exception:
            pass

    if headers is None:
        return False

    for k, v in headers.items():
        if k.lower() == "x-jobping-trace-enabled":
            return str(v).lower() in ("1", "true")
    return False


def _compute_hop(*args: Any, **kwargs: Any) -> int:
    """Extract hop count from trace header if present, else 1."""
    candidate = kwargs.get("request") if "request" in kwargs else (args[0] if args else None)
    if candidate is None:
        return 1
    headers = None
    try:
        hdrs = getattr(candidate, "headers", None)
        if hdrs is not None:
            headers = dict(hdrs)
    except Exception:
        pass
    if headers:
        for k, v in headers.items():
            if k.lower() == "x-jobping-trace-hop":
                try:
                    return int(v) + 1
                except (ValueError, TypeError):
                    pass
    return 1


def _default_job_context_provider_from_transport(transport: TransportLayer):
    """Return a job_context_provider tied to *transport*."""

    def provider(*args, **kwargs):
        candidate = kwargs.get("request") if "request" in kwargs else (args[0] if args else None)
        if candidate is None:
            return None

        headers = None
        try:
            hdrs = getattr(candidate, "headers", None)
            if hdrs is not None:
                headers = dict(hdrs)
        except Exception:
            headers = None

        if headers is None:
            try:
                scope = getattr(candidate, "scope", None)
                if isinstance(scope, dict) and "headers" in scope:
                    raw = scope["headers"]
                    headers = {k.decode(): v.decode() for k, v in raw}
            except Exception:
                headers = None

        if headers is None and isinstance(candidate, dict):
            try:
                headers = {
                    k[5:].replace("_", "-").lower(): v
                    for k, v in candidate.items()
                    if k.startswith("HTTP_")
                }
            except Exception:
                headers = None

        if headers is None:
            return None

        carrier = {"headers": headers}
        try:
            return transport.extract_job_id(carrier)
        except Exception:
            return None

    return provider


class JobPing:
    def __init__(
        self,
        *,
        endpoint_proxy: EndpointProxy,
        job_context_provider: Callable[..., str | None] | None = None,
        peer_id: str | None = None,
        max_trace_depth: int = 10,
    ) -> None:
        self.endpoint_proxy = endpoint_proxy
        self.job_context_provider = job_context_provider or (lambda *args, **kwargs: None)
        self.peer_id = peer_id or create_peer_id()
        self._max_trace_depth = max_trace_depth
        self._broker = None  # set by create_jobping

    async def start_broker(self) -> None:
        """Start the embedded broker if one was created by the factory."""
        if self._broker is not None:
            await self._broker.start()

    # ------------------------------------------------------------------
    # wrap  (normal path — zero trace overhead)
    # ------------------------------------------------------------------

    def wrap(
        self,
    ) -> Callable[[Callable[..., Awaitable[Result]]], Callable[..., Awaitable[Result | JobRef]]]:
        def decorator(
            wrapped_callable: Callable[..., Awaitable[Result]],
        ) -> Callable[..., Awaitable[Result | JobRef]]:
            @wraps(wrapped_callable)
            async def wrapper(*args: Any, **kwargs: Any) -> Result | JobRef:
                if is_jobping_disabled():
                    return await wrapped_callable(*args, **kwargs)

                job_id = self.job_context_provider(*args, **kwargs)
                if job_id is None:
                    return await wrapped_callable(*args, **kwargs)

                # Trace: active when wrap_trace set it, OR inherited via header.
                trace_on = _trace_enabled.get()
                if not trace_on:
                    trace_on = _check_trace_header(*args, **kwargs)

                if trace_on:
                    token = _trace_enabled.set(True)
                    t0 = time.monotonic()
                    hop = _compute_hop(*args, **kwargs)
                    self.endpoint_proxy._active_trace = {
                        "job_id": job_id,
                        "peer_id": self.peer_id,
                        "hop": hop,
                        "sub_jobs": [],
                    }

                try:
                    jp_item = self.endpoint_proxy.offer(job_id)
                    self.endpoint_proxy.defer(jp_item)
                    asyncio.create_task(
                        self.endpoint_proxy.fulfill_later(
                            job_id,
                            lambda: wrapped_callable(*args, **kwargs),
                        ),
                    )
                    return self.endpoint_proxy.make_job_ref(job_id)
                finally:
                    if trace_on:
                        self.endpoint_proxy._active_trace["elapsed"] = time.monotonic() - t0
                        _trace_enabled.reset(token)

            return wrapper

        return decorator

    # ------------------------------------------------------------------
    # wrap_trace  (debug / diagnostic path)
    # ------------------------------------------------------------------

    def wrap_trace(
        self,
    ) -> Callable[[Callable[..., Awaitable[Result]]], Callable[..., Awaitable[Result | JobRef]]]:
        def decorator(
            wrapped_callable: Callable[..., Awaitable[Result]],
        ) -> Callable[..., Awaitable[Result | JobRef]]:
            @wraps(wrapped_callable)
            async def wrapper(*args: Any, **kwargs: Any) -> Result | JobRef:
                if is_jobping_disabled():
                    return await wrapped_callable(*args, **kwargs)

                job_id = self.job_context_provider(*args, **kwargs)
                if job_id is None:
                    return await wrapped_callable(*args, **kwargs)

                token = _trace_enabled.set(True)
                t0 = time.monotonic()
                self.endpoint_proxy._active_trace = {
                    "job_id": job_id,
                    "peer_id": self.peer_id,
                    "hop": 1,
                    "sub_jobs": [],
                }

                try:
                    jp_item = self.endpoint_proxy.offer(job_id)
                    self.endpoint_proxy.defer(jp_item)
                    asyncio.create_task(
                        self.endpoint_proxy.fulfill_later(
                            job_id,
                            lambda: wrapped_callable(*args, **kwargs),
                        ),
                    )
                    return self.endpoint_proxy.make_job_ref(job_id)
                finally:
                    self.endpoint_proxy._active_trace["elapsed"] = time.monotonic() - t0
                    _trace_enabled.reset(token)

            return wrapper

        return decorator


JobPingClass = JobPing


# ------------------------------------------------------------------
# factory
# ------------------------------------------------------------------

def create_jobping(
    broker_port: int,
    *,
    peer_brokers: list[str] | None = None,
    idle_timeout_seconds: int | None = 300,
    max_trace_depth: int = 10,
    job_context_provider: Callable[..., str | None] | None = None,
    sio_kwargs: dict[str, Any] | None = None,
) -> JobPing:
    """Create a JobPing instance with an embedded broker.

    Parameters:
        broker_port: TCP port for the embedded Socket.IO broker (required).
        peer_brokers: URLs of other peers' brokers to connect to.
        idle_timeout_seconds: Per-remote-connection idle timeout (None = never).
        max_trace_depth: Maximum nesting depth for trace collection.
        job_context_provider: Optional callable to extract job_id from call args.
        sio_kwargs: Extra keyword arguments passed to the Socket.IO server.
    """
    from jobping.imp.envelope_endpoint_inmemory import EnvelopeEndpointInMemory
    from jobping.jpitem_queue import JPItemQueueInMemory

    # 1. Embedded broker
    broker = EmbeddedBroker(broker_port, **(sio_kwargs or {}))

    # 2. Local fast path
    local_transport = LocalTransportLayer(broker)

    # 3. Remote connections
    transports: list[TransportLayer] = [local_transport]
    for url in (peer_brokers or []):
        transports.append(TransportLayerWS(url, idle_timeout_seconds=idle_timeout_seconds))

    # 4. Composite (only when needed)
    if len(transports) == 1:
        transport = transports[0]
    else:
        transport = CompositeTransportLayer(transports)

    # 5. Job context provider
    if job_context_provider is None:
        job_context_provider = _default_job_context_provider_from_transport(transport)

    # 6. EndpointProxy
    queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
    endpoint_proxy = EndpointProxy(
        state_sync=StateSync(transport),
        result_handoff=ResultHandoff(transport),
        queue=queue,
        max_trace_depth=max_trace_depth,
    )
    endpoint_proxy._active_trace = None  # set by wrap/wrap_trace

    jp = JobPing(
        endpoint_proxy=endpoint_proxy,
        job_context_provider=job_context_provider,
        peer_id=create_peer_id(),
        max_trace_depth=max_trace_depth,
    )
    jp._broker = broker
    return jp
