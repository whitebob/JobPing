"""JobPing wrapper facade."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar

from jobping.endpoint_proxy import EndpointProxy
from jobping.result_handoff import ResultHandoff
from jobping.state_sync import StateSync
from jobping.transport_layer import TransportLayer
from jobping.imp.transport_layer_ws import TransportLayerWS
from jobping.imp.transport_layer_https import TransportLayerHTTPS


Result = TypeVar("Result")
JobRef = dict[str, str]


# Default transport instances are created at module import time so they can
# reflect environment configuration immediately. This makes the defaults
# visible to callers without having to construct transports inside
# create_jobping every time.
_ws_url = os.environ.get("JOBPING_WS_URL", "http://127.0.0.1:8890")
_http_base = os.environ.get("JOBPING_HTTP_BASE", _ws_url)
DEFAULT_STATUS_TRANSPORT: TransportLayer = TransportLayerWS(_ws_url)
DEFAULT_RESULT_TRANSPORT: TransportLayer = TransportLayerHTTPS(_http_base)

# Default queue uses the in-memory queue and in-memory envelope endpoint so
# examples and tests work out-of-the-box without extra configuration.
# IMPORTANT: DEFAULT_QUEUE is a module-level, shared, mutable instance. Calling
# create_jobping() without passing an explicit queue will return JobPing
# instances that share the same in-memory queue. This is convenient for
# examples and quick-starts but may be undesirable if you need per-JobPing
# isolation (e.g., separate task domains, priorities, or heavy load).
# To opt out, pass queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()) to
# create_jobping so each JobPing gets its own queue instance.
from jobping.jpitem_queue import JPItemQueueInMemory
from jobping.envelope_endpoint import EnvelopeEndpointInMemory

DEFAULT_QUEUE = JPItemQueueInMemory(EnvelopeEndpointInMemory())


def is_jobping_disabled() -> bool:
    return os.environ.get("JOBPING_DISABLED", "").lower() in {"1", "true", "yes", "on"}


def _default_job_context_provider_from_transport(transport: TransportLayer):
    """Return a job_context_provider that tries to extract a job_id from common
    request-like objects using the provided transport's extract_job_id API.

    The provider inspects the first positional argument or a 'request' kwarg and
    attempts to build a carrier mapping with 'headers' to pass to
    transport.extract_job_id(). This is best-effort and intentionally
    framework-agnostic (FastAPI/Starlette/Aiohttp/WSGI-compatible where
    possible).
    """

    def provider(*args, **kwargs):
        candidate = kwargs.get("request") if "request" in kwargs else (args[0] if args else None)
        if candidate is None:
            return None

        headers = None
        # FastAPI / Starlette / aiohttp Request-like: have .headers mapping
        try:
            hdrs = getattr(candidate, "headers", None)
            if hdrs is not None:
                # dict() works on starlette.datastructures.Headers and other mappings
                headers = dict(hdrs)
        except Exception:
            headers = None

        # ASGI scope: bytes header pairs in scope['headers']
        if headers is None:
            try:
                scope = getattr(candidate, "scope", None)
                if isinstance(scope, dict) and "headers" in scope:
                    raw = scope["headers"]
                    headers = {k.decode(): v.decode() for k, v in raw}
            except Exception:
                headers = None

        # WSGI environ style: HTTP_-prefixed keys
        if headers is None and isinstance(candidate, dict):
            try:
                # collect HTTP_* keys
                headers = {k[5:].replace("_", "-").lower(): v for k, v in candidate.items() if k.startswith("HTTP_")}
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
    ) -> None:
        self.endpoint_proxy = endpoint_proxy
        self.job_context_provider = job_context_provider or (lambda *args, **kwargs: None)

    def wrap(
        self,
    ) -> Callable[
        [Callable[..., Awaitable[Result]]], Callable[..., Awaitable[Result | JobRef]]
    ]:
        def decorator(
            wrapped_callable: Callable[..., Awaitable[Result]],
        ) -> Callable[..., Awaitable[Result | JobRef]]:
            @wraps(wrapped_callable)
            async def wrapper(*args: Any, **kwargs: Any) -> Result | JobRef:
                if is_jobping_disabled():
                    return await wrapped_callable(*args, **kwargs)

                job_id = self.job_context_provider(*args, **kwargs)

                if job_id is not None:
                    jp_item = self.endpoint_proxy.offer(job_id)
                    self.endpoint_proxy.defer(jp_item)
                    asyncio.create_task(
                        self.endpoint_proxy.fulfill_later(
                            job_id,
                            lambda: wrapped_callable(*args, **kwargs),
                        ),
                    )
                    return self.endpoint_proxy.make_job_ref(job_id)

                return await wrapped_callable(*args, **kwargs)

            return wrapper

        return decorator

JobPingClass = JobPing


def create_jobping(
    *,
    status_transport_layer: TransportLayer = DEFAULT_STATUS_TRANSPORT,
    result_transport_layer: TransportLayer = DEFAULT_RESULT_TRANSPORT,
    queue: Any = DEFAULT_QUEUE,
    job_context_provider: Callable[..., str | None] | None = None,
) -> JobPing:
    """Create a JobPing instance.

    Parameters:
    - status_transport_layer: used for StateSync (defaults to a TransportLayerWS
      pointed at JOBPING_WS_URL).
    - result_transport_layer: used for ResultHandoff (defaults to a
      TransportLayerHTTPS pointed at JOBPING_HTTP_BASE).
    - queue: JPItem queue implementation (required).
    - job_context_provider: optional callable to extract job_id from call args.
    """

    # If no job_context_provider is given, use a default tied to the
    # status_transport_layer to attempt extracting job_id from incoming
    # request-like objects (headers/scope). This aligns job context
    # detection with the StatusSync transport.
    if job_context_provider is None:
        job_context_provider = _default_job_context_provider_from_transport(status_transport_layer)

    endpoint_proxy = EndpointProxy(
        state_sync=StateSync(status_transport_layer),
        result_handoff=ResultHandoff(result_transport_layer),
        queue=queue,
    )
    return JobPing(
        endpoint_proxy=endpoint_proxy,
        job_context_provider=job_context_provider,
    )
