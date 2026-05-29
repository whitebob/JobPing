"""Lazy singleton proxy — the public face of JobPing v2.

``from jobping import jp`` gives you the module-level _LazyJobPing instance.
No broker, no port binding until the first ``wrap()`` or ``start_broker()``.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import os
import threading
import time
import warnings
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, TypeVar

logger = logging.getLogger("jobping.singleton")

Result = TypeVar("Result")
JobRef = dict[str, str]

# Per-job trace flag — set by wrap_trace() and inherited by nested calls via
# the x-jobping-trace-enabled header.
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


class _LazyJobPing:
    """Project-level lazy singleton proxy for JobPing.

    Import-time: lightweight shell, no broker, no port binding.
    First ``wrap()`` or ``start_broker()`` triggers internal factory.

    Public verbs (only four):
      - ``configure(...)`` — store build params (sync, never builds)
      - ``wrap()`` — decorate a server handler with JobPing capability
      - ``unwrap()`` — decorate a client callable to resolve JobRefs
      - ``start_broker()`` — warm up the broker
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._active: Any = None          # current EndpointProxy (or Composite)
        self._broker: Any = None          # current EmbeddedBroker
        self._jp: Any = None              # internal JobPing instance
        self._stored_params: dict[str, Any] = {}
        self._needs_rebuild: bool = False
        self._migration_in_progress: bool = False
        self._job_context_provider: Callable[..., str | None] | None = None
        self._peer_id: str | None = None

    # ------------------------------------------------------------------
    # configure
    # ------------------------------------------------------------------

    def configure(
        self,
        broker_port: int,
        *,
        force: bool = False,
        peer_brokers: list[str] | None = None,
        idle_timeout_seconds: int | None = 300,
        max_trace_depth: int = 10,
        job_context_provider: Callable[..., str | None] | None = None,
        sio_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Store build parameters.  Sync — never builds.

        NO_INSTANCE: stores params for next wrap()/start_broker().
        RUNNING: no-op unless *force* is True, which stores new params and
        flags a rebuild for the next wrap()/start_broker().
        """
        params = {
            "broker_port": broker_port,
            "peer_brokers": peer_brokers,
            "idle_timeout_seconds": idle_timeout_seconds,
            "max_trace_depth": max_trace_depth,
            "job_context_provider": job_context_provider,
            "sio_kwargs": sio_kwargs,
        }

        with self._lock:
            if self._active is None:
                # NO_INSTANCE
                self._stored_params = params
            else:
                # RUNNING
                if force:
                    if self._migration_in_progress:
                        warnings.warn(
                            "Migration already in progress; ignoring concurrent "
                            "configure(force=True)."
                        )
                        return
                    self._stored_params = params
                    self._needs_rebuild = True
                else:
                    warnings.warn(
                        "JobPing is already running. Use configure(force=True) "
                        "to trigger a blue-green migration."
                    )

    # ------------------------------------------------------------------
    # wrap
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

                self._ensure_built_sync()
                await self._maybe_rebuild()

                job_id = self._job_context_provider(*args, **kwargs) if self._job_context_provider else None
                if job_id is None:
                    return await wrapped_callable(*args, **kwargs)

                trace_on = _trace_enabled.get()
                if not trace_on:
                    trace_on = _check_trace_header(*args, **kwargs)

                active = self.currentActive()

                if trace_on:
                    token = _trace_enabled.set(True)
                    t0 = time.monotonic()
                    hop = _compute_hop(*args, **kwargs)
                    active._active_trace = {
                        "job_id": job_id,
                        "peer_id": self._peer_id,
                        "hop": hop,
                        "sub_jobs": [],
                    }

                try:
                    jp_item = active.offer(job_id)
                    active.defer(jp_item)
                    asyncio.create_task(
                        active.fulfill_later(
                            job_id,
                            lambda: wrapped_callable(*args, **kwargs),
                        ),
                    )
                    return active.make_job_ref(job_id)
                finally:
                    if trace_on:
                        active._active_trace["elapsed"] = time.monotonic() - t0
                        _trace_enabled.reset(token)

            return wrapper

        return decorator

    # ------------------------------------------------------------------
    # wrap_trace
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

                self._ensure_built_sync()
                await self._maybe_rebuild()

                job_id = self._job_context_provider(*args, **kwargs) if self._job_context_provider else None
                if job_id is None:
                    return await wrapped_callable(*args, **kwargs)

                token = _trace_enabled.set(True)
                t0 = time.monotonic()
                active = self.currentActive()
                active._active_trace = {
                    "job_id": job_id,
                    "peer_id": self._peer_id,
                    "hop": 1,
                    "sub_jobs": [],
                }

                try:
                    jp_item = active.offer(job_id)
                    active.defer(jp_item)
                    asyncio.create_task(
                        active.fulfill_later(
                            job_id,
                            lambda: wrapped_callable(*args, **kwargs),
                        ),
                    )
                    return active.make_job_ref(job_id)
                finally:
                    active._active_trace["elapsed"] = time.monotonic() - t0
                    _trace_enabled.reset(token)

            return wrapper

        return decorator

    # ------------------------------------------------------------------
    # unwrap
    # ------------------------------------------------------------------

    def unwrap(
        self,
    ) -> Callable[[Callable[..., Awaitable[Result]]], Callable[..., Awaitable[Result]]]:
        def decorator(
            wrapped_callable: Callable[..., Awaitable[Result]],
        ) -> Callable[..., Awaitable[Result]]:
            @wraps(wrapped_callable)
            async def wrapper(*args: Any, **kwargs: Any) -> Result:
                if is_jobping_disabled():
                    return await wrapped_callable(*args, **kwargs)

                self._ensure_built_sync()
                await self._maybe_rebuild()

                result = await wrapped_callable(*args, **kwargs)
                active = self.currentActive()
                if not active.is_job_ref(result):
                    return result

                job_id = result["job_id"]
                active.accept(job_id)
                completed = await active.await_result(job_id, timeout=30.0)
                active.release(job_id)
                return completed.result

            return wrapper

        return decorator

    # ------------------------------------------------------------------
    # currentActive
    # ------------------------------------------------------------------

    def currentActive(self) -> Any:
        """Return the current active EndpointProxy (or Composite).

        Held under read lock so the caller always gets the correct reference.
        The returned reference may become stale after the lock is released,
        but the underlying object (old broker) is kept alive until migration
        completes.
        """
        with self._lock:
            return self._active

    # ------------------------------------------------------------------
    # broker lifecycle
    # ------------------------------------------------------------------

    async def start_broker(self) -> None:
        """Start the embedded broker (optional warm-up).

        Idempotent — if already running this is a no-op.
        """
        self._ensure_built_sync()
        await self._maybe_rebuild()
        if self._broker is not None:
            await self._broker.start()

    async def stop_broker(self) -> None:
        """Stop the embedded broker."""
        if self._broker is not None:
            await self._broker.stop()

    # ------------------------------------------------------------------
    # internal: lazy build
    # ------------------------------------------------------------------

    def _ensure_built_sync(self) -> None:
        """Ensure the singleton is built (called from sync context).

        The first call triggers _create_jobping(). Subsequent calls are no-op.
        """
        if self._active is not None:
            return
        with self._lock:
            if self._active is not None:
                return
            self._build_from_stored_params()

    def _build_from_stored_params(self) -> None:
        """Create the internal JobPing instance from stored or env-var params."""
        params = dict(self._stored_params) if self._stored_params else {}

        # Env-var fallback
        if "broker_port" not in params:
            env_port = os.environ.get("JOBPING_BROKER_PORT")
            if env_port:
                params["broker_port"] = int(env_port)
            else:
                params["broker_port"] = 0

        if "peer_brokers" not in params or params["peer_brokers"] is None:
            env_peers = os.environ.get("JOBPING_PEER_BROKERS")
            if env_peers:
                params["peer_brokers"] = [p.strip() for p in env_peers.split(",") if p.strip()]

        ep, broker, jp = _create_jobping(**params)
        self._active = ep
        self._broker = broker
        self._jp = jp
        self._job_context_provider = jp.job_context_provider
        self._peer_id = jp.peer_id

    # ------------------------------------------------------------------
    # internal: blue-green rebuild
    # ------------------------------------------------------------------

    async def _maybe_rebuild(self) -> None:
        """If _needs_rebuild is set, perform blue-green migration."""
        if not self._needs_rebuild:
            return
        await self._rebuild()

    async def _rebuild(self) -> None:
        """Execute blue-green migration: build new broker, swap via Composite."""
        from jobping.composite_endpoint_proxy import CompositeEndpointProxy

        with self._lock:
            if not self._needs_rebuild:
                return
            if self._migration_in_progress:
                return
            self._migration_in_progress = True
            self._needs_rebuild = False
            params = dict(self._stored_params)
            old_broker = self._broker

        try:
            # 1. Build and start new broker (fail before swap = safe abort)
            new_ep, new_broker, new_jp = _create_jobping(**params)
            await new_broker.start()

            # 2. Construct Composite
            old_ep = self._active
            composite = CompositeEndpointProxy(
                old=old_ep,
                new=new_ep,
                on_dissolve=self._on_composite_dissolved,
            )

            # 3. No remote peer short-circuit
            pending = getattr(old_broker, '_pending_migrations', set())
            if not pending:
                composite.dissolve()
                # _on_composite_dissolved already updated _active
            else:
                old_broker.on_all_migrated = composite._on_old_broker_ready
                with self._lock:
                    self._active = composite
                new_port = params.get("broker_port", 0)
                old_broker.broadcast_migrate(new_port)

            # Update internal references
            self._broker = new_broker
            self._jp = new_jp
            self._job_context_provider = new_jp.job_context_provider
            self._peer_id = new_jp.peer_id

        except Exception:
            with self._lock:
                self._migration_in_progress = False
                self._needs_rebuild = True  # retry on next wrap
            raise

    def _on_composite_dissolved(self, new_ep: Any) -> None:
        """Callback from Composite.dissolve() — swap _active to new_ep."""
        with self._lock:
            self._active = new_ep
            self._migration_in_progress = False

    # ------------------------------------------------------------------
    # _reset (test only)
    # ------------------------------------------------------------------

    def _reset(self) -> None:
        """Reset singleton to NO_INSTANCE state.  Test-only."""
        with self._lock:
            self._active = None
            self._broker = None
            self._jp = None
            self._stored_params = {}
            self._needs_rebuild = False
            self._migration_in_progress = False
            self._job_context_provider = None
            self._peer_id = None

    # ------------------------------------------------------------------
    # attribute delegation
    # ------------------------------------------------------------------

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        if self._jp is not None:
            return getattr(self._jp, name)
        raise AttributeError(
            f"JobPing not yet built — call configure() first, "
            f"or invoke wrap()/start_broker() to auto-build. "
            f"Attribute '{name}' not available."
        )


# ------------------------------------------------------------------
# internal factory
# ------------------------------------------------------------------

def _create_jobping(
    broker_port: int,
    *,
    peer_brokers: list[str] | None = None,
    idle_timeout_seconds: int | None = 300,
    max_trace_depth: int = 10,
    job_context_provider: Callable[..., str | None] | None = None,
    sio_kwargs: dict[str, Any] | None = None,
) -> tuple[Any, Any, Any]:
    """Internal factory. Returns (endpoint_proxy, broker, jobping)."""
    from jobping.imp.broker import EmbeddedBroker
    from jobping.imp.envelope_endpoint_inmemory import EnvelopeEndpointInMemory
    from jobping.imp.jpitem_queue_inmemory import JPItemQueueInMemory
    from jobping.imp.transport_layer_local import LocalTransportLayer
    from jobping.imp.transport_layer_ws import TransportLayerWS
    from jobping.imp.transport_layer_composite import CompositeTransportLayer
    from jobping.endpoint_proxy import EndpointProxy
    from jobping.result_handoff import ResultHandoff
    from jobping.state_sync import StateSync
    from jobping.jobping import JobPing
    from jobping.id import create_peer_id
    from jobping.transport_layer import TransportLayer

    broker = EmbeddedBroker(broker_port, **(sio_kwargs or {}))
    local_transport: TransportLayer = LocalTransportLayer(broker)

    transports: list[TransportLayer] = [local_transport]
    for url in (peer_brokers or []):
        transports.append(TransportLayerWS(url, idle_timeout_seconds=idle_timeout_seconds))

    if len(transports) == 1:
        transport: TransportLayer = transports[0]
    else:
        transport = CompositeTransportLayer(transports)

    if job_context_provider is None:
        job_context_provider = _default_job_context_provider_from_transport(transport)

    queue = JPItemQueueInMemory(EnvelopeEndpointInMemory())
    endpoint_proxy = EndpointProxy(
        state_sync=StateSync(transport),
        result_handoff=ResultHandoff(transport),
        queue=queue,
        max_trace_depth=max_trace_depth,
    )
    endpoint_proxy._active_trace = None

    jp = JobPing(
        endpoint_proxy=endpoint_proxy,
        job_context_provider=job_context_provider,
        peer_id=create_peer_id(),
        max_trace_depth=max_trace_depth,
    )
    jp._broker = broker
    return endpoint_proxy, broker, jp


def _default_job_context_provider_from_transport(transport: Any) -> Callable[..., str | None]:
    """Return a job_context_provider tied to *transport*."""
    def provider(*args: Any, **kwargs: Any) -> str | None:
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


# ------------------------------------------------------------------
# module-level singleton
# ------------------------------------------------------------------

jp = _LazyJobPing()
