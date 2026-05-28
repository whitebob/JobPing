"""Composite EndpointProxy — temporary _active slot occupant during blue-green migration.

Writes delegate to new_ep. Results arriving at old_ep's envelope_endpoint are
intercepted via _on_intercept and forwarded to new_ep. Dissolves when old_broker
retires (all remote peers migrated).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from jobping.envelope import JobPingEnvelope

logger = logging.getLogger("jobping.composite")


class CompositeEndpointProxy:
    """Temporary bridge between old and new EndpointProxy during migration.

    Put in singleton's _active slot during reconfigure. All callers go through
    this transparently to new_ep. Old result envelopes are intercepted and
    forwarded.
    """

    def __init__(
        self,
        *,
        old: Any,
        new: Any,
        on_dissolve: Callable[[Any], Any] | None = None,
    ) -> None:
        self._old = old
        self._new = new
        self._on_dissolve = on_dissolve
        self._dissolved = False

        # Register intercept on old_ep's envelope_endpoint
        old.queue.envelope_endpoint._on_intercept = self._on_old_result

    # ------------------------------------------------------------------
    # intercept
    # ------------------------------------------------------------------

    def _on_old_result(self, envelope: JobPingEnvelope) -> bool:
        """Callback for old_ep.queue.envelope_endpoint._on_intercept.

        Returns True if the envelope was handled (forwarded to new_ep),
        False to let old_ep process it normally.
        """
        if self._dissolved:
            return False
        job_id = envelope.get("job_id", "")
        try:
            if self._new.queue.get(job_id) is not None:
                self._new.queue.envelope_endpoint.send(envelope)
                return True
        except Exception:
            pass
        return False

    # ------------------------------------------------------------------
    # dissolve
    # ------------------------------------------------------------------

    def dissolve(self) -> None:
        """Remove intercept callback and notify singleton."""
        if self._dissolved:
            return
        try:
            self._old.queue.envelope_endpoint._on_intercept = None
        except Exception:
            pass
        self._dissolved = True
        if self._on_dissolve is not None:
            self._on_dissolve(self._new)

    def _on_old_broker_ready(self) -> None:
        """Called by old_broker.on_all_migrated when pending_migrations is empty."""
        import asyncio
        try:
            asyncio.create_task(self._old_broker_stop_and_dissolve())
        except Exception:
            logger.exception("Failed to schedule old broker stop")

    async def _old_broker_stop_and_dissolve(self) -> None:
        self.dissolve()

    # ------------------------------------------------------------------
    # explicit delegation — all public EndpointProxy methods + properties
    # ------------------------------------------------------------------

    def create_job_id(self) -> str:
        return self._new.create_job_id()

    def make_job_ref(self, job_id: str) -> dict[str, str]:
        return self._new.make_job_ref(job_id)

    def is_job_ref(self, value: Any) -> bool:
        return self._new.is_job_ref(value)

    def offer(self, job_id: str | None = None) -> Any:
        return self._new.offer(job_id)

    def accept(self, job_id: str) -> Any:
        return self._new.accept(job_id)

    def defer(self, item_or_job_id: Any | str) -> Any:
        return self._new.defer(item_or_job_id)

    def publish_state(
        self,
        job_id: str,
        status: str,
        state_context: Any = None,
    ) -> None:
        self._new.publish_state(job_id, status, state_context)

    async def wait_for_state(
        self,
        job_id: str,
        *,
        status: str | None = None,
        timeout: float = 1.0,
    ) -> Any:
        return await self._new.wait_for_state(job_id, status=status, timeout=timeout)

    def fulfill(self, job_id: str, result: Any) -> Any:
        return self._new.fulfill(job_id, result)

    async def fulfill_later(
        self,
        job_id: str,
        task: Callable[[], Awaitable[Any]],
    ) -> Any:
        return await self._new.fulfill_later(job_id, task)

    async def await_result(
        self,
        job_id: str,
        *,
        timeout: float = 1.0,
    ) -> Any:
        return await self._new.await_result(job_id, timeout=timeout)

    def release(self, job_id: str) -> Any:
        return self._new.release(job_id)

    # -- properties -----------------------------------------------------------

    @property
    def state_sync(self) -> Any:
        return self._new.state_sync

    @property
    def result_handoff(self) -> Any:
        return self._new.result_handoff

    @property
    def queue(self) -> Any:
        return self._new.queue

    @property
    def _active_trace(self) -> Any:
        return self._new._active_trace

    @_active_trace.setter
    def _active_trace(self, value: Any) -> None:
        self._new._active_trace = value

    @property
    def _sub_traces(self) -> Any:
        return self._new._sub_traces

    @_sub_traces.setter
    def _sub_traces(self, value: Any) -> None:
        self._new._sub_traces = value

    @property
    def max_trace_depth(self) -> int:
        return self._new.max_trace_depth

    # -- __getattr__ fallback for future methods -----------------------------

    def __getattr__(self, name: str) -> Any:
        # Only called when normal lookup fails
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._new, name)
