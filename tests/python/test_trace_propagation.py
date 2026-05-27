"""Tests for trace propagation — wrap vs wrap_trace, ContextVar, depth limiting."""

from __future__ import annotations

import asyncio

import pytest

from jobping.endpoint_proxy import EndpointProxy, _limit_trace_depth as limit_trace_depth
from jobping.jobping import JobPing, _check_trace_header, _trace_enabled, is_jobping_disabled
from jobping.result_handoff import ResultHandoff
from jobping.state_sync import StateSync
from jobping.imp.envelope_endpoint_inmemory import EnvelopeEndpointInMemory
from jobping.imp.jpitem_queue_inmemory import JPItemQueueInMemory
from jobping_sandbox.transport_layer_mock import TransportLayerMock


# ---------------------------------------------------------------------------
# _check_trace_header unit tests
# ---------------------------------------------------------------------------

class FakeRequest:
    def __init__(self, headers=None):
        self.headers = headers or {}

    def get(self, key, default=None):
        return getattr(self, key, default)


def test_check_trace_header_detects_enabled():
    req = FakeRequest({"x-jobping-trace-enabled": "1"})
    assert _check_trace_header(req) is True


def test_check_trace_header_detects_true_string():
    req = FakeRequest({"x-jobping-trace-enabled": "true"})
    assert _check_trace_header(req) is True


def test_check_trace_header_ignores_false():
    req = FakeRequest({"x-jobping-trace-enabled": "false"})
    assert _check_trace_header(req) is False


def test_check_trace_header_ignores_missing():
    req = FakeRequest({"other-header": "value"})
    assert _check_trace_header(req) is False


def test_check_trace_header_handles_no_headers():
    req = FakeRequest()
    assert _check_trace_header(req) is False


def test_check_trace_header_case_insensitive():
    req = FakeRequest({"X-JOBPING-TRACE-ENABLED": "1"})
    assert _check_trace_header(req) is True


# ---------------------------------------------------------------------------
# _limit_trace_depth unit tests
# ---------------------------------------------------------------------------

def test_limit_trace_depth_zero_truncates():
    result = limit_trace_depth(
        [{"job_id": "a", "sub_jobs": [{"job_id": "b"}]}],
        max_depth=0,
    )
    assert result == [{"_truncated": True}]


def test_limit_trace_depth_shallow_allows_level():
    result = limit_trace_depth(
        [{"job_id": "a", "sub_jobs": [{"job_id": "b"}]}],
        max_depth=2,
    )
    assert len(result) == 1
    assert result[0]["job_id"] == "a"
    assert result[0]["sub_jobs"][0]["job_id"] == "b"


def test_limit_trace_depth_deep_truncation():
    """At max_depth=1, immediate children are truncated."""
    result = limit_trace_depth(
        [{"job_id": "a", "sub_jobs": [{"job_id": "b"}]}],
        max_depth=1,
    )
    assert result[0]["job_id"] == "a"
    assert result[0]["sub_jobs"] == [{"_truncated": True}]


# ---------------------------------------------------------------------------
# ContextVar isolation
# ---------------------------------------------------------------------------

def test_trace_enabled_defaults_to_false():
    """_trace_enabled ContextVar defaults to False."""
    assert _trace_enabled.get() is False


def test_trace_enabled_context_isolation():
    """Two concurrent contexts get independent trace_enabled values."""
    async def run():
        # Context A: enabled
        async def context_a():
            token = _trace_enabled.set(True)
            await asyncio.sleep(0)
            result = _trace_enabled.get()
            _trace_enabled.reset(token)
            return result

        # Context B: still default
        async def context_b():
            await asyncio.sleep(0)
            return _trace_enabled.get()

        a_val, b_val = await asyncio.gather(context_a(), context_b())
        assert a_val is True
        assert b_val is False

    asyncio.run(run())


def test_trace_enabled_reset_after_context():
    """After reset(token), the ContextVar returns to its previous value."""
    prev = _trace_enabled.get()
    token = _trace_enabled.set(True)
    assert _trace_enabled.get() is True
    _trace_enabled.reset(token)
    assert _trace_enabled.get() == prev


# ---------------------------------------------------------------------------
# wrap_trace produces active trace
# ---------------------------------------------------------------------------

def test_wrap_trace_sets_active_trace():
    """wrap_trace sets _active_trace on the endpoint proxy."""
    async def run():
        mock_transport = TransportLayerMock()
        state_sync = StateSync(mock_transport)
        result_handoff = ResultHandoff(mock_transport)
        producer_proxy = EndpointProxy(
            state_sync=state_sync,
            result_handoff=result_handoff,
            queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()),
            max_trace_depth=10,
        )
        consumer_proxy = EndpointProxy(
            state_sync=state_sync,
            result_handoff=result_handoff,
            queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()),
        )
        job_id = producer_proxy.create_job_id()

        jp = JobPing(
            endpoint_proxy=producer_proxy,
            job_context_provider=lambda *args, **kwargs: job_id,
            peer_id="test-peer",
        )

        trace_set = False

        @jp.wrap_trace()
        async def my_handler(value):
            nonlocal trace_set
            trace_set = producer_proxy._active_trace is not None
            if producer_proxy._active_trace:
                assert producer_proxy._active_trace["job_id"] == job_id
                assert producer_proxy._active_trace["hop"] == 1
                assert producer_proxy._active_trace["peer_id"] == "test-peer"
            return {"value": value}

        job_ref = await my_handler(42)
        assert producer_proxy.is_job_ref(job_ref)

        # Handler runs in background; await_result lets it complete.
        consumer_proxy.accept(job_id)
        completed = await consumer_proxy.await_result(job_id, timeout=1.0)
        assert completed.result == {"value": 42}
        assert trace_set is True

    asyncio.run(run())


def test_wrap_with_trace_header_sets_active_trace():
    """wrap() with x-jobping-trace-enabled header activates tracing."""
    async def run():
        mock_transport = TransportLayerMock()
        state_sync = StateSync(mock_transport)
        result_handoff = ResultHandoff(mock_transport)
        producer_proxy = EndpointProxy(
            state_sync=state_sync,
            result_handoff=result_handoff,
            queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()),
            max_trace_depth=10,
        )

        job_id = producer_proxy.create_job_id()

        jp = JobPing(
            endpoint_proxy=producer_proxy,
            job_context_provider=lambda *args, **kwargs: job_id,
            peer_id="test-peer",
        )

        trace_set = False

        @jp.wrap()
        async def my_handler(request):
            nonlocal trace_set
            trace_set = producer_proxy._active_trace is not None
            return {"value": request.get("v")}

        # Call with trace-enabled header
        req = FakeRequest({"x-jobping-trace-enabled": "true"})
        job_ref = await my_handler(req)
        assert producer_proxy.is_job_ref(job_ref)

        # Handler runs in background; await_result lets it complete.
        job_id = job_ref["job_id"]
        consumer_proxy = EndpointProxy(
            state_sync=producer_proxy.state_sync,
            result_handoff=producer_proxy.result_handoff,
            queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()),
        )
        consumer_proxy.accept(job_id)
        await consumer_proxy.await_result(job_id, timeout=1.0)
        assert trace_set is True

    asyncio.run(run())


def test_wrap_without_trace_header_does_not_set_active_trace():
    """wrap() without trace header does NOT activate tracing."""
    async def run():
        mock_transport = TransportLayerMock()
        state_sync = StateSync(mock_transport)
        result_handoff = ResultHandoff(mock_transport)
        producer_proxy = EndpointProxy(
            state_sync=state_sync,
            result_handoff=result_handoff,
            queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()),
        )

        job_id = producer_proxy.create_job_id()
        trace_was_set = False

        def my_job_context_provider(*args, **kwargs):
            # Returns job_id based on request header
            req = args[0] if args else None
            if req and isinstance(req, FakeRequest) and req.headers.get("x-jobping-job-id"):
                return req.headers["x-jobping-job-id"]
            return None

        jp = JobPing(
            endpoint_proxy=producer_proxy,
            job_context_provider=my_job_context_provider,
        )

        @jp.wrap()
        async def my_handler(request):
            nonlocal trace_was_set
            trace_was_set = producer_proxy._active_trace is not None
            return {"status": "ok"}

        # Call WITHOUT trace header
        req = FakeRequest({"x-jobping-job-id": job_id})
        result = await my_handler(req)
        # No trace header, so trace should NOT be activated
        assert trace_was_set is False
        # Since job_context returns job_id, it returns a job_ref
        assert isinstance(result, dict) and result.get("type") == "job_ref"

    asyncio.run(run())
