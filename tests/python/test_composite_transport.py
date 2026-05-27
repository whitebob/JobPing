"""Tests for CompositeTransportLayer — multi-transport fan-out, race, error handling."""

from __future__ import annotations

import asyncio

import pytest

from jobping.imp.transport_layer_composite import CompositeTransportLayer
from jobping.imp.envelope_endpoint_inmemory import EnvelopeEndpointInMemory
from jobping_sandbox.transport_layer_mock import TransportLayerMock


def _make_mock():
    """Create a TransportLayerMock with envelope endpoint for envelope tests."""
    return TransportLayerMock(EnvelopeEndpointInMemory())


def test_recv_timeout_when_all_sub_transports_timeout():
    """When every sub-transport times out, the composite propagates the TimeoutError."""
    async def run():
        mock1 = TransportLayerMock()
        mock2 = TransportLayerMock()
        composite = CompositeTransportLayer([mock1, mock2])

        # No messages in either mock — both should time out
        with pytest.raises(TimeoutError):
            await composite.recv_message(timeout=0.01)

    asyncio.run(run())


def test_recv_sub_transport_exception_cancels_others():
    """When one sub-transport raises a non-TimeoutError, the other tasks are cancelled."""
    async def run():
        mock_good = TransportLayerMock()
        mock_bad = TransportLayerMock()

        # Override mock_bad's recv_message to raise an error
        async def raise_err(*args, **kwargs):
            raise RuntimeError("boom")

        mock_bad.recv_message = raise_err

        composite = CompositeTransportLayer([mock_good, mock_bad])

        with pytest.raises(RuntimeError, match="boom"):
            await composite.recv_message(timeout=0.2)

    asyncio.run(run())


def test_send_envelope_fanout():
    """send_envelope fans out to all child transports."""
    async def run():
        mock1 = _make_mock()
        mock2 = _make_mock()
        composite = CompositeTransportLayer([mock1, mock2])

        from jobping.envelope import box_result
        env = box_result("j", {"ok": True})
        composite.send_envelope(env)

        r1 = await mock1.recv_envelope(job_id="j", timeout=0.1)
        r2 = await mock2.recv_envelope(job_id="j", timeout=0.1)
        assert r1 == env
        assert r2 == env

    asyncio.run(run())


def test_recv_envelope_race():
    """recv_envelope races child transports, first to deliver wins."""
    async def run():
        mock1 = _make_mock()
        mock2 = _make_mock()
        composite = CompositeTransportLayer([mock1, mock2])

        from jobping.envelope import box_result
        env = box_result("j", {"x": 1})

        # Pre-seed mock2 so it wins
        mock2.send_envelope(env)

        received = await composite.recv_envelope(job_id="j", timeout=0.1)
        assert received == env

    asyncio.run(run())


def test_transports_property_returns_copy():
    """The transports property returns a copy, not the internal list."""
    mock1 = TransportLayerMock()
    mock2 = TransportLayerMock()
    composite = CompositeTransportLayer([mock1, mock2])

    t = composite.transports
    t.append(TransportLayerMock())
    # The composite still only has 2
    assert len(composite.transports) == 2
