"""Tests for LocalTransportLayer — local loopback with and without local transport.

Two scenarios:
A. Pure local loopback: EmbeddedBroker + LocalTransportLayer (in-process queues)
B. No local loopback: TransportLayerMock ×2 + CompositeTransportLayer (simulates remote-only)
C. Mailbox edge cases: waiters, timeout, message storage
"""

from __future__ import annotations

import asyncio

import pytest

from jobping.imp.transport_layer_local import LocalTransportLayer
from jobping.imp.transport_layer_composite import CompositeTransportLayer
from jobping.imp.broker import EmbeddedBroker
from jobping_sandbox.transport_layer_mock import TransportLayerMock


# ---------------------------------------------------------------------------
# Scenario A: Pure local loopback
# ---------------------------------------------------------------------------

def test_local_transport_message_roundtrip():
    """send_message through broker's local queues → recv_message gets it."""
    async def run():
        broker = EmbeddedBroker(0)
        local = LocalTransportLayer(broker)
        await broker.start()

        msg = {"kind": "test", "job_id": "j1", "payload": "hello"}
        local.send_message(msg)

        received = await local.recv_message(kind="test", timeout=0.1)
        assert received == msg

        await broker.stop()

    asyncio.run(run())


def test_local_transport_envelope_roundtrip():
    """send_envelope through broker's local queues → recv_envelope gets it."""
    async def run():
        broker = EmbeddedBroker(0)
        local = LocalTransportLayer(broker)
        await broker.start()

        from jobping.envelope import JobPingEnvelope
        env = JobPingEnvelope(type="result", job_id="j2", data={"x": 1})
        local.send_envelope(env)

        received = await local.recv_envelope(job_id="j2", timeout=0.1)
        assert received == env

        await broker.stop()

    asyncio.run(run())


def test_local_transport_message_filter_by_kind():
    """recv_message with kind filter only returns matching messages."""
    async def run():
        broker = EmbeddedBroker(0)
        local = LocalTransportLayer(broker)
        await broker.start()

        local.send_message({"kind": "a", "job_id": "j1"})
        local.send_message({"kind": "b", "job_id": "j2"})

        received = await local.recv_message(kind="b", timeout=0.1)
        assert received["kind"] == "b"
        assert received["job_id"] == "j2"

        await broker.stop()

    asyncio.run(run())


def test_local_transport_message_filter_by_job_id():
    """recv_message with job_id filter only returns matching messages."""
    async def run():
        broker = EmbeddedBroker(0)
        local = LocalTransportLayer(broker)
        await broker.start()

        local.send_message({"kind": "m", "job_id": "target"})
        local.send_message({"kind": "m", "job_id": "other"})

        received = await local.recv_message(job_id="target", timeout=0.1)
        assert received["job_id"] == "target"

        await broker.stop()

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Scenario B: No local loopback (remote-only via TransportLayerMock)
# ---------------------------------------------------------------------------

def test_composite_with_mocks_fanout_send():
    """send on composite broadcasts to all child transports."""
    async def run():
        mock1 = TransportLayerMock()
        mock2 = TransportLayerMock()
        composite = CompositeTransportLayer([mock1, mock2])

        msg = {"kind": "msg", "job_id": "j1"}
        composite.send_message(msg)

        # Both mocks should have received the message
        r1 = await mock1.recv_message(timeout=0.1)
        r2 = await mock2.recv_message(timeout=0.1)
        assert r1 == msg
        assert r2 == msg

    asyncio.run(run())


def test_composite_with_mocks_recv_race():
    """recv on composite races child transports, first to deliver wins."""
    async def run():
        mock1 = TransportLayerMock()
        mock2 = TransportLayerMock()
        composite = CompositeTransportLayer([mock1, mock2])

        msg = {"kind": "msg", "job_id": "j1"}

        # Pre-seed mock2 so it wins the race
        mock2.send_message(msg)

        received = await composite.recv_message(timeout=0.1)
        assert received == msg

    asyncio.run(run())


def test_two_peers_via_shared_mock_transports():
    """Two composites over the same mocks simulate P2P without local loopback."""
    async def run():
        mock1 = TransportLayerMock()
        mock2 = TransportLayerMock()

        peer_a = CompositeTransportLayer([mock1, mock2])
        peer_b = CompositeTransportLayer([mock1, mock2])

        # Peer A sends a message
        msg = {"kind": "request", "job_id": "job-42"}
        peer_a.send_message(msg)

        # Peer B receives it via race
        received = await peer_b.recv_message(kind="request", timeout=0.1)
        assert received == msg

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Scenario C: Mailbox edge cases
# ---------------------------------------------------------------------------

def test_mailbox_timeout_cleans_up_waiter():
    """A waiter that times out is removed from the mailbox."""
    async def run():
        mock = TransportLayerMock()
        with pytest.raises(TimeoutError):
            await mock.recv_message(timeout=0.01)
        # After timeout, no waiter remains
        assert mock.size()["waiters"] == 0

    asyncio.run(run())


def test_mailbox_stored_message_retrieved_by_late_waiter():
    """Message sent before recv is stored and found by later recv."""
    async def run():
        mock = TransportLayerMock()
        msg = {"kind": "early", "job_id": "j1"}
        mock.send_message(msg)

        # No waiter is registered yet, so message is stored
        assert mock.size()["messages"] == 1

        received = await mock.recv_message(kind="early", timeout=0.1)
        assert received == msg
        assert mock.size()["messages"] == 0

    asyncio.run(run())


def test_mailbox_multiple_waiters_first_match_wins():
    """Two waiters registered, message delivered to first matching one."""
    async def run():
        mock = TransportLayerMock()

        # Register two waiters — both match the same message
        async def wait_first():
            return await mock.recv_message(kind="x", timeout=0.5)

        async def wait_second():
            return await mock.recv_message(kind="x", timeout=0.5)

        task1 = asyncio.create_task(wait_first())
        task2 = asyncio.create_task(wait_second())
        await asyncio.sleep(0)

        msg = {"kind": "x", "job_id": "j"}
        mock.send_message(msg)

        # The first matching waiter in insertion order gets the message
        result = await asyncio.wait_for(
            asyncio.gather(task1, task2, return_exceptions=True), timeout=1.0
        )
        # Exactly one should succeed, the other times out
        successes = [r for r in result if not isinstance(r, Exception)]
        assert len(successes) == 1
        assert successes[0] == msg

    asyncio.run(run())


def test_composite_requires_at_least_two_transports():
    with pytest.raises(ValueError):
        CompositeTransportLayer([])
    with pytest.raises(ValueError):
        CompositeTransportLayer([TransportLayerMock()])


def test_composite_metadata_delegates_to_first_transport():
    mock1 = TransportLayerMock()
    mock2 = TransportLayerMock()
    composite = CompositeTransportLayer([mock1, mock2])

    carrier = composite.attach_job_id(None, "j99")
    assert carrier["headers"]["x-jobping-job-id"] == "j99"
    assert composite.extract_job_id(carrier) == "j99"
