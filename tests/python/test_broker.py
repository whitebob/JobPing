"""Tests for EmbeddedBroker — routing table, broadcast, lifecycle idempotency.

All tests use only local queues (no Socket.IO server started) to verify
the in-process routing logic in isolation.
"""

from __future__ import annotations

import asyncio

import pytest

from jobping.imp.broker import EmbeddedBroker, LOCAL_PEER_ID
from jobping.imp.transport_layer_local import LocalTransportLayer


def test_broker_local_roundtrip_message():
    """Message sent through local queues reaches the LocalTransportLayer mailbox."""
    async def run():
        broker = EmbeddedBroker(0)
        local = LocalTransportLayer(broker)
        await broker.start()

        msg = {"kind": "hello", "job_id": "j1", "data": "world"}
        local.send_message(msg)
        received = await local.recv_message(kind="hello", timeout=0.1)
        assert received == msg

        await broker.stop()

    asyncio.run(run())


def test_broker_register_consumer_routes_to_local():
    """When a consumer is registered for LOCAL_PEER_ID, messages route to local."""
    async def run():
        broker = EmbeddedBroker(0)
        local = LocalTransportLayer(broker)

        broker.register_consumer("j42", LOCAL_PEER_ID)
        await broker.start()

        msg = {"kind": "result", "job_id": "j42"}
        local.send_message(msg)
        received = await local.recv_message(job_id="j42", timeout=0.1)
        assert received == msg

        await broker.stop()

    asyncio.run(run())


def test_broker_unregister_consumer_falls_back_to_broadcast():
    """After unregistering, messages fall back to broadcast (local delivery)."""
    async def run():
        broker = EmbeddedBroker(0)
        local = LocalTransportLayer(broker)

        broker.register_consumer("j7", LOCAL_PEER_ID)
        broker.unregister_consumer("j7")
        await broker.start()

        msg = {"kind": "fallback", "job_id": "j7"}
        local.send_message(msg)
        received = await local.recv_message(job_id="j7", timeout=0.1)
        assert received == msg

        await broker.stop()

    asyncio.run(run())


def test_broker_unregister_unknown_job_is_noop():
    """Unregistering a job_id that was never registered does not crash."""
    broker = EmbeddedBroker(0)
    broker.unregister_consumer("nonexistent")  # should not raise


def test_broker_route_overwrite():
    """Registering the same job_id twice overwrites the previous peer_id."""
    broker = EmbeddedBroker(0)
    broker.register_consumer("j1", "peer-a")
    broker.register_consumer("j1", "peer-b")
    # Last write wins — broker._job_routes is a plain dict
    assert broker._job_routes["j1"] == "peer-b"


def test_broker_start_idempotent():
    """Calling start() twice does not create a second server."""
    async def run():
        broker = EmbeddedBroker(0)
        await broker.start()
        assert broker._server_started is True
        # Second start should be a no-op
        await broker.start()
        assert broker._server_started is True
        await broker.stop()

    asyncio.run(run())


def test_broker_stop_before_start_is_noop():
    """Calling stop() without start() does not crash."""
    async def run():
        broker = EmbeddedBroker(0)
        await broker.stop()  # should not raise

    asyncio.run(run())


def test_broker_broadcast_with_empty_remote_delivers_locally():
    """When _remote_sockets is empty, broadcast delivers only to local callbacks."""
    async def run():
        broker = EmbeddedBroker(0)
        local = LocalTransportLayer(broker)
        await broker.start()

        assert len(broker._remote_sockets) == 0

        msg = {"kind": "broadcast_test", "job_id": "b1"}
        local.send_message(msg)
        received = await local.recv_message(job_id="b1", timeout=0.1)
        assert received == msg

        await broker.stop()

    asyncio.run(run())


def test_broker_stop_cleans_up():
    """After stop(), the server is marked as not started."""
    async def run():
        broker = EmbeddedBroker(0)
        await broker.start()
        assert broker._server_started is True
        await broker.stop()
        assert broker._server_started is False

    asyncio.run(run())
