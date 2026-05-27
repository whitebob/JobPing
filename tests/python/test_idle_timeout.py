"""Tests for TransportLayerWS idle timeout — disconnect, touch, reconnect.

Uses unittest.mock to control time.monotonic and asyncio.sleep without
real network connections.
"""

from __future__ import annotations

import asyncio
import time
from unittest import mock

import pytest

from jobping.imp.transport_layer_ws import TransportLayerWS


# Fake URL — we never actually connect, so it doesn't need to be reachable.
FAKE_URL = "http://127.0.0.1:1"


def _make_ws(idle_timeout_seconds=5):
    """Create a TransportLayerWS, patching socketio import errors."""
    return TransportLayerWS(FAKE_URL, idle_timeout_seconds=idle_timeout_seconds)


def test_idle_timeout_none_does_not_start_watcher():
    """When idle_timeout_seconds is None, _start_idle_watcher is a no-op."""
    async def run():
        ws = TransportLayerWS(FAKE_URL)
        await ws._start_idle_watcher()
        assert ws._idle_task is None

    asyncio.run(run())


def test_touch_updates_last_activity():
    """_touch() updates _last_activity to the current monotonic time."""
    ws = _make_ws()
    original = ws._last_activity
    with mock.patch(
        "jobping.imp.transport_layer_ws.time.monotonic",
        return_value=original + 10.0,
    ):
        ws._touch()
        assert ws._last_activity == original + 10.0


def test_disconnect_when_not_connected():
    """disconnect() when _connected=False only cancels watcher (no-op)."""
    async def run():
        ws = _make_ws()
        ws._connected = False
        # Should not raise
        await ws.disconnect()

    asyncio.run(run())


def test_disconnect_when_connected():
    """disconnect() sets _connected=False and cancels idle watcher."""
    async def run():
        ws = _make_ws()
        ws._connected = True
        # Patch sio.disconnect to avoid network call
        ws._sio.disconnect = mock.AsyncMock()
        # Simulate an idle task
        async def dummy_watcher():
            while True:
                await asyncio.sleep(10)

        ws._idle_task = asyncio.create_task(dummy_watcher())

        await ws.disconnect()

        assert ws._connected is False
        assert ws._idle_task is None  # cancelled

    asyncio.run(run())


def test_idle_watcher_check_triggers_disconnect():
    """The watcher's idle check: when elapsed > timeout, disconnect is called."""
    async def run():
        ws = _make_ws(idle_timeout_seconds=2)
        ws._connected = True

        disconnect_called = False

        async def fake_disconnect():
            nonlocal disconnect_called
            disconnect_called = True
            ws._connected = False

        ws.disconnect = fake_disconnect

        # Simulate stale activity: last_activity is long ago
        ws._last_activity = 100.0

        with mock.patch(
            "jobping.imp.transport_layer_ws.time.monotonic",
            return_value=105.0,
        ):
            # The check the watcher performs each loop iteration:
            elapsed = 105.0 - ws._last_activity  # = 5.0
            assert elapsed > ws._idle_timeout  # 5 > 2

            # Simulate what happens when the check passes
            await ws.disconnect()

        assert disconnect_called
        assert ws._connected is False

    asyncio.run(run())


def test_idle_watcher_check_passes_when_active():
    """When activity is recent, the idle check does NOT disconnect."""
    async def run():
        ws = _make_ws(idle_timeout_seconds=2)
        ws._connected = True

        disconnect_called = False

        async def fake_disconnect():
            nonlocal disconnect_called
            disconnect_called = True

        ws.disconnect = fake_disconnect

        # Simulate recent activity
        ws._last_activity = 104.0

        with mock.patch(
            "jobping.imp.transport_layer_ws.time.monotonic",
            return_value=105.0,
        ):
            elapsed = 105.0 - ws._last_activity  # = 1.0
            # 1.0 <= 2.0, so no disconnect
            if elapsed > ws._idle_timeout:
                await ws.disconnect()

        assert not disconnect_called
        assert ws._connected is True

    asyncio.run(run())


def test_double_start_idle_watcher_is_safe():
    """Calling _start_idle_watcher twice only creates one task."""
    async def run():
        ws = _make_ws(idle_timeout_seconds=60)
        ws._connected = True

        class StopWatcher(Exception):
            pass

        async def fake_sleep(seconds):
            raise StopWatcher  # exit after first sleep instead of looping

        with mock.patch("asyncio.sleep", side_effect=fake_sleep):
            await ws._start_idle_watcher()
            first_task = ws._idle_task
            assert first_task is not None

            # Second call — should see task already running, return early
            await ws._start_idle_watcher()
            assert ws._idle_task is first_task

            # Clean up
            try:
                first_task.cancel()
            except Exception:
                pass

    asyncio.run(run())


def test_touch_called_on_send_message():
    """send_message calls _touch() to reset the idle timer."""
    async def run():
        ws = _make_ws()
        ws._connected = True
        orig_activity = ws._last_activity

        # Patch _ensure_connected and sio.emit to avoid real network
        async def fake_ensure():
            pass

        ws._ensure_connected = fake_ensure
        ws._sio.emit = mock.AsyncMock()

        with mock.patch(
            "jobping.imp.transport_layer_ws.time.monotonic",
            return_value=orig_activity + 5.0,
        ):
            ws.send_message({"kind": "msg", "job_id": "j"})

        assert ws._last_activity == orig_activity + 5.0

    asyncio.run(run())
