"""Embedded Socket.IO broker — every peer runs one.

Routes messages between local and remote clients. Maintains a job_id → peer_id
routing table so that fulfill can be unicast to the registered consumer.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from jobping.envelope import is_envelope, JobPingEnvelope
from jobping.transport_layer import TransportMessage


logger = logging.getLogger("jobping.broker")

# Sentinel peer_id for the local client (not connected via Socket.IO).
LOCAL_PEER_ID = "__local__"

# Migration protocol message kinds
BROKER_MIGRATE_KIND = "jobping:broker_migrate"
MIGRATION_COMPLETE_KIND = "jobping:migration_complete"


class EmbeddedBroker:
    """Lightweight Socket.IO broker embedded in every JobPing peer.

    The broker listens on *port* for remote peer connections.  A single local
    client (the peer's own ``LocalTransportLayer``) communicates through
    in-process queues instead of going through the network stack.
    """

    def __init__(self, port: int, **sio_kwargs: Any) -> None:
        self.port = port

        # --- in-process queues for the local client ---
        self._local_msg_queue: asyncio.Queue[dict] = asyncio.Queue()
        self._local_env_queue: asyncio.Queue[dict] = asyncio.Queue()

        # --- routing table: job_id → peer_id (registered on consumer await) ---
        self._job_routes: dict[str, str] = {}

        # --- remote socket registry: peer_id → socket ---
        self._remote_sockets: dict[str, Any] = {}

        # --- callbacks set by LocalTransportLayer ---
        self._on_local_message: Any = None
        self._on_local_envelope: Any = None

        # --- migration support ---
        self._pending_migrations: set[str] = set()
        self.on_all_migrated: Callable[[], Any] | None = None
        self._broker_migrate_handler: Callable[[dict], Any] | None = None

        # Start the Socket.IO server lazily (deferred to _ensure_server).
        self._sio_kwargs = sio_kwargs
        self._server_started = False

    # ------------------------------------------------------------------
    # local fast path (called by LocalTransportLayer)
    # ------------------------------------------------------------------

    def local_send_message(self, msg: TransportMessage) -> None:
        """Push a message from the local client into the in-process queue."""
        self._local_msg_queue.put_nowait(msg)

    def local_send_envelope(self, env: JobPingEnvelope) -> None:
        """Push an envelope from the local client into the in-process queue."""
        self._local_env_queue.put_nowait(env)

    # ------------------------------------------------------------------
    # routing
    # ------------------------------------------------------------------

    def register_consumer(self, job_id: str, peer_id: str) -> None:
        """Register that *peer_id* is awaiting results for *job_id*."""
        self._job_routes[job_id] = peer_id

    def unregister_consumer(self, job_id: str) -> None:
        """Remove the route entry after fulfill."""
        self._job_routes.pop(job_id, None)

    async def _route_message(self, msg: dict) -> None:
        """Route a TransportMessage: unicast if target known, else broadcast."""
        # Migration protocol messages
        kind = msg.get("kind", "")
        if kind == MIGRATION_COMPLETE_KIND:
            await self._handle_migration_complete(msg)
            return
        if kind == BROKER_MIGRATE_KIND and self._broker_migrate_handler is not None:
            self._broker_migrate_handler(msg)
            return

        target = self._job_routes.get(msg.get("job_id"))
        if target is None:
            await self._broadcast_message(msg)
        elif target == LOCAL_PEER_ID:
            self._deliver_local_message(msg)
        else:
            await self._unicast_remote_message(target, msg)

    async def _route_envelope(self, env: dict) -> None:
        """Route an envelope the same way."""
        target = self._job_routes.get(env.get("job_id"))
        if target is None:
            await self._broadcast_envelope(env)
        elif target == LOCAL_PEER_ID:
            self._deliver_local_envelope(env)
        else:
            await self._unicast_remote_envelope(target, env)

    # ------------------------------------------------------------------
    # delivery
    # ------------------------------------------------------------------

    def _deliver_local_message(self, msg: dict) -> None:
        if self._on_local_message is not None:
            self._on_local_message(msg)

    def _deliver_local_envelope(self, env: dict) -> None:
        if self._on_local_envelope is not None:
            self._on_local_envelope(env)

    async def _broadcast_message(self, msg: dict) -> None:
        # deliver locally
        self._deliver_local_message(msg)
        # deliver to every remote socket
        for sid in list(self._remote_sockets):
            try:
                await self._sio_server.emit("jobping:message", msg, to=sid)
            except Exception:
                pass

    async def _broadcast_envelope(self, env: dict) -> None:
        self._deliver_local_envelope(env)
        for sid in list(self._remote_sockets):
            try:
                await self._sio_server.emit("jobping:envelope", env, to=sid)
            except Exception:
                pass

    async def _unicast_remote_message(self, target_peer_id: str, msg: dict) -> None:
        if target_peer_id in self._remote_sockets:
            try:
                await self._sio_server.emit("jobping:message", msg, to=target_peer_id)
            except Exception:
                pass
        else:
            self._deliver_local_message(msg)  # fallback

    async def _unicast_remote_envelope(self, target_peer_id: str, env: dict) -> None:
        if target_peer_id in self._remote_sockets:
            try:
                await self._sio_server.emit("jobping:envelope", env, to=target_peer_id)
            except Exception:
                pass
        else:
            self._deliver_local_envelope(env)

    # ------------------------------------------------------------------
    # Socket.IO server lifecycle
    # ------------------------------------------------------------------

    async def _ensure_server(self) -> None:
        """Start the Socket.IO server if not already running."""
        if self._server_started:
            return

        try:
            import socketio  # type: ignore
        except Exception as exc:
            raise RuntimeError(
                "python-socketio is required for EmbeddedBroker: "
                "install 'python-socketio[asyncio_server]'"
            ) from exc

        self._sio_server = socketio.AsyncServer(**self._sio_kwargs)
        self._sio_server.on("connect", self._on_remote_connect)
        self._sio_server.on("disconnect", self._on_remote_disconnect)
        self._sio_server.on("jobping:message", self._on_remote_message)
        self._sio_server.on("jobping:envelope", self._on_remote_envelope)

        # Use aiohttp as the ASGI server if available, else fall back to
        # socketio's built-in aiohttp integration.
        try:
            from aiohttp import web
        except Exception:
            raise RuntimeError("aiohttp is required for EmbeddedBroker")

        app = web.Application()
        self._sio_server.attach(app)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await site.start()
        self._server_started = True

    async def _on_remote_connect(self, sid: str, environ: dict) -> None:
        """Register a remote peer connection."""
        self._remote_sockets[sid] = True

    async def _on_remote_disconnect(self, sid: str) -> None:
        """Remove a disconnected remote peer."""
        self._remote_sockets.pop(sid, None)

    async def _on_remote_message(self, sid: str, data: dict) -> None:
        """Handle incoming message from a remote peer."""
        await self._route_message(data)

    async def _on_remote_envelope(self, sid: str, data: dict) -> None:
        """Handle incoming envelope from a remote peer."""
        await self._route_envelope(data)

    # ------------------------------------------------------------------
    # migration
    # ------------------------------------------------------------------

    def broadcast_migrate(self, new_port: int) -> None:
        """Broadcast broker_migrate to all remote peers.

        Called by the singleton during reconfigure. Each remote peer receives
        the message and should connect to *new_port*.
        """
        msg = {
            "kind": BROKER_MIGRATE_KIND,
            "new_port": new_port,
        }
        # register all current remote peers as pending
        self._pending_migrations = set(self._remote_sockets.keys())
        if not self._pending_migrations:
            return
        for sid in self._remote_sockets:
            try:
                asyncio.create_task(self._sio_server.emit("jobping:message", msg, to=sid))
            except Exception:
                pass

    async def _handle_migration_complete(self, message: dict) -> None:
        """Handle a migration_complete message from a remote peer."""
        data = message.get("data", {}) if isinstance(message.get("data"), dict) else {}
        peer_id = data.get("peer_id", "") if isinstance(data, dict) else ""
        if not peer_id:
            return
        self._pending_migrations.discard(peer_id)
        logger.info("Migration complete from peer %s (%d remaining)",
                     peer_id, len(self._pending_migrations))
        if not self._pending_migrations and self.on_all_migrated is not None:
            try:
                self.on_all_migrated()
            except Exception:
                logger.exception("on_all_migrated callback failed")

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the broker (Socket.IO server + local queue processing)."""
        await self._ensure_server()
        asyncio.create_task(self._process_local_queues())

    async def _process_local_queues(self) -> None:
        """Continuously drain local queues and route messages."""
        while True:
            done, _ = await asyncio.wait(
                [
                    asyncio.create_task(self._local_msg_queue.get()),
                    asyncio.create_task(self._local_env_queue.get()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                item = task.result()
                if isinstance(item, dict) and "kind" in item:
                    await self._route_message(item)
                else:
                    await self._route_envelope(item)

    async def stop(self) -> None:
        """Shut down the broker."""
        if self._server_started:
            await self._runner.cleanup()
            self._server_started = False
