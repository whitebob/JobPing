// Embedded Socket.IO broker — every peer runs one.
//
// Routes messages between local and remote clients. Maintains a job_id → peer_id
// routing table so that fulfill can be unicast to the registered consumer.

import { createServer } from "node:http";
import { Server } from "socket.io";

const LOCAL_PEER_ID = "__local__";

export class EmbeddedBroker {
  constructor(port, sioOpts = {}) {
    this.port = port;

    // --- in-process queues for the local client ---
    this._localMsgQueue = [];
    this._localEnvQueue = [];
    this._localMsgWaiters = [];
    this._localEnvWaiters = [];

    // --- routing table: job_id → peer_id ---
    this._jobRoutes = new Map();

    // --- remote socket registry: sid → socket ---
    this._remoteSockets = new Map();

    // --- callbacks set by LocalTransportLayer ---
    this._onLocalMessage = null;
    this._onLocalEnvelope = null;

    // Start immediately.
    this._httpServer = createServer();
    this._sio = new Server(this._httpServer, {
      cors: { origin: "*" },
      ...sioOpts,
    });

    this._sio.on("connection", (socket) => {
      this._remoteSockets.set(socket.id, socket);

      socket.on("jobping:message", (data) => this._routeMessage(data));
      socket.on("jobping:envelope", (data) => this._routeEnvelope(data));

      socket.on("disconnect", () => {
        this._remoteSockets.delete(socket.id);
      });
    });
  }

  async start() {
    return new Promise((resolve) => {
      this._httpServer.listen(this.port, () => resolve());
    });
  }

  async stop() {
    return new Promise((resolve) => {
      this._sio.close();
      this._httpServer.close(() => resolve());
    });
  }

  // -- local fast path ---------------------------------------------------

  localSendMessage(msg) {
    this._routeMessage(msg);
  }

  localSendEnvelope(env) {
    this._routeEnvelope(env);
  }

  // -- routing -----------------------------------------------------------

  registerConsumer(jobId, peerId) {
    this._jobRoutes.set(jobId, peerId);
  }

  unregisterConsumer(jobId) {
    this._jobRoutes.delete(jobId);
  }

  _routeMessage(msg) {
    const target = this._jobRoutes.get(msg.job_id);
    if (!target) {
      this._broadcastMessage(msg);
    } else if (target === LOCAL_PEER_ID) {
      this._deliverLocalMessage(msg);
    } else {
      this._unicastRemoteMessage(target, msg);
    }
  }

  _routeEnvelope(env) {
    const target = this._jobRoutes.get(env.job_id);
    if (!target) {
      this._broadcastEnvelope(env);
    } else if (target === LOCAL_PEER_ID) {
      this._deliverLocalEnvelope(env);
    } else {
      this._unicastRemoteEnvelope(target, env);
    }
  }

  // -- delivery -----------------------------------------------------------

  _deliverLocalMessage(msg) {
    if (this._onLocalMessage) this._onLocalMessage(msg);
  }

  _deliverLocalEnvelope(env) {
    if (this._onLocalEnvelope) this._onLocalEnvelope(env);
  }

  _broadcastMessage(msg) {
    this._deliverLocalMessage(msg);
    for (const [, socket] of this._remoteSockets) {
      try { socket.emit("jobping:message", msg); } catch (_) {}
    }
  }

  _broadcastEnvelope(env) {
    this._deliverLocalEnvelope(env);
    for (const [, socket] of this._remoteSockets) {
      try { socket.emit("jobping:envelope", env); } catch (_) {}
    }
  }

  _unicastRemoteMessage(targetPeerId, msg) {
    const socket = this._remoteSockets.get(targetPeerId);
    if (socket) {
      try { socket.emit("jobping:message", msg); } catch (_) {}
    } else {
      this._deliverLocalMessage(msg);
    }
  }

  _unicastRemoteEnvelope(targetPeerId, env) {
    const socket = this._remoteSockets.get(targetPeerId);
    if (socket) {
      try { socket.emit("jobping:envelope", env); } catch (_) {}
    } else {
      this._deliverLocalEnvelope(env);
    }
  }
}
