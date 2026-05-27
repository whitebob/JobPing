// WebSocket TransportLayer using socket.io-client (implementation under imp)
// Implements a thin TransportLayer over Socket.IO. This is intentionally
// small and relies on the host application to run the Socket.IO server.

import { TransportLayer, JOBPING_JOB_ID_HEADER } from "../transport_layer.mjs";

let io;
try {
  // lazy import so environments without socket.io-client won't fail until used
  // eslint-disable-next-line no-undef
  io = await import("socket.io-client");
} catch (err) {
  // leave io undefined; constructor will throw if used without dependency
}

class Mailbox {
  constructor() {
    this._messages = [];
    this._waiters = [];
  }

  put(data) {
    // Try to match a waiting consumer
    const waiterIndex = this._waiters.findIndex((w) => w.matches(data));
    if (waiterIndex !== -1) {
      const [waiter] = this._waiters.splice(waiterIndex, 1);
      waiter.resolve(data);
      return;
    }
    // No matching waiter — store
    this._messages.push(data);
  }

  get(matches, timeout) {
    // Check stored messages first
    const msgIndex = this._messages.findIndex((m) => matches(m));
    if (msgIndex !== -1) {
      return Promise.resolve(this._messages.splice(msgIndex, 1)[0]);
    }

    // Register waiter
    return new Promise((resolve, reject) => {
      const waiter = { matches, resolve };
      const timer = setTimeout(() => {
        const i = this._waiters.indexOf(waiter);
        if (i !== -1) this._waiters.splice(i, 1);
        reject(new Error("Timed out waiting for message"));
      }, timeout);

      waiter.resolve = (data) => {
        clearTimeout(timer);
        resolve(data);
      };

      this._waiters.push(waiter);
    });
  }

  size() {
    return { messages: this._messages.length, waiters: this._waiters.length };
  }
}

export class TransportLayerWS extends TransportLayer {
  constructor({ url, opts, idleTimeoutSeconds } = {}) {
    super();
    if (!io) {
      throw new Error("socket.io-client must be installed to use TransportLayerWS");
    }

    this.url = url;
    this.socket = io.io(url, opts);
    this._messageMailbox = new Mailbox();
    this._envelopeMailbox = new Mailbox();
    this._idleTimeout = idleTimeoutSeconds || null;
    this._lastActivity = Date.now();
    this._idleTimer = null;

    this.socket.on("jobping:envelope", (envelope) => {
      this._envelopeMailbox.put(envelope);
    });

    this.socket.on("jobping:message", (message) => {
      this._messageMailbox.put(message);
    });
  }

  _touch() {
    this._lastActivity = Date.now();
  }

  _startIdleWatcher() {
    if (!this._idleTimeout || this._idleTimer) return;
    this._idleTimer = setInterval(() => {
      if (Date.now() - this._lastActivity > this._idleTimeout * 1000) {
        this.disconnect();
      }
    }, (this._idleTimeout * 1000) / 2);
  }

  disconnect() {
    if (this._idleTimer) {
      clearInterval(this._idleTimer);
      this._idleTimer = null;
    }
    if (this.socket) {
      this.socket.disconnect();
    }
  }

  attachJobId(carrier = {}, jobId) {
    if (typeof jobId !== "string" || jobId.length === 0) {
      throw new Error("job_id must be a non-empty string");
    }

    return {
      ...carrier,
      headers: {
        ...(carrier.headers ?? {}),
        [JOBPING_JOB_ID_HEADER]: jobId,
      },
    };
  }

  extractJobId(carrier = {}) {
    const headers = carrier.headers ?? {};
    for (const [k, v] of Object.entries(headers)) {
      if (k.toLowerCase() === JOBPING_JOB_ID_HEADER.toLowerCase()) return v;
    }
    return undefined;
  }

  attachEnvelope(carrier = {}, envelope) {
    return { ...carrier, envelope };
  }

  extractEnvelope(carrier = {}) {
    const e = carrier.envelope;
    return e && typeof e === "object" ? e : undefined;
  }

  sendEnvelope(envelope) {
    if (!this.socket) throw new Error("No socket configured");
    this._touch();
    this.socket.emit("jobping:envelope", envelope);
  }

  recvEnvelope({ jobId, type, timeout, timeoutMs = 1000 } = {}) {
    const matches = (envelope) =>
      (jobId == null || envelope.job_id === jobId) &&
      (type == null || envelope.type === type);

    return this._envelopeMailbox.get(matches, timeout ?? timeoutMs);
  }

  sendMessage(message) {
    if (!this.socket) throw new Error("No socket configured");
    this._touch();
    this.socket.emit("jobping:message", message);
  }

  recvMessage({ kind, jobId, timeout, timeoutMs = 1000 } = {}) {
    const matches = (message) =>
      typeof message === "object" &&
      message !== null &&
      (kind == null || message.kind === kind) &&
      (jobId == null || message.job_id === jobId);

    return this._messageMailbox.get(matches, timeout ?? timeoutMs);
  }

  size() {
    return {
      messages: this._messageMailbox.size(),
      envelopes: this._envelopeMailbox.size(),
    };
  }
}
