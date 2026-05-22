// WebSocket TransportLayer using socket.io-client
// Implements a thin TransportLayer over Socket.IO. This is intentionally
// small and relies on the host application to run the Socket.IO server.

import { TransportLayer, JOBPING_JOB_ID_HEADER } from "./transport_layer.mjs";

let io;
try {
  // lazy import so environments without socket.io-client won't fail until used
  // eslint-disable-next-line no-undef
  io = await import("socket.io-client");
} catch (err) {
  // leave io undefined; constructor will throw if used without dependency
}

export class TransportLayerWS extends TransportLayer {
  constructor({ url, opts } = {}) {
    super();
    if (!io) {
      throw new Error("socket.io-client must be installed to use TransportLayerWS");
    }

    this.url = url;
    this.socket = io.io(url, opts);
    this._messages = [];
    this._waiters = [];

    this.socket.on("jobping:envelope", (envelope) => {
      // no-op here; envelopes typically go to an envelope endpoint
      // but allow clients to listen via recvEnvelope implementation below
      if (!this._envelopePool) this._envelopePool = [];
      this._envelopePool.push(envelope);
      this._drainWaiters();
    });

    this.socket.on("jobping:message", (message) => {
      const waiterIndex = this._waiters.findIndex((w) => w.matches(message));
      if (waiterIndex !== -1) {
        const [waiter] = this._waiters.splice(waiterIndex, 1);
        waiter.resolve(message);
        return;
      }
      this._messages.push(message);
    });
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
    this.socket.emit("jobping:envelope", envelope);
  }

  recvEnvelope({ jobId, type, timeout = 1000 } = {}) {
    // drain local pool first
    this._envelopePool = this._envelopePool ?? [];
    for (let i = 0; i < this._envelopePool.length; i++) {
      const e = this._envelopePool[i];
      if ((jobId == null || e.job_id === jobId) && (type == null || e.type === type)) {
        return Promise.resolve(this._envelopePool.splice(i, 1)[0]);
      }
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Timed out waiting for envelope")), timeout);
      const handler = (envelope) => {
        if ((jobId == null || envelope.job_id === jobId) && (type == null || envelope.type === type)) {
          clearTimeout(timer);
          this.socket.off("jobping:envelope", handler);
          resolve(envelope);
        }
      };
      this.socket.on("jobping:envelope", handler);
    });
  }

  sendMessage(message) {
    if (!this.socket) throw new Error("No socket configured");
    this.socket.emit("jobping:message", message);
  }

  recvMessage({ kind, jobId, timeout = 1000 } = {}) {
    const idx = this._messages.findIndex((m) => this.matchesMessage(m, kind, jobId));
    if (idx !== -1) return Promise.resolve(this._messages.splice(idx, 1)[0]);

    return new Promise((resolve, reject) => {
      const waiter = {
        matches: (message) => this.matchesMessage(message, kind, jobId),
        resolve,
      };
      const timer = setTimeout(() => {
        const i = this._waiters.indexOf(waiter);
        if (i !== -1) this._waiters.splice(i, 1);
        reject(new Error("Timed out waiting for transport message"));
      }, timeout);

      waiter.resolve = (message) => {
        clearTimeout(timer);
        resolve(message);
      };

      this._waiters.push(waiter);
    });
  }

  size() {
    return { messages: this._messages.length, waiters: this._waiters.length };
  }

  matchesMessage(message, kind, jobId) {
    return (
      typeof message === "object" &&
      message !== null &&
      (kind == null || message.kind === kind) &&
      (jobId == null || message.job_id === jobId)
    );
  }

  _drainWaiters() {
    for (let i = 0; i < this._messages.length; i++) {
      const message = this._messages[i];
      const waiterIndex = this._waiters.findIndex((w) => w.matches(message));
      if (waiterIndex !== -1) {
        const [waiter] = this._waiters.splice(waiterIndex, 1);
        this._messages.splice(i, 1);
        waiter.resolve(message);
        i--;
      }
    }
  }
}
