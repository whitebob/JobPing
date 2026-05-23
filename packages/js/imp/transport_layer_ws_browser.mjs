// Browser-specific TransportLayerWS — uses the CDN-loaded global `io` instead
// of dynamically importing socket.io-client (which requires a bundler or import
// map in the browser).
//
// The host page must load socket.io-client before this module, e.g.:
//   <script src="https://cdn.socket.io/4.8.1/socket.io.min.js"></script>

import { TransportLayer, JOBPING_JOB_ID_HEADER } from "../transport_layer.mjs";

class Mailbox {
  constructor() {
    this._messages = [];
    this._waiters = [];
  }

  put(data) {
    const waiterIndex = this._waiters.findIndex((w) => w.matches(data));
    if (waiterIndex !== -1) {
      const [waiter] = this._waiters.splice(waiterIndex, 1);
      waiter.resolve(data);
      return;
    }
    this._messages.push(data);
  }

  get(matches, timeout) {
    const msgIndex = this._messages.findIndex((m) => matches(m));
    if (msgIndex !== -1) {
      return Promise.resolve(this._messages.splice(msgIndex, 1)[0]);
    }

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
  constructor({ url, opts } = {}) {
    super();
    const io = globalThis.io;
    if (!io) {
      throw new Error(
        "socket.io-client must be loaded before TransportLayerWS (add <script src=\"...socket.io.min.js\"></script> to the page)"
      );
    }

    this.url = url;
    this.socket = io(url, opts);
    this._messageMailbox = new Mailbox();
    this._envelopeMailbox = new Mailbox();

    this.socket.on("jobping:envelope", (envelope) => {
      this._envelopeMailbox.put(envelope);
    });

    this.socket.on("jobping:message", (message) => {
      this._messageMailbox.put(message);
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

  recvEnvelope({ jobId, type, timeout, timeoutMs = 1000 } = {}) {
    const matches = (envelope) =>
      (jobId == null || envelope.job_id === jobId) &&
      (type == null || envelope.type === type);
    return this._envelopeMailbox.get(matches, timeout ?? timeoutMs);
  }

  sendMessage(message) {
    if (!this.socket) throw new Error("No socket configured");
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
