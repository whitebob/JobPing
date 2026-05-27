// In-process TransportLayer — connects directly to the local EmbeddedBroker.

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
}

export class LocalTransportLayer extends TransportLayer {
  constructor(broker) {
    super();
    this._broker = broker;
    this._messageMailbox = new Mailbox();
    this._envelopeMailbox = new Mailbox();

    broker._onLocalMessage = (msg) => this._messageMailbox.put(msg);
    broker._onLocalEnvelope = (env) => this._envelopeMailbox.put(env);
  }

  attachJobId(carrier = {}, jobId) {
    if (typeof jobId !== "string" || jobId.length === 0) {
      throw new Error("job_id must be a non-empty string");
    }
    return {
      ...carrier,
      headers: { ...(carrier.headers ?? {}), [JOBPING_JOB_ID_HEADER]: jobId },
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
    this._broker.localSendEnvelope(envelope);
  }

  recvEnvelope({ jobId, type, timeout, timeoutMs = 1000 } = {}) {
    const matches = (env) =>
      (jobId == null || env.job_id === jobId) &&
      (type == null || env.type === type);
    return this._envelopeMailbox.get(matches, timeout ?? timeoutMs);
  }

  sendMessage(message) {
    this._broker.localSendMessage(message);
  }

  recvMessage({ kind, jobId, timeout, timeoutMs = 1000 } = {}) {
    const matches = (msg) =>
      typeof msg === "object" && msg !== null &&
      (kind == null || msg.kind === kind) &&
      (jobId == null || msg.job_id === jobId);
    return this._messageMailbox.get(matches, timeout ?? timeoutMs);
  }
}
