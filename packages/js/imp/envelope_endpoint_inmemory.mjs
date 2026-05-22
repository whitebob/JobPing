import { isEnvelope } from "../envelope.mjs";

class _Waiter {
  constructor(jobId, type, resolve) {
    this.jobId = jobId;
    this.type = type;
    this.resolve = resolve;
  }
}

export class EnvelopeEndpointInMemory {
  constructor() {
    this.pending = [];
    this.waiters = [];
  }

  send(envelope) {
    if (!isEnvelope(envelope)) throw new Error("Can only send JobPing envelopes");

    const idx = this.waiters.findIndex((w) => this._matches(envelope, w.jobId, w.type));
    if (idx !== -1) {
      const [waiter] = this.waiters.splice(idx, 1);
      waiter.resolve(envelope);
      return;
    }

    this.pending.push(envelope);
  }

  recv({ jobId, type, timeoutMs = 1000 } = {}) {
    const idx = this.pending.findIndex((e) => this._matches(e, jobId, type));
    if (idx !== -1) {
      const [envelope] = this.pending.splice(idx, 1);
      return Promise.resolve(envelope);
    }

    return new Promise((resolve, reject) => {
      let timer;
      const waiter = new _Waiter(jobId, type, (envelope) => {
        clearTimeout(timer);
        resolve(envelope);
      });

      timer = setTimeout(() => {
        const waiterIndex = this.waiters.indexOf(waiter);
        if (waiterIndex !== -1) this.waiters.splice(waiterIndex, 1);
        reject(new Error("Timed out waiting for JobPing envelope"));
      }, timeoutMs);

      this.waiters.push(waiter);
    });
  }

  size() {
    return { pending: this.pending.length, waiters: this.waiters.length };
  }

  _matches(envelope, jobId, type) {
    return (
      isEnvelope(envelope) &&
      (jobId === undefined || envelope.job_id === jobId) &&
      (type === undefined || envelope.type === type)
    );
  }
}
