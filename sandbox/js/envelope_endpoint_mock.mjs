import { isEnvelope } from "../../packages/js/envelope.mjs";

export class MockEnvelopeEndpoint {
  constructor() {
    this.pending = [];
    this.waiters = [];
  }

  send(envelope) {
    if (!isEnvelope(envelope)) {
      throw new Error("Can only send JobPing envelopes");
    }

    const waiterIndex = this.waiters.findIndex((waiter) => waiter.matches(envelope));
    if (waiterIndex !== -1) {
      const [waiter] = this.waiters.splice(waiterIndex, 1);
      waiter.resolve(envelope);
      return;
    }

    this.pending.push(envelope);
  }

  recv({ jobId, type, timeoutMs = 1000 } = {}) {
    const pendingIndex = this.pending.findIndex((envelope) =>
      this.matchesEnvelope(envelope, jobId, type),
    );

    if (pendingIndex !== -1) {
      const [envelope] = this.pending.splice(pendingIndex, 1);
      return Promise.resolve(envelope);
    }

    return new Promise((resolve, reject) => {
      const waiter = {
        matches: (envelope) => this.matchesEnvelope(envelope, jobId, type),
        resolve,
      };
      const timer = setTimeout(() => {
        const waiterIndex = this.waiters.indexOf(waiter);
        if (waiterIndex !== -1) {
          this.waiters.splice(waiterIndex, 1);
        }
        reject(new Error("Timed out waiting for JobPing envelope"));
      }, timeoutMs);

      waiter.resolve = (envelope) => {
        clearTimeout(timer);
        resolve(envelope);
      };
      this.waiters.push(waiter);
    });
  }

  size() {
    return {
      pending: this.pending.length,
      waiters: this.waiters.length,
    };
  }

  matchesEnvelope(envelope, jobId, type) {
    return (
      isEnvelope(envelope) &&
      (jobId === undefined || envelope.job_id === jobId) &&
      (type === undefined || envelope.type === type)
    );
  }
}
