// Transport-adapter mock for JobPing metadata carriers.
//
// This layer is allowed to know about carrier details such as header-like
// metadata. It does not manage JPItem lifecycle and does not inspect business
// payloads. A real websocket or HTTP adapter can replace this shape later.

import { isEnvelope } from "./jobping_envelope_mock.mjs";

export const JOBPING_JOB_ID_HEADER = "x-jobping-job-id";

function assertValidJobId(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
}

function findHeader(headers, name) {
  const target = name.toLowerCase();
  for (const [key, value] of Object.entries(headers ?? {})) {
    if (key.toLowerCase() === target) {
      return value;
    }
  }
  return undefined;
}

export class MockTransportAdapter {
  constructor({ envelopeEndpoint } = {}) {
    this.envelopeEndpoint = envelopeEndpoint;
    this.messages = [];
    this.waiters = [];
  }

  attachJobId(carrier = {}, jobId) {
    assertValidJobId(jobId);

    return {
      ...carrier,
      headers: {
        ...(carrier.headers ?? {}),
        [JOBPING_JOB_ID_HEADER]: jobId,
      },
    };
  }

  extractJobId(carrier = {}) {
    const value = findHeader(carrier.headers, JOBPING_JOB_ID_HEADER);
    return typeof value === "string" && value.length > 0 ? value : undefined;
  }

  attachEnvelope(carrier = {}, envelope) {
    if (!isEnvelope(envelope)) {
      throw new Error("Can only attach JobPing envelopes");
    }

    return {
      ...carrier,
      envelope,
    };
  }

  extractEnvelope(carrier = {}) {
    return isEnvelope(carrier.envelope) ? carrier.envelope : undefined;
  }

  sendEnvelope(envelope) {
    if (!this.envelopeEndpoint) {
      throw new Error("No envelope endpoint configured");
    }

    this.envelopeEndpoint.send(envelope);
  }

  recvEnvelope(options) {
    if (!this.envelopeEndpoint) {
      throw new Error("No envelope endpoint configured");
    }

    return this.envelopeEndpoint.recv(options);
  }

  sendMessage(message) {
    const waiterIndex = this.waiters.findIndex((waiter) => waiter.matches(message));
    if (waiterIndex !== -1) {
      const [waiter] = this.waiters.splice(waiterIndex, 1);
      waiter.resolve(message);
      return;
    }

    this.messages.push(message);
  }

  recvMessage({ kind, jobId, timeoutMs = 1000 } = {}) {
    const messageIndex = this.messages.findIndex((message) =>
      this.matchesMessage(message, kind, jobId),
    );

    if (messageIndex !== -1) {
      const [message] = this.messages.splice(messageIndex, 1);
      return Promise.resolve(message);
    }

    return new Promise((resolve, reject) => {
      const waiter = {
        matches: (message) => this.matchesMessage(message, kind, jobId),
        resolve,
      };
      const timer = setTimeout(() => {
        const waiterIndex = this.waiters.indexOf(waiter);
        if (waiterIndex !== -1) {
          this.waiters.splice(waiterIndex, 1);
        }
        reject(new Error("Timed out waiting for transport message"));
      }, timeoutMs);

      waiter.resolve = (message) => {
        clearTimeout(timer);
        resolve(message);
      };
      this.waiters.push(waiter);
    });
  }

  size() {
    return {
      messages: this.messages.length,
      waiters: this.waiters.length,
    };
  }

  matchesMessage(message, kind, jobId) {
    return (
      typeof message === "object" &&
      message !== null &&
      (kind === undefined || message.kind === kind) &&
      (jobId === undefined || message.job_id === jobId)
    );
  }
}
