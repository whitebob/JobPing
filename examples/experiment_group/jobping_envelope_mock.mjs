// Transport-neutral JobPing envelope mock.

export const JOBPING_ENVELOPE_KIND = "jobping.envelope.v1";
export const JOBPING_JOB_REF = "job_ref";
export const JOBPING_RESULT = "result";

function assertValidJobId(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
}

export function boxJobRef(jobId) {
  assertValidJobId(jobId);

  return {
    jobping: JOBPING_ENVELOPE_KIND,
    type: JOBPING_JOB_REF,
    job_id: jobId,
  };
}

export function boxResult(jobId, payload) {
  assertValidJobId(jobId);

  return {
    jobping: JOBPING_ENVELOPE_KIND,
    type: JOBPING_RESULT,
    job_id: jobId,
    payload,
  };
}

export function isEnvelope(value) {
  return (
    typeof value === "object" &&
    value !== null &&
    value.jobping === JOBPING_ENVELOPE_KIND &&
    typeof value.type === "string" &&
    typeof value.job_id === "string" &&
    value.job_id.length > 0
  );
}

export function isJobRefEnvelope(value) {
  return isEnvelope(value) && value.type === JOBPING_JOB_REF;
}

export function isResultEnvelope(value) {
  return isEnvelope(value) && value.type === JOBPING_RESULT && Object.hasOwn(value, "payload");
}

export function unboxResult(envelope, expectedJobId) {
  if (!isResultEnvelope(envelope)) {
    throw new Error("Expected JobPing result envelope");
  }

  if (expectedJobId !== undefined && envelope.job_id !== expectedJobId) {
    throw new Error("Unexpected JobPing result job_id");
  }

  return envelope.payload;
}

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
