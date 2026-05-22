// Transport-neutral JobPing result envelope.

export const JOBPING_ENVELOPE_KIND = "jobping.envelope.v1";
export const JOBPING_RESULT = "result";

function assertValidJobId(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
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
