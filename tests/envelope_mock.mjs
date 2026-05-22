import assert from "node:assert/strict";
import {
  JOBPING_JOB_REF,
  JOBPING_RESULT,
  MockEnvelopeEndpoint,
  boxJobRef,
  boxResult,
  isEnvelope,
  isJobRefEnvelope,
  isResultEnvelope,
  unboxResult,
} from "../experiment_group/jobping_envelope_mock.mjs";

const jobRef = boxJobRef("job-1");
assert.deepEqual(jobRef, {
  jobping: "jobping.envelope.v1",
  type: JOBPING_JOB_REF,
  job_id: "job-1",
});
assert.equal(isEnvelope(jobRef), true);
assert.equal(isJobRefEnvelope(jobRef), true);
assert.equal(isResultEnvelope(jobRef), false);

const payload = { status: "OK", value: 42 };
const result = boxResult("job-1", payload);
assert.equal(isEnvelope(result), true);
assert.equal(isResultEnvelope(result), true);
assert.equal(unboxResult(result, "job-1"), payload);
assert.throws(() => unboxResult(result, "job-2"), /Unexpected JobPing result job_id/);
assert.throws(() => unboxResult(jobRef), /Expected JobPing result envelope/);
assert.throws(() => boxJobRef(""), /job_id must be a non-empty string/);

const endpoint = new MockEnvelopeEndpoint();
endpoint.send(result);
assert.deepEqual(endpoint.size(), { pending: 1, waiters: 0 });
assert.deepEqual(await endpoint.recv({ jobId: "job-1", type: JOBPING_RESULT }), result);
assert.deepEqual(endpoint.size(), { pending: 0, waiters: 0 });

const waited = endpoint.recv({ jobId: "job-2", type: JOBPING_RESULT });
assert.deepEqual(endpoint.size(), { pending: 0, waiters: 1 });
const waitedResult = boxResult("job-2", { status: "DONE" });
endpoint.send(waitedResult);
assert.deepEqual(await waited, waitedResult);
assert.deepEqual(endpoint.size(), { pending: 0, waiters: 0 });

await assert.rejects(
  endpoint.recv({ jobId: "missing", timeoutMs: 1 }),
  /Timed out waiting for JobPing envelope/,
);
assert.deepEqual(endpoint.size(), { pending: 0, waiters: 0 });

console.log("PASS envelope mock boxing, unboxing, send, and recv");
