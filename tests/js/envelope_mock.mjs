import assert from "node:assert/strict";
import {
  JOBPING_RESULT,
  boxResult,
  isEnvelope,
  isResultEnvelope,
  unboxResult,
} from "../../packages/js/envelope.mjs";
import { MockEnvelopeEndpoint } from "../../sandbox/js/envelope_endpoint_mock.mjs";

const payload = { status: "OK", value: 42 };
const result = boxResult("job-1", payload);
assert.equal(isEnvelope(result), true);
assert.equal(isResultEnvelope(result), true);
assert.equal(unboxResult(result, "job-1"), payload);
assert.throws(() => unboxResult(result, "job-2"), /Unexpected JobPing result job_id/);
assert.throws(
  () => unboxResult({ jobping: "jobping.envelope.v1", type: "job_ref", job_id: "job-1" }),
  /Expected JobPing result envelope/,
);
assert.throws(() => boxResult("", payload), /job_id must be a non-empty string/);

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
