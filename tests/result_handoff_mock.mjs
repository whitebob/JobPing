import assert from "node:assert/strict";
import { createJobId } from "../experiment_group/jobping_id.mjs";
import { ResultHandoff } from "../experiment_group/jobping_result_handoff.mjs";
import { MockTransportAdapter } from "../experiment_group/jobping_transport_mock.mjs";

const transport = new MockTransportAdapter();
const resultHandoff = new ResultHandoff({ transportLayer: transport });
const jobId = createJobId();

const waiting = resultHandoff.awaitResult(jobId);
assert.deepEqual(transport.size(), { messages: 0, waiters: 1 });

const result = { status: "OK", rows: [1, 2, 3] };
resultHandoff.fulfill(jobId, result);

assert.equal(await waiting, result);
assert.deepEqual(transport.size(), { messages: 0, waiters: 0 });

assert.throws(() => resultHandoff.fulfill("", result), /job_id must be a non-empty string/);

console.log("PASS ResultHandoff over mock TransportLayer");
