import assert from "node:assert/strict";
import { createJobId } from "../../packages/js/id.mjs";
import { ResultHandoff } from "../../packages/js/result_handoff.mjs";
import { TransportLayerMock } from "../../sandbox/js/transport_layer_mock.mjs";

const transport = new TransportLayerMock();
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
