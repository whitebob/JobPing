import assert from "node:assert/strict";
import { createJobId } from "../experiment_group/jobping_id.mjs";
import { MockStateSync } from "../experiment_group/jobping_state_sync_mock.mjs";
import { MockTransportAdapter } from "../experiment_group/jobping_transport_mock.mjs";

const transport = new MockTransportAdapter();
const stateSync = new MockStateSync({ transportLayer: transport });
const jobId = createJobId();

const waiting = stateSync.waitFor(jobId, { status: "running" });
assert.deepEqual(transport.size(), { messages: 0, waiters: 1 });

stateSync.publish(jobId, "queued", { path: ["created", "queued"] });
stateSync.publish(jobId, "running", { path: ["created", "queued", "running"] });

assert.deepEqual(await waiting, {
  status: "running",
  state_context: {
    path: ["created", "queued", "running"],
  },
});
assert.deepEqual(transport.size(), { messages: 0, waiters: 0 });

assert.throws(() => stateSync.publish("", "running"), /job_id must be a non-empty string/);
assert.throws(() => stateSync.publish(jobId, ""), /status must be a non-empty string/);

console.log("PASS StateSync mock over TransportLayer");
