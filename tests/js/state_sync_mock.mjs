import assert from "node:assert/strict";
import { createJobId } from "../../packages/js/id.mjs";
import { StateSync } from "../../packages/js/state_sync.mjs";
import { TransportLayerMock } from "../../sandbox/js/transport_layer_mock.mjs";

const transport = new TransportLayerMock();
const stateSync = new StateSync({ transportLayer: transport });
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

console.log("PASS StateSync over mock TransportLayer");
