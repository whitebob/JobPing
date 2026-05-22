import assert from "node:assert/strict";
import { EndpointProxy } from "../../packages/js/endpoint_proxy.mjs";
import { EnvelopeEndpointInMemory as MockEnvelopeEndpoint } from "../../packages/js/envelope_endpoint.mjs";
import { JPITEM_COMPLETED, JPITEM_QUEUED, JPItemQueueInMemory as MockJPItemQueue } from "../../packages/js/jpitem_queue.mjs";
import { ResultHandoff } from "../../packages/js/result_handoff.mjs";
import { StateSync } from "../../packages/js/state_sync.mjs";
import { TransportLayerMock } from "../../sandbox/js/transport_layer_mock.mjs";

function makeProxy({ stateSync, resultHandoff }) {
  return new EndpointProxy({
    stateSync,
    resultHandoff,
    queue: new MockJPItemQueue(new MockEnvelopeEndpoint()),
  });
}

const stateTransport = new TransportLayerMock();
const resultTransport = new TransportLayerMock();
const stateSync = new StateSync({ transportLayer: stateTransport });
const resultHandoff = new ResultHandoff({ transportLayer: resultTransport });

const producer = makeProxy({ stateSync, resultHandoff });
const consumer = makeProxy({ stateSync, resultHandoff });

const jobId = producer.createJobId();

const offered = producer.offer(jobId);
assert.equal(offered.job_id, jobId);
assert.equal(producer.defer(jobId).status, JPITEM_QUEUED);

const stateWait = consumer.waitForState(jobId, { status: "running" });
producer.publishState(jobId, "running", {
  path: ["created", "queued", "running"],
});
assert.deepEqual(await stateWait, {
  status: "running",
  state_context: {
    path: ["created", "queued", "running"],
  },
});

const accepted = consumer.accept(jobId);
assert.equal(accepted.job_id, jobId);

const resultWait = consumer.awaitResult(jobId);
const result = { status: "OK", rows: [1, 2, 3] };
assert.equal(await producer.fulfillLater(jobId, async () => result), result);

const completed = await resultWait;
assert.equal(completed.status, JPITEM_COMPLETED);
assert.equal(completed.result, result);
assert.equal(producer.queue.get(jobId).status, JPITEM_COMPLETED);
assert.equal(producer.queue.get(jobId).result, result);

consumer.release(jobId);
producer.release(jobId);
assert.equal(consumer.queue.get(jobId), undefined);
assert.equal(producer.queue.get(jobId), undefined);

assert.throws(() => consumer.fulfill(jobId, result), /Only offered JPItems can be fulfilled/);

console.log("PASS EndpointProxy composes StateSync, ResultHandoff, and JPItemQueue");
