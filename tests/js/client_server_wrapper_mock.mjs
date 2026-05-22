import assert from "node:assert/strict";
import { EndpointProxy } from "../../packages/js/endpoint_proxy.mjs";
import { MockEnvelopeEndpoint } from "../../sandbox/js/envelope_endpoint_mock.mjs";
import { MockJPItemQueue } from "../../sandbox/js/jpitem_queue_mock.mjs";
import { JobPing } from "../../packages/js/jobping.mjs";
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

const stateSync = new StateSync({ transportLayer: new TransportLayerMock() });
const resultHandoff = new ResultHandoff({
  transportLayer: new TransportLayerMock(),
});
const producerProxy = makeProxy({ stateSync, resultHandoff });
const consumerProxy = makeProxy({ stateSync, resultHandoff });
const clientJobPing = new JobPing({ endpointProxy: consumerProxy });
const jobId = producerProxy.createJobId();

producerProxy.offer(jobId);
producerProxy.defer(jobId);

const wrapped = clientJobPing.wrap(async () => producerProxy.makeJobRef(jobId));
const waiting = wrapped();
const result = { status: "OK", value: 42 };
producerProxy.fulfill(jobId, result);

assert.equal(await waiting, result);
assert.equal(consumerProxy.queue.get(jobId), undefined);

console.log("PASS client wrapper consumes job_ref through EndpointProxy");
