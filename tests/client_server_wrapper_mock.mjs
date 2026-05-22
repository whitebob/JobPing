import assert from "node:assert/strict";
import { EndpointProxy } from "../experiment_group/jobping_endpoint_proxy.mjs";
import { MockEnvelopeEndpoint } from "../experiment_group/jobping_envelope_mock.mjs";
import { MockJPItemQueue } from "../experiment_group/jobping_jpitem_queue_mock.mjs";
import { JobPingClientMock } from "../experiment_group/jobping_client_mock.mjs";
import { ResultHandoff } from "../experiment_group/jobping_result_handoff.mjs";
import { StateSync } from "../experiment_group/jobping_state_sync.mjs";
import { MockTransportAdapter } from "../experiment_group/jobping_transport_mock.mjs";

function makeProxy({ stateSync, resultHandoff }) {
  return new EndpointProxy({
    stateSync,
    resultHandoff,
    queue: new MockJPItemQueue(new MockEnvelopeEndpoint()),
  });
}

const stateSync = new StateSync({ transportLayer: new MockTransportAdapter() });
const resultHandoff = new ResultHandoff({
  transportLayer: new MockTransportAdapter(),
});
const producerProxy = makeProxy({ stateSync, resultHandoff });
const consumerProxy = makeProxy({ stateSync, resultHandoff });
const clientJobPing = new JobPingClientMock({ endpointProxy: consumerProxy });
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
