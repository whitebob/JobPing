import assert from "node:assert/strict";
import { JOBPING_RESULT, boxResult } from "../../packages/js/envelope.mjs";
import { createJobId } from "../../packages/js/id.mjs";
import {
  JOBPING_JOB_ID_HEADER,
  TransportLayer,
} from "../../packages/js/transport_layer.mjs";
import { MockEnvelopeEndpoint } from "../../sandbox/js/envelope_endpoint_mock.mjs";
import { TransportLayerMock } from "../../sandbox/js/transport_layer_mock.mjs";

const jobId = createJobId();
assert.equal(typeof jobId, "string");
assert.match(jobId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
assert.notEqual(createJobId(), jobId);

assert.throws(() => new TransportLayer(), /TransportLayer is abstract/);

const adapter = new TransportLayerMock();
const originalCarrier = { headers: { "x-existing": "yes" } };
const carrier = adapter.attachJobId(originalCarrier, jobId);
assert.equal(originalCarrier.headers[JOBPING_JOB_ID_HEADER], undefined);
assert.equal(carrier.headers["x-existing"], "yes");
assert.equal(carrier.headers[JOBPING_JOB_ID_HEADER], jobId);
assert.equal(adapter.extractJobId({ headers: { "X-JobPing-Job-Id": jobId } }), jobId);
assert.equal(adapter.extractJobId({ headers: {} }), undefined);

const envelope = boxResult(jobId, { status: "OK" });
const envelopeCarrier = adapter.attachEnvelope(carrier, envelope);
assert.equal(adapter.extractEnvelope(envelopeCarrier), envelope);
assert.throws(() => adapter.attachEnvelope({}, { bad: "shape" }), /Can only attach JobPing envelopes/);

const endpoint = new MockEnvelopeEndpoint();
const transportWithEndpoint = new TransportLayerMock({ envelopeEndpoint: endpoint });
transportWithEndpoint.sendEnvelope(envelope);
assert.deepEqual(endpoint.size(), { pending: 1, waiters: 0 });
assert.deepEqual(
  await transportWithEndpoint.recvEnvelope({ jobId, type: JOBPING_RESULT }),
  envelope,
);
assert.deepEqual(endpoint.size(), { pending: 0, waiters: 0 });

assert.throws(() => adapter.sendEnvelope(envelope), /No envelope endpoint configured/);

console.log("PASS JobPing id generation and TransportLayerMock");
