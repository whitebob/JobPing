import assert from "node:assert/strict";
import { JOBPING_RESULT, MockEnvelopeEndpoint, boxResult } from "../experiment_group/jobping_envelope_mock.mjs";
import { createJobId } from "../experiment_group/jobping_id.mjs";
import {
  JOBPING_JOB_ID_HEADER,
  MockTransportAdapter,
} from "../experiment_group/jobping_transport_mock.mjs";

const jobId = createJobId();
assert.equal(typeof jobId, "string");
assert.match(jobId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
assert.notEqual(createJobId(), jobId);

const adapter = new MockTransportAdapter();
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
const transportWithEndpoint = new MockTransportAdapter({ envelopeEndpoint: endpoint });
transportWithEndpoint.sendEnvelope(envelope);
assert.deepEqual(endpoint.size(), { pending: 1, waiters: 0 });
assert.deepEqual(
  await transportWithEndpoint.recvEnvelope({ jobId, type: JOBPING_RESULT }),
  envelope,
);
assert.deepEqual(endpoint.size(), { pending: 0, waiters: 0 });

assert.throws(() => adapter.sendEnvelope(envelope), /No envelope endpoint configured/);

console.log("PASS JobPing id generation and transport adapter mock");
