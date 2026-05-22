import assert from "node:assert/strict";
import { MockEnvelopeEndpoint } from "../../sandbox/js/envelope_endpoint_mock.mjs";
import {
  JPITEM_COMPLETED,
  JPITEM_CREATED,
  JPITEM_QUEUED,
  JPITEM_WAITING,
  MockJPItemQueue,
} from "../../sandbox/js/jpitem_queue_mock.mjs";

const endpoint = new MockEnvelopeEndpoint();
const clientQueue = new MockJPItemQueue(endpoint);
const serverQueue = new MockJPItemQueue(endpoint);

const acceptedItem = clientQueue.accept("job-1");
assert.equal(acceptedItem.status, JPITEM_WAITING);

const offeredItem = serverQueue.offer("job-1");
assert.equal(offeredItem.status, JPITEM_CREATED);
assert.equal(serverQueue.defer(offeredItem).status, JPITEM_QUEUED);

const waited = clientQueue.awaitResult("job-1");
assert.deepEqual(clientQueue.snapshot(), {
  items: 1,
  statuses: { [JPITEM_WAITING]: 1 },
  envelopes: { pending: 0, waiters: 1 },
});

const payload = { status: "OK", value: 42 };
serverQueue.fulfill("job-1", payload);

const completedItem = await waited;
assert.equal(completedItem.status, JPITEM_COMPLETED);
assert.equal(completedItem.result, payload);
assert.equal(serverQueue.get("job-1").status, JPITEM_COMPLETED);
assert.equal(serverQueue.get("job-1").result, payload);
assert.deepEqual(endpoint.size(), { pending: 0, waiters: 0 });

assert.throws(() => clientQueue.accept("job-1"), /JPItem already exists/);
assert.throws(() => clientQueue.fulfill("job-1", payload), /Only offered JPItems can be fulfilled/);
await assert.rejects(
  serverQueue.awaitResult("job-1"),
  /Only accepted JPItems can await results/,
);

clientQueue.release("job-1");
serverQueue.release("job-1");
assert.deepEqual(clientQueue.snapshot(), {
  items: 0,
  statuses: {},
  envelopes: { pending: 0, waiters: 0 },
});
assert.deepEqual(serverQueue.snapshot(), {
  items: 0,
  statuses: {},
  envelopes: { pending: 0, waiters: 0 },
});

console.log("PASS JPItem queue mock lifecycle and envelope handoff");
