// Endpoint-local JPItem queue mock.
//
// Minimum mock surface:
// - offer(jobId): this endpoint promises to fulfill a JPItem later.
// - accept(jobId): this endpoint accepts a peer's job_ref and prepares to wait.
// - defer(item): marks an offered JPItem as deferred work.
// - fulfill(jobId, result): boxes output through the envelope mock and sends it.
// - awaitResult(jobId): receives a result envelope and unboxes it into the accepted item.
// - release(jobId): explicit cleanup for leak-sensitive tests.
//
// Future scheduling APIs such as fulfillLater(jobId, task) are intentionally left
// as endpoint-proxy pseudocode until execution and exception semantics are clearer.

import {
  JOBPING_RESULT,
  boxResult,
  unboxResult,
} from "../../packages/js/envelope.mjs";

export const JPITEM_CREATED = "created";
export const JPITEM_WAITING = "waiting";
export const JPITEM_QUEUED = "queued";
export const JPITEM_COMPLETED = "completed";
export const JPITEM_DESTROYED = "destroyed";

function assertValidJobId(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
}

function createItem(jobId, role, status) {
  return {
    job_id: jobId,
    role,
    status,
    result: undefined,
  };
}

export class MockJPItemQueue {
  constructor(envelopeEndpoint) {
    if (!envelopeEndpoint || typeof envelopeEndpoint.send !== "function") {
      throw new Error("MockJPItemQueue requires an envelope endpoint");
    }

    this.envelopeEndpoint = envelopeEndpoint;
    this.items = new Map();
  }

  accept(jobId) {
    assertValidJobId(jobId);
    this.assertMissing(jobId);

    const item = createItem(jobId, "consumer", JPITEM_WAITING);
    this.items.set(jobId, item);
    return item;
  }

  offer(jobId) {
    assertValidJobId(jobId);
    this.assertMissing(jobId);

    const item = createItem(jobId, "producer", JPITEM_CREATED);
    this.items.set(jobId, item);
    return item;
  }

  defer(itemOrJobId) {
    const item = this.resolveItem(itemOrJobId);
    if (item.role !== "producer") {
      throw new Error("Only offered JPItems can be deferred");
    }

    item.status = JPITEM_QUEUED;
    return item;
  }

  fulfill(jobId, result) {
    const item = this.resolveItem(jobId);
    if (item.role !== "producer") {
      throw new Error("Only offered JPItems can be fulfilled");
    }

    item.status = JPITEM_COMPLETED;
    item.result = result;
    this.envelopeEndpoint.send(boxResult(jobId, result));
    return item;
  }

  async awaitResult(jobId, { timeoutMs = 1000 } = {}) {
    const item = this.resolveItem(jobId);
    if (item.role !== "consumer") {
      throw new Error("Only accepted JPItems can await results");
    }

    item.status = JPITEM_WAITING;
    const envelope = await this.envelopeEndpoint.recv({
      jobId,
      type: JOBPING_RESULT,
      timeoutMs,
    });
    const result = unboxResult(envelope, jobId);

    item.status = JPITEM_COMPLETED;
    item.result = result;
    return item;
  }

  release(jobId) {
    const item = this.resolveItem(jobId);
    item.status = JPITEM_DESTROYED;
    this.items.delete(jobId);
    return item;
  }

  get(jobId) {
    assertValidJobId(jobId);
    return this.items.get(jobId);
  }

  snapshot() {
    const statuses = {};
    for (const item of this.items.values()) {
      statuses[item.status] = (statuses[item.status] ?? 0) + 1;
    }

    return {
      items: this.items.size,
      statuses,
      envelopes: this.envelopeEndpoint.size(),
    };
  }

  assertMissing(jobId) {
    if (this.items.has(jobId)) {
      throw new Error(`JPItem already exists: ${jobId}`);
    }
  }

  resolveItem(itemOrJobId) {
    const jobId = typeof itemOrJobId === "string" ? itemOrJobId : itemOrJobId?.job_id;
    assertValidJobId(jobId);

    const item = this.items.get(jobId);
    if (!item) {
      throw new Error(`Unknown JPItem: ${jobId}`);
    }

    return item;
  }
}
