import { JOBPING_RESULT, boxResult, unboxResult } from "../envelope.mjs";

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

export class JPItemQueueInMemory {
  constructor(envelopeEndpoint) {
    if (!envelopeEndpoint || typeof envelopeEndpoint.send !== "function") {
      throw new Error("JPItemQueueInMemory requires an envelope endpoint");
    }
    this.envelopeEndpoint = envelopeEndpoint;
    this.items = new Map();
  }

  accept(jobId) {
    assertValidJobId(jobId);
    if (this.items.has(jobId)) throw new Error(`JPItem already exists: ${jobId}`);
    const item = createItem(jobId, "consumer", JPITEM_WAITING);
    this.items.set(jobId, item);
    return item;
  }

  offer(jobId) {
    assertValidJobId(jobId);
    if (this.items.has(jobId)) throw new Error(`JPItem already exists: ${jobId}`);
    const item = createItem(jobId, "producer", JPITEM_CREATED);
    this.items.set(jobId, item);
    return item;
  }

  defer(itemOrJobId) {
    const item = this._resolveItem(itemOrJobId);
    if (item.role !== "producer") throw new Error("Only offered JPItems can be deferred");
    item.status = JPITEM_QUEUED;
    return item;
  }

  fulfill(jobId, result) {
    const item = this._resolveItem(jobId);
    if (item.role !== "producer") throw new Error("Only offered JPItems can be fulfilled");
    item.status = JPITEM_COMPLETED;
    item.result = result;
    this.envelopeEndpoint.send(boxResult(jobId, result));
    return item;
  }

  async awaitResult(jobId, { timeoutMs = 1000 } = {}) {
    const item = this._resolveItem(jobId);
    if (item.role !== "consumer") throw new Error("Only accepted JPItems can await results");
    item.status = JPITEM_WAITING;
    const envelope = await this.envelopeEndpoint.recv({ jobId, type: JOBPING_RESULT, timeoutMs });
    const result = unboxResult(envelope, jobId);
    item.status = JPITEM_COMPLETED;
    item.result = result;
    return item;
  }

  release(jobId) {
    const item = this._resolveItem(jobId);
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
    for (const item of this.items.values()) statuses[item.status] = (statuses[item.status] ?? 0) + 1;
    return { items: this.items.size, statuses, envelopes: this.envelopeEndpoint.size() };
  }

  _resolveItem(itemOrJobId) {
    const jobId = typeof itemOrJobId === "string" ? itemOrJobId : itemOrJobId?.job_id;
    assertValidJobId(jobId);
    const item = this.items.get(jobId);
    if (!item) throw new Error(`Unknown JPItem: ${jobId}`);
    return item;
  }
}
