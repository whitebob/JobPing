import { createJobId as defaultCreateJobId } from "./jobping_id.mjs";
import { JPITEM_COMPLETED } from "./jobping_jpitem_queue_mock.mjs";

const JOBPING_JOB_REF_KIND = "jobping.job_ref.v1";

function assertValidJobId(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
}

function requireMethod(value, method, owner) {
  if (!value || typeof value[method] !== "function") {
    throw new Error(`${owner} requires ${method}()`);
  }
}

export class EndpointProxy {
  constructor({
    stateSync,
    resultHandoff,
    queue,
    createJobId = defaultCreateJobId,
  }) {
    requireMethod(stateSync, "publish", "EndpointProxy stateSync");
    requireMethod(stateSync, "waitFor", "EndpointProxy stateSync");
    requireMethod(resultHandoff, "fulfill", "EndpointProxy resultHandoff");
    requireMethod(resultHandoff, "awaitResult", "EndpointProxy resultHandoff");
    requireMethod(queue, "offer", "EndpointProxy queue");
    requireMethod(queue, "accept", "EndpointProxy queue");
    requireMethod(queue, "defer", "EndpointProxy queue");
    requireMethod(queue, "release", "EndpointProxy queue");

    this.stateSync = stateSync;
    this.resultHandoff = resultHandoff;
    this.queue = queue;
    this.createJobIdFn = createJobId;
  }

  createJobId() {
    return this.createJobIdFn();
  }

  makeJobRef(jobId) {
    assertValidJobId(jobId);
    return {
      jobping: JOBPING_JOB_REF_KIND,
      type: "job_ref",
      job_id: jobId,
    };
  }

  isJobRef(value) {
    return (
      typeof value === "object" &&
      value !== null &&
      value.jobping === JOBPING_JOB_REF_KIND &&
      value.type === "job_ref" &&
      typeof value.job_id === "string" &&
      value.job_id.length > 0
    );
  }

  offer(jobId = this.createJobId()) {
    return this.queue.offer(jobId);
  }

  accept(jobId) {
    assertValidJobId(jobId);
    return this.queue.accept(jobId);
  }

  defer(itemOrJobId) {
    return this.queue.defer(itemOrJobId);
  }

  publishState(jobId, status, stateContext = {}) {
    return this.stateSync.publish(jobId, status, stateContext);
  }

  waitForState(jobId, options = {}) {
    return this.stateSync.waitFor(jobId, options);
  }

  fulfill(jobId, result) {
    assertValidJobId(jobId);
    const item = this.queue.get(jobId);
    if (!item || item.role !== "producer") {
      throw new Error("Only offered JPItems can be fulfilled");
    }

    item.status = JPITEM_COMPLETED;
    item.result = result;
    this.resultHandoff.fulfill(jobId, result);
    return item;
  }

  async fulfillLater(jobId, task) {
    assertValidJobId(jobId);
    if (typeof task !== "function") {
      throw new Error("fulfillLater requires a task function");
    }

    const result = await task();
    this.fulfill(jobId, result);
    return result;
  }

  async awaitResult(jobId, options = {}) {
    assertValidJobId(jobId);
    const item = this.queue.get(jobId);
    if (!item || item.role !== "consumer") {
      throw new Error("Only accepted JPItems can await results");
    }

    const result = await this.resultHandoff.awaitResult(jobId, options);
    item.status = JPITEM_COMPLETED;
    item.result = result;
    return item;
  }

  release(jobId) {
    return this.queue.release(jobId);
  }
}
