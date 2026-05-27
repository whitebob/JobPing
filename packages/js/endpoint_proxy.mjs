import { createJobId as defaultCreateJobId } from "./id.mjs";

const JPITEM_COMPLETED = "completed";

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

function limitTraceDepth(subJobs, maxDepth) {
  if (maxDepth <= 0) return [{ _truncated: true }];
  return subJobs.map((sj) => ({
    ...sj,
    sub_jobs: limitTraceDepth(sj.sub_jobs || [], maxDepth - 1),
  }));
}

export class EndpointProxy {
  constructor({
    stateSync,
    resultHandoff,
    queue,
    createJobId = defaultCreateJobId,
    maxTraceDepth = 10,
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
    this.maxTraceDepth = maxTraceDepth;
    this._activeTrace = null;   // set by JobPing wrap/wrap_trace
    this._subTraces = [];       // accumulated from nested awaitResult calls
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

    let trace = null;
    if (this._activeTrace) {
      trace = { ...this._activeTrace };
      trace.sub_jobs = limitTraceDepth(this._subTraces, this.maxTraceDepth);
      this._subTraces = [];
      this._activeTrace = null;
    }
    this.resultHandoff.fulfill(jobId, result, { trace });
    return item;
  }

  async fulfillLater(jobId, task) {
    assertValidJobId(jobId);
    if (typeof task !== "function") {
      throw new Error("fulfillLater requires a task function");
    }

    try {
      const result = await task();
      this.fulfill(jobId, result);
      return result;
    } catch (e) {
      this._activeTrace = null;
      this._subTraces = [];
      throw e;
    }
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
