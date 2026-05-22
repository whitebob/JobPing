import { JOBPING_RESULT, boxResult, unboxResult } from "./envelope.mjs";

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

// Concrete implementations moved to ./imp — import and re-export the in-memory
// implementation for backward compatibility.
import { JPItemQueueInMemory as _Impl } from "./imp/jpitem_queue_inmemory.mjs";

export class JPItemQueue {
  constructor() {
    if (new.target === JPItemQueue) {
      throw new Error("JPItemQueue is abstract; use a concrete implementation");
    }
  }

  offer() {
    throw new Error("JPItemQueue.offer() must be implemented");
  }

  accept() {
    throw new Error("JPItemQueue.accept() must be implemented");
  }

  defer() {
    throw new Error("JPItemQueue.defer() must be implemented");
  }

  fulfill() {
    throw new Error("JPItemQueue.fulfill() must be implemented");
  }

  async awaitResult() {
    throw new Error("JPItemQueue.awaitResult() must be implemented");
  }

  release() {
    throw new Error("JPItemQueue.release() must be implemented");
  }

  get() {
    throw new Error("JPItemQueue.get() must be implemented");
  }

  snapshot() {
    throw new Error("JPItemQueue.snapshot() must be implemented");
  }
}

export const JPItemQueueInMemory = _Impl;
