// Core JobPing class and helpers — no Node-specific dependencies.
// Safe to import from both Node and browser entry points.
//
// The factory function createJobPing lives in separate modules because
// Node (jobping.mjs) and browser (browser_entry.mjs) wire up different
// transport/broker stacks.

import { createPeerId } from "./id_browser.mjs";

let _traceEnabled = false;

export function isJobPingDisabled() {
  if (globalThis.__JOBPING_DISABLED__ === true) return true;
  const value = globalThis.process?.env?.JOBPING_DISABLED;
  return typeof value === "string" && /^(1|true|yes|on)$/i.test(value);
}

function checkTraceHeader(...args) {
  try {
    const candidate = args[0];
    if (!candidate || !candidate.headers) return false;
    const h = candidate.headers;
    for (const [k, v] of Object.entries(h)) {
      if (k.toLowerCase() === "x-jobping-trace-enabled") {
        return String(v).toLowerCase() === "1" || String(v).toLowerCase() === "true";
      }
    }
  } catch (_) {}
  return false;
}

function computeHop(...args) {
  try {
    const candidate = args[0];
    if (!candidate || !candidate.headers) return 1;
    const h = candidate.headers;
    for (const [k, v] of Object.entries(h)) {
      if (k.toLowerCase() === "x-jobping-trace-hop") {
        return (parseInt(String(v), 10) || 0) + 1;
      }
    }
  } catch (_) {}
  return 1;
}

export class JobPing {
  constructor({ endpointProxy, peerId, maxTraceDepth = 10 } = {}) {
    if (!endpointProxy) throw new Error("JobPing requires an endpointProxy");
    this.endpointProxy = endpointProxy;
    this.peerId = peerId || createPeerId();
    this._maxTraceDepth = maxTraceDepth;
  }

  // -- wrap (normal path) -------------------------------------------------

  wrap(wrappedCallable) {
    return async function jobpingWrappedCallable(...args) {
      if (isJobPingDisabled()) return wrappedCallable(...args);

      const output = await wrappedCallable(...args);
      if (!this.endpointProxy.isJobRef(output)) return output;

      let traceOn = _traceEnabled;
      if (!traceOn) traceOn = checkTraceHeader(...args);

      const wasActive = _traceEnabled;
      if (traceOn) {
        _traceEnabled = true;
        const hop = computeHop(...args);
        this.endpointProxy._activeTrace = {
          job_id: output.job_id,
          peer_id: this.peerId,
          hop,
          sub_jobs: [],
          started_at: Date.now(),
        };
      }

      try {
        this.endpointProxy.accept(output.job_id);
        const completedItem = await this.endpointProxy.awaitResult(output.job_id, { timeoutMs: 30000 });
        this.endpointProxy.release(output.job_id);
        return completedItem.result;
      } finally {
        if (traceOn && !wasActive) {
          this.endpointProxy._activeTrace = null;
          _traceEnabled = false;
        }
      }
    }.bind(this);
  }

  // -- unwrap (client-side — normal path) ----------------------------------

  unwrap(wrappedCallable) {
    return async function jobpingUnwrappedCallable(...args) {
      if (isJobPingDisabled()) return wrappedCallable(...args);

      const output = await wrappedCallable(...args);
      if (!this.endpointProxy.isJobRef(output)) return output;

      this.endpointProxy.accept(output.job_id);
      const completedItem = await this.endpointProxy.awaitResult(output.job_id, { timeoutMs: 30000 });
      this.endpointProxy.release(output.job_id);
      return completedItem.result;
    }.bind(this);
  }

  // -- wrap_trace (debug / diagnostic path) -------------------------------

  wrap_trace(wrappedCallable) {
    return async function jobpingTraceCallable(...args) {
      if (isJobPingDisabled()) return wrappedCallable(...args);

      const output = await wrappedCallable(...args);
      if (!this.endpointProxy.isJobRef(output)) return output;

      const wasActive = _traceEnabled;
      _traceEnabled = true;
      this.endpointProxy._activeTrace = {
        job_id: output.job_id,
        peer_id: this.peerId,
        hop: 1,
        sub_jobs: [],
        started_at: Date.now(),
      };

      try {
        this.endpointProxy.accept(output.job_id);
        const completedItem = await this.endpointProxy.awaitResult(output.job_id, { timeoutMs: 30000 });
        this.endpointProxy.release(output.job_id);
        return completedItem.result;
      } finally {
        if (!wasActive) {
          this.endpointProxy._activeTrace = null;
          _traceEnabled = false;
        }
      }
    }.bind(this);
  }
}

export const JobPingClass = JobPing;
