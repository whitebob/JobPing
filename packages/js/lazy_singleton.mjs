// Lazy singleton proxy — the public face of JobPing v2 (JS).
//
// import { jp } from "jobping" gives you the module-level LazyJobPing instance.
// No broker, no port binding until the first wrap() / wrapClient() / startBroker().
//
// Port of Python's _lazy_singleton.py to JavaScript.

import { isJobPingDisabled } from "./jobping_core.mjs";
import { _createJobPing } from "./jobping.mjs";

// ---------------------------------------------------------------------------
// Default job context provider — inspects call arguments for a request-like
// object whose .headers contains x-jobping-job-id.
// ---------------------------------------------------------------------------

function defaultJobContextProvider(...args) {
  for (const arg of args) {
    if (!arg || typeof arg !== "object") continue;
    const headers = arg.headers;
    if (!headers || typeof headers !== "object") continue;
    for (const [k, v] of Object.entries(headers)) {
      if (k.toLowerCase() === "x-jobping-job-id") return String(v);
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// LazyJobPing
// ---------------------------------------------------------------------------

export class LazyJobPing {
  constructor() {
    this._active = null; // current EndpointProxy
    this._broker = null; // current EmbeddedBroker
    this._jp = null; // internal JobPing instance
    this._storedParams = {};
    this._built = false;
    this._needsRebuild = false;
    this._jobContextProvider = null;
  }

  // -- configure ----------------------------------------------------------

  configure(opts = {}) {
    const { force = false, ...params } = opts;
    if (!this._built) {
      Object.assign(this._storedParams, params);
    } else if (force) {
      Object.assign(this._storedParams, params);
      this._needsRebuild = true;
    }
  }

  // -- lazy build ---------------------------------------------------------

  _ensureBuilt() {
    if (this._built) return;
    // _createJobPing is fully synchronous — no await interleaving concern.
    const { endpointProxy, broker, jobping } = _createJobPing(this._storedParams);
    this._active = endpointProxy;
    this._broker = broker;
    this._jp = jobping;
    this._jobContextProvider =
      this._storedParams.jobContextProvider || defaultJobContextProvider;
    this._built = true;
  }

  // -- currentActive ------------------------------------------------------

  currentActive() {
    return this._active;
  }

  // -- wrap (server-side — matches Python behaviour) ----------------------
  //
  //   1. Disabled?  Pass through.
  //   2. No job-id on the request?  Pass through.
  //   3. Otherwise: offer → defer → fulfillLater → return job_ref.

  wrap(fn) {
    const self = this;
    return async function jobpingWrapped(...args) {
      if (isJobPingDisabled()) return fn(...args);

      self._ensureBuilt();

      const jobId = self._jobContextProvider(...args);
      if (!jobId) return fn(...args);

      const active = self.currentActive();
      active.offer(jobId);
      active.defer(jobId);
      active.fulfillLater(jobId, () => fn(...args)).catch((err) => {
        console.error("fulfillLater error:", err);
      });
      return active.makeJobRef(jobId);
    };
  }

  // -- wrapClient (client-side — existing JS behaviour) --------------------
  //
  //   1. Call the wrapped function.
  //   2. If the output is a job_ref: accept → awaitResult → release → return
  //      the unwrapped result.
  //   3. Otherwise return the output unchanged.

  wrapClient(fn) {
    const self = this;
    return async function jobpingClientWrapped(...args) {
      if (isJobPingDisabled()) return fn(...args);

      self._ensureBuilt();

      const output = await fn(...args);
      const active = self.currentActive();
      if (!active.isJobRef(output)) return output;

      active.accept(output.job_id);
      const completed = await active.awaitResult(output.job_id, { timeoutMs: 30000 });
      active.release(output.job_id);
      return completed.result;
    };
  }

  // -- broker lifecycle ---------------------------------------------------

  async startBroker() {
    this._ensureBuilt();
    if (this._broker) {
      await this._broker.start();
    }
  }

  async stopBroker() {
    if (this._broker) {
      await this._broker.stop();
    }
  }

  // -- delegation ---------------------------------------------------------

  // Forward any other property lookups to the internal JobPing instance.
  // This lets callers access jp.peerId, jp.endpointProxy, etc. after build.
  get peerId() {
    this._ensureBuilt();
    return this._jp ? this._jp.peerId : undefined;
  }
}

// ---------------------------------------------------------------------------
// Module-level singleton
// ---------------------------------------------------------------------------

export const jp = new LazyJobPing();
