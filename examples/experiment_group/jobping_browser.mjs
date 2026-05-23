// packages/js/id_browser.mjs
function createJobId() {
  return crypto.randomUUID();
}

// packages/js/endpoint_proxy.mjs
var JPITEM_COMPLETED = "completed";
var JOBPING_JOB_REF_KIND = "jobping.job_ref.v1";
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
var EndpointProxy = class {
  constructor({
    stateSync,
    resultHandoff,
    queue,
    createJobId: createJobId2 = createJobId
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
    this.createJobIdFn = createJobId2;
  }
  createJobId() {
    return this.createJobIdFn();
  }
  makeJobRef(jobId) {
    assertValidJobId(jobId);
    return {
      jobping: JOBPING_JOB_REF_KIND,
      type: "job_ref",
      job_id: jobId
    };
  }
  isJobRef(value) {
    return typeof value === "object" && value !== null && value.jobping === JOBPING_JOB_REF_KIND && value.type === "job_ref" && typeof value.job_id === "string" && value.job_id.length > 0;
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
};

// packages/js/envelope.mjs
var JOBPING_ENVELOPE_KIND = "jobping.envelope.v1";
var JOBPING_RESULT = "result";
function assertValidJobId2(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
}
function boxResult(jobId, payload) {
  assertValidJobId2(jobId);
  return {
    jobping: JOBPING_ENVELOPE_KIND,
    type: JOBPING_RESULT,
    job_id: jobId,
    payload
  };
}
function isEnvelope(value) {
  return typeof value === "object" && value !== null && value.jobping === JOBPING_ENVELOPE_KIND && typeof value.type === "string" && typeof value.job_id === "string" && value.job_id.length > 0;
}
function isResultEnvelope(value) {
  return isEnvelope(value) && value.type === JOBPING_RESULT && Object.hasOwn(value, "payload");
}
function unboxResult(envelope, expectedJobId) {
  if (!isResultEnvelope(envelope)) {
    throw new Error("Expected JobPing result envelope");
  }
  if (expectedJobId !== void 0 && envelope.job_id !== expectedJobId) {
    throw new Error("Unexpected JobPing result job_id");
  }
  return envelope.payload;
}

// packages/js/result_handoff.mjs
var JOBPING_RESULT_HANDOFF = "jobping.result_handoff.v1";
function assertValidJobId3(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
}
var ResultHandoff = class {
  constructor({ transportLayer }) {
    if (!transportLayer?.sendMessage || !transportLayer?.recvMessage) {
      throw new Error("ResultHandoff requires a transport layer with sendMessage/recvMessage");
    }
    this.transportLayer = transportLayer;
  }
  fulfill(jobId, result) {
    assertValidJobId3(jobId);
    this.transportLayer.sendMessage({
      kind: JOBPING_RESULT_HANDOFF,
      job_id: jobId,
      data: boxResult(jobId, result)
    });
  }
  async awaitResult(jobId, { timeoutMs = 1e3 } = {}) {
    assertValidJobId3(jobId);
    const message = await this.transportLayer.recvMessage({
      kind: JOBPING_RESULT_HANDOFF,
      jobId,
      timeoutMs
    });
    return unboxResult(message.data, jobId);
  }
};

// packages/js/state_sync.mjs
var JOBPING_STATE_UPDATE = "jobping.state_update.v1";
function assertValidJobId4(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
}
function assertValidStatus(status) {
  if (typeof status !== "string" || status.length === 0) {
    throw new Error("status must be a non-empty string");
  }
}
var StateSync = class {
  constructor({ transportLayer }) {
    if (!transportLayer?.sendMessage || !transportLayer?.recvMessage) {
      throw new Error("StateSync requires a transport layer with sendMessage/recvMessage");
    }
    this.transportLayer = transportLayer;
  }
  publish(jobId, status, stateContext = {}) {
    assertValidJobId4(jobId);
    assertValidStatus(status);
    this.transportLayer.sendMessage({
      kind: JOBPING_STATE_UPDATE,
      job_id: jobId,
      data: {
        status,
        state_context: stateContext
      }
    });
  }
  async waitFor(jobId, { status, timeoutMs = 1e3 } = {}) {
    assertValidJobId4(jobId);
    while (true) {
      const message = await this.transportLayer.recvMessage({
        kind: JOBPING_STATE_UPDATE,
        jobId,
        timeoutMs
      });
      const state = message.data;
      if (status === void 0 || state.status === status) {
        return state;
      }
    }
  }
};

// packages/js/jobping.mjs
function isJobPingDisabled() {
  if (globalThis.__JOBPING_DISABLED__ === true) {
    return true;
  }
  const value = globalThis.process?.env?.JOBPING_DISABLED;
  return typeof value === "string" && /^(1|true|yes|on)$/i.test(value);
}
var JobPing = class {
  constructor({ endpointProxy }) {
    if (!endpointProxy) {
      throw new Error("JobPing requires an endpointProxy");
    }
    this.endpointProxy = endpointProxy;
  }
  wrap(wrappedCallable) {
    return async function jobpingWrappedCallable(...args) {
      if (isJobPingDisabled()) {
        return wrappedCallable(...args);
      }
      const output = await wrappedCallable(...args);
      if (!this.endpointProxy.isJobRef(output)) {
        return output;
      }
      this.endpointProxy.accept(output.job_id);
      const completedItem = await this.endpointProxy.awaitResult(output.job_id, { timeoutMs: 3e4 });
      this.endpointProxy.release(output.job_id);
      return completedItem.result;
    }.bind(this);
  }
};
function createJobPing({
  transportLayer,
  queue,
  resultTransportLayer = transportLayer
}) {
  if (!transportLayer) {
    throw new Error("createJobPing requires a transportLayer");
  }
  if (!queue) {
    throw new Error("createJobPing requires a queue");
  }
  return new JobPing({
    endpointProxy: new EndpointProxy({
      stateSync: new StateSync({ transportLayer }),
      resultHandoff: new ResultHandoff({ transportLayer: resultTransportLayer }),
      queue
    })
  });
}

// packages/js/imp/jpitem_queue_inmemory.mjs
var JPITEM_CREATED = "created";
var JPITEM_WAITING = "waiting";
var JPITEM_QUEUED = "queued";
var JPITEM_COMPLETED2 = "completed";
var JPITEM_DESTROYED = "destroyed";
function assertValidJobId5(jobId) {
  if (typeof jobId !== "string" || jobId.length === 0) {
    throw new Error("job_id must be a non-empty string");
  }
}
function createItem(jobId, role, status) {
  return {
    job_id: jobId,
    role,
    status,
    result: void 0
  };
}
var JPItemQueueInMemory = class {
  constructor(envelopeEndpoint) {
    if (!envelopeEndpoint || typeof envelopeEndpoint.send !== "function") {
      throw new Error("JPItemQueueInMemory requires an envelope endpoint");
    }
    this.envelopeEndpoint = envelopeEndpoint;
    this.items = /* @__PURE__ */ new Map();
  }
  accept(jobId) {
    assertValidJobId5(jobId);
    if (this.items.has(jobId)) throw new Error(`JPItem already exists: ${jobId}`);
    const item = createItem(jobId, "consumer", JPITEM_WAITING);
    this.items.set(jobId, item);
    return item;
  }
  offer(jobId) {
    assertValidJobId5(jobId);
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
    item.status = JPITEM_COMPLETED2;
    item.result = result;
    this.envelopeEndpoint.send(boxResult(jobId, result));
    return item;
  }
  async awaitResult(jobId, { timeoutMs = 1e3 } = {}) {
    const item = this._resolveItem(jobId);
    if (item.role !== "consumer") throw new Error("Only accepted JPItems can await results");
    item.status = JPITEM_WAITING;
    const envelope = await this.envelopeEndpoint.recv({ jobId, type: JOBPING_RESULT, timeoutMs });
    const result = unboxResult(envelope, jobId);
    item.status = JPITEM_COMPLETED2;
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
    assertValidJobId5(jobId);
    return this.items.get(jobId);
  }
  snapshot() {
    const statuses = {};
    for (const item of this.items.values()) statuses[item.status] = (statuses[item.status] ?? 0) + 1;
    return { items: this.items.size, statuses, envelopes: this.envelopeEndpoint.size() };
  }
  _resolveItem(itemOrJobId) {
    const jobId = typeof itemOrJobId === "string" ? itemOrJobId : itemOrJobId?.job_id;
    assertValidJobId5(jobId);
    const item = this.items.get(jobId);
    if (!item) throw new Error(`Unknown JPItem: ${jobId}`);
    return item;
  }
};

// packages/js/jpitem_queue.mjs
var JPItemQueueInMemory2 = JPItemQueueInMemory;

// packages/js/envelope_endpoint.mjs
var EnvelopeEndpoint = class _EnvelopeEndpoint {
  constructor() {
    if (new.target === _EnvelopeEndpoint) {
      throw new Error("EnvelopeEndpoint is abstract; use a concrete implementation");
    }
  }
  send() {
    throw new Error("EnvelopeEndpoint.send() must be implemented");
  }
  recv() {
    throw new Error("EnvelopeEndpoint.recv() must be implemented");
  }
  size() {
    throw new Error("EnvelopeEndpoint.size() must be implemented");
  }
};
var _Waiter = class {
  constructor(jobId, type, resolve) {
    this.jobId = jobId;
    this.type = type;
    this.resolve = resolve;
  }
};
var EnvelopeEndpointInMemory = class extends EnvelopeEndpoint {
  constructor() {
    super();
    this.pending = [];
    this.waiters = [];
  }
  send(envelope) {
    if (!isEnvelope(envelope)) throw new Error("Can only send JobPing envelopes");
    const idx = this.waiters.findIndex((w) => this._matches(envelope, w.jobId, w.type));
    if (idx !== -1) {
      const [waiter] = this.waiters.splice(idx, 1);
      waiter.resolve(envelope);
      return;
    }
    this.pending.push(envelope);
  }
  recv({ jobId, type, timeoutMs = 1e3 } = {}) {
    const idx = this.pending.findIndex((e) => this._matches(e, jobId, type));
    if (idx !== -1) {
      const [envelope] = this.pending.splice(idx, 1);
      return Promise.resolve(envelope);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const waiterIndex = this.waiters.indexOf(waiter);
        if (waiterIndex !== -1) this.waiters.splice(waiterIndex, 1);
        reject(new Error("Timed out waiting for JobPing envelope"));
      }, timeoutMs);
      let waiter = new _Waiter(jobId, type, (envelope) => {
        clearTimeout(timer);
        resolve(envelope);
      });
      this.waiters.push(waiter);
    });
  }
  size() {
    return { pending: this.pending.length, waiters: this.waiters.length };
  }
  _matches(envelope, jobId, type) {
    return isEnvelope(envelope) && (jobId === void 0 || envelope.job_id === jobId) && (type === void 0 || envelope.type === type);
  }
};

// packages/js/transport_layer.mjs
var JOBPING_JOB_ID_HEADER = "x-jobping-job-id";
var TransportLayer = class _TransportLayer {
  constructor() {
    if (new.target === _TransportLayer) {
      throw new Error("TransportLayer is abstract; use a concrete implementation");
    }
  }
  attachJobId() {
    throw new Error("TransportLayer.attachJobId() must be implemented");
  }
  extractJobId() {
    throw new Error("TransportLayer.extractJobId() must be implemented");
  }
  attachEnvelope() {
    throw new Error("TransportLayer.attachEnvelope() must be implemented");
  }
  extractEnvelope() {
    throw new Error("TransportLayer.extractEnvelope() must be implemented");
  }
  sendEnvelope() {
    throw new Error("TransportLayer.sendEnvelope() must be implemented");
  }
  recvEnvelope() {
    throw new Error("TransportLayer.recvEnvelope() must be implemented");
  }
  sendMessage() {
    throw new Error("TransportLayer.sendMessage() must be implemented");
  }
  recvMessage() {
    throw new Error("TransportLayer.recvMessage() must be implemented");
  }
};

// packages/js/imp/transport_layer_ws_browser.mjs
var Mailbox = class {
  constructor() {
    this._messages = [];
    this._waiters = [];
  }
  put(data) {
    const waiterIndex = this._waiters.findIndex((w) => w.matches(data));
    if (waiterIndex !== -1) {
      const [waiter] = this._waiters.splice(waiterIndex, 1);
      waiter.resolve(data);
      return;
    }
    this._messages.push(data);
  }
  get(matches, timeout) {
    const msgIndex = this._messages.findIndex((m) => matches(m));
    if (msgIndex !== -1) {
      return Promise.resolve(this._messages.splice(msgIndex, 1)[0]);
    }
    return new Promise((resolve, reject) => {
      const waiter = { matches, resolve };
      const timer = setTimeout(() => {
        const i = this._waiters.indexOf(waiter);
        if (i !== -1) this._waiters.splice(i, 1);
        reject(new Error("Timed out waiting for message"));
      }, timeout);
      waiter.resolve = (data) => {
        clearTimeout(timer);
        resolve(data);
      };
      this._waiters.push(waiter);
    });
  }
  size() {
    return { messages: this._messages.length, waiters: this._waiters.length };
  }
};
var TransportLayerWS = class extends TransportLayer {
  constructor({ url, opts } = {}) {
    super();
    const io = globalThis.io;
    if (!io) {
      throw new Error(
        'socket.io-client must be loaded before TransportLayerWS (add <script src="...socket.io.min.js"><\/script> to the page)'
      );
    }
    this.url = url;
    this.socket = io(url, opts);
    this._messageMailbox = new Mailbox();
    this._envelopeMailbox = new Mailbox();
    this.socket.on("jobping:envelope", (envelope) => {
      this._envelopeMailbox.put(envelope);
    });
    this.socket.on("jobping:message", (message) => {
      this._messageMailbox.put(message);
    });
  }
  attachJobId(carrier = {}, jobId) {
    if (typeof jobId !== "string" || jobId.length === 0) {
      throw new Error("job_id must be a non-empty string");
    }
    return {
      ...carrier,
      headers: {
        ...carrier.headers ?? {},
        [JOBPING_JOB_ID_HEADER]: jobId
      }
    };
  }
  extractJobId(carrier = {}) {
    const headers = carrier.headers ?? {};
    for (const [k, v] of Object.entries(headers)) {
      if (k.toLowerCase() === JOBPING_JOB_ID_HEADER.toLowerCase()) return v;
    }
    return void 0;
  }
  attachEnvelope(carrier = {}, envelope) {
    return { ...carrier, envelope };
  }
  extractEnvelope(carrier = {}) {
    const e = carrier.envelope;
    return e && typeof e === "object" ? e : void 0;
  }
  sendEnvelope(envelope) {
    if (!this.socket) throw new Error("No socket configured");
    this.socket.emit("jobping:envelope", envelope);
  }
  recvEnvelope({ jobId, type, timeout, timeoutMs = 1e3 } = {}) {
    const matches = (envelope) => (jobId == null || envelope.job_id === jobId) && (type == null || envelope.type === type);
    return this._envelopeMailbox.get(matches, timeout ?? timeoutMs);
  }
  sendMessage(message) {
    if (!this.socket) throw new Error("No socket configured");
    this.socket.emit("jobping:message", message);
  }
  recvMessage({ kind, jobId, timeout, timeoutMs = 1e3 } = {}) {
    const matches = (message) => typeof message === "object" && message !== null && (kind == null || message.kind === kind) && (jobId == null || message.job_id === jobId);
    return this._messageMailbox.get(matches, timeout ?? timeoutMs);
  }
  size() {
    return {
      messages: this._messageMailbox.size(),
      envelopes: this._envelopeMailbox.size()
    };
  }
};
export {
  EnvelopeEndpointInMemory,
  JPItemQueueInMemory2 as JPItemQueueInMemory,
  TransportLayerWS,
  createJobPing,
  isJobPingDisabled
};
