// Client-side JobPing SDK facade.

import { EndpointProxy } from "./endpoint_proxy.mjs";
import { ResultHandoff } from "./result_handoff.mjs";
import { StateSync } from "./state_sync.mjs";
import { TransportLayerWS } from "./imp/transport_layer_ws.mjs";
import { LocalTransportLayer } from "./imp/transport_layer_local.mjs";
import { CompositeTransportLayer } from "./imp/transport_layer_composite.mjs";
import { EmbeddedBroker } from "./imp/broker.mjs";
import { JPItemQueueInMemory } from "./jpitem_queue.mjs";
import { EnvelopeEndpointInMemory } from "./envelope_endpoint.mjs";
import { createJobId, createPeerId } from "./id.mjs";
import { JOBPING_JOB_ID_HEADER } from "./transport_layer.mjs";

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

      // Trace: inherited from header if present.
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

export function createJobPing({
  brokerPort,
  peerBrokers = null,
  idleTimeoutSeconds = 300,
  maxTraceDepth = 10,
  sioOpts = {},
} = {}) {
  if (brokerPort == null) {
    throw new Error("createJobPing requires a brokerPort");
  }

  // 1. Embedded broker
  const broker = new EmbeddedBroker(brokerPort, sioOpts);

  // 2. Local fast path
  const localTransport = new LocalTransportLayer(broker);

  // 3. Remote connections
  const transports = [localTransport];
  if (peerBrokers) {
    for (const url of peerBrokers) {
      transports.push(new TransportLayerWS({ url, idleTimeoutSeconds }));
    }
  }

  // 4. Composite (only when needed)
  const transport = transports.length === 1
    ? transports[0]
    : new CompositeTransportLayer(transports);

  // 5. EndpointProxy
  const queue = new JPItemQueueInMemory(new EnvelopeEndpointInMemory());
  const endpointProxy = new EndpointProxy({
    stateSync: new StateSync({ transportLayer: transport }),
    resultHandoff: new ResultHandoff({ transportLayer: transport }),
    queue,
    maxTraceDepth,
  });
  endpointProxy._activeTrace = null;

  return new JobPing({
    endpointProxy,
    peerId: createPeerId(),
    maxTraceDepth,
  });
}
