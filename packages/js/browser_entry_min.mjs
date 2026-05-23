// Browser entry point for the minified IIFE bundle.
//
// This entry avoids top-level await (no defaults.mjs, no Node transport) so it
// can be bundled as IIFE. Exposes a global `jobping` namespace.

import { EndpointProxy } from "./endpoint_proxy.mjs";
import { ResultHandoff } from "./result_handoff.mjs";
import { StateSync } from "./state_sync.mjs";

export { EndpointProxy } from "./endpoint_proxy.mjs";
export { StateSync } from "./state_sync.mjs";
export { ResultHandoff } from "./result_handoff.mjs";
export { TransportLayer, JOBPING_JOB_ID_HEADER } from "./transport_layer.mjs";
export { JPItemQueue, JPItemQueueInMemory } from "./jpitem_queue.mjs";
export { EnvelopeEndpoint, EnvelopeEndpointInMemory } from "./envelope_endpoint.mjs";
export { boxResult, unboxResult, isEnvelope, isResultEnvelope } from "./envelope.mjs";
export { createJobId } from "./id_browser.mjs";
export { TransportLayerWS } from "./imp/transport_layer_ws_browser.mjs";

// Inlined JobPing class (avoids importing jobping.mjs which pulls in
// defaults.mjs with top-level await, incompatible with IIFE).
export class JobPing {
  constructor({ endpointProxy }) {
    if (!endpointProxy) throw new Error("JobPing requires an endpointProxy");
    this.endpointProxy = endpointProxy;
  }

  wrap(wrappedCallable) {
    return async function jobpingWrappedCallable(...args) {
      const output = await wrappedCallable(...args);
      if (!this.endpointProxy.isJobRef(output)) return output;
      this.endpointProxy.accept(output.job_id);
      const completedItem = await this.endpointProxy.awaitResult(output.job_id, { timeoutMs: 30000 });
      this.endpointProxy.release(output.job_id);
      return completedItem.result;
    }.bind(this);
  }
}

export function isJobPingDisabled() {
  return false;
}

export function createJobPing({
  transportLayer,
  queue,
  resultTransportLayer = transportLayer,
} = {}) {
  if (!transportLayer) throw new Error("createJobPing requires a transportLayer");
  if (!queue) throw new Error("createJobPing requires a queue");
  return new JobPing({
    endpointProxy: new EndpointProxy({
      stateSync: new StateSync({ transportLayer }),
      resultHandoff: new ResultHandoff({ transportLayer: resultTransportLayer }),
      queue,
    }),
  });
}
