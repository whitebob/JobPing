// Browser entry point — exports the full JobPing namespace using the
// browser-compatible WebSocket transport (CDN-loaded socket.io).
//
// Bundle with: npm run build:browser

// Re-export everything except TransportLayerWS (we override it below)
export {
  createJobPing,
  JobPing,
  JobPingClass,
  isJobPingDisabled,
} from "./jobping.mjs";
export { EndpointProxy } from "./endpoint_proxy.mjs";
export { StateSync } from "./state_sync.mjs";
export { ResultHandoff } from "./result_handoff.mjs";
export { TransportLayer, JOBPING_JOB_ID_HEADER } from "./transport_layer.mjs";
export { JPItemQueue, JPItemQueueInMemory } from "./jpitem_queue.mjs";
export { EnvelopeEndpoint, EnvelopeEndpointInMemory } from "./envelope_endpoint.mjs";
export { boxResult, unboxResult, isEnvelope, isResultEnvelope } from "./envelope.mjs";
export { createJobId } from "./id_browser.mjs";

// Browser-specific transport (uses globalThis.io from CDN script tag)
export { TransportLayerWS } from "./imp/transport_layer_ws_browser.mjs";
