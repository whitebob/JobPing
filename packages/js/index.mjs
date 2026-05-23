// JobPing SDK — single-import namespace.
//
// Quick start (all defaults):
//   import jp from "jobping";
//   const jobping = jp.createJobPing();
//
// Custom transports or queue:
//   const jobping = jp.createJobPing({
//     transportLayer: new jp.TransportLayerWS({ url: "..." }),
//     queue: new jp.JPItemQueueInMemory(new jp.EnvelopeEndpointInMemory()),
//   });

export { createJobPing, JobPing, JobPingClass, isJobPingDisabled } from "./jobping.mjs";
export { EndpointProxy } from "./endpoint_proxy.mjs";
export { StateSync } from "./state_sync.mjs";
export { ResultHandoff } from "./result_handoff.mjs";
export { TransportLayer, JOBPING_JOB_ID_HEADER } from "./transport_layer.mjs";
export { JPItemQueue, JPItemQueueInMemory } from "./jpitem_queue.mjs";
export { EnvelopeEndpoint, EnvelopeEndpointInMemory } from "./envelope_endpoint.mjs";
export { TransportLayerWS } from "./imp/transport_layer_ws.mjs";
export { TransportLayerHTTPS } from "./imp/transport_layer_https.mjs";
export { boxResult, unboxResult, isEnvelope, isResultEnvelope } from "./envelope.mjs";
export { createJobId } from "./id.mjs";
