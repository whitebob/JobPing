// Sensible defaults for createJobPing().
//
// Defaults follow environment variables when available, falling back to localhost.
// They are loaded at module import time via top-level await so that construction
// failures surface early (missing socket.io-client, etc.).

let _defaultTransportLayer;
let _defaultQueue;

// --- Transport default ---
try {
  const { TransportLayerWS } = await import("./imp/transport_layer_ws.mjs");
  const wsUrl =
    (typeof process !== "undefined" && process?.env?.JOBPING_WS_URL) ||
    "http://127.0.0.1:8890";
  _defaultTransportLayer = new TransportLayerWS({ url: wsUrl });
} catch (_) {
  _defaultTransportLayer = undefined;
}

// --- Queue default ---
try {
  const { JPItemQueueInMemory } = await import("./jpitem_queue.mjs");
  const { EnvelopeEndpointInMemory } = await import("./envelope_endpoint.mjs");
  _defaultQueue = new JPItemQueueInMemory(new EnvelopeEndpointInMemory());
} catch (_) {
  _defaultQueue = undefined;
}

export const DEFAULT_TRANSPORT_LAYER = _defaultTransportLayer;
export const DEFAULT_QUEUE = _defaultQueue;

// Shared defaults mirror Python's DEFAULT_QUEUE semantics: calling
// createJobPing() without arguments shares the same instances.
