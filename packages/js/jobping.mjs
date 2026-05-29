// JobPing SDK — Node.js entry point.
//
// Re-exports the core JobPing class plus a createJobPing factory that
// wires up an embedded broker, local transport, and optional peer
// connections.  For browser use, see browser_entry.mjs.

import { JobPing, JobPingClass, isJobPingDisabled } from "./jobping_core.mjs";
import { EndpointProxy } from "./endpoint_proxy.mjs";
import { ResultHandoff } from "./result_handoff.mjs";
import { StateSync } from "./state_sync.mjs";
import { TransportLayerWS } from "./imp/transport_layer_ws.mjs";
import { LocalTransportLayer } from "./imp/transport_layer_local.mjs";
import { CompositeTransportLayer } from "./imp/transport_layer_composite.mjs";
import { EmbeddedBroker } from "./imp/broker.mjs";
import { JPItemQueueInMemory } from "./jpitem_queue.mjs";
import { EnvelopeEndpointInMemory } from "./envelope_endpoint.mjs";
import { createPeerId } from "./id.mjs";

export { JobPing, JobPingClass, isJobPingDisabled };

// Internal factory: builds the full stack but does NOT start the broker.
// Returns { endpointProxy, broker, jobping } so the caller (singleton or
// public createJobPing) decides when / whether to start listening.
export function _createJobPing({
  brokerPort,
  peerBrokers = null,
  idleTimeoutSeconds = 300,
  maxTraceDepth = 10,
  sioOpts = {},
} = {}) {
  // Env-var fallback
  if (brokerPort == null) {
    const envPort = process.env.JOBPING_BROKER_PORT;
    brokerPort = envPort ? parseInt(envPort, 10) : 0;
  }
  if (peerBrokers == null) {
    const envPeers = process.env.JOBPING_PEER_BROKERS;
    if (envPeers) {
      peerBrokers = envPeers.split(",").map((s) => s.trim()).filter(Boolean);
    }
  }

  // 1. Embedded broker (not started yet)
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

  const jobping = new JobPing({
    endpointProxy,
    peerId: createPeerId(),
    maxTraceDepth,
  });
  jobping._broker = broker;

  return { endpointProxy, broker, jobping };
}

export function createJobPing(opts = {}) {
  const { jobping, broker } = _createJobPing(opts);
  broker.start().catch((err) => {
    console.error("EmbeddedBroker failed to start:", err);
  });
  return jobping;
}
