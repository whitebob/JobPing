import { EndpointProxy } from "../../packages/js/endpoint_proxy.mjs";
import { ResultHandoff } from "../../packages/js/result_handoff.mjs";
import { StateSync } from "../../packages/js/state_sync.mjs";
import { MockEnvelopeEndpoint } from "./envelope_endpoint_mock.mjs";
import { MockJPItemQueue } from "./jpitem_queue_mock.mjs";
import { TransportLayerMock } from "./transport_layer_mock.mjs";

export function createMockEndpointProxy() {
  return new EndpointProxy({
    stateSync: new StateSync({
      transportLayer: new TransportLayerMock(),
    }),
    resultHandoff: new ResultHandoff({
      transportLayer: new TransportLayerMock(),
    }),
    queue: new MockJPItemQueue(new MockEnvelopeEndpoint()),
  });
}
