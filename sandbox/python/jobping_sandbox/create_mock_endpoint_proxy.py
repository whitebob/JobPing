from jobping.endpoint_proxy import EndpointProxy
from jobping.result_handoff import ResultHandoff
from jobping.state_sync import StateSync
from jobping.imp.envelope_endpoint_inmemory import EnvelopeEndpointInMemory
from jobping.imp.jpitem_queue_inmemory import JPItemQueueInMemory
from jobping_sandbox.transport_layer_mock import TransportLayerMock


def create_mock_endpoint_proxy() -> EndpointProxy:
    return EndpointProxy(
        state_sync=StateSync(TransportLayerMock()),
        result_handoff=ResultHandoff(TransportLayerMock()),
        queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()),
    )
