from jobping.endpoint_proxy import EndpointProxy
from jobping.result_handoff import ResultHandoff
from jobping.state_sync import StateSync
from jobping_sandbox.envelope_endpoint_mock import MockEnvelopeEndpoint
from jobping_sandbox.jpitem_queue_mock import MockJPItemQueue
from jobping_sandbox.transport_layer_mock import TransportLayerMock


def create_mock_endpoint_proxy() -> EndpointProxy:
    return EndpointProxy(
        state_sync=StateSync(TransportLayerMock()),
        result_handoff=ResultHandoff(TransportLayerMock()),
        queue=MockJPItemQueue(MockEnvelopeEndpoint()),
    )
