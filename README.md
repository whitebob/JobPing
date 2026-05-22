# JobPing

JobPing is a small endpoint rendezvous bridge for JPItem state synchronization and result handoff. It moves necessary waiting from remote endpoints to a local wait point while preserving the wrapped function's input/output and failure semantics.

Quick start

- Python: create a JobPing with sensible defaults for examples and development:

```py
from jobping import create_jobping
jp = create_jobping()
```

Defaults

- status transport (StateSync): WebSocket TransportLayerWS configured via JOBPING_WS_URL (default: http://127.0.0.1:8890)
- result transport (ResultHandoff): HTTP TransportLayerHTTPS configured via JOBPING_HTTP_BASE (default: same as WS URL)
- queue: DEFAULT_QUEUE = JPItemQueueInMemory(EnvelopeEndpointInMemory())

Important note about DEFAULT_QUEUE

DEFAULT_QUEUE is a module-level, shared, mutable instance. Calling create_jobping() without passing a queue means multiple JobPing instances will share the same in-memory queue. This is intentional for examples and quick evaluation. If you need per-JobPing isolation (separate queues, different lifetimes, or priority domains), pass an explicit queue instance:

```py
from jobping.jpitem_queue import JPItemQueueInMemory
from jobping.envelope_endpoint import EnvelopeEndpointInMemory

jp = create_jobping(queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()))
```

Customization

Pass explicit transports and a queue to create_jobping to override defaults:

```py
jp = create_jobping(
  status_transport_layer=my_ws_transport,
  result_transport_layer=my_http_transport,
  queue=my_queue,
)
```

Design notes

- StateSync: synchronizes job_id + status + optional state context (lightweight; frequently updated)
- ResultHandoff: delivers boxed final results (may require different reliability or storage)
- TransportLayer: abstract boundary for message movement (WebSocket, HTTP, SSE, Kafka, etc.)

Examples and tests live under `examples/` and `tests/`.

License / contributing

See repository files for contribution guidelines and license notes.
