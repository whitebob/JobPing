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

Benchmark

200 concurrent requests, 20 s simulated work per request (httpx connection pool default 100).

| Group | Server | Client | Elapsed | max_active | Success |
|---|---|---|---|---|---|
| Control | FastAPI (no JP) | httpx | 40.28 s | 100 | 200/200 |
| Control | FastAPI (no JP) | httpx + JP | 40.30 s | 100 | 200/200 |
| Experiment | FastAPI + JP | httpx | 40.30 s | 100 | 200/200 |
| Experiment | FastAPI + JP | httpx + JP | **20.35 s** | **3** | **200/200** |

When both sides use JobPing, the server returns a job_ref immediately and releases the HTTP connection. The client connection pool is freed within milliseconds instead of being held for 20 s. 200 concurrent requests complete in 20 s with a peak of only 3 active server-side HTTP handlers.

Node.js / 1000 concurrent

| Group | Server | Client | Elapsed | max_active | Success |
|---|---|---|---|---|---|
| Control | Node (no JP) | Node | 20.54 s | 1000 | 1000/1000 |
| Control | Node (no JP) | Node + JP | 20.55 s | 1000 | 1000/1000 |
| Experiment | Node + JP | Node | 20.34 s | 1000 | 1000/1000 |
| Experiment | Node + JP | Node + JP | **20.30 s** | **1** | **1000/1000** |

1000 concurrent requests on a single Node thread. Without JobPing, the server holds 1000 open HTTP connections for 20 s. With JobPing on both sides, connections are released in milliseconds — peak active handlers drops from 1000 to 1.

Node server + Python client / 200 concurrent

| Group | Server | Client | Elapsed | max_active | Success |
|---|---|---|---|---|---|
| Control | Node (no JP) | httpx | 40.29 s | 100 | 200/200 |
| Control | Node (no JP) | httpx + JP | 40.29 s | 100 | 200/200 |
| Experiment | Node + JP | httpx | 40.29 s | 100 | 200/200 |
| Experiment | Node + JP | httpx + JP | **20.33 s** | **1** | **200/200** |

Cross-runtime: Node server, Python client. The httpx connection pool limit (100) still dominates the control pairs. JobPing on both sides eliminates the bottleneck regardless of language — 200 requests complete in 20 s with a single active handler.

Python server + Node client / 200 concurrent

| Group | Server | Client | Elapsed | max_active | Success |
|---|---|---|---|---|---|
| Control | FastAPI (no JP) | Node | 20.15 s | 200 | 200/200 |
| Control | FastAPI (no JP) | Node + JP | 20.11 s | 200 | 200/200 |
| Experiment | FastAPI + JP | Node | 20.11 s | 200 | 200/200 |
| Experiment | FastAPI + JP | Node + JP | 20.13 s | 177 | 200/200 |

Cross-runtime: Python server, Node client. Node's unbounded concurrency sends all 200 requests in a single burst, briefly overlapping handlers even in experiment mode (handlers are fast but overlap in a microsecond window). The key benefit still holds: handlers release connections immediately instead of holding them for 20 s.

Python server + Node client / 1000 concurrent

| Group | Server | Client | Elapsed | max_active | Success |
|---|---|---|---|---|---|
| Control | FastAPI (no JP) | Node | 20.53 s | 1000 | 1000/1000 |
| Control | FastAPI (no JP) | Node + JP | 20.52 s | 1000 | 1000/1000 |
| Experiment | FastAPI + JP | Node | 20.52 s | 1000 | 1000/1000 |
| Experiment | FastAPI + JP | Node + JP | 20.66 s | 954 | 1000/1000 |

1000 concurrent requests from Node into a single-process FastAPI server. The burst overlap pushes max_active to 954 even in experiment mode, confirming that the counter captures instantaneous peak overlap rather than steady-state connection holding. Control pairs hold 1000 connections for the full 20 s; experiment releases them in milliseconds.

Design notes

- StateSync: synchronizes job_id + status + optional state context (lightweight; frequently updated)
- ResultHandoff: delivers boxed final results (may require different reliability or storage)
- TransportLayer: abstract boundary for message movement (WebSocket, HTTP, SSE, Kafka, etc.)

Examples and tests live under `examples/` and `tests/`.

License / contributing

See repository files for contribution guidelines and license notes.
