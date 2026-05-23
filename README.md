# JobPing

JobPing is a lightweight endpoint rendezvous bridge for `JPItem` state synchronization and result handoff. It is not a queue system, worker framework, scheduler, or background task platform.

The core goal is narrow: shift necessary waiting from remote application connections to a local wait point, while preserving the original input, output, and failure semantics of the wrapped service.

## Quick start

Create a JobPing with sensible defaults for examples and development:

```py
from jobping import create_jobping
jp = create_jobping()
```

```js
import * as jp from "jobping";
const jobping = jp.createJobPing();
```

### Defaults

- status transport (StateSync): WebSocket `TransportLayerWS` configured via `JOBPING_WS_URL` (default: `http://127.0.0.1:8890`)
- result transport (ResultHandoff): HTTP `TransportLayerHTTPS` configured via `JOBPING_HTTP_BASE` (default: same as WS URL)
- queue: `DEFAULT_QUEUE = JPItemQueueInMemory(EnvelopeEndpointInMemory())`

### Important note about DEFAULT_QUEUE

`DEFAULT_QUEUE` is a module-level, shared, mutable instance. Calling `create_jobping()` without passing a queue means multiple JobPing instances will share the same in-memory queue. This is intentional for examples and quick evaluation. If you need per-JobPing isolation (separate queues, different lifetimes, or priority domains), pass an explicit queue instance:

```py
from jobping.jpitem_queue import JPItemQueueInMemory
from jobping.envelope_endpoint import EnvelopeEndpointInMemory

jp = create_jobping(queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()))
```

### Customization

Pass explicit transports and a queue to `create_jobping` to override defaults:

```py
jp = create_jobping(
  status_transport_layer=my_ws_transport,
  result_transport_layer=my_http_transport,
  queue=my_queue,
)
```

## Current directory structure

The current branch separates SDK code, runnable examples, tests, and mock helpers:

- `packages/js/` — JavaScript SDK classes.
- `packages/python/jobping/` — Python SDK package.
- `examples/control_group/` — Baseline FastAPI + JavaScript client behavior without JobPing.
- `examples/experiment_group/` — Runnable examples in the current SDK shape.
- `sandbox/js/` and `sandbox/python/jobping_sandbox/` — Mock implementations and factories used by examples/tests.
- `tests/js/` and `tests/python/` — Regression tests split by runtime.

## Benchmark

200 concurrent requests, 20 s simulated work per request (httpx connection pool default 100).

| Group | Server | Client | Elapsed | max_active | Success |
|---|---|---|---|---|---|
| Control | FastAPI (no JP) | httpx | 40.28 s | 100 | 200/200 |
| Control | FastAPI (no JP) | httpx + JP | 40.30 s | 100 | 200/200 |
| Experiment | FastAPI + JP | httpx | 40.30 s | 100 | 200/200 |
| Experiment | FastAPI + JP | httpx + JP | **20.35 s** | **3** | **200/200** |

When both sides use JobPing, the server returns a job_ref immediately and releases the HTTP connection. The client connection pool is freed within milliseconds instead of being held for 20 s. This eliminates the connection bottleneck: 200 concurrent requests complete in 20 s with a peak of only 3 active server-side HTTP handlers.

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

Cross-runtime: Node server, Python client. The httpx connection pool limit (100) still dominates the control pairs. JobPing on both sides eliminates the bottleneck across languages — 200 requests complete in 20 s with a single active handler.

Python server + Node client / 200 concurrent

| Group | Server | Client | Elapsed | max_active | Success |
|---|---|---|---|---|---|
| Control | FastAPI (no JP) | Node | 20.15 s | 200 | 200/200 |
| Control | FastAPI (no JP) | Node + JP | 20.11 s | 200 | 200/200 |
| Experiment | FastAPI + JP | Node | 20.11 s | 200 | 200/200 |
| Experiment | FastAPI + JP | Node + JP | 20.13 s | 177 | 200/200 |

Cross-runtime: Python server, Node client. Node has no connection pool limit — 200 requests flood in simultaneously, briefly overlapping handlers even in experiment mode (handlers are fast but overlap in a microsecond window). max_active is pushed to 177. The core benefit still holds: handlers release connections immediately instead of holding them for 20 s.

Python server + Node client / 1000 concurrent

| Group | Server | Client | Elapsed | max_active | Success |
|---|---|---|---|---|---|
| Control | FastAPI (no JP) | Node | 20.53 s | 1000 | 1000/1000 |
| Control | FastAPI (no JP) | Node + JP | 20.52 s | 1000 | 1000/1000 |
| Experiment | FastAPI + JP | Node | 20.52 s | 1000 | 1000/1000 |
| Experiment | FastAPI + JP | Node + JP | 20.66 s | 954 | 1000/1000 |

1000 concurrent requests from Node into a single-process FastAPI server. The burst overlap pushes max_active to 954 in experiment mode, confirming that the counter captures instantaneous peak overlap rather than steady-state connection holding. Control pairs hold 1000 connections for the full 20 s; experiment releases them in milliseconds.

## Design lens

The important symmetry is not "server" vs "client" — those are deployment roles. The protocol roles that matter are: is the endpoint going to produce a value later, or is it waiting for the other side to produce a value?

The same endpoint can be a producer in one interaction and a consumer in another:

- Browser/client waiting for server results
- Server waiting for content provided by browser/client
- Server waiting for server
- Python waiting for Node, or Node waiting for Python

JobPing's API should therefore avoid server/client-specific naming where behavior is actually symmetric.

## JPItem queue semantics

The current mock API uses producer/consumer rendezvous naming:

| Role | Flow | Meaning |
|---|---|---|
| Producer endpoint | `offer -> defer -> fulfill` | This endpoint promises to produce a result later, optionally defers work, then fulfills the `JPItem`. |
| Consumer endpoint | `accept -> awaitResult -> release` | This endpoint accepts the peer's `job_ref`, waits for fulfillment, then releases local ownership. |

Recommended public terminology:

- `offer(job_id)` — create a producer-side `JPItem`.
- `accept(job_id)` — create a consumer-side `JPItem` from a peer's offer.
- `defer(job_id | item)` — mark an offered item for deferred work.
- `makeJobRef(job_id)` / `make_job_ref(job_id)` — create a wrapper-facing rendezvous signal for an offered job.
- `isJobRef(value)` / `is_job_ref(value)` — detect a wrapper-facing job reference, rather than treating it as a result envelope.
- `fulfill(job_id, result)` — box and send the result through the result handoff layer.
- `fulfillLater(job_id, task)` / `fulfill_later(job_id, task)` — perform producer work and fulfill the item via a proxy handoff helper.
- `awaitResult(job_id)` / `await_result(job_id)` — wait for the result envelope and unbox.
- `release(job_id)` — remove endpoint ownership after the item is no longer needed.

`fulfillLater` is currently a mock-level helper. It records the intended producer-work-to-result-handoff semantics but does not lock in a final scheduler API.

## Semantic services and transport

JobPing separates semantic services from transport mechanisms:

| Layer | Responsibility |
|---|---|
| `StateSync` | Synchronize `job_id + status + state_context`. |
| `ResultHandoff` | Transfer ownership/availability of `job_id + result`. |
| `TransportLayer` | Move messages via HTTP, WebSocket, SSE+POST, Kafka, Redis, RabbitMQ, or other carriers. |

`StateSync` and `ResultHandoff` are peers. They may share a transport implementation, but are not required to. Status updates are typically lightweight and frequent, while result handoff may require stronger reliability, larger payload support, or different storage/retrieval paths.

## Envelope mock semantics

The envelope layer is result-shape-agnostic. It knows nothing about HTTP, WebSocket, FastAPI, fetch, routing, state machines, or any business-related return structure.

Current result envelope operations:

- `boxResult` / `box_result`
- `isEnvelope` / `is_envelope`
- `isResultEnvelope` / `is_result_envelope`
- `unboxResult` / `unbox_result`
- `MockEnvelopeEndpoint.send`
- `MockEnvelopeEndpoint.recv`

`job_ref` and routing are closer to the `TransportLayer`/`EndpointProxy` signaling layer than to result envelope semantics.

## JobPing facade

`JobPing` is the top-level wrapper facade. Its public interface is deliberately minimal: `wrap(...)`.

Current role-specific behavior lives at the facade edge:

- Client-side `JobPing.wrap(callable)` calls the opaque callable, detects a returned `job_ref`, then performs `accept -> awaitResult -> release` through `EndpointProxy`.
- Server-side `JobPing.wrap()(callable)` checks for an injected job context provider; if job context is present, it does `offer -> defer -> fulfill_later` and returns a `job_ref`; otherwise it calls the opaque callable normally.

## Job ID and transport layer

`job_id` generation is not mock. The current JavaScript and Python helpers use UUID v4 directly.

`TransportLayer` is now the formal abstraction boundary for moving JobPing metadata and semantic service messages. It is intentionally kept thin: it does not manage JPItem lifecycle, nor does it inspect business results. `TransportLayerMock` is the current concrete implementation for testing, using header-like metadata and in-memory message queues.

## Failure semantics

JobPing should not convert producer exceptions into success-shaped payloads. The principle is: if the wrapped service would have failed before JobPing was introduced, it should still fail after JobPing is introduced.

## Unload switch

JobPing follows the scout principle: joining it should not make the system harder to debug. If a JobPing boundary exception is confusing, a developer should be able to unload JobPing and compare against the original call path.

Current mock unload controls:

- Python/server: set `JOBPING_DISABLED=1`.
- JavaScript side: set `JOBPING_DISABLED=1` or `globalThis.__JOBPING_DISABLED__ = true`.

When disabled at the `wrap` entry point, JobPing performs no capture, envelope, JPItem, print, or queue behavior. It directly invokes the wrapped callable.

## Examples and tests

Examples and tests live under `examples/` and `tests/`.

## License / contributing

See repository files for contribution guidelines and license notes.
