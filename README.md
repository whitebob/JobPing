# JobPing

JobPing keeps your server responsive under load. It wraps your existing request handlers so long-running work doesn't hold HTTP connections hostage.

Works across Python, Node.js, and the browser — in any combination.

## Why use it?

If your server does work that takes seconds (or minutes), every concurrent request ties up a connection for the entire duration. Your throughput caps at the connection pool limit.

JobPing lets the server hand back a ticket immediately, do the work in the background, and deliver the result when it's ready.

| Scenario | Without JobPing | With JobPing |
|---|---|---|
| Python, 200 concurrent, 20 s work | 40 s, 100 connections held | **20 s, 3 active connections** |
| Node, 1000 concurrent, 20 s work | 1000 connections held | **1 active connection** |

Same work, same results. Your server just stops being the bottleneck.

## How it works

```
Request arrives → handler returns a ticket (job_ref) → connection freed immediately
                                    ↓
          work runs in background → result placed in shared queue
                                    ↓
          client picks up result → unwraps → caller sees normal response
```

- The side that **does the work** (producer): `offer` → `defer` → `fulfill`
- The side that **waits** (consumer): `accept` → `awaitResult` → `release`

`wrap()` automates both sides. You wrap your existing function and JobPing handles the ticket exchange transparently.

## Quick start

```py
from jobping import create_jobping
jp = create_jobping(broker_port=8900)
```

```js
import * as jp from "jobping";
const jobping = jp.createJobPing({ brokerPort: 8900 });
```

Every node runs its own embedded broker. `broker_port` is the TCP port for the local Socket.IO broker. To connect to peers, pass their broker URLs:

```py
jp = create_jobping(
    broker_port=8900,
    peer_brokers=["http://other-host:8890"],
)
```

```js
const jobping = jp.createJobPing({
  brokerPort: 8900,
  peerBrokers: ["http://other-host:8890"],
});
```

## Usage

### Server-side

Wrap your handler. If the request carries a JobPing header, the handler returns a ticket and fulfills the result in the background. No header? Runs normally — zero behavior change.

```py
@app.get("/work")
async def work(request_id: str, sleep_seconds: float):
    return await jp.wrap(do_work)(request_id, sleep_seconds)
```

```js
// Inside your HTTP handler:
const result = await endpointProxy.wrap(doWork)(requestId, sleepSeconds);
res.end(JSON.stringify(result));
```

### Client-side

Wrap your fetch. If the server returns a ticket, JobPing waits for the real result transparently.

```js
const callServer = jobping.wrap(async (id) => {
  const resp = await fetch(`/work?request_id=${id}`);
  return resp.json();
});
const result = await callServer(42);  // your code sees the final result
```

### Browser

```html
<script src="https://cdn.socket.io/4.8.1/socket.io.min.js"></script>
<script type="module">
import * as jp from "./jobping_browser.mjs";
const jobping = jp.createJobPing();
const callServer = jobping.wrap(async (id) => {
  const resp = await fetch(`/work?request_id=${id}`);
  return resp.json();
});
</script>
```

### Debugging: disable at runtime

```
JOBPING_DISABLED=1
```

Or in JS: `globalThis.__JOBPING_DISABLED__ = true`. `wrap()` becomes a no-op — your callable runs exactly as it would without JobPing. Useful for A/B comparison without changing code.

## Idle timeout

WebSocket connections to peer brokers automatically disconnect after a period of inactivity, releasing resources when they're not needed. The connection re-establishes transparently on the next message.

```py
# Disconnect after 5 minutes of idle (default: 300 s)
jp = create_jobping(broker_port=8900, idle_timeout_seconds=300)

# Never disconnect
jp = create_jobping(broker_port=8900, idle_timeout_seconds=None)
```

```js
const jobping = jp.createJobPing({ brokerPort: 8900, idleTimeoutSeconds: 300 });
```

Every `send_message` call resets the idle timer. A background watcher checks at half the timeout interval and disconnects if no activity occurred. The disconnect is clean — the Socket.IO client closes gracefully and the watcher task cancels itself.

Peers that disconnect are not removed from the routing table. When a message targets a disconnected peer, the broker reconnects on demand (via the `_ensure_connected` path).

## Trace

Trace lets you see exactly where time goes across a distributed call tree. It is **per-job** — you opt in for specific jobs without paying overhead on the rest. When a parent job has tracing enabled, child jobs inherit it automatically so you get the full picture.

### Enabling trace

Two ways to turn tracing on:

| Method | When to use |
|---|---|
| `@jp.wrap_trace()` | You control the entry point. Trace always on for this handler. |
| `@jp.wrap()` + `x-jobping-trace-enabled: 1` header | Downstream jobs inherit from an upstream trace. |

**`wrap_trace()`** — explicit opt-in. Use this on the outermost handler where you want the trace to start:

```py
@app.get("/debug/work")
@jp.wrap_trace()
async def do_work(request: Request, request_id: int) -> dict:
    ...
```

Every call through this handler records a trace, regardless of headers. The `hop` counter starts at 1.

**`wrap()` with header** — inherited propagation. When a handler wrapped with `@jp.wrap()` receives `x-jobping-trace-enabled: 1` (or `true`), it activates tracing for that job and forwards the header to any nested JobPing calls. This is how a trace started at the edge propagates through the entire call tree:

```
Client (trace enabled)
  → Service A  @jp.wrap()  + header → trace ON, hop=1
    → Service B  @jp.wrap()  + header → trace ON, hop=2
      → Service C  @jp.wrap()  no header → trace OFF (normal path)
```

The ContextVar that carries the trace flag is isolated per async task — two concurrent requests never interfere.

### Reading trace data

When a traced job completes, the trace payload is attached to the fulfill result. Use `parse_trace` to turn the raw dict into a structured report:

```py
from jobping.trace import parse_trace, find_bottleneck

# Raw trace dict (attached to fulfill, stored however you like)
raw_trace = {
    "job_id": "abc123",
    "peer_id": "api-gateway",
    "hop": 1,
    "elapsed": 2.5,
    "sub_jobs": [
        {"job_id": "def456", "peer_id": "worker-1", "hop": 2, "elapsed": 1.8, "sub_jobs": []},
    ],
}

report = parse_trace(raw_trace)
print(f"Total: {report.total_elapsed:.2f}s")
print(f"Critical path: {' → '.join(n.peer_id for n in report.critical_path)}")
print(find_bottleneck(report))
# Bottleneck: worker-1 (job def456) — self_time=1.80s (72% of total 2.50s)
```

`TraceReport` fields:

| Field | Description |
|---|---|
| `root` | Root `TraceNode` of the call tree |
| `total_elapsed` | Wall-clock time for the entire traced job |
| `critical_path` | Longest path from root to leaf (the chain you'd optimize first) |
| `bottleneck` | Node with the largest self-time (`elapsed - sum(children)`) |
| `call_graph` | Adjacency map: `job_id → [child_job_ids]` |

`find_bottleneck(report)` returns a human-readable string. When one node dominates (>50% of total), it calls it out. Otherwise it reports "Balanced."

### Trace depth limiting

Nested traces can grow unbounded. `max_trace_depth` (default 10) caps the nesting. Subtrees beyond the limit are replaced with `{"_truncated": true}` — you know something was there, but the payload stays bounded.

```py
jp = create_jobping(broker_port=8900, max_trace_depth=5)
```

## Benchmarks

All tests: 20 s simulated work per request.

### Python ↔ Python (200 concurrent)

| Group | Server | Client | Elapsed | Peak handlers | Success |
|---|---|---|---|---|---|
| Control | FastAPI | httpx | 40.28 s | 100 | 200/200 |
| Control | FastAPI | httpx + JP | 40.30 s | 100 | 200/200 |
| Experiment | FastAPI + JP | httpx | 40.30 s | 100 | 200/200 |
| **Experiment** | **FastAPI + JP** | **httpx + JP** | **20.35 s** | **3** | **200/200** |

httpx connection pool (100) is the bottleneck. JobPing on both sides eliminates it.

### Node ↔ Node (1000 concurrent)

| Group | Server | Client | Elapsed | Peak handlers | Success |
|---|---|---|---|---|---|
| Control | Node | Node | 20.54 s | 1000 | 1000/1000 |
| Control | Node | Node + JP | 20.55 s | 1000 | 1000/1000 |
| Experiment | Node + JP | Node | 20.34 s | 1000 | 1000/1000 |
| **Experiment** | **Node + JP** | **Node + JP** | **20.30 s** | **1** | **1000/1000** |

Single-threaded Node. 1000 connections → 1 with JobPing on both sides.

### Node server + Python client (200 concurrent)

| Group | Server | Client | Elapsed | Peak handlers | Success |
|---|---|---|---|---|---|
| Control | Node | httpx | 40.29 s | 100 | 200/200 |
| Control | Node | httpx + JP | 40.29 s | 100 | 200/200 |
| Experiment | Node + JP | httpx | 40.29 s | 100 | 200/200 |
| **Experiment** | **Node + JP** | **httpx + JP** | **20.33 s** | **1** | **200/200** |

Cross-runtime: httpx pool still governs control. JobPing eliminates the bottleneck across languages.

### Python server + Node client (200 concurrent)

| Group | Server | Client | Elapsed | Peak handlers | Success |
|---|---|---|---|---|---|
| Control | FastAPI | Node | 20.15 s | 200 | 200/200 |
| Control | FastAPI | Node + JP | 20.11 s | 200 | 200/200 |
| Experiment | FastAPI + JP | Node | 20.11 s | 200 | 200/200 |
| **Experiment** | **FastAPI + JP** | **Node + JP** | **20.13 s** | **177** | **200/200** |

Node's unbounded concurrency floods all 200 requests simultaneously — `max_active` captures sub-millisecond handler overlap. Connections still release instantly.

### Python server + Node client (1000 concurrent)

| Group | Server | Client | Elapsed | Peak handlers | Success |
|---|---|---|---|---|---|
| Control | FastAPI | Node | 20.53 s | 1000 | 1000/1000 |
| Control | FastAPI | Node + JP | 20.52 s | 1000 | 1000/1000 |
| Experiment | FastAPI + JP | Node | 20.52 s | 1000 | 1000/1000 |
| **Experiment** | **FastAPI + JP** | **Node + JP** | **20.66 s** | **954** | **1000/1000** |

Burst overlap pushes `max_active` to 954 — this is instantaneous peak overlap, not steady-state holding. Control holds 1000 connections for 20 s; experiment releases in milliseconds.

## Customization

`create_jobping` accepts these optional parameters:

```py
jp = create_jobping(
    broker_port=8900,
    peer_brokers=["http://peer:8890"],   # other brokers to connect to
    idle_timeout_seconds=300,            # auto-disconnect after idle (None = never)
    max_trace_depth=10,                  # max nesting depth for trace payloads
    job_context_provider=my_provider,    # custom job_id extraction from requests
    sio_kwargs={"engineio_logger": True},
)
```

**job_context_provider:** A callable `(*args, **kwargs) -> str | None` that extracts the job_id from incoming request arguments. The default inspects request headers for `x-jobping-job-id`. Return `None` to run the handler normally (no JobPing wrapping).

**Advanced — construct directly:** When you need full control over transports and queues, construct `JobPing` manually:

```py
from jobping import JobPing, EndpointProxy, StateSync, ResultHandoff
from jobping.imp import TransportLayerWS, JPItemQueueInMemory, EnvelopeEndpointInMemory

transport = TransportLayerWS("http://broker:8890")
endpoint_proxy = EndpointProxy(
    state_sync=StateSync(transport),
    result_handoff=ResultHandoff(transport),
    queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()),
)
jp = JobPing(endpoint_proxy=endpoint_proxy, job_context_provider=my_provider)
```

## Design

### Producer / consumer, not server / client

"Server" and "client" are deployment labels. What matters is protocol role: who produces a value, and who waits for it. The same endpoint can be a producer in one interaction and a consumer in another — a server awaiting browser input is a consumer; a browser awaiting server computation is also a consumer. JobPing uses producer/consumer naming where behavior is actually symmetric.

### Layers

| Layer | Responsibility |
|---|---|
| `StateSync` | Lightweight job status updates (`job_id + status`) |
| `ResultHandoff` | Boxed result delivery (may need stronger reliability) |
| `TransportLayer` | Wire-level message movement (WebSocket, HTTP, Kafka, etc.) |

`StateSync` and `ResultHandoff` are independent peers. They can share a transport or use separate ones.

### Guarantees

- **Failure passthrough:** if your function throws, JobPing propagates the exception. It never converts errors into success-shaped payloads.
- **Shape transparency:** the result envelope is completely agnostic to your return type.

## Project layout

| Directory | Purpose |
|---|---|
| `packages/js/` | JavaScript SDK (npm: `jobping`) |
| `packages/python/jobping/` | Python SDK (pip: `jobping`) |
| `examples/control_group/` | Baseline without JobPing |
| `examples/experiment_group/` | Runnable examples with JobPing |
| `sandbox/` | Mock transports and factories |
| `tests/` | Regression tests |

## License

See repository files.
