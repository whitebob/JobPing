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
jp = create_jobping()
```

```js
import * as jp from "jobping";
const jobping = jp.createJobPing();
```

No arguments needed. Defaults: WebSocket transport at `JOBPING_WS_URL` (default `http://127.0.0.1:8890`) and an in-memory queue. Set `JOBPING_WS_URL` to point at your broker.

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

Pass explicit transports and queue to override defaults:

```py
jp = create_jobping(
  status_transport_layer=my_ws_transport,
  result_transport_layer=my_http_transport,
  queue=my_queue,
)
```

**Defaults:** `StateSync` uses WebSocket; `ResultHandoff` uses HTTP. The default queue is a shared module-level `JPItemQueueInMemory` — fine for development. Pass an explicit instance when you need per-instance isolation.

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
