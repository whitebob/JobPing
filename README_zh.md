# JobPing

JobPing is a small endpoint rendezvous bridge for `JPItem` state synchronization and result handoff. It is not a queue system, worker framework, scheduler, or background job platform.

The core goal is narrow: move necessary waiting away from a remote application connection and onto a local endpoint wait point, while preserving the wrapped service's original input, output, and failure semantics.

## Current layout

The current branch separates SDK code, runnable examples, tests, and mock-only helpers:

- `packages/js/`: JavaScript SDK classes.
- `packages/python/jobping/`: Python SDK package.
- `examples/control_group/`: baseline FastAPI + JavaScript client behavior without JobPing.
- `examples/experiment_group/`: runnable example wiring around the current SDK shape.
- `sandbox/js/` and `sandbox/python/jobping_sandbox/`: mock-only implementations and factories used by examples/tests.
- `tests/js/` and `tests/python/`: regression tests split by runtime.

## 性能基准

200 并发请求，每个请求模拟 20 s 工作（httpx 默认连接池 100）。

| 组别 | 服务端 | 客户端 | 耗时 | 最大并发 | 成功率 |
|---|---|---|---|---|---|
| 对照 | FastAPI（无 JP） | httpx | 40.28 s | 100 | 200/200 |
| 对照 | FastAPI（无 JP） | httpx + JP | 40.30 s | 100 | 200/200 |
| 实验 | FastAPI + JP | httpx | 40.30 s | 100 | 200/200 |
| **实验** | **FastAPI + JP** | **httpx + JP** | **20.35 s** | **3** | **200/200** |

当双方都使用 JobPing 时，服务端立即返回 job_ref 并释放 HTTP 连接，客户端连接池在毫秒级回收，而非被占用 20 s。这消除了连接瓶颈：200 个并发请求在 20 s 内完成，服务端同时活跃的 HTTP handler 峰值仅为 3。

Node.js / 1000 并发

| 组别 | 服务端 | 客户端 | 耗时 | 最大并发 | 成功率 |
|---|---|---|---|---|---|
| 对照 | Node（无 JP） | Node | 20.54 s | 1000 | 1000/1000 |
| 对照 | Node（无 JP） | Node + JP | 20.55 s | 1000 | 1000/1000 |
| 实验 | Node + JP | Node | 20.34 s | 1000 | 1000/1000 |
| **实验** | **Node + JP** | **Node + JP** | **20.30 s** | **1** | **1000/1000** |

单线程 Node 上 1000 并发请求。不使用 JobPing 时，服务端需同时维持 1000 个 HTTP 长连接 20 s。双方使用 JobPing 后，连接在毫秒级释放——活跃 handler 峰值从 1000 降至 1。

Node 服务端 + Python 客户端 / 200 并发

| 组别 | 服务端 | 客户端 | 耗时 | 最大并发 | 成功率 |
|---|---|---|---|---|---|
| 对照 | Node（无 JP） | httpx | 40.29 s | 100 | 200/200 |
| 对照 | Node（无 JP） | httpx + JP | 40.29 s | 100 | 200/200 |
| 实验 | Node + JP | httpx | 40.29 s | 100 | 200/200 |
| **实验** | **Node + JP** | **httpx + JP** | **20.33 s** | **1** | **200/200** |

跨运行时：Node 服务端 + Python 客户端。httpx 连接池限制（100）仍然支配对照组的耗时。双方使用 JobPing 后，跨语言场景下同样消除了连接瓶颈——200 请求 20 s 完成，活跃 handler 峰值仅 1。

Python 服务端 + Node 客户端 / 200 并发

| 组别 | 服务端 | 客户端 | 耗时 | 最大并发 | 成功率 |
|---|---|---|---|---|---|
| 对照 | FastAPI（无 JP） | Node | 20.15 s | 200 | 200/200 |
| 对照 | FastAPI（无 JP） | Node + JP | 20.11 s | 200 | 200/200 |
| 实验 | FastAPI + JP | Node | 20.11 s | 200 | 200/200 |
| 实验 | FastAPI + JP | Node + JP | 20.13 s | 177 | 200/200 |

跨运行时：Python 服务端 + Node 客户端。Node 无连接池限制，200 请求瞬间涌入，实验组 handler 在微秒窗口内大量重叠，max_active 被推到 177。核心收益不变：handler 毫秒级释放而非持有 20 s。

Python 服务端 + Node 客户端 / 1000 并发

| 组别 | 服务端 | 客户端 | 耗时 | 最大并发 | 成功率 |
|---|---|---|---|---|---|
| 对照 | FastAPI（无 JP） | Node | 20.53 s | 1000 | 1000/1000 |
| 对照 | FastAPI（无 JP） | Node + JP | 20.52 s | 1000 | 1000/1000 |
| 实验 | FastAPI + JP | Node | 20.52 s | 1000 | 1000/1000 |
| 实验 | FastAPI + JP | Node + JP | 20.66 s | 954 | 1000/1000 |

Node 客户端 1000 并发打入单进程 FastAPI。burst 重叠将实验组的 max_active 推至 954，印证该计数器捕捉的是瞬时峰值而非稳态连接占用。对照组 1000 连接被占满 20 s；实验组毫秒级释放。

## Current design lens

The important symmetry is not `server` versus `client`. Those are deployment roles. The protocol role that matters is whether an endpoint is producing a value later or waiting for a peer to produce it.

An endpoint may be a producer in one interaction and a consumer in another:

- browser/client waits for server result
- server waits for browser/client-provided content
- server waits for server
- Python waits for Node, or Node waits for Python

This means JobPing APIs should avoid server/client-specific names where the behavior is actually symmetric.

## JPItem queue semantics

The current mock API uses producer/consumer rendezvous names:

| Role | Flow | Meaning |
|---|---|---|
| Producer endpoint | `offer -> defer -> fulfill` | This endpoint promises to produce a result later, optionally defers work, then fulfills the `JPItem`. |
| Consumer endpoint | `accept -> awaitResult -> release` | This endpoint accepts a peer's `job_ref`, waits for fulfillment, then releases local ownership. |

Preferred public vocabulary:

- `offer(job_id)`: create a producer-side `JPItem`.
- `accept(job_id)`: create a consumer-side `JPItem` from a peer offer.
- `defer(job_id | item)`: mark an offered item as deferred work.
- `makeJobRef(job_id)` / `make_job_ref(job_id)`: create wrapper-facing rendezvous signaling for an offered job.
- `isJobRef(value)` / `is_job_ref(value)`: detect wrapper-facing job references without treating them as result envelopes.
- `fulfill(job_id, result)`: box and send the result through the result handoff layer.
- `fulfillLater(job_id, task)` / `fulfill_later(job_id, task)`: run producer work through the proxy handoff helper and fulfill the item.
- `awaitResult(job_id)` / `await_result(job_id)`: wait for a result envelope and unbox it.
- `release(job_id)`: remove endpoint ownership once the item is no longer needed.

`fulfillLater` is intentionally still a mock-level helper. It records the intended producer-work-to-result-handoff semantics without locking down the final scheduler API.

## Semantic services and transport

JobPing separates semantic services from transport mechanisms:

| Layer | Responsibility |
|---|---|
| `StateSync` | Synchronizes `job_id + status + state_context`. |
| `ResultHandoff` | Transfers `job_id + result` ownership/availability. |
| `TransportLayer` | Moves messages through HTTP, WebSocket, SSE+POST, Kafka, Redis, RabbitMQ, or another carrier. |

`StateSync` and `ResultHandoff` are peers. They may share one transport implementation, but they do not have to. Status updates are often lightweight and frequent, while result handoff may need stronger reliability, larger payload support, or a different storage/retrieval path.

## Envelope mock semantics

The envelope layer is result-shape-neutral. It does not know HTTP, WebSocket, FastAPI, fetch, routing, status state machines, or any business-specific result shape.

Current result envelope operations:

- `boxResult` / `box_result`
- `isEnvelope` / `is_envelope`
- `isResultEnvelope` / `is_result_envelope`
- `unboxResult` / `unbox_result`
- `MockEnvelopeEndpoint.send`
- `MockEnvelopeEndpoint.recv`

`job_ref` and routing belong closer to `TransportLayer`/`EndpointProxy` signaling than to result envelope semantics.

## JobPing facade

`JobPing` is the top-level wrapper facade. Its public surface is intentionally small: `wrap(...)`.

Current role-specific behavior lives at the facade edge:

- client-side `JobPing.wrap(callable)` calls the opaque callable, detects a returned `job_ref`, then uses `EndpointProxy` to `accept -> awaitResult -> release`.
- server-side `JobPing.wrap()(callable)` inspects the injected job context provider; with a job context it `offer -> defer -> fulfill_later` and returns a `job_ref`, otherwise it calls the opaque callable normally.

## Job IDs and transport layers

`job_id` generation is not mocked. Current JavaScript and Python helpers use UUID v4 directly.

`TransportLayer` is now the formal abstract boundary for moving JobPing metadata and semantic-service messages. It remains deliberately thin: it does not manage JPItem lifecycle or inspect business results. `TransportLayerMock` is the current concrete test implementation, using header-like metadata and in-memory message queues.

## Failure semantics

JobPing should not convert producer exceptions into success-shaped payloads. The principle is: if the wrapped service would have failed before JobPing, it should still fail after JobPing.

## Unload switch

JobPing follows a scout rule: adding it should not make the system harder to debug. If a JobPing boundary exception is confusing, developers should be able to unload JobPing and compare against the original call path.

Current mock unload controls:

- Python/server side: set `JOBPING_DISABLED=1`.
- JavaScript side: set `JOBPING_DISABLED=1` or `globalThis.__JOBPING_DISABLED__ = true`.

When disabled at the `wrap` entry point, JobPing performs no capture, envelope, JPItem, print, or queue behavior. It calls the wrapped callable directly.

