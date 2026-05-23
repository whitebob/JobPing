# JobPing

JobPing 是一个轻量级的端点会合桥接层，用于 `JPItem` 状态同步和结果交接。它不是队列系统、worker 框架、调度器或后台任务平台。

核心目标很窄：将必要的等待从远端应用连接转移到本地端点的等待点，同时保持被包裹服务的原始输入、输出和失败语义。

## 快速开始

为示例和开发环境创建使用合理默认值的 JobPing：

```py
from jobping import create_jobping
jp = create_jobping()
```

```js
import * as jp from "jobping";
const jobping = jp.createJobPing();
```

### 默认值

- status transport（StateSync）：WebSocket `TransportLayerWS`，通过 `JOBPING_WS_URL` 配置（默认：`http://127.0.0.1:8890`）
- result transport（ResultHandoff）：HTTP `TransportLayerHTTPS`，通过 `JOBPING_HTTP_BASE` 配置（默认：与 WS URL 相同）
- queue：`DEFAULT_QUEUE = JPItemQueueInMemory(EnvelopeEndpointInMemory())`

### 关于 DEFAULT_QUEUE 的重要说明

`DEFAULT_QUEUE` 是模块级别的共享可变实例。调用 `create_jobping()` 而不传入 queue 意味着多个 JobPing 实例将共享同一个内存队列。这是为示例和快速评估有意设计的。如果需要按 JobPing 隔离（独立队列、不同生命周期或优先级域），请传入显式的 queue 实例：

```py
from jobping.jpitem_queue import JPItemQueueInMemory
from jobping.envelope_endpoint import EnvelopeEndpointInMemory

jp = create_jobping(queue=JPItemQueueInMemory(EnvelopeEndpointInMemory()))
```

### 自定义

向 `create_jobping` 传入显式的 transport 和 queue 以覆盖默认值：

```py
jp = create_jobping(
  status_transport_layer=my_ws_transport,
  result_transport_layer=my_http_transport,
  queue=my_queue,
)
```

## 当前目录结构

当前分支将 SDK 代码、可运行示例、测试和 mock 辅助模块分开：

- `packages/js/`：JavaScript SDK 类。
- `packages/python/jobping/`：Python SDK 包。
- `examples/control_group/`：不含 JobPing 的基线 FastAPI + JavaScript 客户端行为。
- `examples/experiment_group/`：基于当前 SDK 形态的可运行示例。
- `sandbox/js/` 和 `sandbox/python/jobping_sandbox/`：示例/测试使用的 mock 实现和工厂。
- `tests/js/` 和 `tests/python/`：按运行时分拆的回归测试。

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

## 设计视角

重要的对称性不在于 `服务端` 与 `客户端` 之分——那是部署角色。真正重要的协议角色是：端点是稍后产出值，还是在等待对端产出值。

同一个端点可以在一次交互中是 producer，在另一次中是 consumer：

- 浏览器/客户端等待服务端结果
- 服务端等待浏览器/客户端提供的内容
- 服务端等待服务端
- Python 等待 Node，或 Node 等待 Python

因此 JobPing 的 API 应在行为实际对称的地方避免使用 server/client 特定的命名。

## JPItem 队列语义

当前 mock API 使用 producer/consumer 会合命名：

| 角色 | 流程 | 含义 |
|---|---|---|
| Producer 端点 | `offer -> defer -> fulfill` | 该端点承诺稍后产出结果，可选推迟工作，然后 fulfill `JPItem`。 |
| Consumer 端点 | `accept -> awaitResult -> release` | 该端点接受对端的 `job_ref`，等待 fulfill，然后释放本地所有权。 |

推荐的公开术语：

- `offer(job_id)`：创建一个 producer 侧的 `JPItem`。
- `accept(job_id)`：根据对端 offer 创建一个 consumer 侧的 `JPItem`。
- `defer(job_id | item)`：将已 offer 的 item 标记为延迟工作。
- `makeJobRef(job_id)` / `make_job_ref(job_id)`：为已 offer 的 job 创建面向 wrapper 的会合信号。
- `isJobRef(value)` / `is_job_ref(value)`：检测面向 wrapper 的 job 引用，而非将其当作结果 envelope 处理。
- `fulfill(job_id, result)`：装箱并通过 result handoff 层发送结果。
- `fulfillLater(job_id, task)` / `fulfill_later(job_id, task)`：通过 proxy handoff 辅助方法执行 producer 工作并 fulfill item。
- `awaitResult(job_id)` / `await_result(job_id)`：等待 result envelope 并拆箱。
- `release(job_id)`：在 item 不再需要后移除端点所有权。

`fulfillLater` 目前仍是 mock 级别的辅助方法。它记录了预期的 producer 工作到结果交接的语义，但不锁定最终的调度器 API。

## 语义服务与传输

JobPing 将语义服务与传输机制分离：

| 层 | 职责 |
|---|---|
| `StateSync` | 同步 `job_id + status + state_context`。 |
| `ResultHandoff` | 传输 `job_id + result` 的所有权/可用性。 |
| `TransportLayer` | 通过 HTTP、WebSocket、SSE+POST、Kafka、Redis、RabbitMQ 或其他载体移动消息。 |

`StateSync` 和 `ResultHandoff` 是对等的。它们可以共享一个传输实现，但不是必须的。状态更新通常轻量且频繁，而 result handoff 可能需要更强的可靠性、更大的载荷支持或不同的存储/检索路径。

## Envelope mock 语义

envelope 层是结果形状无关的。它不了解 HTTP、WebSocket、FastAPI、fetch、路由、状态机或任何业务相关的返回结构。

当前 result envelope 操作：

- `boxResult` / `box_result`
- `isEnvelope` / `is_envelope`
- `isResultEnvelope` / `is_result_envelope`
- `unboxResult` / `unbox_result`
- `MockEnvelopeEndpoint.send`
- `MockEnvelopeEndpoint.recv`

`job_ref` 和路由更接近 `TransportLayer`/`EndpointProxy` 的信号层，而非 result envelope 语义。

## JobPing 门面

`JobPing` 是顶层 wrapper 门面。其公开接口刻意保持精简：`wrap(...)`。

当前角色特定的行为位于门面边缘：

- 客户端 `JobPing.wrap(callable)` 调用不透明的 callable，检测返回的 `job_ref`，然后通过 `EndpointProxy` 执行 `accept -> awaitResult -> release`。
- 服务端 `JobPing.wrap()(callable)` 检查注入的 job context provider；如果有 job context，则 `offer -> defer -> fulfill_later` 并返回 `job_ref`；否则正常调用不透明的 callable。

## Job ID 与 transport layer

`job_id` 生成不是 mock 的。当前 JavaScript 和 Python 辅助方法直接使用 UUID v4。

`TransportLayer` 现在是移动 JobPing 元数据和语义服务消息的正式抽象边界。它刻意保持很薄：不管理 JPItem 生命周期，也不检查业务结果。`TransportLayerMock` 是当前的测试用具体实现，使用类 header 的元数据和内存消息队列。

## 失败语义

JobPing 不应将 producer 异常转换为成功形态的载荷。原则是：如果被包裹的服务在引入 JobPing 之前会失败，那么在引入 JobPing 之后它仍然应该失败。

## 卸载开关

JobPing 遵循侦察兵原则：加入它不应使系统更难调试。如果 JobPing 边界异常令人困惑，开发者应能卸载 JobPing 并与原始调用路径进行对比。

当前 mock 卸载控制：

- Python/服务端：设置 `JOBPING_DISABLED=1`。
- JavaScript 端：设置 `JOBPING_DISABLED=1` 或 `globalThis.__JOBPING_DISABLED__ = true`。

当在 `wrap` 入口点禁用时，JobPing 不执行任何 capture、envelope、JPItem、print 或队列行为。它直接调用被包裹的 callable。

## 示例和测试

示例和测试位于 `examples/` 和 `tests/` 目录下。

## 许可证 / 贡献

贡献指南和许可证说明请参见仓库中的相关文件。
