# JobPing

JobPing 让服务器在高负载下保持响应。它包裹你现有的请求处理函数，使长时间运行的工作不再占用 HTTP 连接。

支持 Python、Node.js 和浏览器——任意组合。

## 为什么用它？

如果你的服务器需要几秒（甚至几分钟）来处理一个请求，每个并发请求都会全程占用一个连接。吞吐量被连接池上限卡死。

JobPing 让服务器立即返回一张"票据"，在后台完成工作，结果就绪后再交付。

| 场景 | 不用 JobPing | 用 JobPing |
|---|---|---|
| Python，200 并发，20 s 工作 | 40 s，100 连接被占满 | **20 s，峰值 3 个活跃连接** |
| Node，1000 并发，20 s 工作 | 1000 连接被占满 | **峰值 1 个活跃连接** |

同样的工作，同样的结果。只是你的服务器不再是瓶颈。

## 工作原理

```
请求到达 → handler 返回票据 (job_ref) → 连接立即释放
                            ↓
          工作后台运行 → 结果放入共享队列
                            ↓
          客户端获取结果 → 拆箱 → 调用方拿到正常响应
```

- **干活的一方**（producer）：`offer` → `defer` → `fulfill`
- **等待的一方**（consumer）：`accept` → `awaitResult` → `release`

`wrap()` 自动处理两端。你只需包裹现有函数，JobPing 透明地处理票据交换。

## 快速开始

```py
from jobping import create_jobping
jp = create_jobping(broker_port=8900)
```

```js
import * as jp from "jobping";
const jobping = jp.createJobPing({ brokerPort: 8900 });
```

每个节点运行自己的嵌入式 broker。`broker_port` 是本地 Socket.IO broker 的 TCP 端口。要连接其他节点，传入它们的 broker URL：

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

## 用法

### 服务端

包裹你的 handler。如果请求携带 JobPing 头，handler 返回票据并在后台完成任务。没有 JobPing 头？正常运行——零行为变化。

```py
@app.get("/work")
async def work(request_id: str, sleep_seconds: float):
    return await jp.wrap(do_work)(request_id, sleep_seconds)
```

```js
// 在 HTTP handler 内：
const result = await endpointProxy.wrap(doWork)(requestId, sleepSeconds);
res.end(JSON.stringify(result));
```

### 客户端

包裹你的 fetch。如果服务端返回票据，JobPing 透明地等待真实结果。

```js
const callServer = jobping.wrap(async (id) => {
  const resp = await fetch(`/work?request_id=${id}`);
  return resp.json();
});
const result = await callServer(42);  // 你的代码拿到的是最终结果
```

### 浏览器

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

### 调试：运行时禁用

```
JOBPING_DISABLED=1
```

或 JS 中：`globalThis.__JOBPING_DISABLED__ = true`。`wrap()` 变为无操作透传——你的 callable 完全按没有 JobPing 的方式运行。无需改代码即可 A/B 对比。

## 空闲超时

与 peer broker 的 WebSocket 连接在空闲一段时间后自动断开，释放不再需要的资源。下次发送消息时连接会透明地重新建立。

```py
# 空闲 5 分钟后断开（默认 300 秒）
jp = create_jobping(broker_port=8900, idle_timeout_seconds=300)

# 永不断开
jp = create_jobping(broker_port=8900, idle_timeout_seconds=None)
```

```js
const jobping = jp.createJobPing({ brokerPort: 8900, idleTimeoutSeconds: 300 });
```

每次 `send_message` 调用都会重置空闲计时器。后台 watcher 以超时间隔的一半为周期检查，如果无活动则断开连接。断开是干净的——Socket.IO 客户端优雅关闭，watcher 任务自行取消。

已断开的 peer 不会从路由表中移除。当有消息需要发送到已断开的 peer 时，broker 会按需重连。

## 调用链追踪（Trace）

Trace 让你看清分布式调用树中时间花在了哪里。它是**按 Job 粒度**启用的——你只为特定 Job 开启追踪，不影响其他调用的性能。当父 Job 启用了追踪，子 Job 自动继承，确保你拿到完整的调用链画面。

### 启用 Trace

两种开启方式：

| 方式 | 适用场景 |
|---|---|
| `@jp.wrap_trace()` | 你控制入口点。此 handler 始终开启 trace。 |
| `@jp.wrap()` + `x-jobping-trace-enabled: 1` 头 | 子 Job 从上游继承 trace。 |

**`wrap_trace()`** — 显式开启。用在你想开始追踪的最外层 handler 上：

```py
@app.get("/debug/work")
@jp.wrap_trace()
async def do_work(request: Request, request_id: int) -> dict:
    ...
```

通过此 handler 的每次调用都会记录 trace，无视请求头。`hop` 计数器从 1 开始。

**`wrap()` + 请求头** — 继承传播。当用 `@jp.wrap()` 包裹的 handler 收到 `x-jobping-trace-enabled: 1`（或 `true`）请求头时，它为此 Job 激活追踪，并将请求头转发给所有嵌套的 JobPing 调用。这样在入口处启动的 trace 就能传播到整个调用树：

```
Client（已启用 trace）
  → Service A  @jp.wrap()  + 请求头 → trace 开启，hop=1
    → Service B  @jp.wrap()  + 请求头 → trace 开启，hop=2
      → Service C  @jp.wrap()  无请求头 → trace 关闭（正常路径）
```

承载 trace 标志的 ContextVar 在 async task 之间相互隔离——两个并发请求互不干扰。

### 读取 Trace 数据

当带 trace 的 Job 完成时，trace 载荷会附加到 fulfill 结果中。使用 `parse_trace` 将原始 dict 转为结构化报告：

```py
from jobping.trace import parse_trace, find_bottleneck

# 原始 trace dict（附加在 fulfill 中，可按需存储）
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
print(f"总耗时: {report.total_elapsed:.2f}s")
print(f"关键路径: {' → '.join(n.peer_id for n in report.critical_path)}")
print(find_bottleneck(report))
# Bottleneck: worker-1 (job def456) — self_time=1.80s (72% of total 2.50s)
```

`TraceReport` 字段：

| 字段 | 描述 |
|---|---|
| `root` | 调用树的根 `TraceNode` |
| `total_elapsed` | 整个被追踪 Job 的墙上时间 |
| `critical_path` | 从根到叶的最长路径（应优先优化的调用链） |
| `bottleneck` | 自身耗时最大（`elapsed - 子节点耗时之和`）的节点 |
| `call_graph` | 邻接表：`job_id → [子 job_id 列表]` |

`find_bottleneck(report)` 返回人类可读的摘要。当某个节点的自身耗时超过总耗时 50% 时，明确指出瓶颈；否则报告"Balanced"。

### Trace 深度限制

嵌套 trace 可能无限增长。`max_trace_depth`（默认 10）限制嵌套深度。超出限制的子树被替换为 `{"_truncated": true}`——你知道那里有东西，但载荷保持有界。

```py
jp = create_jobping(broker_port=8900, max_trace_depth=5)
```

## 性能基准

所有测试：每个请求模拟 20 s 工作。

### Python ↔ Python（200 并发）

| 组别 | 服务端 | 客户端 | 耗时 | 峰值 handler | 成功率 |
|---|---|---|---|---|---|
| 对照 | FastAPI | httpx | 40.28 s | 100 | 200/200 |
| 对照 | FastAPI | httpx + JP | 40.30 s | 100 | 200/200 |
| 实验 | FastAPI + JP | httpx | 40.30 s | 100 | 200/200 |
| **实验** | **FastAPI + JP** | **httpx + JP** | **20.35 s** | **3** | **200/200** |

httpx 连接池（100）是瓶颈所在。双方使用 JobPing 后瓶颈消失。

### Node ↔ Node（1000 并发）

| 组别 | 服务端 | 客户端 | 耗时 | 峰值 handler | 成功率 |
|---|---|---|---|---|---|
| 对照 | Node | Node | 20.54 s | 1000 | 1000/1000 |
| 对照 | Node | Node + JP | 20.55 s | 1000 | 1000/1000 |
| 实验 | Node + JP | Node | 20.34 s | 1000 | 1000/1000 |
| **实验** | **Node + JP** | **Node + JP** | **20.30 s** | **1** | **1000/1000** |

单线程 Node。双方使用 JobPing 后，1000 连接降至 1。

### Node 服务端 + Python 客户端（200 并发）

| 组别 | 服务端 | 客户端 | 耗时 | 峰值 handler | 成功率 |
|---|---|---|---|---|---|
| 对照 | Node | httpx | 40.29 s | 100 | 200/200 |
| 对照 | Node | httpx + JP | 40.29 s | 100 | 200/200 |
| 实验 | Node + JP | httpx | 40.29 s | 100 | 200/200 |
| **实验** | **Node + JP** | **httpx + JP** | **20.33 s** | **1** | **200/200** |

跨运行时：httpx 连接池仍然支配对照组。JobPing 跨语言消除瓶颈。

### Python 服务端 + Node 客户端（200 并发）

| 组别 | 服务端 | 客户端 | 耗时 | 峰值 handler | 成功率 |
|---|---|---|---|---|---|
| 对照 | FastAPI | Node | 20.15 s | 200 | 200/200 |
| 对照 | FastAPI | Node + JP | 20.11 s | 200 | 200/200 |
| 实验 | FastAPI + JP | Node | 20.11 s | 200 | 200/200 |
| **实验** | **FastAPI + JP** | **Node + JP** | **20.13 s** | **177** | **200/200** |

Node 无连接池限制，200 请求瞬间涌入——`max_active` 捕捉的是亚毫秒级的 handler 重叠。连接仍然瞬间释放。

### Python 服务端 + Node 客户端（1000 并发）

| 组别 | 服务端 | 客户端 | 耗时 | 峰值 handler | 成功率 |
|---|---|---|---|---|---|
| 对照 | FastAPI | Node | 20.53 s | 1000 | 1000/1000 |
| 对照 | FastAPI | Node + JP | 20.52 s | 1000 | 1000/1000 |
| 实验 | FastAPI + JP | Node | 20.52 s | 1000 | 1000/1000 |
| **实验** | **FastAPI + JP** | **Node + JP** | **20.66 s** | **954** | **1000/1000** |

Burst 重叠将 `max_active` 推至 954——这是瞬时峰值重叠，而非稳态连接占用。对照组 1000 连接被占满 20 s；实验组毫秒级释放。

## 自定义

`create_jobping` 支持以下可选参数：

```py
jp = create_jobping(
    broker_port=8900,
    peer_brokers=["http://peer:8890"],   # 要连接的其他 broker
    idle_timeout_seconds=300,            # 空闲超时自动断开（None = 不断开）
    max_trace_depth=10,                  # trace 载荷最大嵌套深度
    job_context_provider=my_provider,    # 自定义从请求中提取 job_id 的方式
    sio_kwargs={"engineio_logger": True},
)
```

**job_context_provider：** 一个可调用对象 `(*args, **kwargs) -> str | None`，从传入的请求参数中提取 job_id。默认从请求头中检查 `x-jobping-job-id`。返回 `None` 则正常执行 handler（不启用 JobPing 包裹）。

**高级 — 直接构造：** 需要完全控制传输层和队列时，手动构造 `JobPing`：

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

## 设计

### Producer / consumer，而非 server / client

"服务端"和"客户端"是部署标签。真正重要的是协议角色：谁产出值，谁等待值。同一个端点可以在一次交互中是 producer，另一次是 consumer——等待浏览器输入的服务端是 consumer，等待服务端计算的浏览器同样是 consumer。JobPing 在行为对称的地方使用 producer/consumer 命名。

### 分层

| 层 | 职责 |
|---|---|
| `StateSync` | 轻量级 job 状态更新（`job_id + status`） |
| `ResultHandoff` | 装箱结果交付（可能需要更强的可靠性） |
| `TransportLayer` | 传输层消息移动（WebSocket、HTTP、Kafka 等） |

`StateSync` 和 `ResultHandoff` 是独立的对等层，可以共享传输也可以分开。

### 保障

- **失败透传：** 如果你的函数抛出异常，JobPing 原样传播。绝不将错误转换为成功形态的载荷。
- **形状无关：** 传输结果的信封层完全不关心你的返回类型。

## 项目结构

| 目录 | 用途 |
|---|---|
| `packages/js/` | JavaScript SDK（npm: `jobping`） |
| `packages/python/jobping/` | Python SDK（pip: `jobping`） |
| `examples/control_group/` | 不含 JobPing 的基线 |
| `examples/experiment_group/` | 含 JobPing 的可运行示例 |
| `sandbox/` | Mock 传输和工厂 |
| `tests/` | 回归测试 |

## 许可证

Apache License 2.0 — 详见 [LICENSE](LICENSE)。
