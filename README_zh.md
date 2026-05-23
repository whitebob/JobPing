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
jp = create_jobping()
```

```js
import * as jp from "jobping";
const jobping = jp.createJobPing();
```

无需参数。默认使用 WebSocket 传输（`JOBPING_WS_URL`，默认 `http://127.0.0.1:8890`）和内存队列。设置 `JOBPING_WS_URL` 指向你的 broker 即可。

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

传入显式的 transport 和 queue 覆盖默认值：

```py
jp = create_jobping(
  status_transport_layer=my_ws_transport,
  result_transport_layer=my_http_transport,
  queue=my_queue,
)
```

**默认值：** `StateSync` 使用 WebSocket；`ResultHandoff` 使用 HTTP。默认 queue 是模块级共享的 `JPItemQueueInMemory`——适合开发使用。需要实例隔离时传入独立的 queue。

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

参见仓库文件。
