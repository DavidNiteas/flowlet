# Edge 模式

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

Edge 模式是 flowlet 中一套**有状态、命令式、异步执行**的节点抽象。每个 `EdgeNode` 对应一个真实执行后端：

- `ThreadEdgeNode`：同进程 worker 线程，无需序列化，适合同步任务和调试。
- `AsyncioEdgeNode`：独立 asyncio event loop 线程，支持 awaitable initializer / target，适合异步 IO 状态节点。
- `RayEdgeNode`：独立 Ray Actor，可跨进程持有不可序列化对象。

三种后端共享同一套 API，区别在于执行位置、是否支持 awaitable、是否跨进程序列化。

---

## 什么时候用 Edge 模式？

| 场景 | 推荐后端 |
|------|----------|
| 需要持有不可序列化的 C 对象 / 大模型句柄 | `RayEdgeNode` |
| 想手动控制数据存在哪个进程 | `RayEdgeNode` |
| 本地同步逻辑、快速调试、无需跨进程 | `ThreadEdgeNode` |
| async initializer / async target / 异步 IO 状态节点 | `AsyncioEdgeNode` |
| 中间产物不可序列化，但想持续操作它 | `RayEdgeNode` |

如果你要的是**声明式、无状态、一次性执行完整 DAG**，请使用 [`flowlet.compute_graph`](compute_graph.md)。

---

## 核心概念

```
主进程                 后端进程
--------               --------
EdgeNode  ──remote──>  Worker / Actor
  │                        │
  │ push / apply / pull    │ 持有 state（C 对象、数据、配置…）
  │                        │
  │ join / close / kill    │ 执行函数 / Kernel / Workflow / 任务图
```

- **state**：节点内部的数据包，是一个 namespace dict。
- **push / pull**：固定 IO 操作。
- **apply / bind+run**：让节点用它的 state 执行一段逻辑。
- **join**：等待所有 pending 操作完成。
- **close / kill**：释放节点，区别是“关机”还是“断电”。

---

## 快速开始

```python
import asyncio
from flowlet.edge import ThreadEdgeNode, AsyncioEdgeNode, RayEdgeNode

# Thread 后端：同进程，无需序列化
node = ThreadEdgeNode(initializer=lambda: {"value": 0})
node.apply(lambda state, x: state.update(value=state["value"] + x) or state["value"],
           output="value", x=10)
node.join()
print(node.pull("value"))  # 10
node.close()

# Asyncio 后端：在独立 event loop 线程中执行 awaitable
async def async_add(state, x):
    await asyncio.sleep(0.01)
    state["value"] = state.get("value", 0) + x
    return state["value"]

node = AsyncioEdgeNode(initializer=lambda: {"value": 0})
node.apply(async_add, output="value", x=10)
node.join()
print(node.pull("value"))  # 10
node.close()

# Ray 后端：跨进程 Actor
node = RayEdgeNode(initializer=lambda: {"value": 0}, num_cpus=1)
node.apply(lambda state, x: state.update(value=state["value"] + x) or state["value"],
           output="value", x=10)
node.join()
print(node.pull("value"))  # 10
node.close()
```

---

## API 详解

### 创建节点

```python
ThreadEdgeNode(
    initializer=None,   # Callable[[], Any]，在 worker 线程中执行，返回初始 state
    name=None,          # 节点名称
    config=None,        # EdgeConfig 实例
)

AsyncioEdgeNode(
    initializer=None,   # Callable[[], Any]，可返回 awaitable
    name=None,
    config=None,
)

RayEdgeNode(
    initializer=None,
    name=None,
    config=None,
    num_cpus=1.0,
    num_gpus=0.0,
    memory=None,
    max_restarts=0,
    max_task_retries=0,
    **ray_kwargs,
)
```

`initializer` 返回的值会被包成 namespace dict：

- 返回 dict → 直接作为 state。
- 返回其他类型 → 包成 `{"_data": value}`。

### IO：push / pull

```python
node.push("epochs", 10)
node.push_many({"lr": 0.01, "batch_size": 32})

# pull 是同步阻塞的，调用前会自动 join 所有 pending 操作
epochs = node.pull("epochs")
outs = node.pull_many(["lr", "batch_size"])
```

只有 `pull` / `pull_many` 会返回值，其他方法都返回 `None`。

### 执行：apply / bind+run

```python
# 一次性执行
def train_step(state, lr):
    model = state["model"]
    loss = model.step(lr)
    return loss

node.apply(train_step, output="loss", lr=0.01)
node.join()
loss = node.pull("loss")

# 绑定默认目标，之后多次 run
node.bind(train_step, output="loss")
node.run(lr=0.01)
node.run(lr=0.001)
node.join()
```

`target` 支持三种类型：

| target 类型 | 调用方式 |
|---|---|
| `Callable` | `target(state, **kwargs)` |
| `ExecutableUnit`（Kernel / Workflow / Dispatcher / Strategy） | `target.bind_input(state, **kwargs).execute()` |
| `TaskNode` 图 | `target.execute(inputs={**state, **kwargs})` |

结果会写入 `state[output]`。`output=None` 表示只产生副作用，不存储结果。

### 生命周期

```python
node.join()   # 等待所有 pending 完成，有异常则抛出
node.close()  # 先 join，再释放后端（优雅关机）
node.kill()   # 立即释放后端，pending 丢弃（断电）

# with 上下文：正常退出 close，异常退出 kill
with RayEdgeNode(initializer=...) as node:
    node.apply(...)
    node.join()
```

---

## Thread / Asyncio / Ray 后端对比

| 特性 | ThreadEdgeNode | AsyncioEdgeNode | RayEdgeNode |
|---|---|---|---|
| 执行位置 | 同进程 worker 线程 | 同进程 event loop 线程 | 独立 Ray Actor 进程 |
| awaitable target | 不支持 | 支持 | 不支持 |
| state 序列化 | 不需要 | 不需要 | 跨进程参数/返回值需要 |
| 可拉不可序列化对象 | 可以 | 可以 | 不可以（会抛序列化错误） |
| 资源隔离 | 无 | 无 | 有（num_cpus / num_gpus / memory） |
| 适用场景 | 同步逻辑、调试 | async IO、有状态异步节点 | 持有 C 对象、大模型、跨进程并行 |

---

## 典型用例：跨进程持有 C 对象

```python
from flowlet.edge import RayEdgeNode

# 这个对象不可序列化，不能 ray.put
def make_model():
    from some_c_extension import HeavyModel
    return {"model": HeavyModel()}

node = RayEdgeNode(initializer=make_model, num_cpus=2)

# 传可序列化的数据和函数进去遥控
def preprocess(state, data_path):
    raw = load_csv(data_path)
    state["raw"] = state["model"].featurize(raw)
    return len(state["raw"])

node.apply(preprocess, output="count", data_path="data.csv")
node.join()
print(node.pull("count"))

# 最终只把可序列化的结果拉回来
def extract_metrics(state):
    return state["model"].metrics

node.apply(extract_metrics, output="metrics")
metrics = node.pull("metrics")

node.close()
```

---

## 注意事项

1. **函数必须可序列化**：传给 `apply` / `bind` 的函数如果是模块级别的，Ray worker 必须能导入该模块；建议放在项目包内，而不是测试脚本里。
2. **pull 只能拉可序列化数据**：Ray 后端拉不可序列化对象会报错，应先用 `apply` 提取出可序列化结果。
3. **state 是 namespace dict**：函数通过 `state[key]` 访问节点已有数据；调用时传入的 kwargs 是“现在才给”的数据。
4. **TaskNode 图可复用**：同一张图 `bind` 后多次 `run`，每次都会重新执行（内部会自动深拷贝并清空缓存）。
5. **异常传播**：`apply` 内部抛出的异常会在 `join()` 时抛出；`kill()` 会丢弃 pending，不会传播异常。
6. **异步函数选择 Asyncio 后端**：`ThreadEdgeNode` 和 `RayEdgeNode` 使用同步分发；异步 target 建议使用 `AsyncioEdgeNode`。

---

## 与 compute_graph 的关系

- `flowlet.compute_graph`：声明式、无状态、一次性执行完整 DAG。
- `flowlet.edge`：命令式、有状态、异步、手动控制。

两者互补。你也可以把 `TaskNode` 图作为 target 喂给 `EdgeNode`，让 Actor 在本地执行一段子图。
