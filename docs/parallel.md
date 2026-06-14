# 并行工作流

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

`flowlet.parallel_unit` 模块提供统一的并行执行抽象，支持线程池、协程池和 Ray 三类后端。

---

## ParallelConfig（并行配置）

统一的并行工作流配置类，适用于线程池、协程池和 Ray 三类后端。

```python
from flowlet import ParallelConfig

config = ParallelConfig(
    # 通用配置
    max_concurrent_tasks=4,  # 最大并发任务数
    show_progress=True,  # 显示进度条
    progress_description="Processing",  # 进度条描述
    progress_bar_type="rich",  # 进度条类型：rich, tqdm, jupyter
    use_concurrent_io=True,  # 使用并发 IO
    
    # Ray 特有配置（线程池会自动忽略）
    per_task_num_cpus=1.0,  # 每个任务的 CPU 核心数
    per_task_num_gpus=0.0,  # 每个任务的 GPU 数量
    per_task_memory=None,  # 每个任务的内存限制（字节）
    ray_runtime_env=None,  # Ray 运行时环境
    ray_dashboard_port=None,  # Ray 仪表板端口
    ray_include_dashboard=False,  # 启用 Ray 仪表板
    ray_log_to_driver=True,  # 发送日志到驱动程序
)
```

---

## ThreadParallelWorkflow（线程并行工作流）

基于 `ThreadPoolExecutor` 的线程池工作流，适用于 IO 密集型任务。

### 基本用法

```python
from flowlet import ThreadParallelWorkflow

workflow = ThreadParallelWorkflow(max_concurrent_tasks=4)

# 提交单个任务
future = workflow.submit(lambda x: x * 2, 10)
result = workflow.fetch(future)  # 20

# 提交多个任务
futures = workflow.submit_many(lambda x: x * 2, [1, 2, 3, 4])
results = workflow.gather(futures)  # [2, 4, 6, 8]

# 使用 map（同步）
results = workflow.map(lambda x: x * 2, [1, 2, 3, 4])  # [2, 4, 6, 8]

# 等待所有任务完成
workflow.wait(futures, timeout=10.0)

# 按完成顺序处理
for future in workflow.as_completed(futures):
    result = workflow.fetch(future)
    print(result)

# 使用上下文管理器
with ThreadParallelWorkflow(max_concurrent_tasks=4) as workflow:
    results = workflow.map(lambda x: x * 2, [1, 2, 3, 4])
```

### 原子操作接口

- `submit(func, *args, **kwargs)`：提交单个任务（发送）
- `is_ready(future)`：判断任务是否完成（判断）
- `fetch(future)`：拉取任务结果（拉取）
- `wait(futures, timeout)`：等待所有任务完成（等待）

### 批量操作接口

- `submit_many(func, iterable)`：批量提交任务
- `gather(futures)`：批量拉取结果
- `map(func, iterable)`：同步批量处理（submit_many + gather）

---

## CoroutineParallelWorkflow（协程并行工作流）

`CoroutineParallelWorkflow` 基于 `CoroutinePool`，在独立的 asyncio event loop 线程中执行 awaitable。它适合异步 IO 任务，也可以通过 `blocking=True` 把阻塞同步函数交给 `asyncio.to_thread`。

```python
import asyncio
from flowlet import CoroutineParallelWorkflow, ParallelConfig

async def fetch_one(i: int) -> str:
    await asyncio.sleep(0.01)
    return f"item-{i}"

with CoroutineParallelWorkflow(ParallelConfig(max_concurrent_tasks=4)) as workflow:
    future = workflow.submit(fetch_one, 1)
    assert workflow.fetch(future) == "item-1"

    results = workflow.map(fetch_one, range(3))
    assert results == ["item-0", "item-1", "item-2"]

    results = workflow.gather([fetch_one(3), fetch_one(4)])
    assert results == ["item-3", "item-4"]
```

### 阻塞函数

```python
import time
from flowlet import CoroutineParallelWorkflow

def blocking_read(path: str) -> str:
    time.sleep(0.1)
    return path

with CoroutineParallelWorkflow(max_concurrent_tasks=8) as workflow:
    results = workflow.map(blocking_read, ["a.txt", "b.txt"], blocking=True)
```

### 后端选择

| 后端 | 适合场景 | 说明 |
|------|----------|------|
| `ThreadParallelWorkflow` | 普通同步 IO、已有阻塞函数 | 基于 `ThreadPoolExecutor` |
| `CoroutineParallelWorkflow` | 原生 async IO、awaitable 批量调度 | 使用独立 event loop，不占用调用方 loop |
| `RayParallelWorkflow` | CPU/GPU 密集、跨进程/分布式 | 需要 Ray 可用，函数和参数需可序列化 |

---

## RayParallelWorkflow（Ray 并行工作流）

基于 Ray 的分布式工作流，适用于 CPU/GPU 密集型任务。

### 基本用法

```python
from flowlet import RayParallelWorkflow

workflow = RayParallelWorkflow(
    max_concurrent_tasks=8,
    per_task_num_cpus=2,
    per_task_num_gpus=0.5,
)

# 提交单个任务
future = workflow.submit(lambda x: x * 2, 10)
result = workflow.fetch(future)  # 20

# 提交多个任务
futures = workflow.submit_many(lambda x: x * 2, [1, 2, 3, 4])
results = workflow.gather(futures)  # [2, 4, 6, 8]

# 使用 map（同步）
results = workflow.map(lambda x: x * 2, [1, 2, 3, 4])  # [2, 4, 6, 8]

# 等待所有任务完成
workflow.wait(futures, timeout=60.0)

# 按完成顺序处理
for future in workflow.as_completed(futures):
    result = workflow.fetch(future)
    print(result)
```

### Ray 资源管理

```python
# 精细控制每个任务的资源
workflow = RayParallelWorkflow(
    per_task_num_cpus=4,      # 每个任务 4 个 CPU 核心
    per_task_num_gpus=1,      # 每个任务 1 个 GPU
    per_task_memory=1024*1024*1024,  # 每个任务 1GB 内存
)

# 配置 Ray 运行时环境
workflow = RayParallelWorkflow(
    ray_runtime_env={
        "pip": ["requests", "numpy"],
        "env_vars": {"MY_VAR": "value"}
    },
    ray_dashboard_port=8265,
    ray_include_dashboard=True,
)
```

---

## RayPoolCreatorWorkflow（Ray 池创建器）

专门用于初始化 Ray 集群的工作流。

```python
from flowlet import RayPoolCreatorWorkflow, ParallelConfig
import ray

config = ParallelConfig(
    ray_dashboard_port=8265,
    ray_include_dashboard=True,
    ray_log_to_driver=True,
)

# 初始化 Ray 集群
initializer = RayPoolCreatorWorkflow(config)
initializer.bind_input().execute()

# 检查是否成功初始化
if ray.is_initialized():
    print("Ray cluster initialized")
```
