# 进度管理

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

`flowlet.base.progress` 模块提供统一的进度跟踪和 CLI 进度条渲染能力，支持同进程直接访问和跨进程代理访问。

---

## 核心特性

- **任务粒度追踪**：注册、更新、查询任意命名任务的进度
- **CLI 进度条渲染**：基于 rich 的独立渲染线程，支持动态添加/更新任务
- **同进程模式**：`threading.RLock` 保护内部状态，多线程安全
- **跨进程代理（multiprocessing）**：`MPProgressProxy` 使用 `multiprocessing.Queue`，适用于标准库多进程
- **跨进程代理（Ray）**：`RayProgressProxy` 使用 `ray.util.queue.Queue`，可安全通过 Ray 序列化/传递
- **按需创建 Queue**：Queue 在获取代理时才创建，未使用时零开销
- **覆盖/累加双模式**：`update_progress` 支持 `mode="set"`（覆盖，默认）和 `mode="increment"`（累加）

---

## 基本用法

```python
from flowlet import ProgressManager

# 创建监视器
manager = ProgressManager()

# 注册任务
manager.register_task("batch_write", "Writing files", total=100)

# 更新进度（覆盖模式，默认）
for i in range(100):
    manager.update_progress("batch_write", current=i + 1)

# 累加模式
manager.update_progress("batch_write", current=5, mode="increment")  # current += 5

# 查询进度
prog = manager.get_progress("batch_write")
print(f"{prog.current}/{prog.total}")
```

---

## CLI 进度条

```python
manager = ProgressManager(bar_type="rich")

# 启动渲染线程
manager.start_display()

# 注册并更新任务（进度条自动渲染）
manager.register_task("task1", "Processing A", total=50)
manager.register_task("task2", "Processing B", total=100)

for i in range(50):
    manager.update_progress("task1", current=i + 1)

# 停止渲染
manager.stop_display()
```

---

## 跨进程模式（multiprocessing）

使用 `MPProgressProxy`，基于 `multiprocessing.Queue`：

```python
# 主进程中创建监视器
manager = ProgressManager()
proxy = manager.get_mp_proxy()  # 按需创建 mp.Queue 并启动消费线程

# 子进程中使用代理（通过 Queue 发送消息）
proxy.register_task("remote_task", "Remote work", total=10)
proxy.update_progress("remote_task", current=5, status="running")

# 主进程中查询（自动同步）
time.sleep(0.3)  # 等待消费线程处理
prog = manager.get_progress("remote_task")
```

**类型安全**：`MPProgressProxy` 不可通过 Ray 传递（`multiprocessing.Queue` 无法被 Ray 序列化），误用会在序列化阶段报错。

---

## 跨进程模式（Ray）

使用 `RayProgressProxy`，基于 `ray.util.queue.Queue`：

```python
import ray
from flowlet import ProgressManager

ray.init()
manager = ProgressManager()
proxy = manager.get_ray_proxy()  # 按需创建 Ray Queue 并启动消费线程

# proxy 可安全通过 Ray 传递（ray.put / ray.get / remote 函数参数）
ref = ray.put(proxy)
proxy_remote = ray.get(ref)

# Ray worker 中使用代理
@ray.remote
def worker(p):
    p.register_task("ray_task", "Ray work", total=10)
    p.update_progress("ray_task", current=5)

ray.get(worker.remote(proxy_remote))

# 主进程中查询（自动同步）
time.sleep(0.3)
prog = manager.get_progress("ray_task")
```

**关键区别**：`RayProgressProxy` 经 Ray 序列化后 `_queue` 仍然有效，可正常跨进程通信；`MPProgressProxy` 经序列化后 `_queue` 变为 `None`（Ray 安全设计）。

---

## 多后端共存

同一个 `ProgressManager` 可以同时创建两种代理，分别消费各自的 Queue：

```python
manager = ProgressManager()
mp_proxy = manager.get_mp_proxy()     # multiprocessing 场景
ray_proxy = manager.get_ray_proxy()   # Ray 场景

# 两个代理的更新都会同步到同一个 manager
```

---

## 与并行工作流集成

`ProgressManager` 作为运行时对象，直接传给 Workflow 构造函数，不通过 `ParallelConfig`：

```python
from flowlet import ThreadParallelWorkflow, ParallelConfig, ProgressManager

manager = ProgressManager()
config = ParallelConfig()

with ThreadParallelWorkflow(config, progress_monitor=manager) as workflow:
    results = workflow.map(lambda x: x * 2, range(1000))

# map/gather 会自动注册 "map"/"gather" 任务并更新进度
prog = manager.get_progress("map")
print(f"Completed: {prog.current}/{prog.total}")
```
