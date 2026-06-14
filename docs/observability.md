# 运行观测

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

Flowlet 提供三类轻量观测工具：

- `LogManager`：结构化日志记录和跨进程日志汇聚
- `StreamManager`：捕获 Python 层 `stdout` / `stderr`
- `TelemetryManager`：指标、checkpoint、系统资源和进程资源采样

三者都采用与 `ProgressManager` 类似的模式：主进程持有 manager，子进程或 Ray worker 使用 proxy 发送消息。

---

## 日志汇聚

```python
from flowlet import LogManager

manager = LogManager(max_records=1000)

manager.info("load started", run_id="r1", stage="load")
manager.warning("missing optional field", metadata={"field": "age"})

records = manager.get_records()
print(records[-1].level, records[-1].message)

manager.to_jsonl("logs.jsonl")
manager.close()
```

也可以接入标准库 `logging`：

```python
import logging
from flowlet import FlowletLogHandler, LogManager

manager = LogManager()
logger = logging.getLogger("pipeline")
logger.addHandler(FlowletLogHandler(manager))
logger.setLevel(logging.INFO)

logger.info("from std logging")
```

跨进程代理：

```python
proxy = manager.get_mp_proxy()   # multiprocessing.Queue
proxy.info("worker log", task_id="task-1")

ray_proxy = manager.get_ray_proxy()  # ray.util.queue.Queue
```

---

## stdout / stderr 捕获

```python
from flowlet import StreamManager

streams = StreamManager(max_chunks=1000)

with streams.capture(source="unit-test", mode="capture"):
    print("hello stdout")
    print("hello stderr", file=__import__("sys").stderr)

chunks = streams.get_chunks()
assert chunks[0].source == "unit-test"

streams.to_text("stdout.txt", stream="stdout")
streams.close()
```

`capture(mode=...)` 支持：

| mode | 行为 |
|------|------|
| `"capture"` | 捕获到 manager，不再写回原始 stdout/stderr |
| `"tee"` | 捕获，同时写回原始 stdout/stderr |
| `"inherit"` | 不捕获，保持原始 stdout/stderr |
| `"silent"` | 捕获但不写回原始 stdout/stderr |

跨进程代理：

```python
proxy = streams.get_mp_proxy()
with proxy.capture(source="worker"):
    print("worker stdout")

ray_proxy = streams.get_ray_proxy()
```

---

## 遥测事件

```python
from flowlet import TelemetryManager

telemetry = TelemetryManager(max_events=5000)

telemetry.metric("workers", 4, unit="count", run_id="r1")
telemetry.checkpoint("load_started", run_id="r1", stage="load")

system = telemetry.sample_system()
process = telemetry.sample_process(worker_id="main", run_id="r1")

events = telemetry.get_events()
telemetry.to_jsonl("telemetry.jsonl")
telemetry.close()
```

事件类型：

| event_type | 来源 |
|------------|------|
| `"metric"` | `metric(name, value, unit=...)` |
| `"checkpoint"` | `checkpoint(name, ...)`，自动记录 since start / previous elapsed |
| `"system_resource"` | `sample_system()` 或系统采样线程 |
| `"process_resource"` | `sample_process()` 或进程采样线程 |

资源采样依赖 `psutil`。如果未安装，snapshot 仍会生成，并在 `metadata["psutil_available"]` 中标记为 `False`。

---

## 采样线程

```python
telemetry = TelemetryManager()
telemetry.start_system_sampler(interval=1.0)
telemetry.start_process_sampler(interval=1.0, worker_id="main", run_id="r1")

# ... run workload ...

telemetry.close()  # 自动停止采样线程并关闭队列消费线程
```

---

## Ray worker 示例

```python
import ray
from flowlet import LogManager, StreamManager, TelemetryManager

logs = LogManager()
streams = StreamManager()
telemetry = TelemetryManager()

log_proxy = logs.get_ray_proxy()
stream_proxy = streams.get_ray_proxy()
telemetry_proxy = telemetry.get_ray_proxy()

@ray.remote
def worker(logs, streams, telemetry):
    logs.info("ray worker started", node_id="w1")
    telemetry.metric("items", 10, worker_id="w1")
    with streams.capture(source="ray-worker"):
        print("processing")
    return "ok"

ray.get(worker.remote(log_proxy, stream_proxy, telemetry_proxy))
```

---

## 注意事项

1. Manager 保存的是本地权威记录；proxy 自身也保留创建时的快照和本地追加记录。
2. `max_records` / `max_chunks` / `max_events` 用于限制内存中的历史长度。
3. 使用 `get_mp_proxy()` / `get_ray_proxy()` 会按需创建队列和后台消费线程。
4. 长时间运行任务结束时应调用 `close()`，释放队列和消费线程。
5. `StreamManager.capture()` 捕获的是 Python 层 `sys.stdout` / `sys.stderr` 写入，不等同于 OS 文件描述符级重定向。
