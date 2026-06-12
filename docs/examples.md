# 示例与最佳实践

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

---

## 示例 1：数据处理流水线

```python
from flowlet import Kernel, Workflow
from flowlet.config import BaseConfigContainer

class DataProcessorConfig(BaseConfigContainer):
    batch_size: int = 100
    threshold: float = 0.5

class DataCleaner(Kernel[DataProcessorConfig, list]):
    config: DataProcessorConfig = DataProcessorConfig()
    
    def __call__(self, raw_data: list[dict]) -> list[dict]:
        # 清洗数据
        return [item for item in raw_data if item.get("value", 0) > self.config.threshold]

class DataTransformer(Kernel[DataProcessorConfig, list]):
    config: DataProcessorConfig = DataProcessorConfig()
    
    def __call__(self, data: list[dict]) -> list[dict]:
        # 转换数据
        return [{**item, "transformed": True} for item in data]

class DataPipeline(Workflow[DataProcessorConfig, list]):
    config: DataProcessorConfig = DataProcessorConfig()
    
    def __call__(self, raw_data: list[dict]) -> list[dict]:
        # 执行流水线
        cleaned = DataCleaner.run(raw_data, config=self.config)
        transformed = DataTransformer.run(cleaned, config=self.config)
        return transformed

# 使用流水线
pipeline = DataPipeline(batch_size=200, threshold=0.7)
result = pipeline.bind_input(raw_data).execute()
```

---

## 示例 2：并行数据处理

```python
from flowlet import ThreadParallelWorkflow, RayParallelWorkflow, ParallelConfig

def process_item(item):
    # 耗时的处理逻辑
    return item * 2

# 使用线程池（IO 密集型）
monitor = ProgressManager()
monitor.start_display()

config = ParallelConfig(max_concurrent_tasks=8)

with ThreadParallelWorkflow(config, progress_monitor=monitor) as workflow:
    results = workflow.map(process_item, range(1000))

monitor.stop_display()

# 使用 Ray（CPU 密集型）
config = ParallelConfig(
    max_concurrent_tasks=16,
    per_task_num_cpus=2,
    show_progress=True,
)

workflow = RayParallelWorkflow(config)
results = workflow.map(process_item, range(1000))
```

---

## 示例 3：智能分发器

```python
from flowlet.dispatcher import Dispatcher
from flowlet.config import BaseConfigContainer

class RouterConfig(BaseConfigContainer):
    default_timeout: int = 30

class ImageProcessor(Kernel[RouterConfig, dict]):
    config: RouterConfig = RouterConfig()
    
    def __call__(self, file: dict) -> dict:
        # 处理图片
        return {"type": "image", "processed": True, **file}

class TextProcessor(Kernel[RouterConfig, dict]):
    config: RouterConfig = RouterConfig()
    
    def __call__(self, file: dict) -> dict:
        # 处理文本
        return {"type": "text", "processed": True, **file}

class VideoProcessor(Kernel[RouterConfig, dict]):
    config: RouterConfig = RouterConfig()
    
    def __call__(self, file: dict) -> dict:
        # 处理视频
        return {"type": "video", "processed": True, **file}

class FileRouter(Dispatcher[RouterConfig, dict]):
    config: RouterConfig = RouterConfig()

# 注册处理器
@FileRouter.register(priority=10, condition=lambda args, kwargs: args[0].get("type") == "image")
class ImageProcessor(Kernel[RouterConfig, dict]):
    config: RouterConfig = RouterConfig()
    
    def __call__(self, file: dict) -> dict:
        return {"type": "image", "processed": True, **file}

@FileRouter.register(priority=10, condition=lambda args, kwargs: args[0].get("type") == "text")
class TextProcessor(Kernel[RouterConfig, dict]):
    config: RouterConfig = RouterConfig()
    
    def __call__(self, file: dict) -> dict:
        return {"type": "text", "processed": True, **file}

@FileRouter.register(priority=0, condition=lambda args, kwargs: True)
class DefaultProcessor(Kernel[RouterConfig, dict]):
    config: RouterConfig = RouterConfig()
    
    def __call__(self, file: dict) -> dict:
        return {"type": "default", "processed": True, **file}

# 使用路由器
router = FileRouter()
result1 = router.bind_input({"type": "image", "name": "test.jpg"}).execute()
result2 = router.bind_input({"type": "text", "name": "test.txt"}).execute()
result3 = router.bind_input({"type": "video", "name": "test.mp4"}).execute()
```

---

## 示例 4：Edge 模式持有不可序列化对象

```python
from flowlet.edge import RayEdgeNode

# 假设 HeavyModel 来自 C 扩展，无法被 pickle
def init_model():
    from heavy_extension import HeavyModel
    return {"model": HeavyModel()}

node = RayEdgeNode(initializer=init_model, num_cpus=2)

# 用可序列化的数据和函数遥控 Actor 中的模型
def train(state, epochs):
    model = state["model"]
    for _ in range(epochs):
        model.step()
    return model.loss

node.apply(train, output="loss", epochs=10)
node.join()
print(node.pull("loss"))

# 最终把可序列化的 metrics 拉回主进程
def get_metrics(state):
    return state["model"].metrics

node.apply(get_metrics, output="metrics")
metrics = node.pull("metrics")

node.close()
```

---

## 最佳实践

### 1. 配置设计

- 使用 `BaseConfigContainer` 作为复杂配置的基类
- 使用 `BaseConfig` 作为简单配置的基类
- 为配置字段提供合理的默认值
- 使用 Pydantic 的验证功能确保配置有效性

### 2. 错误处理

```python
class RobustKernel(Kernel[Config, Result]):
    config: Config = Config()
    
    def __call__(self, data: Any) -> Result:
        try:
            # 核心逻辑
            return self.process(data)
        except SpecificError as e:
            # 处理特定异常
            self.handle_specific_error(e)
        except Exception as e:
            # 处理通用异常
            self.handle_general_error(e)
            raise
```

### 3. 日志记录

```python
import logging

class LoggedKernel(Kernel[Config, Result]):
    config: Config = Config()
    logger = logging.getLogger(__name__)
    
    def __call__(self, data: Any) -> Result:
        self.logger.info(f"Processing data: {data}")
        try:
            result = self.process(data)
            self.logger.info(f"Processing completed: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Processing failed: {e}")
            raise
```

### 4. 性能优化

- 对于 IO 密集型任务，使用 `ThreadParallelWorkflow`
- 对于 CPU 密集型任务，使用 `RayParallelWorkflow`
- 如果需要跨进程持有不可序列化对象，使用 `RayEdgeNode`
- 合理设置 `max_concurrent_tasks` 避免资源耗尽
- 使用进度条监控长时间运行的任务
- 推荐使用 `ProgressManager` 替代内置进度条，功能更灵活，支持跨进程

### 5. 测试

```python
import pytest

def test_kernel():
    config = MyConfig(param1="test")
    kernel = MyKernel(config)
    bound_kernel = kernel.bind_input("input_data")
    result = bound_kernel.execute()
    assert result == expected_result

def test_workflow():
    result = MyWorkflow.run(input_data, steps=3)
    assert len(result) == 4

def test_parallel_workflow():
    with ThreadParallelWorkflow(max_concurrent_tasks=4) as workflow:
        results = workflow.map(lambda x: x * 2, range(10))
        assert results == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
```

---

## 注意事项

1. **线程安全**：在调用 `execute_async()` 或 `execute_ray()` 之前，确保对象不在执行状态
2. **Ray 依赖**：使用 `execute_ray()` 需要安装 Ray 库
3. **序列化**：确保配置和输入数据可序列化（特别是使用 Ray 时）；Edge 模式下不可序列化的中间产物应留在节点内部
4. **资源管理**：使用上下文管理器或显式调用 `shutdown()` 释放并行工作流资源；EdgeNode 使用 `close()` 或 `kill()` 释放
