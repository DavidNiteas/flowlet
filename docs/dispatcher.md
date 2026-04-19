# 分发器

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

`flowlet.dispatcher` 模块提供基于输入条件的路由分发能力。

---

## Dispatcher

`Dispatcher` 是 `Kernel` 的子类，用于基于输入的分支逻辑执行。在同一个输入下，有且只有一个分支被执行。

### 基本用法

```python
from flowlet.dispatcher import Dispatcher
from flowlet.config import BaseConfigContainer

class DispatcherConfig(BaseConfigContainer):
    common_param: str = "default"

class EvenProcessor(Kernel[DispatcherConfig, int]):
    config: DispatcherConfig = DispatcherConfig()
    
    def __call__(self, number: int) -> int:
        return number * 2

class OddProcessor(Kernel[DispatcherConfig, int]):
    config: DispatcherConfig = DispatcherConfig()
    
    def __call__(self, number: int) -> int:
        return number * 3

class MyDispatcher(Dispatcher[DispatcherConfig, int]):
    config: DispatcherConfig = DispatcherConfig()

# 注册分支（使用装饰器）
@MyDispatcher.register(priority=10, condition=lambda args, kwargs: args[0] % 2 == 0)
class EvenProcessor(Kernel[DispatcherConfig, int]):
    config: DispatcherConfig = DispatcherConfig()
    
    def __call__(self, number: int) -> int:
        return number * 2

@MyDispatcher.register(priority=10, condition=lambda args, kwargs: args[0] % 2 != 0)
class OddProcessor(Kernel[DispatcherConfig, int]):
    config: DispatcherConfig = DispatcherConfig()
    
    def __call__(self, number: int) -> int:
        return number * 3

# 使用分发器
dispatcher = MyDispatcher()
result1 = dispatcher.bind_input(4).execute()  # 8 (偶数处理器)
result2 = dispatcher.bind_input(5).execute()  # 15 (奇数处理器)

# 或使用即时执行
result1 = MyDispatcher.run(4)  # 8
result2 = MyDispatcher.run(5)  # 15
```

### 注册机制

- **优先级**：数值越高优先级越高，默认为 0
- **条件函数**：接收 `(args, kwargs)` 返回 bool，决定是否匹配
- **执行顺序**：按优先级从高到低检查，第一个满足条件的分支被执行

```python
# 多优先级注册
@MyDispatcher.register(priority=20, condition=lambda args, kwargs: args[0] > 100)
class HighPriorityProcessor(Kernel[DispatcherConfig, int]):
    # 处理大于 100 的数
    pass

@MyDispatcher.register(priority=10, condition=lambda args, kwargs: args[0] > 50)
class MediumPriorityProcessor(Kernel[DispatcherConfig, int]):
    # 处理 50-100 的数
    pass

@MyDispatcher.register(priority=0, condition=lambda args, kwargs: True)
class DefaultProcessor(Kernel[DispatcherConfig, int]):
    # 默认处理器
    pass
```
