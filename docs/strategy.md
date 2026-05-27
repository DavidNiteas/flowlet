# 策略

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

`flowlet.strategy` 模块提供策略模式支持，通过 `BaseStrategy` 基类和 `@mount` 装饰器将一组相关的 `ExecutableUnit` 聚合为策略对象，共享配置并自动处理跨切关注点。

---

## 概述

`BaseStrategy` 是 `Kernel` 的子类，参与 flowlet 的执行生命周期。通过 `@mount` 装饰器，可以将多个 `ExecutableUnit`（如 `Kernel`、`Workflow`、`Dispatcher`）挂载为策略的属性，访问时自动注入配置。

核心理念类似于 pandas 的 `df.loc`——属性访问返回已绑定配置的 unit 实例，调用者自行决定执行方式。

---

## BaseStrategy

### 基本用法

```python
from flowlet import BaseStrategy, Kernel, mount
from flowlet.config import BaseConfigContainer

class ReadConfig(BaseConfigContainer):
    encoding: str = "utf-8"

class WriteConfig(BaseConfigContainer):
    output_dir: str = "./output"

class StrategyConfig(BaseConfigContainer):
    read: ReadConfig = ReadConfig()
    write: WriteConfig = WriteConfig()

class ReadKernel(Kernel[ReadConfig, str]):
    config: ReadConfig = ReadConfig()

    def __call__(self, path: str) -> str:
        with open(path, encoding=self.config.encoding) as f:
            return f.read()

class WriteKernel(Kernel[WriteConfig, None]):
    config: WriteConfig = WriteConfig()

    def __call__(self, data: str) -> None:
        import os
        os.makedirs(self.config.output_dir, exist_ok=True)

class MyStrategy(BaseStrategy[StrategyConfig, None]):
    config: StrategyConfig = StrategyConfig()

    @mount(ReadKernel)
    def read(self) -> ReadKernel: pass

    @mount(WriteKernel)
    def write(self) -> WriteKernel: pass

    def process(self, path: str) -> None:
        data = self.read(path)
        self.write(data)
```

### 配置继承

`BaseStrategy` 继承自 `Kernel[ConfigT, ResultT]`，因此具备 `Kernel` 的全部能力：

- 通过 `config` 类属性管理策略级配置
- 通过 `run` 类方法即时执行
- 通过 `bind_input` / `execute` 分步控制

---

## @mount 装饰器

`@mount` 将策略方法注册为挂载点。被装饰的方法体不需要实现（通常为 `pass`），装饰器会将其替换为 `MountPoint` 属性。

### 参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `unit_cls` | `type[ExecutableUnit]` | 要挂载的 ExecutableUnit 类 |
| `config_key` | `str \| None` | 从 `strategy.config` 中提取子配置的字段名，支持点号路径（如 `"io.write"`）。默认使用被装饰的方法名 |
| `config_factory` | `Callable \| None` | 自定义配置构造函数，接收 `strategy.config`，返回 unit 需要的配置实例。如果提供，优先于 `config_key` |

### 配置提取

`@mount` 支持三种配置提取方式：

```python
class MyStrategy(BaseStrategy[StrategyConfig, None]):
    config: StrategyConfig = StrategyConfig()

    # 方式 1：默认使用方法名作为 config_key
    # 自动从 strategy.config 中提取 strategy.config.read
    @mount(ReadKernel)
    def read(self) -> ReadKernel: pass

    # 方式 2：显式指定 config_key，支持点号路径
    @mount(WriteKernel, config_key="io.write")
    def write(self) -> WriteKernel: pass

    # 方式 3：自定义配置工厂函数
    @mount(
        ProcessKernel,
        config_factory=lambda cfg: ProcessConfig(
            threshold=cfg.read.threshold * 2
        ),
    )
    def process(self) -> ProcessKernel: pass
```

---

## MountPoint

`MountPoint` 继承自 `property`，是挂载点的实际运行时表示。属性访问时返回已基于 `strategy.config` 构造好的 unit 实例。

### 使用方式

返回的 unit 实例仅绑定配置，不执行。调用者自行决定执行方式：

```python
strategy = MyStrategy()

# 直接调用 __call__
result = strategy.read(path)

# 调用 run（走完整生命周期）
result = strategy.read.run(path)

# 分步控制
result = strategy.read.bind_input(path).execute()

# 索引访问（若 unit 实现了 __getitem__）
item = strategy.read[index]
```

### 配置隔离

每次访问挂载点时，配置会通过 `copy.deepcopy` 深拷贝，确保各 unit 实例之间的配置互不干扰：

```python
# 两次访问返回独立的 unit 实例，配置互不影响
unit_a = strategy.read
unit_b = strategy.read
assert unit_a is not unit_b
```

---

## 操作注册表

`BaseStrategy` 通过 `__init_subclass__` 自动收集所有 `@mount` 注册的挂载点，存储在类级别的 `_operations` 字典中：

```python
class MyStrategy(BaseStrategy[StrategyConfig, None]):
    config: StrategyConfig = StrategyConfig()

    @mount(ReadKernel)
    def read(self) -> ReadKernel: pass

    @mount(WriteKernel)
    def write(self) -> WriteKernel: pass

# 查看已注册的操作
print(MyStrategy._operations)
# {'read': <MountSpec ...>, 'write': <MountSpec ...>}
```

### 必需操作

通过 `_required_operations` 可以声明子类必须实现的操作，缺失时在类定义阶段即报错：

```python
class AbstractPipeline(BaseStrategy[StrategyConfig, None]):
    _required_operations = ("read", "write")

# 以下类定义会抛出 TypeError：
# "IncompletePipeline must register operation: 'read'"
class IncompletePipeline(AbstractPipeline):
    config: StrategyConfig = StrategyConfig()

    @mount(WriteKernel)
    def write(self) -> WriteKernel: pass
```

### 继承

子类自动继承父类的 `_operations`，可以覆盖或新增：

```python
class BasePipeline(BaseStrategy[StrategyConfig, None]):
    config: StrategyConfig = StrategyConfig()

    @mount(ReadKernel)
    def read(self) -> ReadKernel: pass

class ExtendedPipeline(BasePipeline):
    @mount(WriteKernel)
    def write(self) -> WriteKernel: pass

# ExtendedPipeline._operations 包含 read 和 write
```

---

## 完整示例

```python
from flowlet import BaseStrategy, Kernel, Workflow, mount
from flowlet.config import BaseConfigContainer

class FilterConfig(BaseConfigContainer):
    threshold: float = 0.5

class AggregateConfig(BaseConfigContainer):
    method: str = "sum"

class PipelineConfig(BaseConfigContainer):
    filter: FilterConfig = FilterConfig()
    aggregate: AggregateConfig = AggregateConfig()

class FilterKernel(Kernel[FilterConfig, list]):
    config: FilterConfig = FilterConfig()

    def __call__(self, data: list[float]) -> list[float]:
        return [x for x in data if x > self.config.threshold]

class AggregateKernel(Kernel[AggregateConfig, float]):
    config: AggregateConfig = AggregateConfig()

    def __call__(self, data: list[float]) -> float:
        if self.config.method == "sum":
            return sum(data)
        return sum(data) / len(data)

class DataPipeline(BaseStrategy[PipelineConfig, float]):
    config: PipelineConfig = PipelineConfig()

    @mount(FilterKernel)
    def filter(self) -> FilterKernel: pass

    @mount(AggregateKernel)
    def aggregate(self) -> AggregateKernel: pass

    def __call__(self, data: list[float]) -> float:
        filtered = self.filter(data)
        return self.aggregate(filtered)

# 使用
pipeline = DataPipeline()
result = pipeline([0.1, 0.7, 0.3, 0.9, 0.2, 0.8])

# 即时执行
result = DataPipeline.run([0.1, 0.7, 0.3, 0.9], threshold=0.4)
```
