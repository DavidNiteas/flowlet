# Flowlet

轻量的工作流、配置、分发与并行执行基础库。

## 安装

```bash
pip install -e .
```

或使用 [pixi](https://pixi.sh)：

```bash
pixi install
```

## 核心特性

- **统一执行抽象**：`ExecutableUnit` / `Kernel` / `Workflow` 提供一致的业务逻辑封装
- **分支分发**：`Dispatcher` 基于条件函数实现输入路由
- **配置与逻辑分离**：`BaseConfig` / `BaseConfigContainer` / `BaseBranchConfig` 支持 JSON / TOML / MsgPack
- **并行透明**：`ThreadParallelWorkflow`（IO 密集型）与 `RayParallelWorkflow`（CPU/GPU 密集型）统一接口
- **跨进程进度**：`ProgressManager` + `MPProgressProxy` / `RayProgressProxy`，本地/多进程/Ray 三端汇聚
- **语义占位符**：`Default` / `Placeholder` / `Emptyholder` / `Voidholder` 精确表达空值语义

## 快速开始

```python
from flowlet import Kernel, ThreadParallelWorkflow
from flowlet.config import BaseConfigContainer

class MyConfig(BaseConfigContainer):
    threshold: float = 0.5

class MyKernel(Kernel[MyConfig, list]):
    config: MyConfig = MyConfig()
    
    def __call__(self, data: list[float]) -> list[float]:
        return [x for x in data if x > self.config.threshold]

# 即时执行
result = MyKernel.run([0.1, 0.7, 0.3, 0.9], threshold=0.6)
# → [0.7, 0.9]

# 并行执行
with ThreadParallelWorkflow(max_concurrent_tasks=4) as wf:
    results = wf.map(lambda x: x * 2, range(10))
# → [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
```

## 模块导航

| 模块 | 说明 | 文档 |
|------|------|------|
| `flowlet.executable_unit` | 可执行单元（ExecutableUnit / Kernel / Workflow） | [docs/executable_unit.md](docs/executable_unit.md) |
| `flowlet.dispatcher` | 条件分发器（Dispatcher） | [docs/dispatcher.md](docs/dispatcher.md) |
| `flowlet.strategy` | 策略模式（BaseStrategy / @mount） | [docs/strategy.md](docs/strategy.md) |
| `flowlet.config` | 配置系统（BaseConfig / BaseConfigContainer / BaseBranchConfig） | [docs/config.md](docs/config.md) |
| `flowlet.parallel_unit` | 并行工作流（Thread / Ray / RayPoolCreator） | [docs/parallel.md](docs/parallel.md) |
| `flowlet.base.progress` | 进度管理（ProgressManager / Proxy） | [docs/progress.md](docs/progress.md) |
| `flowlet.base` | 基础工具（语义占位符 / 类型注解） | [docs/base.md](docs/base.md) |
| — | 完整示例、最佳实践、注意事项 | [docs/examples.md](docs/examples.md) |

完整架构概览见 [docs/index.md](docs/index.md)。
