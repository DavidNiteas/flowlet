# 可执行单元

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

`flowlet.executable_unit` 模块提供统一的执行抽象，所有可执行组件都继承自 `ExecutableUnit`，通过泛型配置类型管理执行参数。

---

## ExecutableUnit（可执行单元）

`ExecutableUnit` 是所有可执行组件的抽象基类，定义了统一的执行接口和基础功能。

### 核心特性

- **配置管理**：通过泛型配置类型管理执行参数
- **输入绑定**：支持通过 `bind_input` 绑定执行参数
- **同步执行**：通过 `execute` 方法同步执行
- **异步执行**：支持线程和 Ray 两种异步执行方式
- **即时执行**：通过 `run` 类方法无需实例化即可执行

### 基本用法

```python
from flowlet import ExecutableUnit
from flowlet.config import BaseConfigContainer

class MyConfig(BaseConfigContainer):
    param1: str = "default"
    param2: int = 10

class MyUnit(ExecutableUnit[MyConfig, str]):
    config: MyConfig = MyConfig()
    
    def __call__(self, input_data: str) -> str:
        return f"{self.config.param1}: {input_data}"

# 方式 1：实例化后执行
unit = MyUnit(param1="custom")
bound_unit = unit.bind_input("hello")
result = bound_unit.execute()

# 方式 2：即时执行（推荐）
result = MyUnit.run("hello", param1="custom")
```

### 配置构造

`build_config` 类方法用于从输入参数构造配置实例：

```python
# 构造默认配置
config = MyUnit.build_config()

# 带参数构造配置
config = MyUnit.build_config(param1="value1", param2=20)

# 使用配置对象更新
new_config = MyConfig(param1="new_value")
config = MyUnit.build_config(new_config)
```

### 异步执行

**基于线程的异步执行：**

```python
unit = MyUnit()
unit = unit.bind_input("hello")
unit.execute_async()

# 等待执行完成
while unit._thread is not None:
    time.sleep(0.01)

# 获取执行结果
result = unit.result
```

**基于 Ray 的异步执行：**

```python
unit = MyUnit()
unit = unit.bind_input("hello")
unit.execute_ray()

# 等待执行完成
while unit._thread is not None:
    time.sleep(0.01)

# 获取执行结果
result = unit.result
```

### 序列化支持

`ExecutableUnit` 重写了序列化方法以支持 Ray 等分布式场景：

- `__getstate__()`：序列化时跳过 `_thread` 属性
- `__setstate__()`：反序列化时正确初始化 `_thread` 属性

---

## Kernel（核心执行单元）

`Kernel` 是 `ExecutableUnit` 的子类，专门用于执行具体的计算或处理任务。

```python
from flowlet.executable_unit import Kernel
from flowlet.config import BaseConfigContainer

class MyKernelConfig(BaseConfigContainer):
    threshold: float = 0.5

class MyKernel(Kernel[MyKernelConfig, dict]):
    config: MyKernelConfig = MyKernelConfig()
    
    def __call__(self, data: list[float]) -> dict:
        return {
            "above": [x for x in data if x > self.config.threshold],
            "below": [x for x in data if x <= self.config.threshold]
        }

# 使用
result = MyKernel.run([0.3, 0.7, 0.5, 0.9], threshold=0.6)
```

---

## Workflow（工作流）

`Workflow` 是 `ExecutableUnit` 的子类，用于协调多个任务的执行，可以包含多个步骤或子任务。

```python
from flowlet.executable_unit import Workflow
from flowlet.config import BaseConfig

class MyWorkflowConfig(BaseConfig):
    steps: int = 3

class MyWorkflow(Workflow[MyWorkflowConfig, list]):
    config: MyWorkflowConfig = MyWorkflowConfig()
    
    def __call__(self, initial_data: Any) -> list:
        results = [initial_data]
        for i in range(self.config.steps):
            results.append(self.process(results[-1]))
        return results
    
    def process(self, data: Any) -> Any:
        # 具体的处理逻辑
        return data
```
