# Flowlet 模块文档

## 架构概览

Flowlet 是一个灵活的工作流和并行执行框架，提供了统一的抽象接口来管理可执行单元、配置和并行任务。

### 设计哲学

1. **统一的可执行抽象**：所有可执行组件都继承自 `ExecutableUnit`，提供一致的执行接口
2. **配置与逻辑分离**：配置系统独立于执行逻辑，支持灵活配置管理
3. **并行透明**：提供线程和 Ray 两种并行后端，使用统一的接口
4. **类型安全**：使用泛型和类型注解确保类型安全
5. **组合优于继承**：通过配置组合和行为混入实现功能扩展

### 核心组件

```
Flowlet 架构
├── 可执行单元体系
│   ├── ExecutableUnit (抽象基类)
│   │   ├── Kernel (核心执行单元)
│   │   │   └── Dispatcher (分发器)
│   │   └── Workflow (工作流)
│   │       ├── ThreadParallelWorkflow
│   │       ├── RayParallelWorkflow
│   │       └── RayPoolCreatorWorkflow
│   └── 类型注解工具
│       ├── get_class_from_annotation
│       ├── is_subclass_in_annotation
│       ├── extract_target_subclass_from_annotation
│       └── is_instance_in_annotation
└── 配置系统
    ├── BaseConfig (基础配置)
    ├── BaseBranchConfig (分支配置)
    ├── BaseConfigContainer (配置容器)
    ├── Sentinel (语义占位符)
    └── 配置行为混入
        ├── DictConfigBehavior
        ├── JsonConfigBehavior
        ├── TomlConfigBehavior
        └── MsgpackConfigBehavior
```

## 可执行单元体系

### ExecutableUnit（可执行单元）

`ExecutableUnit` 是所有可执行组件的抽象基类，定义了统一的执行接口和基础功能。

#### 核心特性

- **配置管理**：通过泛型配置类型管理执行参数
- **输入绑定**：支持通过 `bind_input` 绑定执行参数
- **同步执行**：通过 `execute` 方法同步执行
- **异步执行**：支持线程和 Ray 两种异步执行方式
- **即时执行**：通过 `run` 类方法无需实例化即可执行

#### 基本用法

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

#### 配置构造

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

#### 异步执行

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

#### 序列化支持

`ExecutableUnit` 重写了序列化方法以支持 Ray 等分布式场景：

- `__getstate__()`：序列化时跳过 `_thread` 属性
- `__setstate__()`：反序列化时正确初始化 `_thread` 属性

### Kernel（核心执行单元）

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

### Workflow（工作流）

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

## 语义占位符

Flowlet 提供统一的语义占位符体系，用于切分 `None` 在不同场景下的含义。

推荐导入方式：

```python
from flowlet import Default, Placeholder, Emptyholder, Voidholder
```

或者：

```python
from flowlet.base.sentinel import Default, Placeholder, Emptyholder, Voidholder
```

语义说明：

- `Default`：参数边界哨兵，表示调用方未显式提供值
- `Placeholder`：过渡态占位符，表示此处应有值但尚未构建完成
- `Emptyholder`：合法空值占位符，表示此处允许为空且当前为空
- `Voidholder`：虚空占位符，表示此处必须为空

使用约定：

- `Default` 只用于函数参数、构造参数、`update()` 一类调用边界
- `Placeholder`、`Emptyholder`、`Voidholder` 用于对象内部状态表达
- 业务语义上的真实空值仍使用 `None`

## 配置系统

### 配置架构

Flowlet 的配置系统采用多层架构：

```
配置层次
├── BaseConfig (基础配置)
│   ├── JsonConfigBehavior (JSON 序列化)
│   ├── TomlConfigBehavior (TOML 序列化)
│   └── MsgpackConfigBehavior (MsgPack 序列化)
├── BaseBranchConfig (分支配置)
│   └── BranchConfigBehavior (分支逻辑)
└── BaseConfigContainer (配置容器)
    └── Pydantic BaseModel (数据验证)
```

### BaseConfig（基础配置）

`BaseConfig` 提供基础的序列化能力，支持 JSON、TOML 和 MsgPack 格式。
同时支持使用 `update()` 更新配置，以及使用 `from_other_config()` 从其他对象构建配置。

```python
from flowlet.config import BaseConfig

class MyConfig(BaseConfig):
    param1: str = "default"
    param2: int = 10

# 创建配置
config = MyConfig(param1="custom", param2=20)

# 序列化为 JSON
json_str = config.to_json_string()
config_from_json = MyConfig.from_json_string(json_str)

# 保存到文件
config.to_json(Path("config.json"))
config_from_file = MyConfig.from_json(Path("config.json"))

# TOML 格式
toml_str = config.to_toml_string()
config_from_toml = MyConfig.from_toml_string(toml_str)

# MsgPack 格式
msgpack_bytes = config.to_msgpack_bytes()
config_from_msgpack = MyConfig.from_msgpack_bytes(msgpack_bytes)

# 从其他对象构建
config2 = MyConfig.from_other_config({"param1": "copied", "param2": 30})

# 使用其他对象更新
config.update(config2)
```

### BaseConfigContainer（配置容器）

`BaseConfigContainer` 基于 Pydantic BaseModel，提供更强大的配置管理能力。
它的正式接口与 `BaseConfig` 保持一致：使用 `update()` 更新，用 `from_other_config()` 构建。

```python
from flowlet.config import BaseConfigContainer, BaseConfig

class SubConfig(BaseConfig):
    sub_param: str = "sub_default"

class ContainerConfig(BaseConfigContainer):
    main_param: int = 10
    sub_config: SubConfig = Field(default_factory=SubConfig)

# 创建配置
config = ContainerConfig(main_param=20)

# 更新配置
config.update(main_param=30)
config.update(SubConfig(sub_param="updated"))

# 从另一个实例更新
other_config = ContainerConfig(main_param=40)
config.update(other_config)

# 从另一个实例构建新实例
new_config = ContainerConfig.from_other_config(other_config)
```

### BaseBranchConfig（分支配置）

`BaseBranchConfig` 支持基于方法名的分支配置，适用于需要动态切换配置的场景。

```python
from flowlet.config import BaseBranchConfig, BaseConfig

class MethodAConfig(BaseConfig):
    param_a: str = "a_default"

class MethodBConfig(BaseConfig):
    param_b: int = 10

class BranchConfig(BaseBranchConfig):
    configs_type = {
        "method_a": MethodAConfig,
        "method_b": MethodBConfig,
    }
    method_name: str = "method_a"

# 创建分支配置
branch_config = BranchConfig(method_name="method_a")

# 访问当前分支配置
current_config = branch_config.config  # 返回 MethodAConfig 实例

# 切换分支
branch_config.method_name = "method_b"
current_config = branch_config.config  # 返回 MethodBConfig 实例

# 访问特定分支配置
method_a_config = branch_config["method_a"]
branch_config["method_b"] = MethodBConfig(param_b=20)
```

### 配置辅助函数

`flowlet.config.funcs` 模块提供配置提取工具函数：

```python
from flowlet.config.funcs import (
    extract_config_from_container,
    extract_config_from_mapping,
    extract_config_from_instance,
    extract_config_from_FieldInfo,
)

# 从容器中提取配置
configs = [MyConfig(), OtherConfig()]
my_config = extract_config_from_container(configs, MyConfig)

# 从映射中提取配置
config_map = {"config": MyConfig(), "other": OtherConfig()}
my_config = extract_config_from_mapping(config_map, MyConfig, key="config")

# 从实例中提取配置
obj = SomeClass()
my_config = extract_config_from_instance(obj, MyConfig)

# 从 FieldInfo 中提取配置
field_info = FieldInfo(json_schema_extra={"config": MyConfig()})
my_config = extract_config_from_FieldInfo(field_info, MyConfig)
```

## 并行工作流

### ParallelConfig（并行配置）

统一的并行工作流配置类，适用于线程池和 Ray 两种后端。

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

### ThreadParallelWorkflow（线程并行工作流）

基于 `ThreadPoolExecutor` 的线程池工作流，适用于 IO 密集型任务。

#### 基本用法

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

#### 原子操作接口

- `submit(func, *args, **kwargs)`：提交单个任务（发送）
- `is_ready(future)`：判断任务是否完成（判断）
- `fetch(future)`：拉取任务结果（拉取）
- `wait(futures, timeout)`：等待所有任务完成（等待）

#### 批量操作接口

- `submit_many(func, iterable)`：批量提交任务
- `gather(futures)`：批量拉取结果
- `map(func, iterable)`：同步批量处理（submit_many + gather）

### RayParallelWorkflow（Ray 并行工作流）

基于 Ray 的分布式工作流，适用于 CPU/GPU 密集型任务。

#### 基本用法

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

#### Ray 资源管理

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

### RayPoolCreatorWorkflow（Ray 池创建器）

专门用于初始化 Ray 集群的工作流。

```python
from flowlet import RayPoolCreatorWorkflow, ParallelConfig

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

## 分发器（Dispatcher）

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

## 类型注解工具

`flowlet.base.type_annotation` 模块提供类型注解处理工具函数。

### get_class_from_annotation

从类型注解中提取具体类：

```python
from flowlet.base.type_annotation import get_class_from_annotation

# 泛型类型
get_class_from_annotation(list[int])  # list
get_class_from_annotation(dict[str, int])  # dict

# Union 类型
get_class_from_annotation(int | str)  # int (第一个非 None 类型)
get_class_from_annotation(Union[int, None])  # int

# 普通类型
get_class_from_annotation(str)  # str
get_class_from_annotation(None)  # None
```

### is_subclass_in_annotation

判断类型注解中是否包含目标类的子类：

```python
from flowlet.base.type_annotation import is_subclass_in_annotation

class MyBase:
    pass

class MySub(MyBase):
    pass

# 检查类型注解
is_subclass_in_annotation(MySub, MyBase)  # True
is_subclass_in_annotation(list[MySub], MyBase)  # True
is_subclass_in_annotation(MySub | str, MyBase)  # True
is_subclass_in_annotation(int, MyBase)  # False
```

### extract_target_subclass_from_annotation

从类型注解中提取目标类的子类：

```python
from flowlet.base.type_annotation import extract_target_subclass_from_annotation

class MyBase:
    pass

class MySub(MyBase):
    pass

# 提取子类
extract_target_subclass_from_annotation(MySub, MyBase)  # MySub
extract_target_subclass_from_annotation(list[MySub], MyBase)  # list
extract_target_subclass_from_annotation(MySub | str, MyBase)  # MySub
extract_target_subclass_from_annotation(int, MyBase)  # None
```

### is_instance_in_annotation

判断实例是否是类型注解所标注的类的实例：

```python
from flowlet.base.type_annotation import is_instance_in_annotation

# 基本类型检查
is_instance_in_annotation(42, int)  # True
is_instance_in_annotation("hello", str)  # True

# 泛型类型检查
is_instance_in_annotation([1, 2, 3], list[int])  # True
is_instance_in_annotation({"a": 1}, dict[str, int])  # True

# Union 类型检查
is_instance_in_annotation(42, int | str)  # True
is_instance_in_annotation("hello", int | str)  # True

# 嵌套类型检查
is_instance_in_annotation([[1, 2], [3, 4]], list[list[int]])  # True
```

## 使用示例

### 示例 1：数据处理流水线

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

### 示例 2：并行数据处理

```python
from flowlet import ThreadParallelWorkflow, RayParallelWorkflow, ParallelConfig

def process_item(item):
    # 耗时的处理逻辑
    return item * 2

# 使用线程池（IO 密集型）
config = ParallelConfig(
    max_concurrent_tasks=8,
    show_progress=True,
    progress_description="Processing items"
)

with ThreadParallelWorkflow(config) as workflow:
    results = workflow.map(process_item, range(1000))

# 使用 Ray（CPU 密集型）
config = ParallelConfig(
    max_concurrent_tasks=16,
    per_task_num_cpus=2,
    show_progress=True,
)

workflow = RayParallelWorkflow(config)
results = workflow.map(process_item, range(1000))
```

### 示例 3：智能分发器

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
- 合理设置 `max_concurrent_tasks` 避免资源耗尽
- 使用进度条监控长时间运行的任务

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

## 注意事项

1. **线程安全**：在调用 `execute_async()` 或 `execute_ray()` 之前，确保对象不在执行状态
2. **Ray 依赖**：使用 `execute_ray()` 需要安装 Ray 库
3. **序列化**：确保配置和输入数据可序列化（特别是使用 Ray 时）
4. **资源管理**：使用上下文管理器或显式调用 `shutdown()` 释放并行工作流资源
5. **异常处理**：异步执行的异常会被捕获，建议在 `__call__` 中实现异常处理
