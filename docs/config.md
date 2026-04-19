# 配置系统

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

`flowlet.config` 模块提供分层配置架构，支持 JSON / TOML / MsgPack 序列化、分支配置和配置容器。

---

## 配置架构

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

---

## BaseConfig（基础配置）

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

---

## BaseConfigContainer（配置容器）

`BaseConfigContainer` 基于 Pydantic BaseModel，提供更强大的配置管理能力。
它的正式接口与 `BaseConfig` 保持一致：使用 `update()` 更新，用 `from_other_config()` 构建。

```python
from flowlet.config import BaseConfigContainer, BaseConfig
from pydantic import Field

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

---

## BaseBranchConfig（分支配置）

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

---

## 配置辅助函数

`flowlet.config.funcs` 模块提供配置提取工具函数：

```python
from flowlet.config.funcs import (
    extract_config_from_container,
    extract_config_from_mapping,
    extract_config_from_instance,
    extract_config_from_FieldInfo,
)
from pydantic import FieldInfo

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
