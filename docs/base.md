# 基础工具

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

`flowlet.base` 模块提供语义占位符、懒加载工具和类型注解工具，是框架的基础设施。

---

## 懒加载工具

`flowlet.base.lazy` 模块提供延迟加载机制，将数据加载/计算推迟到首次显式取值时执行。

### LazyHolder

延迟值占位符，属于 sentinel 占位符家族的动态扩展：首次访问后自动"实体化"。

```python
from flowlet.base.lazy import LazyHolder

# 定义加载函数
def load_data(file_path: str):
    import pandas as pd
    return pd.read_parquet(file_path)

# 创建懒加载占位符（此时数据尚未加载）
lazy = LazyHolder(load_data, file_path="data.parquet")

# 检查是否已解析
assert not lazy.is_resolved()

# 首次取值时触发实际加载
value = lazy.resolve()

# 再次取值时返回缓存结果（不会重新加载）
value2 = lazy.resolve()

# 使缓存失效，下次 resolve 将重新加载
lazy.invalidate()
```

### LazyUnitKernel

懒加载执行单元，继承自 `Workflow`。用于将 `Callable` 或 `ExecutableUnit` 包装为延迟执行的组件。

```python
from flowlet.base.lazy import LazyUnitKernel
from flowlet.executable_unit import Kernel

class MyKernel(Kernel[MyConfig, str]):
    def __call__(self, data: str) -> str:
        return data.upper()

# 绑定一个 ExecutableUnit 作为懒加载目标
lazy_unit = LazyUnitKernel()
lazy_unit = lazy_unit.bind_input(MyKernel())

# 执行时自动调用绑定的单元
result = lazy_unit.execute()
```

---

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

---

## 类型注解工具

`flowlet.base.type_annotation` 模块提供类型注解处理工具函数，基于 `TypeKind` 分类系统实现。

### TypeKind 类型分类

`TypeKind` 枚举用于分类不同类型的对象：

```python
from flowlet.base.type_annotation import TypeKind, classify_object

# 类型分类
classify_object(int)                    # TypeKind.BARE_CLASS
classify_object(list[str])              # TypeKind.GENERIC_ALIAS
classify_object(int | str)              # TypeKind.UNION_TYPE
classify_object(Any)                    # TypeKind.TYPING_FORM
classify_object(123)                    # TypeKind.INSTANCE
```

**TypeKind 分类说明：**

- `BARE_CLASS`：裸类（int, str, MyClass），纯净的类对象，可用于实例化
- `GENERIC_ALIAS`：泛型别名（list[str], dict[int, str]），Python 3.9+
- `UNION_TYPE`：联合类型（str | int, Optional[str]），Python 3.10+
- `TYPING_FORM`：typing 特殊形式（Any, TypeVar, ForwardRef, NoReturn）
- `INSTANCE`：实例值（123, "hello", []），已经创建的数据值
- `UNKNOWN`：未知类型（如模块、函数等不好分类的）

### classify_object

对任意对象进行类型分类：

```python
from flowlet.base.type_annotation import classify_object, TypeKind

# 检查各种类型
classify_object(int) == TypeKind.BARE_CLASS           # True
classify_object(list[str]) == TypeKind.GENERIC_ALIAS  # True
classify_object(str | int) == TypeKind.UNION_TYPE     # True
classify_object(Any) == TypeKind.TYPING_FORM          # True
classify_object([1, 2, 3]) == TypeKind.INSTANCE       # True
```

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

### unwrap_type

将类型注解展开为基础类型的扁平列表：

```python
from flowlet.base.type_annotation import unwrap_type

# 基本类型
unwrap_type(int)  # [int]
unwrap_type(list[str])  # [list]

# Union 类型
unwrap_type(str | int)  # [str, int]
unwrap_type(list[str] | dict[int, bool])  # [list, dict]
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
