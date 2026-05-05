# 计算图（Compute Graph）

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

`flowlet.compute_graph` 提供轻量级的**反向 DAG** 任务图系统。每个 `TaskNode` 只记录依赖的上级节点（parents），不记录下级节点。验证（环路检测、命名冲突）在 `bind` 时完成，执行时向上追溯、拓扑排序、按层并行。

---

## 设计哲学

1. **完全解耦**：`TaskNode` 包装任何 `Callable`，不限于 `ExecutableUnit`
2. **隐式构图**：没有显式的 `Graph` 对象，每个节点自身就是以它为 root 的追溯图
3. **编译期验证**：环路、重名、缺失输入在 `bind` 时抛出
4. **轻量级优化**：拓扑排序缓存、纯常量子图增量执行、同层并行

---

## 核心类型

### InputSlot / OutputSpec

```python
from flowlet.compute_graph import InputSlot, OutputSpec

# 输入槽：name, required, default
slot = InputSlot("data", required=True)
slot_opt = InputSlot("ratio", default=0.2)

# 输出规格：single / sequence / mapping / tuple
out = OutputSpec("tuple")
```

### InputVar

显式声明需要外部提供的命名输入。会沿着 parent 链**向上冒泡**，最终集中在 root 节点。

```python
from flowlet.compute_graph import InputVar

var = InputVar("dataset_path")
```

### TaskNode

任务图节点，核心 API：

```python
from flowlet.compute_graph import TaskNode, InputSlot, OutputSpec, InputVar

# 1. 定义节点
load = TaskNode(
    func=pd.read_csv,
    inputs=[InputSlot("path")],
    outputs=OutputSpec("single"),
    name="load",
)

split = TaskNode(
    func=lambda df, ratio: train_test_split(df, test_size=ratio),
    inputs=[InputSlot("data"), InputSlot("ratio", default=0.2)],
    outputs=OutputSpec("tuple"),
    name="split",
)

# 2. 绑定（构建图连接）
raw = load.bind(path=InputVar("dataset_path"))
dataset = split.bind(data=raw, ratio=0.2)

# 3. 执行
result = dataset.execute({"dataset_path": "data.csv"})
```

---

## 绑定语义

### 绑定值类型

| 值类型 | 行为 | 示例 |
|--------|------|------|
| 常量 | 直接作为输入 | `node.bind(x=42)` |
| `InputVar` | 声明外部输入，向上冒泡 | `node.bind(x=InputVar("name"))` |
| `TaskNode` | 记录为 parent，继承其缺失输入 | `node.bind(x=parent_node)` |
| `OutputRef` | 绑定 parent 的子输出 | `node.bind(x=parent[0])` |

### 缺失输入冒泡

父节点未绑定的 `InputVar` 会自动传递给子节点：

```python
raw = load.bind(path=InputVar("dataset_path"))
# raw._missing = {"dataset_path": InputVar("dataset_path")}

dataset = split.bind(data=raw)
# dataset._missing = {"dataset_path": InputVar("dataset_path")}  ← 从 raw 冒泡上来

# 执行时只需在 root 提供
dataset.execute({"dataset_path": "data.csv"})
```

### 验证（bind 时完成）

```python
# 1. 输入槽存在性
node.bind(unknown="value")  # → UnknownSlotError

# 2. 环路检测
a = node_a.bind(x=InputVar("start"))
b = node_b.bind(y=a)
a.bind(x=b)  # → CyclicDependencyError

# 3. 命名冲突
var1 = InputVar("x")
var2 = InputVar("x")  # 不同对象，同名
node.bind(a=var1, b=var2)  # → DuplicateNameError
```

---

## 多输出拆分

```python
dataset = split.bind(data=raw)

train = dataset[0]   # tuple/sequence 索引
test = dataset[1]

metrics = eval_node.bind(model=model, data=test)
acc = metrics["acc"]  # mapping key

# 嵌套选择
batch = dataset[0]["train"]  # sequence of mappings
```

---

## 管道语法糖 `>>`

自动推断唯一的必填输入槽：

```python
# 单输入槽，自动推断
pipeline = load >> process >> train

# OutputRef 也支持
model = (load >> split)[0] >> train

# 多输入槽时报错，引导显式 bind
with pytest.raises(ValueError, match="无法为 'multi' 推断"):
    raw >> multi_input_node
```

---

## 执行与优化

### 执行流程

```python
# 1. 向上追溯收集所有祖先节点
# 2. 拓扑排序（Kahn 算法，按层分组）
# 3. 逐层执行（同层并行 via ThreadParallelWorkflow）
# 4. 每层结束后等待 future-like 结果
result = node.execute(inputs)
```

### 内置优化

| 优化 | 说明 |
|------|------|
| **死节点消除** | `_trace_ancestors()` 只收集 root 可达节点 |
| **共享子图去重** | `visited` set 确保同一节点只执行一次 |
| **拓扑排序缓存** | 不可变图结构，排序结果永久复用 |
| **纯常量增量执行** | 无 `InputVar` 的子图执行一次后永久缓存 |
| **同层并行** | `ThreadParallelWorkflow` 并行执行无依赖节点 |

---

## 图外并行：map

对同一个图应用到多组输入：

```python
dataset = load.bind(path=InputVar("dataset_path"))

results = dataset.map([
    {"dataset_path": "a.csv"},
    {"dataset_path": "b.csv"},
    {"dataset_path": "c.csv"},
])
# 底层使用 ThreadParallelWorkflow 并行执行
```

---

## 可视化与调试

### trace() — 显式图表示

```python
traced = node.trace()

# 类 Dask dict
traced.to_dict()

# Mermaid 流程图
traced.to_mermaid()

# Graphviz DOT
traced.to_dot()

# JSON 序列化
traced.to_json()

# 树形依赖图（从 root 向上）
print(traced.render_tree())
# 🎯 eval
# └── [model] train
#     └── [data] split
#         └── [data] load
#             └── [path] InputVar(dataset_path)

# 层化执行计划
print(traced.render_layers())
# 📋 Execution Plan: 'eval'
# =================================================
#   Layer  1 ➡️   load
#   Layer  2 ➡️   split
#   Layer  3 ➡️   train
#   Layer  4 ➡️   eval
```

---

## 与 ExecutableUnit 集成

### input_field / output_field 元数据

在 `ExecutableUnit` 子类中声明输入输出元数据，实现一键转 `TaskNode`：

```python
from flowlet import Kernel
from flowlet.compute_graph import InputField, OutputField

class MyKernel(Kernel):
    config = {}

    # 方式 1：使用 InputField / OutputField 对象
    input_field = [
        InputField("data", required=True),
        InputField("ratio", default=0.2),
    ]
    output_field = OutputField("tuple")

    # 方式 2：使用字典（更轻量）
    # input_field = [
    #     {"name": "data", "required": True},
    #     {"name": "ratio", "default": 0.2},
    # ]
    # output_field = {"type": "tuple"}

    def __call__(self, data, ratio=0.2):
        return train_test_split(data, test_size=ratio)

# 一键转 TaskNode，无需手动传入 inputs/outputs
unit = MyKernel()
task = unit.as_task()  # 自动识别
```

### as_task() 方法

```python
# 自动识别（类定义了 input_field / output_field）
task = unit.as_task()

# 手动传入（覆盖自动识别）
task = unit.as_task(
    inputs=[InputSlot("data")],
    outputs=OutputSpec("single"),
    name="custom_name",
)
```

### ExecutionFuture（异步执行）

`execute_async()` / `execute_ray()` 返回 `ExecutionFuture`，可用于计算图中的异步节点：

```python
from flowlet import ExecutionFuture

# 返回 future-like 对象
future = unit.execute_async()

future.is_done()   # 检查是否完成
future.join()      # 阻塞等待
future.get()       # 阻塞等待并返回结果
future()           # 快捷方式

# 在 TaskNode 中使用
async_node = TaskNode(unit.execute_async, inputs=[...], outputs=...)
# execute() 每层结束后自动等待 future-like 完成
```

---

## 错误参考

| 异常 | 触发时机 | 说明 |
|------|----------|------|
| `UnknownSlotError` | `bind()` | 绑定了不存在的输入槽 |
| `DuplicateNameError` | `bind()` | 两个不同的 `InputVar` 同名 |
| `CyclicDependencyError` | `bind()` / `execute()` | 图中存在环 |
| `UnboundInputError` | `execute()` | 必填输入未绑定或外部输入未提供 |
