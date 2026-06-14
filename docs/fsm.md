# 有限状态机（FSM）

> [← 返回 README](../README.md) | [↑ 文档首页](index.md)

`flowlet.fsm` 提供建立在 EdgeNode 之上的有限状态机控制层。它适合把一个有状态节点的执行过程拆成可审计的状态迁移：每一步由 `TransitionSpec` 定义，运行时状态保存在 EdgeNode 的 `state` 中。

---

## 核心类型

| 类型 | 说明 |
|------|------|
| `TransitionSpec` | 单条状态迁移定义：source、target、event、action、guard、on_error |
| `StateMachineSpec` | 静态状态机定义：initial、transitions、terminal、runtime_key |
| `FSMRuntime` | 运行时状态：当前 state、status、history、last_error |
| `TransitionRecord` | 每次迁移的历史记录：名称、源状态、目标状态、事件、耗时、错误 |
| `FSMEdgeNode` | 使用 `ThreadEdgeNode` / `AsyncioEdgeNode` / `RayEdgeNode` 执行状态机 |

`FSMStatus` 包含：

- `NOT_STARTED`
- `RUNNING`
- `TERMINAL`
- `FAILED`

---

## 基本用法

```python
from flowlet import FSMEdgeNode, FSMStatus, StateMachineSpec, TransitionSpec

def load_action(state, runtime, event):
    state["loaded"] = True

def add_action(state, runtime, event):
    state["value"] = state.get("value", 0) + 1

def require_loaded(state, runtime, event):
    return bool(state.get("loaded"))

spec = StateMachineSpec.create(
    initial="new",
    terminal={"done"},
    transitions=[
        TransitionSpec("load", "new", "loaded", action=load_action),
        TransitionSpec("add", "loaded", "done", action=add_action, guard=require_loaded),
    ],
)

with FSMEdgeNode(spec, backend="thread", initializer=lambda: {"value": 0}) as fsm:
    runtime = fsm.run_until_terminal()

    assert runtime.state == "done"
    assert runtime.status == FSMStatus.TERMINAL
    assert fsm.pull("value") == 1
    assert [r.transition for r in runtime.history] == ["load", "add"]
```

---

## 事件驱动迁移

`TransitionSpec.event` 用于限定迁移只响应指定事件。`FSMEdgeNode.run_until_terminal(events)` 会按顺序送入事件。

```python
spec = StateMachineSpec.create(
    initial="idle",
    terminal={"done"},
    transitions=[
        TransitionSpec("start", "idle", "running", event="start"),
        TransitionSpec("finish", "running", "done", event="finish", action=add_action),
    ],
)

with FSMEdgeNode(spec, backend="thread", initializer=lambda: {"value": 0}) as fsm:
    runtime = fsm.run_until_terminal(["start", "finish"])
    assert runtime.state == "done"
```

如果当前状态没有匹配迁移，运行时会进入 `FAILED`，`last_error` 会记录错误原因。

---

## Guard 与错误迁移

`guard(state, runtime, event)` 返回 `False` 时，迁移被拒绝，状态机进入 `FAILED`。

`action` 抛出异常时：

- 如果设置了 `on_error`，状态会迁移到 `on_error` 指定状态，并把错误写入历史记录。
- 如果未设置 `on_error`，状态机进入 `FAILED`。

```python
def fail_action(state, runtime, event):
    raise ValueError("step failed")

spec = StateMachineSpec.create(
    initial="new",
    terminal={"failed"},
    transitions=[
        TransitionSpec("fail", "new", "done", action=fail_action, on_error="failed"),
    ],
)

with FSMEdgeNode(spec, backend="thread") as fsm:
    runtime = fsm.run_until_terminal()
    assert runtime.state == "failed"
    assert runtime.status == FSMStatus.TERMINAL
    assert runtime.last_error == "step failed"
```

---

## 后端选择

`FSMEdgeNode` 的 `backend` 可以是 `"thread"`、`"asyncio"`、`"ray"`，也可以传入已经创建好的 `EdgeNode` 实例。

| backend | 使用场景 |
|---------|----------|
| `"thread"` | 同步 action、同进程调试、无需跨进程序列化 |
| `"asyncio"` | async action / awaitable initializer / 异步 IO 状态机 |
| `"ray"` | 跨进程持有状态或不可序列化对象，action 和参数需满足 Ray 可导入/可序列化约束 |

异步 action 需要 `backend="asyncio"`：

```python
import asyncio
from flowlet import FSMEdgeNode, StateMachineSpec, TransitionSpec

async def async_add(state, runtime, event):
    await asyncio.sleep(0.01)
    state["value"] = state.get("value", 0) + 1

spec = StateMachineSpec.create(
    initial="new",
    terminal={"done"},
    transitions=[
        TransitionSpec("async_add", "new", "done", action=async_add),
    ],
)

with FSMEdgeNode(spec, backend="asyncio", initializer=lambda: {"value": 0}) as fsm:
    runtime = fsm.run_until_terminal()
    assert runtime.state == "done"
```

同步后端遇到 awaitable action 会进入错误处理路径；如果迁移定义了 `on_error`，会按 `on_error` 迁移，否则进入 `FAILED`。

---

## 分步控制

```python
with FSMEdgeNode(spec, backend="thread") as fsm:
    fsm.start(reset=True)
    fsm.join()

    fsm.step("start")
    fsm.join()

    print(fsm.current_state())
    print(fsm.status())
    print(fsm.runtime().history)
```

`start(reset=False)` 会初始化或恢复运行时；`reset=True` 会从 `initial` 状态重新开始。

---

## 注意事项

1. `StateMachineSpec` 要求 `initial` 非空、`transitions` 非空，迁移名称不能重复。
2. `source` 可以是单个状态字符串，也可以是状态集合。
3. `runtime_key` 默认是 `"_fsm"`，运行时对象会存入 Edge state 的这个 key。
4. Ray 后端中传入的 `action` / `guard` 建议定义在可导入模块中，避免 worker 无法反序列化。
5. `run_until_terminal(max_steps=100)` 无事件列表时会持续执行无事件迁移，直到 `TERMINAL` / `FAILED` 或达到步数上限。
