"""Edge 节点内部的目标统一分发。

无论是 Thread 后端还是 Ray 后端，都在 worker / actor 进程内调用这里的
``_execute_target`` 来执行函数、Kernel / Workflow / Dispatcher / Strategy
或 TaskNode 图。
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from ..compute_graph import TaskNode
from ..executable_unit import ExecutableUnit
from .errors import UnsupportedTargetError


def _normalize_state(value: Any) -> dict[str, Any]:
    """把 initializer 返回值统一为 namespace dict。"""
    if isinstance(value, Mapping):
        return dict(value)
    return {"_data": value}


def _execute_target(
    state: dict[str, Any],
    target: Any,
    output: str | None,
    kwargs: dict[str, Any],
) -> None:
    """在 state 上执行 target，并把结果写入 ``state[output]``。

    Args:
        state: Edge 节点内部状态（namespace dict）。
        target: 函数、ExecutableUnit 实例或 TaskNode 图。
        output: 结果存储的目标变量名；为 ``None`` 时不存储。
        kwargs: 本次执行额外传入的命名参数。

    Raises:
        UnsupportedTargetError: target 类型无法识别。
    """
    if isinstance(target, TaskNode):
        result = _execute_task_node(state, target, kwargs)
    elif isinstance(target, ExecutableUnit):
        result = target.bind_input(state, **kwargs).execute()
    elif callable(target):
        result = target(state, **kwargs)
    else:
        raise UnsupportedTargetError(f"EdgeNode 不支持的 target 类型: {type(target).__name__}")

    if output is not None:
        state[output] = result


def _execute_task_node(
    state: dict[str, Any],
    graph: TaskNode,
    kwargs: dict[str, Any],
) -> Any:
    """执行 TaskNode 图，state 中的 key 自动映射为 InputVar。"""
    # 深拷贝并清空执行缓存，确保同一张图在节点内多次执行时不会被旧结果污染。
    graph = copy.deepcopy(graph)
    _reset_task_node(graph)
    inputs = {**state, **kwargs}
    return graph.execute(inputs)


def _reset_task_node(node: TaskNode) -> None:
    """递归清空 TaskNode 的执行状态。"""
    node._executed = False
    node._result = None
    node._cached_nodes = None
    node._cached_order = None
    for parent in node._parents:
        _reset_task_node(parent)
