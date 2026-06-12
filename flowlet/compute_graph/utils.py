"""计算图辅助函数。"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .node import TaskNode


def _resolve_selector(result: Any, selector: Any) -> Any:
    """从结果中提取子元素。

    支持嵌套选择器（tuple）。
    """
    if isinstance(selector, tuple):
        for s in selector:
            result = result[s]
        return result
    return result[selector]


def _has_cycle(potential_parent: TaskNode, origin: TaskNode) -> bool:
    """检查 potential_parent 的祖先链中是否包含 origin。

    如果包含，说明将 potential_parent 绑定为 origin 的 parent 会形成环。
    """
    visited: set[int] = set()

    def dfs(node: TaskNode) -> bool:
        if node is origin:
            return True
        node_id = id(node)
        if node_id in visited:
            return False
        visited.add(node_id)
        return any(dfs(p) for p in node._parents)

    return dfs(potential_parent)


def _nest_selector(base: Any, key: Any) -> Any:
    """构建嵌套选择器。"""
    if isinstance(base, tuple):
        return base + (key,)
    return (base, key)


def _is_future_like(obj: Any) -> bool:
    """检查对象是否为 future-like（有 is_done/join/get 接口）。"""
    return hasattr(obj, "is_done") and hasattr(obj, "get")


def _resolve_future(future: Any) -> Any:
    """等待 future-like 对象完成并返回结果。"""
    return future.get()


def _pipe_bind(target: TaskNode, source: TaskNode | Any) -> TaskNode:
    """将 source 作为 target 的输入，自动推断输入槽。"""
    required_slots = [name for name, slot in target._inputs.items() if slot.required and slot.default is None]
    if len(required_slots) == 1:
        return target.bind(**{required_slots[0]: source})

    all_slots = list(target._inputs.keys())
    if len(all_slots) == 1:
        return target.bind(**{all_slots[0]: source})

    raise ValueError(
        f"无法为 '{target._name}' 推断唯一的输入槽。"
        f"必填槽: {required_slots}, 所有槽: {all_slots}。"
        f"请使用显式绑定，如 {target._name}.bind(slot={source})"
    )
