"""轻量级反向 DAG 计算图系统。

每个 TaskNode 只记录上级节点（parents）。bind 时信息向上传递：
父节点的缺失输入（InputVar）自然冒泡到子节点。
验证（环路检测、命名冲突）在 bind 时完成。
执行时向上追溯，拓扑排序，同层并行。
"""

from .graph import TracedGraph
from .node import OutputRef, TaskNode
from .types import InputField, InputSlot, InputVar, OutputField, OutputSpec

__all__ = [
    "InputField",
    "InputSlot",
    "InputVar",
    "OutputField",
    "OutputRef",
    "OutputSpec",
    "TaskNode",
    "TracedGraph",
]
