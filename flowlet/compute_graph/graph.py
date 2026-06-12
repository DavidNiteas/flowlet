"""TracedGraph：从 TaskNode 向上追溯得到的显式图表示及导出。"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .types import InputVar

if TYPE_CHECKING:
    from .node import TaskNode


class TracedGraph:
    """从某个 TaskNode 向上追溯得到的显式图表示。"""

    def __init__(self, root: TaskNode) -> None:
        self._root = root
        self._nodes: dict[int, TaskNode] = {}
        self._edges: list[tuple[int, int, str]] = []  # (parent_id, child_id, slot_name)
        self._input_vars: dict[str, InputVar] = {}
        # children: {parent_id: [(slot_name, child_node), ...]}
        self._children: dict[int, list[tuple[str, TaskNode]]] = {}
        self._build()

    def _build(self) -> None:
        """从 root 向上追溯，构建节点和边的集合。"""
        visited: set[int] = set()

        def dfs(node: TaskNode) -> None:
            node_id = id(node)
            if node_id in visited:
                return
            visited.add(node_id)
            self._nodes[node_id] = node

            for slot_name, value in node._bindings.items():
                if isinstance(value, InputVar):
                    self._input_vars[value.name] = value
                elif hasattr(value, "_parents"):
                    # TaskNode（避免循环导入，不使用 isinstance）
                    parent_id = id(value)
                    self._edges.append((parent_id, node_id, slot_name))
                    self._children.setdefault(parent_id, []).append((slot_name, node))
                    dfs(value)
                elif hasattr(value, "_node"):
                    # OutputRef
                    parent_id = id(value._node)
                    self._edges.append((parent_id, node_id, slot_name))
                    self._children.setdefault(parent_id, []).append((slot_name, node))
                    dfs(value._node)

        dfs(self._root)

    def to_dict(self) -> dict[str, Any]:
        """导出为类 Dask 的字典格式。

        Returns:
            {node_name: (func_name, {slot: parent_name_or_value})}
        """
        name_map: dict[int, str] = {}
        for node_id, node in self._nodes.items():
            name_map[node_id] = node._name

        result: dict[str, Any] = {}
        for node_id, node in self._nodes.items():
            name = name_map[node_id]
            deps: dict[str, Any] = {}
            for slot_name, value in node._bindings.items():
                if isinstance(value, InputVar):
                    deps[slot_name] = f"InputVar({value.name})"
                elif hasattr(value, "_parents"):
                    deps[slot_name] = name_map[id(value)]
                elif hasattr(value, "_selector"):
                    deps[slot_name] = f"{name_map[id(value._node)]}[{value._selector!r}]"
                else:
                    deps[slot_name] = repr(value)
            result[name] = (node._name, deps)
        return result

    def to_mermaid(self, direction: str = "TB") -> str:
        """导出为 Mermaid 流程图语法。

        Args:
            direction: 图方向，"TB"（上下）或 "LR"（左右）。

        Returns:
            Mermaid 流程图字符串。
        """
        lines = [f"flowchart {direction}"]

        # 输入变量节点
        lines.extend(f'    input_{name}["📝 {name}"]' for name in self._input_vars)

        # 任务节点
        lines.extend(f'    node_{node_id}["⚙️ {node._name}"]' for node_id, node in self._nodes.items())

        # 边
        lines.extend(
            f"    node_{parent_id} -->|{slot_name}| node_{child_id}" for parent_id, child_id, slot_name in self._edges
        )

        # 输入变量到节点的边
        lines.extend(
            f"    input_{value.name} -->|{slot_name}| node_{node_id}"
            for node_id, node in self._nodes.items()
            for slot_name, value in node._bindings.items()
            if isinstance(value, InputVar)
        )

        return "\n".join(lines)

    def to_json(self) -> dict[str, Any]:
        """导出为 JSON 可序列化的字典。"""
        name_map: dict[int, str] = {}
        for idx, (node_id, node) in enumerate(self._nodes.items(), 1):
            name_map[node_id] = f"{node._name}_{idx}"

        nodes = []
        for node_id, node in self._nodes.items():
            bindings = {}
            for slot_name, value in node._bindings.items():
                if isinstance(value, InputVar):
                    bindings[slot_name] = {"type": "input_var", "name": value.name}
                elif hasattr(value, "_parents"):
                    bindings[slot_name] = {"type": "node", "ref": name_map[id(value)]}
                elif hasattr(value, "_selector"):
                    bindings[slot_name] = {
                        "type": "output_ref",
                        "ref": name_map[id(value._node)],
                        "selector": value._selector,
                    }
                else:
                    bindings[slot_name] = {"type": "constant", "value": repr(value)}
            nodes.append(
                {
                    "id": name_map[node_id],
                    "name": node._name,
                    "bindings": bindings,
                    "missing": list(node._missing.keys()),
                }
            )

        edges = [
            {
                "from": name_map[parent_id],
                "to": name_map[child_id],
                "slot": slot_name,
            }
            for parent_id, child_id, slot_name in self._edges
        ]

        return {
            "root": name_map[id(self._root)],
            "input_vars": list(self._input_vars.keys()),
            "nodes": nodes,
            "edges": edges,
        }

    def to_dot(self) -> str:
        """导出为 Graphviz DOT 格式。"""
        lines = ["digraph G {"]
        name_map: dict[int, str] = {}
        for idx, (node_id, node) in enumerate(self._nodes.items(), 1):
            name_map[node_id] = f"n{idx}_{node._name}"

        # 节点定义
        for node_id, node in self._nodes.items():
            label = node._name
            if node._missing:
                label += f"\\n(missing: {', '.join(node._missing.keys())})"
            lines.append(f'    {name_map[node_id]} [label="{label}"];')

        # 输入变量节点
        for name in self._input_vars:
            var_id = f"input_{name}"
            lines.append(f'    {var_id} [label="📝 {name}", shape=note];')

        # 边
        for parent_id, child_id, slot_name in self._edges:
            lines.append(f'    {name_map[parent_id]} -> {name_map[child_id]} [label="{slot_name}"];')

        # 输入变量边
        for node_id, node in self._nodes.items():
            for slot_name, value in node._bindings.items():
                if isinstance(value, InputVar):
                    lines.append(f'    input_{value.name} -> {name_map[node_id]} [label="{slot_name}", style=dashed];')

        lines.append("}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 可视化 / 调试渲染
    # ------------------------------------------------------------------
    def render_tree(self) -> str:
        """从 root 向上渲染依赖树（我依赖谁），参考 StaticStructureDAG.render_str。

        Returns:
            带连接线的文本树形表示。
        """
        lines = [f"🎯 {self._root._name}"]

        def walk(node, prefix: str) -> None:
            """向上遍历 node 的 parents。"""
            parents: list[tuple[str, Any]] = []
            for slot_name, value in node._bindings.items():
                if hasattr(value, "_parents"):
                    parents.append((slot_name, value))
                elif hasattr(value, "_node"):
                    parents.append((slot_name, value._node))

            for index, (slot_name, parent) in enumerate(parents):
                is_last = index == len(parents) - 1
                connector = "└── " if is_last else "├── "
                lines.append(f"{prefix}{connector}[{slot_name}] {parent._name}")
                extension = "    " if is_last else "│   "
                walk(parent, prefix + extension)

        walk(self._root, "")
        return "\n".join(lines)

    def render_layers(self) -> str:
        """渲染按层分组的执行计划（层化流程模式）。

        显示拓扑排序后每一层的节点，同层可并行。

        Returns:
            带层号的文本表示。
        """

        layers = self._root._topological_sort(self._nodes)

        lines = [f"📋 Execution Plan: '{self._root._name}'"]
        lines.append("=" * 48)

        for i, layer in enumerate(layers, 1):
            names = [node._name for node in layer]
            parallel_icon = "⚡" if len(layer) > 1 else "➡️"
            lines.append(f"  Layer {i:2d} {parallel_icon}  {', '.join(names)}")
            # 显示每个节点的绑定信息
            for node in layer:
                bound_info = []
                for slot, value in node._bindings.items():
                    if isinstance(value, InputVar):
                        bound_info.append(f"{slot}=InputVar({value.name})")
                    elif hasattr(value, "_parents"):
                        bound_info.append(f"{slot}={value._name}")
                    elif hasattr(value, "_node"):
                        bound_info.append(f"{slot}={value._node._name}[{value._selector!r}]")
                    else:
                        bound_info.append(f"{slot}={value!r}")
                if bound_info:
                    lines.append(f"           └─ {node._name}({', '.join(bound_info)})")

        # 外部输入变量汇总
        if self._input_vars:
            lines.append("")
            lines.append("📝 External Inputs:")
            lines.extend(f"    • {name}" for name in self._input_vars)

        return "\n".join(lines)

    def __repr__(self) -> str:
        return (
            f"TracedGraph(nodes={len(self._nodes)}, edges={len(self._edges)}, inputs={list(self._input_vars.keys())})"
        )
