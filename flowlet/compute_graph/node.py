"""反向 DAG 任务图节点。

设计要点：
- 每个节点只记录 parents（上级依赖），不记录 children。
- bind 时向上传递缺失输入（InputVar 冒泡）。
- 环路检测和命名冲突在 bind 时完成。
- execute 时向上追溯、拓扑排序执行。
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

from .errors import (
    CyclicDependencyError,
    DuplicateNameError,
    UnboundInputError,
    UnknownSlotError,
)
from .types import InputSlot, InputVar, OutputSpec
from .utils import (
    _has_cycle,
    _is_future_like,
    _pipe_bind,
    _resolve_future,
    _resolve_selector,
)


class OutputRef:
    """对 TaskNode 子输出的引用（序列索引、映射 key、元组索引）。"""

    def __init__(self, node: TaskNode, selector: Any) -> None:
        self._node = node
        self._selector = selector

    def __getitem__(self, key: Any) -> OutputRef:
        """支持嵌套选择，如 result['batch'][0]。"""
        from .utils import _nest_selector

        return OutputRef(self._node, _nest_selector(self._selector, key))

    def __rshift__(self, other: TaskNode) -> TaskNode:
        """管道操作：ref >> other。

        将子输出引用作为 other 的输入。
        """
        if not isinstance(other, TaskNode):
            return NotImplemented
        return _pipe_bind(other, self)

    def __repr__(self) -> str:
        return f"OutputRef({self._node._name!r}, {self._selector!r})"


class TaskNode:
    """任务图节点。

    只记录 parents（我依赖谁），不记录 children（谁依赖我）。
    bind 时父节点的缺失输入（InputVar）向上冒泡。
    """

    def __init__(
        self,
        func: Callable,
        inputs: list[InputSlot],
        outputs: OutputSpec,
        name: str | None = None,
    ) -> None:
        self._func = func
        self._inputs = {slot.name: slot for slot in inputs}
        self._output_spec = outputs
        self._name = name or getattr(func, "__name__", repr(func))
        self._bindings: dict[str, Any] = {}
        self._parents: list[TaskNode] = []
        self._missing: dict[str, InputVar] = {}
        self._result: Any = None
        self._executed: bool = False
        # 拓扑排序缓存（TaskNode 不可变，图结构不变，排序结果可复用）
        self._cached_nodes: dict[int, TaskNode] | None = None
        self._cached_order: list[list[TaskNode]] | None = None

    # ------------------------------------------------------------------
    # 绑定
    # ------------------------------------------------------------------
    def bind(self, **kwargs: Any) -> TaskNode:
        """绑定输入，返回新节点（原节点不被修改）。

        值可以是：
        - 常量：直接作为输入。
        - InputVar：显式声明需要外部提供的命名输入。
        - TaskNode：绑定另一个节点的整体输出，记录为 parent，继承其缺失输入。
        - OutputRef：绑定另一个节点的子输出，记录为 parent，继承其缺失输入。

        在绑定过程中会检查：
        - 输入槽是否存在。
        - 是否会形成环路。
        - 命名输入变量是否冲突（同名但不同对象）。
        """
        new = copy.copy(self)
        new._bindings = dict(self._bindings)
        new._parents = list(self._parents)
        new._missing = dict(self._missing)
        # bind 改变了图结构，缓存失效
        new._cached_nodes = None
        new._cached_order = None

        for slot_name, value in kwargs.items():
            if slot_name not in self._inputs:
                raise UnknownSlotError(
                    f"节点 '{self._name}' 没有输入槽 '{slot_name}'"
                )

            new._bindings[slot_name] = value

            if isinstance(value, InputVar):
                # 显式声明外部输入变量
                new._merge_missing({value.name: value})

            elif isinstance(value, TaskNode):
                # 绑定另一个节点 → 记录为 parent，继承其缺失输入
                if _has_cycle(value, self):
                    raise CyclicDependencyError(
                        f"将 '{value._name}' 绑定到 '{self._name}.{slot_name}' 会形成环"
                    )
                new._parents.append(value)
                new._merge_missing(value._missing)

            elif isinstance(value, OutputRef):
                parent = value._node
                if _has_cycle(parent, self):
                    raise CyclicDependencyError(
                        f"将 '{parent._name}' 绑定到 '{self._name}.{slot_name}' 会形成环"
                    )
                new._parents.append(parent)
                new._merge_missing(parent._missing)

            # 常量：不影响缺失输入

        return new

    def _merge_missing(self, other: dict[str, InputVar]) -> None:
        """合并另一个节点的缺失输入。

        同名必须是同一对象，否则抛出 DuplicateNameError。
        """
        for name, var in other.items():
            if name in self._missing and self._missing[name] is not var:
                raise DuplicateNameError(
                    f"输入变量 '{name}' 冲突："
                    f"{self._missing[name]!r} vs {var!r}"
                )
            self._missing[name] = var

    # ------------------------------------------------------------------
    # 多输出拆分
    # ------------------------------------------------------------------
    def __getitem__(self, key: Any) -> OutputRef:
        """获取子输出引用。

        Examples:
            node[0]        # 序列/元组的第 0 个元素
            node["train"]  # 映射的 "train" 键
        """
        return OutputRef(self, key)

    def __rshift__(self, other: TaskNode) -> TaskNode:
        """管道操作：self >> other。

        将 self 的输出作为 other 的输入。other 必须只有一个
        必填输入槽（或只有一个输入槽），否则需要显式 bind。

        Examples:
            pipeline = load >> process >> train
            # 等价于 train.bind(data=process.bind(data=load))
        """
        if not isinstance(other, TaskNode):
            return NotImplemented
        return _pipe_bind(other, self)

    # ------------------------------------------------------------------
    # 执行
    # ------------------------------------------------------------------
    def execute(self, inputs: dict[str, Any] | None = None) -> Any:
        """执行以当前节点为 root 的追溯图。

        向上追溯所有祖先节点，拓扑排序后执行。
        同层无依赖节点可并行（使用 ThreadParallelWorkflow）。
        每层执行后，自动等待所有 future-like 结果完成。

        Args:
            inputs: 命名输入变量的值，键为 InputVar.name。

        Returns:
            当前节点的执行结果。

        Raises:
            UnboundInputError: 如果存在未绑定的必填输入或未提供的外部输入。
        """
        inputs = inputs or {}

        # 检查所有 required slot 是否都已绑定
        unbound_slots = [
            name
            for name, slot in self._inputs.items()
            if name not in self._bindings
            and slot.required
            and slot.default is None
        ]
        if unbound_slots:
            raise UnboundInputError(
                f"节点 '{self._name}' 存在未绑定的必填输入: {unbound_slots}"
            )

        # 检查所有缺失的外部输入是否已提供
        missing_names = set(self._missing.keys()) - set(inputs.keys())
        if missing_names:
            raise UnboundInputError(f"未提供的外部输入: {missing_names}")

        # 追溯收集所有祖先节点（利用缓存）
        if self._cached_nodes is None:
            self._cached_nodes = self._trace_ancestors()
            self._cached_order = self._topological_sort(self._cached_nodes)
        order = self._cached_order

        # 逐层执行
        results: dict[int, Any] = {}
        for layer in order:
            # 分离已缓存节点和需执行节点
            cached = []
            to_run = []
            for node in layer:
                if node._executed and not node._missing:
                    cached.append(node)
                else:
                    to_run.append(node)

            # 已缓存节点直接复用结果
            for node in cached:
                results[id(node)] = node._result

            # 执行剩余节点
            if len(to_run) == 1:
                node = to_run[0]
                results[id(node)] = node._run(results, inputs)
            elif to_run:
                results = self._execute_parallel(to_run, results, inputs)

            # 等待 future-like 结果并缓存
            for node in to_run:
                node_id = id(node)
                result = results[node_id]
                if _is_future_like(result):
                    results[node_id] = _resolve_future(result)
                    node._result = results[node_id]
                    node._executed = True

        self._result = results[id(self)]
        self._executed = True
        return self._result

    def _trace_ancestors(self) -> dict[int, TaskNode]:
        """从当前节点向上追溯，收集所有祖先节点（去重）。"""
        all_nodes: dict[int, TaskNode] = {}
        visited: set[int] = set()

        def dfs(node: TaskNode) -> None:
            node_id = id(node)
            if node_id in visited:
                return
            visited.add(node_id)
            all_nodes[node_id] = node
            for p in node._parents:
                dfs(p)

        dfs(self)
        return all_nodes

    def _topological_sort(
        self, all_nodes: dict[int, TaskNode]
    ) -> list[list[TaskNode]]:
        """对所有节点进行拓扑排序，返回按层分组的节点列表。

        每一层内的节点之间没有依赖关系，可以并行执行。
        """
        # 计算每个节点的入度（被多少其他节点依赖）
        in_degree: dict[int, int] = {node_id: 0 for node_id in all_nodes}
        dependents: dict[int, list[int]] = {node_id: [] for node_id in all_nodes}

        for node_id, node in all_nodes.items():
            for p in node._parents:
                parent_id = id(p)
                if parent_id in all_nodes:
                    in_degree[node_id] += 1
                    dependents[parent_id].append(node_id)

        # Kahn 算法，按层处理
        layers: list[list[TaskNode]] = []
        current_layer = [
            all_nodes[node_id]
            for node_id, deg in in_degree.items()
            if deg == 0
        ]

        while current_layer:
            layers.append(current_layer)
            next_layer_ids: list[int] = []
            for node in current_layer:
                node_id = id(node)
                for dependent_id in dependents[node_id]:
                    in_degree[dependent_id] -= 1
                    if in_degree[dependent_id] == 0:
                        next_layer_ids.append(dependent_id)

            current_layer = [all_nodes[node_id] for node_id in next_layer_ids]

        # 检查是否有剩余节点（说明有环，但 bind 时应该已经检测了）
        remaining = [node_id for node_id, deg in in_degree.items() if deg > 0]
        if remaining:
            names = [all_nodes[node_id]._name for node_id in remaining]
            raise CyclicDependencyError(
                f"执行时检测到环（bind 时未捕获）: {names}"
            )

        return layers

    def _run(self, results: dict[int, Any], inputs: dict[str, Any]) -> Any:
        """执行单个节点（所有 parent 的结果已在 results 中）。"""
        kwargs: dict[str, Any] = {}

        # 先填充默认值
        for name, slot in self._inputs.items():
            if slot.default is not None and name not in self._bindings:
                kwargs[name] = slot.default

        # 再覆盖已绑定的值
        for slot_name, value in self._bindings.items():
            if isinstance(value, InputVar):
                kwargs[slot_name] = inputs[value.name]
            elif isinstance(value, TaskNode):
                kwargs[slot_name] = results[id(value)]
            elif isinstance(value, OutputRef):
                parent_result = results[id(value._node)]
                kwargs[slot_name] = _resolve_selector(
                    parent_result, value._selector
                )
            else:
                kwargs[slot_name] = value

        return self._func(**kwargs)

    def _execute_parallel(
        self,
        layer: list[TaskNode],
        results: dict[int, Any],
        inputs: dict[str, Any],
    ) -> dict[int, Any]:
        """并行执行同层节点。"""
        try:
            from flowlet.parallel_unit import ParallelConfig, ThreadParallelWorkflow
        except ImportError:
            # fallback to serial
            for node in layer:
                results[id(node)] = node._run(results, inputs)
            return results

        cfg = ParallelConfig()
        with ThreadParallelWorkflow(cfg) as workflow:

            def _run_node(node: TaskNode) -> tuple[int, Any]:
                return id(node), node._run(results, inputs)

            executed = workflow.map(_run_node, layer)
            results |= {node_id: result for node_id, result in executed}

        return results

    # ------------------------------------------------------------------
    # 图外并行：map
    # ------------------------------------------------------------------
    def map(
        self,
        inputs_list: list[dict[str, Any]],
        config=None,
        progress_manager=None,
        name: str | None = None,
    ) -> list[Any]:
        """批量并行应用当前图到多组输入。

        Args:
            inputs_list: 每组输入是一个 dict，键为 InputVar.name。
            config: 并行配置（ParallelConfig）。
            progress_manager: 可选的进度管理器。
            name: 任务名称前缀。

        Returns:
            每组输入对应的执行结果列表。
        """
        if not inputs_list:
            return []

        try:
            from flowlet.parallel_unit import (
                ParallelConfig,
                ThreadParallelWorkflow,
            )
        except ImportError:
            # fallback to serial
            return [self.execute(inputs) for inputs in inputs_list]

        cfg = config if config is not None else ParallelConfig()
        with ThreadParallelWorkflow(
            cfg, progress_monitor=progress_manager, name=name
        ) as workflow:
            return workflow.map(self.execute, inputs_list)

    # ------------------------------------------------------------------
    # 可视化 / 调试
    # ------------------------------------------------------------------
    def trace(self):
        """向上追溯，返回显式图表示（用于调试/可视化）。"""
        from .graph import TracedGraph

        return TracedGraph(self)

    # ------------------------------------------------------------------
    # 属性访问
    # ------------------------------------------------------------------
    @property
    def result(self) -> Any:
        """返回上次 execute 的结果。如果尚未执行，返回 None。"""
        return self._result

    @property
    def missing_inputs(self) -> dict[str, InputVar]:
        """返回当前节点（含所有祖先）的缺失命名输入。"""
        return dict(self._missing)

    def __repr__(self) -> str:
        bound = ", ".join(self._bindings.keys())
        missing = ", ".join(self._missing.keys())
        executed = "executed" if self._executed else "pending"
        return (
            f"TaskNode({self._name!r}, "
            f"bound=[{bound}], missing=[{missing}], {executed})"
        )
