"""Tests for compute_graph (reverse DAG)."""

from __future__ import annotations

import pytest
from flowlet.compute_graph import InputSlot, InputVar, OutputSpec, TaskNode
from flowlet.compute_graph.errors import (
    CyclicDependencyError,
    DuplicateNameError,
    UnboundInputError,
    UnknownSlotError,
)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------
@pytest.fixture
def load_node():
    return TaskNode(
        func=lambda path: f"loaded:{path}",
        inputs=[InputSlot("path")],
        outputs=OutputSpec("single"),
        name="load",
    )


@pytest.fixture
def split_node():
    return TaskNode(
        func=lambda data, ratio: (f"train:{data}", f"test:{data}:{ratio}"),
        inputs=[InputSlot("data"), InputSlot("ratio", default=0.2)],
        outputs=OutputSpec("tuple"),
        name="split",
    )


@pytest.fixture
def train_node():
    return TaskNode(
        func=lambda data: f"model:{data}",
        inputs=[InputSlot("data")],
        outputs=OutputSpec("single"),
        name="train",
    )


@pytest.fixture
def eval_node():
    return TaskNode(
        func=lambda model, data: {"acc": f"acc:{model}:{data}"},
        inputs=[InputSlot("model"), InputSlot("data")],
        outputs=OutputSpec("mapping"),
        name="eval",
    )


# ------------------------------------------------------------------
# Basic binding
# ------------------------------------------------------------------
class TestBinding:
    def test_bind_constant(self, load_node):
        bound = load_node.bind(path="data.csv")
        assert bound._bindings["path"] == "data.csv"
        assert bound._missing == {}
        assert bound._parents == []

    def test_bind_input_var(self, load_node):
        var = InputVar("dataset_path")
        bound = load_node.bind(path=var)
        assert bound._missing == {"dataset_path": var}

    def test_bind_unknown_slot(self, load_node):
        with pytest.raises(UnknownSlotError):
            load_node.bind(unknown="value")

    def test_bind_returns_new_instance(self, load_node):
        bound = load_node.bind(path="a.csv")
        assert bound is not load_node
        assert "path" not in load_node._bindings

    def test_bind_preserves_existing_bindings(self, load_node):
        load_node.bind(path=InputVar("dataset_path"))
        # load_node only has one input, so we can't bind again
        # but we can test that the original node is unchanged
        assert load_node._bindings == {}


# ------------------------------------------------------------------
# Reverse DAG: missing input bubbling
# ------------------------------------------------------------------
class TestMissingInputBubbling:
    def test_single_node_missing(self, load_node):
        var = InputVar("dataset_path")
        bound = load_node.bind(path=var)
        assert bound.missing_inputs == {"dataset_path": var}

    def test_parent_missing_bubbles_to_child(self, load_node, split_node):
        var = InputVar("dataset_path")
        raw = load_node.bind(path=var)
        dataset = split_node.bind(data=raw)

        assert dataset.missing_inputs == {"dataset_path": var}
        assert raw in dataset._parents

    def test_multi_level_bubbling(self, load_node, split_node, train_node):
        var = InputVar("dataset_path")
        raw = load_node.bind(path=var)
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])

        assert model.missing_inputs == {"dataset_path": var}
        assert dataset in model._parents

    def test_constant_parent_does_not_bubble(self, load_node, split_node):
        raw = load_node.bind(path="data.csv")  # constant, no missing
        dataset = split_node.bind(data=raw)

        assert dataset.missing_inputs == {}
        assert raw in dataset._parents


# ------------------------------------------------------------------
# Cycle detection
# ------------------------------------------------------------------
class TestCycleDetection:
    def test_direct_cycle(self):
        node_a = TaskNode(
            func=lambda x: x,
            inputs=[InputSlot("x")],
            outputs=OutputSpec("single"),
            name="a",
        )
        node_b = TaskNode(
            func=lambda y: y,
            inputs=[InputSlot("y")],
            outputs=OutputSpec("single"),
            name="b",
        )

        a = node_a.bind(x=InputVar("start"))
        b = node_b.bind(y=a)

        # a -> b -> a forms a cycle
        with pytest.raises(CyclicDependencyError):
            a.bind(x=b)

    def test_indirect_cycle(self, load_node, split_node, train_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])

        # model -> dataset -> raw; if raw depends on model, cycle
        with pytest.raises(CyclicDependencyError):
            raw.bind(path=model)

    def test_no_cycle_for_diamond(self, load_node, split_node, train_node, eval_node):
        r"""Diamond shape should be allowed.

              raw
             /   \
        train     test
             \   /
             eval
        """
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])
        metrics = eval_node.bind(model=model, data=dataset[1])

        # Should not raise
        assert metrics.missing_inputs == {"dataset_path": InputVar("dataset_path")}


# ------------------------------------------------------------------
# Duplicate name detection
# ------------------------------------------------------------------
class TestDuplicateName:
    def test_same_input_var_shared_is_ok(self, load_node, split_node):
        var = InputVar("dataset_path")
        raw = load_node.bind(path=var)
        # split also uses the same var object → OK
        dataset = split_node.bind(data=raw, ratio=0.2)
        # ratio is constant, data is raw (which has dataset_path)
        assert dataset.missing_inputs == {"dataset_path": var}

    def test_different_input_var_same_name_conflict(self):
        var1 = InputVar("dataset_path")
        var2 = InputVar("dataset_path")

        node = TaskNode(
            func=lambda x, y: (x, y),
            inputs=[InputSlot("x"), InputSlot("y")],
            outputs=OutputSpec("single"),
            name="merge",
        )

        with pytest.raises(DuplicateNameError):
            # var1 and var2 have same name but different objects
            node.bind(x=var1, y=var2)

    def test_conflict_via_parent_merge(self, split_node):
        var1 = InputVar("dataset_path")
        var2 = InputVar("dataset_path")

        load1 = TaskNode(
            func=lambda path: f"loaded:{path}",
            inputs=[InputSlot("path")],
            outputs=OutputSpec("single"),
            name="load1",
        ).bind(path=var1)
        load2 = TaskNode(
            func=lambda path: f"loaded:{path}",
            inputs=[InputSlot("path")],
            outputs=OutputSpec("single"),
            name="load2",
        ).bind(path=var2)

        dataset = split_node.bind(data=load1)
        merge = TaskNode(
            func=lambda a, b: (a, b),
            inputs=[InputSlot("a"), InputSlot("b")],
            outputs=OutputSpec("single"),
            name="merge",
        )
        with pytest.raises(DuplicateNameError):
            # dataset has dataset_path=var1, load2 has dataset_path=var2
            merge.bind(a=dataset, b=load2)


# ------------------------------------------------------------------
# Execution
# ------------------------------------------------------------------
class TestExecution:
    def test_single_node_constant(self, load_node):
        raw = load_node.bind(path="data.csv")
        result = raw.execute()
        assert result == "loaded:data.csv"

    def test_single_node_with_input_var(self, load_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        result = raw.execute({"dataset_path": "data.csv"})
        assert result == "loaded:data.csv"

    def test_missing_input_var_raises(self, load_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        with pytest.raises(UnboundInputError, match="未提供的外部输入"):
            raw.execute()

    def test_unbound_required_slot_raises(self, split_node):
        # split has 'data' required and 'ratio' with default
        partial = split_node.bind()  # nothing bound
        with pytest.raises(UnboundInputError, match="未绑定的必填输入"):
            partial.execute()

    def test_chain_execution(self, load_node, split_node, train_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])

        result = model.execute({"dataset_path": "data.csv"})
        assert result == "model:train:loaded:data.csv"

    def test_diamond_execution(self, load_node, split_node, train_node, eval_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])
        metrics = eval_node.bind(model=model, data=dataset[1])

        result = metrics.execute({"dataset_path": "data.csv"})
        assert result == {"acc": "acc:model:train:loaded:data.csv:test:loaded:data.csv:0.2"}

    def test_mapping_output_selector(self, eval_node):
        metrics = eval_node.bind(
            model="my_model",
            data="my_data",
        )
        acc_ref = metrics["acc"]

        # acc_ref is an OutputRef, can't execute directly
        # but we can verify it's created correctly
        assert isinstance(acc_ref, TaskNode.__getitem__(eval_node, "acc").__class__)

    def test_default_slot_value(self, split_node):
        dataset = split_node.bind(data="raw_data")
        result = dataset.execute()
        assert result == ("train:raw_data", "test:raw_data:0.2")

    def test_override_default(self, split_node):
        dataset = split_node.bind(data="raw_data", ratio=0.3)
        result = dataset.execute()
        assert result == ("train:raw_data", "test:raw_data:0.3")


# ------------------------------------------------------------------
# OutputRef
# ------------------------------------------------------------------
class TestOutputRef:
    def test_tuple_index(self, split_node):
        dataset = split_node.bind(data="raw")
        train_ref = dataset[0]
        test_ref = dataset[1]

        assert train_ref._node is dataset
        assert train_ref._selector == 0
        assert test_ref._selector == 1

    def test_mapping_key(self, eval_node):
        metrics = eval_node.bind(model="m", data="d")
        acc_ref = metrics["acc"]

        assert acc_ref._node is metrics
        assert acc_ref._selector == "acc"

    def test_nested_selector(self, split_node, eval_node):
        # Not directly testable without a node returning nested structure
        # but we can verify the selector is built correctly
        ref = split_node.bind(data="raw")[0]
        nested = ref["key"]
        assert nested._selector == (0, "key")


# ------------------------------------------------------------------
# Topological sort layers
# ------------------------------------------------------------------
class TestTopologicalSort:
    def test_linear_chain_layers(self, load_node, split_node, train_node):
        raw = load_node.bind(path=InputVar("x"))
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])

        all_nodes = model._trace_ancestors()
        layers = model._topological_sort(all_nodes)

        # layers should be: [[raw], [dataset], [model]]
        assert len(layers) == 3
        assert load_node in layers[0] or raw in layers[0]
        assert split_node in layers[1] or dataset in layers[1]
        assert train_node in layers[2] or model in layers[2]

    def test_diamond_layers(self, load_node, split_node, train_node, eval_node):
        raw = load_node.bind(path=InputVar("x"))
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])
        metrics = eval_node.bind(model=model, data=dataset[1])

        all_nodes = metrics._trace_ancestors()
        layers = metrics._topological_sort(all_nodes)

        # raw -> dataset -> [train, eval]  (train and eval are NOT siblings)
        # actually: raw -> dataset; dataset -> train; dataset -> eval; train -> eval
        # layers: [raw], [dataset], [train], [eval]
        assert len(layers) == 4


class TestResultCache:
    """Test result caching after execution."""

    def test_result_after_execute(self, load_node):
        raw = load_node.bind(path="data.csv")
        result = raw.execute()
        assert raw.result == result
        assert raw._executed

    def test_result_before_execute(self, load_node):
        raw = load_node.bind(path="data.csv")
        assert raw.result is None
        assert not raw._executed


class TestMap:
    """Test graph-level parallel map."""

    def test_map_empty_list(self, load_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        results = raw.map([])
        assert results == []

    def test_map_multiple_inputs(self, load_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        inputs_list = [
            {"dataset_path": "a.csv"},
            {"dataset_path": "b.csv"},
            {"dataset_path": "c.csv"},
        ]
        results = raw.map(inputs_list)
        assert len(results) == 3
        assert results[0] == "loaded:a.csv"
        assert results[1] == "loaded:b.csv"
        assert results[2] == "loaded:c.csv"


class TestTrace:
    """Test graph tracing and visualization."""

    def test_trace_basic(self, load_node, split_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)

        traced = dataset.trace()
        assert len(traced._nodes) == 2
        assert len(traced._edges) == 1
        assert "dataset_path" in traced._input_vars

    def test_trace_to_dict(self, load_node, split_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)

        d = dataset.trace().to_dict()
        assert "load" in d
        assert "split" in d

    def test_trace_to_mermaid(self, load_node, split_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)

        mermaid = dataset.trace().to_mermaid()
        assert "flowchart TB" in mermaid
        assert "load" in mermaid
        assert "split" in mermaid
        assert "dataset_path" in mermaid


class TestExecutableUnitBridge:
    """Test ExecutableUnit.as_task() bridge."""

    def test_as_task_manual(self):
        from flowlet import Kernel
        from flowlet.compute_graph import InputSlot, OutputSpec

        class MyKernel(Kernel):
            config = {}

            def __call__(self, data):
                return f"processed:{data}"

        unit = MyKernel()
        task = unit.as_task(
            inputs=[InputSlot("data")],
            outputs=OutputSpec("single"),
            name="my_task",
        )
        assert task._name == "my_task"
        result = task.bind(data="hello").execute()
        assert result == "processed:hello"

    def test_as_task_auto_from_class_attrs(self):
        from flowlet import Kernel
        from flowlet.compute_graph import InputField, OutputField

        class AutoKernel(Kernel):
            config = {}
            input_field = [
                InputField("data", required=True),
                InputField("ratio", default=0.2),
            ]
            output_field = OutputField("single")

            def __call__(self, data, ratio=0.2):
                return f"processed:{data}:{ratio}"

        unit = AutoKernel()
        task = unit.as_task()  # 自动识别
        assert "data" in task._inputs
        assert "ratio" in task._inputs
        result = task.bind(data="hello").execute()
        assert result == "processed:hello:0.2"

    def test_as_task_auto_from_dict_attrs(self):
        from flowlet import Kernel

        class DictKernel(Kernel):
            config = {}
            input_field = [
                {"name": "data", "required": True},
                {"name": "ratio", "default": 0.3},
            ]
            output_field = {"type": "single"}

            def __call__(self, data, ratio=0.3):
                return f"processed:{data}:{ratio}"

        unit = DictKernel()
        task = unit.as_task()
        result = task.bind(data="hello").execute()
        assert result == "processed:hello:0.3"

    def test_as_task_auto_missing_raises(self):
        from flowlet import Kernel

        class BareKernel(Kernel):
            config = {}

            def __call__(self, data):
                return data

        unit = BareKernel()
        with pytest.raises(ValueError, match="未定义 input_field"):
            unit.as_task()


class TestPipeOperator:
    """Test >> pipeline syntax."""

    def test_pipe_single_required_slot(self, load_node, train_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        pipeline = raw >> train_node
        result = pipeline.execute({"dataset_path": "data.csv"})
        assert result == "model:loaded:data.csv"

    def test_pipe_chain(self, load_node, split_node, train_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        # raw >> split 得到 dataset(tuple)
        # dataset[0] >> train 提取 train 部分传给 train
        dataset = raw >> split_node
        model = dataset[0] >> train_node
        result = model.execute({"dataset_path": "data.csv"})
        assert result == "model:train:loaded:data.csv"

    def test_pipe_ambiguous_raises(self, load_node):
        # 创建一个有两个必填输入的节点
        multi = TaskNode(
            func=lambda a, b: (a, b),
            inputs=[InputSlot("a"), InputSlot("b")],
            outputs=OutputSpec("single"),
            name="multi",
        )
        raw = load_node.bind(path="data.csv")
        with pytest.raises(ValueError, match="无法为 'multi' 推断"):
            raw >> multi

    def test_pipe_with_output_ref(self, load_node, split_node, train_node, eval_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        # raw >> split 后，再用 [0] 取 train 部分
        dataset = raw >> split_node
        model = dataset[0] >> train_node
        result = model.execute({"dataset_path": "data.csv"})
        assert result == "model:train:loaded:data.csv"


class TestExportFormats:
    """Test TracedGraph export formats."""

    def test_to_json(self, load_node, split_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)

        json_data = dataset.trace().to_json()
        assert json_data["root"].startswith("split_")
        assert "dataset_path" in json_data["input_vars"]
        assert len(json_data["nodes"]) == 2
        assert len(json_data["edges"]) == 1

    def test_to_dot(self, load_node, split_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)

        dot = dataset.trace().to_dot()
        assert "digraph G {" in dot
        assert "load" in dot
        assert "split" in dot
        assert "dataset_path" in dot
        assert "}" in dot


class TestRenderTree:
    """Test tree rendering (reference: StaticStructureDAG.render_str)."""

    def test_render_tree_linear(self, load_node, split_node, train_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])

        tree = model.trace().render_tree()
        assert "train" in tree
        assert "load" in tree
        assert "split" in tree
        assert "├──" in tree or "└──" in tree

    def test_render_tree_diamond(self, load_node, split_node, train_node, eval_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])
        metrics = eval_node.bind(model=model, data=dataset[1])

        tree = metrics.trace().render_tree()
        assert "eval" in tree
        assert "train" in tree
        assert "split" in tree
        assert "load" in tree


class TestRenderLayers:
    """Test layer rendering (layered execution plan)."""

    def test_render_layers_linear(self, load_node, split_node, train_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])

        layers = model.trace().render_layers()
        assert "Execution Plan" in layers
        assert "Layer" in layers
        assert "load" in layers
        assert "split" in layers
        assert "train" in layers
        assert "load" in layers
        assert "split" in layers
        assert "train" in layers
        assert "dataset_path" in layers

    def test_render_layers_diamond(self, load_node, split_node, train_node, eval_node):
        raw = load_node.bind(path=InputVar("dataset_path"))
        dataset = split_node.bind(data=raw)
        model = train_node.bind(data=dataset[0])
        metrics = eval_node.bind(model=model, data=dataset[1])

        layers = metrics.trace().render_layers()
        assert "Execution Plan" in layers
        assert "Layer" in layers
        # diamond has 4 layers: load, split, train, eval
        assert "eval" in layers
