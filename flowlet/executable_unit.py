"""可执行单元模块，定义了工作流和核心执行单元的基础结构。"""

from __future__ import annotations

import copy
import importlib.util
import threading
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from typing_extensions import Self

from .base.dual_accessor import dual_method
from .base.sentinel import Default
from .config import BaseBranchConfig, BaseConfig, BaseConfigContainer

ConfigT = TypeVar("ConfigT", bound=BaseConfigContainer | BaseConfig | BaseBranchConfig)
ResultT = TypeVar("ResultT")
WorkflowConfigT = TypeVar("WorkflowConfigT", bound=BaseConfig | BaseBranchConfig)


class ExecutableUnit(ABC, Generic[ConfigT, ResultT]):
    """可执行单元抽象基类，是所有可执行组件的基础。

    此类定义了可执行单元的基本接口和行为，包括配置管理、输入绑定和执行逻辑。
    所有具体的可执行单元（如Kernel和Workflow）都必须继承此类并实现__call__方法。

    泛型参数:
        ConfigT: 配置类型，必须是BaseConfigContainer、BaseConfig或BaseBranchConfig的子类
    """

    config: ConfigT
    """执行单元的配置对象"""
    result: ResultT
    """执行结果"""

    @classmethod
    def build_config(
        cls,
        *args: BaseConfig | BaseBranchConfig | BaseConfigContainer,
        **kwargs: Any | BaseConfig | BaseBranchConfig | BaseConfigContainer | Default,
    ) -> ConfigT:
        """从输入参数构造配置实例。

        Args:
            *args: 配置对象列表，用于更新默认配置
            **kwargs: 配置参数，用于更新默认配置

        Returns:
            ConfigT: 构造的配置实例
        """
        config = copy.deepcopy(cls.config)
        config.update(*args, **kwargs)
        return config

    def __init__(
        self,
        *args: BaseConfig | BaseBranchConfig,
        **kwargs: Any | BaseConfig | BaseBranchConfig | Default,
    ) -> None:
        """初始化可执行单元。

        Args:
            *args: 配置对象列表，用于更新默认配置
            **kwargs: 配置参数，用于更新默认配置
        """
        self.config = self.build_config(*args, **kwargs)

        self.exec_args: tuple[Any, ...] | None = None
        """执行参数元组"""
        self.exec_kwargs: dict[str, Any] | None = None
        """执行参数字典"""
        self.result: ResultT | None = None
        """执行结果"""
        self._thread: threading.Thread | None = None
        """执行线程对象"""
        self._ray_available: bool = False
        """Ray是否可用"""
        if importlib.util.find_spec("ray") is not None:
            self._ray_available = True

    @dual_method
    def run(self, *args, **kwargs) -> ResultT:
        """实例调用模式。

        保留当前实例的配置；如果 ``kwargs`` 非空，先深拷贝实例并
        用 ``kwargs`` 覆盖对应配置字段，再执行。

        Args:
            *args: 传递给 __call__ 的位置参数
            **kwargs: 配置覆盖参数

        Returns:
            Any: 执行结果
        """
        if kwargs:
            unit = copy.deepcopy(self)
            unit.config = copy.deepcopy(self.config)
            unit.config.update(**kwargs)
        else:
            unit = self
        bound_instance = unit.bind_input(*args)
        return bound_instance.execute()

    @run.classmode
    def run(cls, *args, config: ConfigT | Default = Default(), **kwargs) -> ResultT:
        """类调用模式。

        基于 ``config`` 与 ``kwargs`` 构造新实例，直接绑定输入并执行。
        不经过实例的 ``run``，以避免触发子类可能存在的 ``@classmethod``
        覆盖导致的递归。

        Args:
            *args: 传递给 __call__ 的位置参数
            config: 显式传入的配置对象
            **kwargs: 传递给配置构造的参数

        Returns:
            Any: 执行结果
        """
        instance: ExecutableUnit = cls(config, **kwargs)
        bound_instance = instance.bind_input(*args)
        return bound_instance.execute()

    def bind_input(self, *args, **kwargs) -> Self:
        """绑定输入参数到可执行单元。

        创建当前实例的深拷贝，并为拷贝绑定输入参数，
        这样可以保持原实例不变，同时创建一个准备执行的新实例。

        Args:
            *args: 位置参数，将传递给__call__方法
            **kwargs: 关键字参数，将传递给__call__方法

        Returns:
            Self: 绑定了输入参数的新实例
        """
        new_instance = copy.deepcopy(self)
        new_instance.exec_args = args
        new_instance.exec_kwargs = kwargs
        return new_instance

    def execute(self) -> ResultT:
        """执行可执行单元。

        检查输入参数是否已绑定，然后调用__call__方法执行核心逻辑，
        并保存执行结果。

        Returns:
            Any: 执行结果

        Raises:
            ValueError: 如果输入参数未绑定
        """
        if self.exec_args is None or self.exec_kwargs is None:
            raise ValueError("executable unit input not loaded")
        result = self(*self.exec_args, **self.exec_kwargs)
        self.result = result
        return result

    def execute_async(self) -> ExecutionFuture:
        """异步执行可执行单元（基于线程），返回 future-like 对象。

        创建一个线程来异步执行 execute 方法。执行完毕后结果保存到 result 属性。
        返回的 ExecutionFuture 可用于检查完成状态、等待或获取结果。

        Returns:
            ExecutionFuture: 可用于跟踪异步执行并获取结果的对象。

        Raises:
            RuntimeError: 如果该单元已经在运行中。
        """
        if self._thread is not None:
            raise RuntimeError("executable unit is already running")

        def _async_execute():
            try:
                self.execute()
            finally:
                self._thread = None

        thread = threading.Thread(target=_async_execute)
        thread.daemon = True
        thread.start()
        self._thread = thread
        return ExecutionFuture(self, thread)

    def execute_ray(self) -> ExecutionFuture:
        """异步执行可执行单元（基于 Ray），返回 future-like 对象。

        创建一个线程将任务发送到 Ray 集群执行。执行完毕后结果保存到 result 属性。
        返回的 ExecutionFuture 可用于检查完成状态、等待或获取结果。

        Returns:
            ExecutionFuture: 可用于跟踪异步执行并获取结果的对象。

        Raises:
            ImportError: 如果 Ray 不可用。
            RuntimeError: 如果该单元已经在运行中。
        """
        if not self._ray_available:
            raise ImportError("Ray is not available")

        if self._thread is not None:
            raise RuntimeError("executable unit is already running")

        def _ray_execute():
            try:
                import ray

                # 确保 Ray 已初始化
                if not ray.is_initialized():
                    ray.init()

                # 定义远程执行函数
                @ray.remote
                def remote_execute(unit):
                    # 执行并返回结果
                    return unit.execute()

                # 序列化当前实例
                unit_id = ray.put(self)
                # 发送任务到 Ray 集群
                ray_future = remote_execute.remote(unit_id)

                # 等待执行结果
                self.result = ray.get(ray_future)
            finally:
                self._thread = None

        thread = threading.Thread(target=_ray_execute)
        thread.daemon = True
        thread.start()
        self._thread = thread
        return ExecutionFuture(self, thread)

    def __getstate__(self):
        """序列化对象时调用，跳过_thread属性。

        Returns:
            dict: 序列化后的状态
        """
        state = self.__dict__.copy()
        state["_thread"] = None
        return state

    def __setstate__(self, state):
        """反序列化对象时调用，确保_thread属性被正确初始化。

        Args:
            state: 序列化后的状态
        """
        self.__dict__.update(state)
        if "_thread" not in self.__dict__:
            self._thread = None
        if "_ray_available" not in self.__dict__:
            if importlib.util.find_spec("ray") is not None:
                self._ray_available = True
            else:
                self._ray_available = False

    @abstractmethod
    def __call__(self, *args, **kwargs) -> ResultT:
        """执行单元的主要逻辑。

        所有具体的可执行单元必须实现此方法，定义核心执行逻辑。

        Args:
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            ResultT: 执行结果
        """
        ...

    def as_task(self, inputs=None, outputs=None, name=None):
        """将此 ExecutableUnit 包装为 TaskNode，用于计算图。

        如果类定义了 ``input_field`` / ``output_field``，且 ``inputs`` /
        ``outputs`` 未显式传入，则自动从类属性构建。

        Args:
            inputs: list[InputSlot]，输入槽定义。None 时尝试从 ``input_field`` 构建。
            outputs: OutputSpec，输出规格。None 时尝试从 ``output_field`` 构建。
            name: 节点名称，默认使用类名。

        Returns:
            TaskNode: 包装后的任务图节点。

        Raises:
            ValueError: 如果 ``inputs`` / ``outputs`` 为 None 且类未定义对应字段。
        """
        from flowlet.compute_graph import TaskNode

        resolved_inputs = inputs if inputs is not None else self._build_input_slots()
        resolved_outputs = outputs if outputs is not None else self._build_output_spec()

        return TaskNode(self, resolved_inputs, resolved_outputs, name or self.__class__.__name__)

    def _build_input_slots(self):
        """从 ``input_field`` 类属性构建 InputSlot 列表。"""
        from flowlet.compute_graph import InputField, InputSlot

        fields = getattr(self, "input_field", None) or getattr(self.__class__, "input_field", None)
        if fields is None:
            raise ValueError(
                f"{self.__class__.__name__} 未定义 input_field，请显式传入 inputs 参数或在类中定义 input_field"
            )

        slots = []
        for f in fields:
            if isinstance(f, InputField):
                slots.append(InputSlot(f.name, f.required, f.default))
            elif isinstance(f, dict):
                slots.append(
                    InputSlot(
                        f["name"],
                        f.get("required", True),
                        f.get("default", None),
                    )
                )
            else:
                raise TypeError(f"input_field 中的元素类型不支持: {type(f)}")
        return slots

    def _build_output_spec(self):
        """从 ``output_field`` 类属性构建 OutputSpec。"""
        from flowlet.compute_graph import OutputField, OutputSpec

        field = getattr(self, "output_field", None) or getattr(self.__class__, "output_field", None)
        if field is None:
            raise ValueError(
                f"{self.__class__.__name__} 未定义 output_field，请显式传入 outputs 参数或在类中定义 output_field"
            )

        if isinstance(field, OutputField):
            return OutputSpec(field.type)
        elif isinstance(field, dict):
            return OutputSpec(field["type"])
        else:
            raise TypeError(f"output_field 类型不支持: {type(field)}")


class ExecutionFuture:
    """future-like 对象，包装 ExecutableUnit 的异步执行。

    提供检查完成状态、阻塞等待和获取结果的接口。
    """

    def __init__(self, unit: ExecutableUnit, thread: threading.Thread) -> None:
        self._unit = unit
        self._thread = thread

    def is_done(self) -> bool:
        """检查异步执行是否已完成。"""
        return not self._thread.is_alive()

    def join(self, timeout: float | None = None) -> None:
        """阻塞等待异步执行完成。

        Args:
            timeout: 最大等待秒数，None 表示无限等待。
        """
        self._thread.join(timeout)

    def get(self, timeout: float | None = None) -> Any:
        """阻塞等待并返回执行结果。

        Args:
            timeout: 最大等待秒数，None 表示无限等待。

        Returns:
            执行单元的执行结果。

        Raises:
            RuntimeError: 如果执行尚未完成且超时。
        """
        self.join(timeout)
        if not self.is_done():
            raise RuntimeError("Execution timed out")
        return self._unit.result

    def __call__(self) -> Any:
        """快捷方式：阻塞等待并返回结果。"""
        return self.get()


class Kernel(ExecutableUnit[ConfigT, ResultT]):
    """核心执行单元，用于执行具体的任务逻辑。

    Kernel是ExecutableUnit的子类，专门用于执行具体的计算或处理任务。
    它使用BaseConfigContainer作为配置类型。
    """

    config: BaseConfig | BaseBranchConfig | BaseConfigContainer
    """核心执行单元的配置容器"""


class Workflow(ExecutableUnit[WorkflowConfigT, ResultT]):
    """工作流执行单元，用于协调多个任务的执行。

    Workflow是ExecutableUnit的子类，专门用于定义和执行工作流，
    可以包含多个步骤或子任务。它使用BaseConfig或BaseBranchConfig作为配置类型。
    """

    config: BaseConfig | BaseBranchConfig
    """工作流的配置对象"""
