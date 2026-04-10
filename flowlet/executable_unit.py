"""可执行单元模块，定义了工作流和核心执行单元的基础结构。"""
import copy
import importlib.util
import threading
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from typing_extensions import Self

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
        config = copy.copy(cls.config)
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
        if importlib.util.find_spec('ray') is not None:
            self._ray_available = True

    @classmethod
    def run(
        cls,
        *args,
        config: ConfigT | Default = Default(),
        **kwargs,
    ) -> ResultT:
        """即时执行可执行单元。

        构造一个实例并执行其全套调用流程，返回结果。
        此方法不是强制重写的，但推荐在子类中重写以提供更好的参数类型注释与函数注释。

        Args:
            *args: 传递给 __call__ 的位置参数
            **kwargs: 传递给配置构造的参数

        Returns:
            Any: 执行结果

        Example:
            # 基本用法
            result = MyExecutableUnit.run(input_data)

            # 带配置参数
            result = MyExecutableUnit.run(
                input_data,
                param1="value1",
                param2="value2"
            )
        """
        instance = cls(config, **kwargs)
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

    def execute_async(self) -> None:
        """异步执行可执行单元（基于线程）。

        创建一个线程来异步执行execute方法，执行完毕后将结果保存到result属性。

        Returns:
            None
        """
        if self._thread is not None:
            raise RuntimeError("executable unit is already running")

        def _async_execute():
            try:
                self.execute()
            finally:
                self._thread = None

        self._thread = threading.Thread(target=_async_execute)
        self._thread.daemon = True
        self._thread.start()

    def execute_ray(self) -> None:
        """异步执行可执行单元（基于Ray）。

        创建一个线程，将任务发送到Ray集群执行，执行完毕后将结果保存到result属性。

        Returns:
            None

        Raises:
            ImportError: 如果Ray不可用
        """
        if not self._ray_available:
            raise ImportError("Ray is not available")

        if self._thread is not None:
            raise RuntimeError("executable unit is already running")

        def _ray_execute():
            try:
                import ray
                # 确保Ray已初始化
                if not ray.is_initialized():
                    ray.init()

                # 定义远程执行函数
                @ray.remote
                def remote_execute(unit):
                    # 执行并返回结果
                    return unit.execute()

                # 序列化当前实例
                unit_id = ray.put(self)
                # 发送任务到Ray集群
                future = remote_execute.remote(unit_id)

                # 等待执行结果
                self.result = ray.get(future)
            finally:
                self._thread = None

        self._thread = threading.Thread(target=_ray_execute)
        self._thread.daemon = True
        self._thread.start()

    def __getstate__(self):
        """序列化对象时调用，跳过_thread属性。

        Returns:
            dict: 序列化后的状态
        """
        state = self.__dict__.copy()
        state['_thread'] = None
        return state

    def __setstate__(self, state):
        """反序列化对象时调用，确保_thread属性被正确初始化。

        Args:
            state: 序列化后的状态
        """
        self.__dict__.update(state)
        if '_thread' not in self.__dict__:
            self._thread = None
        if '_ray_available' not in self.__dict__:
            if importlib.util.find_spec('ray') is not None:
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

