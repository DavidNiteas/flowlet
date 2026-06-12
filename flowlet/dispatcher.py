"""分发内核模块，定义了Dispatcher类及其相关功能。"""

from collections.abc import Callable
from typing import ClassVar, TypeVar

from .config import BaseBranchConfig, BaseConfig, BaseConfigContainer
from .executable_unit import ExecutableUnit, Kernel

ConfigT = TypeVar("ConfigT", bound=BaseConfigContainer | BaseConfig | BaseBranchConfig)
ResultT = TypeVar("ResultT")


class Dispatcher(Kernel[ConfigT, ResultT]):
    """分发内核类，用于基于输入的分支逻辑执行。

    Dispatcher是Kernel的子类，专门用于基于输入的分支逻辑执行，
    在同一个输入下，有且只有一个分支被执行，返回值只有一个。
    当重复执行时，上一个结果会被这一个覆盖。
    """

    registered_units: ClassVar[list[tuple[int, type[ExecutableUnit], Callable[..., bool], str | None]]] = []
    """已注册的分支列表，每个元素为(优先级, 执行单元类, 判断逻辑函数, config名称)。"""

    config: ConfigT
    """分发内核的配置容器"""

    @classmethod
    def register(
        cls,
        priority: int = 0,
        condition: Callable[..., bool] = lambda *args, **kwargs: True,
        config_name: str | None = None,
    ):
        """
        注册执行单元的装饰器工厂方法。

        Args:
            priority: 优先级，默认为0，数值越高优先级越高
            condition: 判断逻辑函数，以*args和**kwargs为输入，输出bool
            config_name: 配置名称，默认为None，用于从config中获取对应的子配置

        Returns:
            装饰器函数，用于注册执行单元
        """

        def decorator(unit_cls: type[ExecutableUnit]) -> type[ExecutableUnit]:
            cls.registered_units.append((priority, unit_cls, condition, config_name))
            # 按优先级排序，优先级高的在前
            cls.registered_units.sort(key=lambda x: -x[0])
            return unit_cls

        return decorator

    def __call__(self, *args, **kwargs) -> ResultT:
        """
        基于输入的分支逻辑执行，选择第一个满足条件的执行单元执行。

        Args:
            *args: 位置参数，将传递给执行单元
            **kwargs: 关键字参数，将传递给执行单元

        Returns:
            ResultT: 执行结果
        """
        for _, unit_cls, condition, config_name in self.registered_units:
            if condition(*args, **kwargs):
                # 根据config_name获取相应的配置
                if config_name is None:
                    config_to_use = self.config
                else:
                    config_to_use = getattr(self.config, config_name)
                return unit_cls.run(*args, config=config_to_use, **kwargs)
        raise ValueError(f"No registered unit meets the condition for the given input {args, kwargs}")
