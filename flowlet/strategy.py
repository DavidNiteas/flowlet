"""策略模块。

提供 Strategy 基类和 @mount 装饰器，用于将一组相关的 ExecutableUnit
聚合为策略对象，共享聚合配置，自动处理跨切关注点。

Strategy 是 Kernel 的子类，参与 flowlet 的执行生命周期。
挂载方法在实例级别是属性，访问时返回已绑定 config 的 unit 实例。
普通方法不受影响，可自由实现策略逻辑。
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Generic, TypeVar, overload

from .config import BaseConfig, BaseConfigContainer
from .executable_unit import ExecutableUnit, Kernel

ConfigT = TypeVar("ConfigT", bound=BaseConfigContainer)
ResultT = TypeVar("ResultT")


class MountSpec:
    """挂载规格：描述一个方法如何绑定到 ExecutableUnit。"""

    def __init__(
        self,
        unit_cls: type[ExecutableUnit],
        config_key: str | None = None,
        config_factory: Callable[[BaseConfig], BaseConfig] | None = None,
    ):
        self.unit_cls = unit_cls
        self.config_key = config_key
        self.config_factory = config_factory


UnitT = TypeVar("UnitT", bound=ExecutableUnit)


class MountPoint(property, Generic[UnitT]):
    """挂载点属性。

    继承 property，语义上与 ``pandas.DataFrame.loc`` 一致：
    属性访问返回已基于 strategy.config 构造好的 unit 实例，
    该实例可继续调用、索引或访问方法。

    返回的 unit 实例仅绑定配置，不执行。调用者自行决定执行方式：
    - ``unit(data)`` — 直接调用 ``__call__``
    - ``unit.run(data, **kwargs)`` — 走完整生命周期
    - ``unit.bind_input(data).execute()`` — 分步控制
    - ``unit[...]`` — 索引访问
    """

    def __init__(self, spec: MountSpec, name: str):
        self.spec = spec
        self.name = name
        super().__init__(self._getter)

    @overload
    def __get__(self, instance: None, owner: type) -> MountPoint[UnitT]: ...
    @overload
    def __get__(self, instance: BaseStrategy, owner: type) -> UnitT: ...

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        return super().__get__(instance, owner)

    def _getter(self, strategy: BaseStrategy) -> UnitT:
        config = self._extract_config(strategy)
        config = copy.deepcopy(config)
        return self.spec.unit_cls(config)

    def _extract_config(self, strategy: BaseStrategy) -> BaseConfig:
        spec = self.spec

        if spec.config_factory is not None:
            return spec.config_factory(strategy.config)

        config_key = spec.config_key or self.name
        config = strategy.config
        for part in config_key.split("."):
            config = getattr(config, part)

        return config


def mount(
    unit_cls: type[UnitT],
    config_key: str | None = None,
    config_factory: Callable[[BaseConfig], BaseConfig] | None = None,
) -> Callable[[Callable], MountPoint[UnitT]]:
    """将 Strategy 方法注册为挂载点。

    被装饰的方法体不需要实现（通常为 ``pass``），装饰器会将其替换为
    :class:`MountPoint` 属性。

    语义等价于 pandas 的 ``df.loc``：属性访问返回 unit 实例，
    该实例可继续调用 ``__call__``、索引或访问方法。

    Args:
        unit_cls: 要挂载的 ExecutableUnit 类（Kernel / Workflow / Dispatcher）
        config_key: 从 ``strategy.config`` 中提取子配置的字段名。
            支持点号路径（如 ``"io.write"``）。
            默认使用被装饰的方法名。
        config_factory: 自定义配置构造函数。
            接收 ``strategy.config``，返回 ``unit_cls`` 需要的配置实例。
            如果提供，优先于 ``config_key``。

    Example::

        class MyStrategy(BaseStrategy):
            @mount(WriteWorkflow)
            def write(self) -> WriteWorkflow: pass

            @mount(ReadWorkflow, config_key="read")
            def read(self) -> ReadWorkflow: pass

        strategy = MyStrategy()
        strategy.write(data)           # 直接调用 WriteWorkflow 的 __call__
        strategy.write.run(data)       # 调用 WriteWorkflow 的 run
        strategy.write[...]            # 索引访问（若实现了 __getitem__）
    """
    spec = MountSpec(unit_cls, config_key, config_factory)

    def decorator(func: Callable) -> MountPoint[UnitT]:
        return MountPoint(spec, func.__name__)

    return decorator


class BaseStrategy(Kernel[ConfigT, ResultT]):
    """策略基类。

    Kernel 的子类，通过 :func:`@mount <mount>` 装饰器将方法注册为挂载点。
    挂载方法访问时返回已绑定 config 的 unit 实例。
    普通方法不受影响，可自由实现策略逻辑。

    Usage::

        class MyStrategy(BaseStrategy):
            config = MyStrategyConfig()

            @mount(WriteWorkflow)
            def write(self): pass

            def get_name(self) -> str:
                return self.name
    """

    # 类级别的操作注册表（__init_subclass__ 自动填充）
    _operations: dict[str, MountSpec] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # 继承父类的 operations
        cls._operations = dict(getattr(cls, "_operations", {}))

        # 扫描当前类定义，收集 @mount 注册的 MountPoint 属性
        for name, value in list(cls.__dict__.items()):
            if isinstance(value, MountPoint):
                cls._operations[name] = value.spec

        # 如果子类自己覆盖了 __init_subclass__，说明它想自行管理检查逻辑，跳过
        if "__init_subclass__" in cls.__dict__:
            return

        # 检查必需操作是否已注册
        required = getattr(cls, "_required_operations", ())
        missing = [op for op in required if op not in cls._operations]
        if missing:
            raise TypeError(
                f"{cls.__name__} must register operation: {missing[0]}"
            )
