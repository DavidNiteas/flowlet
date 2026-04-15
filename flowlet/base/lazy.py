"""懒加载占位符实现。"""

from collections.abc import Callable
from typing import Any, TypeVar

from pydantic import Field
from typing_extensions import Self

from ..config.base_config import BaseConfig
from ..executable_unit import ExecutableUnit, Workflow
from .sentinel import BaseHolder

T = TypeVar("T")


class LazyHolder(BaseHolder):
    """
    延迟值占位符。

    延迟实际的数据加载/计算操作直到第一次显式取值时。
    语义上属于 sentinel 占位符家族的动态扩展：首次访问后自动"实体化"。

    Example:
        >>> lazy = LazyHolder(load_data, file_path="data.parquet")
        >>> # 此时数据尚未加载
        >>> value = lazy.resolve()  # 实际触发加载
    """

    def __init__(
        self,
        func: Callable[..., T],
        **kwargs: Any,
    ) -> None:
        """
        初始化懒加载占位符。

        Args:
            func: 实际加载数据的函数。
            **kwargs: 传递给 func 的关键字参数。
        """
        self.func = func
        self.kwargs = kwargs
        self._value: T | None = None
        self._resolved = False

    def resolve(self) -> T:
        """
        触发实际加载并返回结果。

        首次调用时执行加载函数，后续调用返回缓存结果。

        Returns:
            加载后的数据值。
        """
        if not self._resolved:
            self._value = self.func(**self.kwargs)
            self._resolved = True
        return self._value  # type: ignore[return-value]

    def is_resolved(self) -> bool:
        """检查是否已解析。"""
        return self._resolved

    def invalidate(self) -> None:
        """使缓存失效，下次 resolve 将重新加载。"""
        self._resolved = False
        self._value = None


class LazyUnitConfig(BaseConfig):
    reserved_in_module: bool = Field(
        default=False,
        description="是否在模块中保留该延迟执行单元，"
        "如果为True，模块在操作时会将其视为一个合法的整体单元，"
        "否则在模块在遇到该延迟执行单元时，会将其执行，用返回值替代这个单元。",
    )


class LazyUnitKernel(Workflow):
    config = LazyUnitConfig()

    @property
    def lazy_func(self) -> Callable | ExecutableUnit | None:
        if isinstance(self.exec_args, tuple):
            return self.exec_args[0]
        return None

    def bind_input(
        self,
        lazy_func: Callable | ExecutableUnit,
        **kwargs,
    ) -> Self:
        return super().bind_input((lazy_func), **kwargs)

    @staticmethod
    def __call__(
        *args,
        **kwargs,
    ):
        if isinstance(args[0], ExecutableUnit):
            return args[0].execute()
        elif isinstance(args[0], Callable):
            return args[0](**kwargs)
        else:
            raise TypeError(f"lazy_func must be Callable or ExecutableUnit, but got {type(args[0])}")
