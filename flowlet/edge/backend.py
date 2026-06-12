"""Edge 后端抽象。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from .config import EdgeConfig


class EdgeBackend(ABC):
    """EdgeNode 执行后端抽象。

    Thread 后端与 Ray 后端都实现此接口，使 EdgeNode 的公共 API 与后端无关。
    """

    def __init__(
        self,
        initializer: Callable[[], Any] | None = None,
        config: EdgeConfig | None = None,
        name: str | None = None,
    ) -> None:
        self.config = config if config is not None else EdgeConfig()
        self.name = name or self._default_name()

    @abstractmethod
    def _default_name(self) -> str:
        """返回默认节点名称。"""
        ...

    @abstractmethod
    def push(self, name: str, value: Any) -> None:
        """异步推送一个命名变量。"""
        ...

    @abstractmethod
    def push_many(self, mapping: dict[str, Any]) -> None:
        """异步批量推送多个命名变量。"""
        ...

    @abstractmethod
    def pull(self, name: str) -> Any:
        """拉取一个命名变量（同步阻塞）。"""
        ...

    @abstractmethod
    def pull_many(self, names: list[str]) -> dict[str, Any]:
        """批量拉取多个命名变量（同步阻塞）。"""
        ...

    @abstractmethod
    def apply(
        self,
        target: Any,
        output: str | None,
        kwargs: dict[str, Any],
    ) -> None:
        """异步执行 target，结果写入 ``state[output]``。"""
        ...

    @abstractmethod
    def bind(self, target: Any, output: str | None) -> None:
        """绑定默认 target 与输出变量名。"""
        ...

    @abstractmethod
    def run(self, kwargs: dict[str, Any]) -> None:
        """异步执行已绑定的 target。"""
        ...

    @abstractmethod
    def join(self) -> None:
        """等待所有 pending 操作完成，有异常时抛出。"""
        ...

    @abstractmethod
    def close(self) -> None:
        """优雅关闭：等待 pending 完成后释放后端。"""
        ...

    @abstractmethod
    def kill(self) -> None:
        """强制关闭：立即释放后端。"""
        ...

    @property
    @abstractmethod
    def closed(self) -> bool:
        """后端是否已关闭。"""
        ...
