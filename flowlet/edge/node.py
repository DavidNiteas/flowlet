"""EdgeNode 公共 API。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..base.sentinel import Default
from .asyncio_backend import AsyncioEdgeBackend
from .backend import EdgeBackend
from .config import EdgeConfig
from .errors import DeadNodeError
from .ray_backend import RayEdgeBackend
from .thread_backend import ThreadEdgeBackend


class EdgeNode:
    """Edge 模式节点：主进程面对的唯一对象。

    内部绑定一个 ``EdgeBackend``（Thread 或 Ray），所有操作都通过后端异步执行。
    """

    def __init__(
        self,
        backend: type[EdgeBackend] | EdgeBackend,
        initializer: Callable[[], Any] | None = None,
        name: str | None = None,
        config: EdgeConfig | None = None,
    ) -> None:
        if isinstance(backend, EdgeBackend):
            self._backend = backend
        else:
            self._backend = backend(initializer=initializer, config=config, name=name)
        self._name = name or self._backend.name

    @property
    def name(self) -> str:
        return self._name

    @property
    def alive(self) -> bool:
        """节点是否仍然存活。"""
        return not self._backend.closed

    def _check_alive(self) -> None:
        if self._backend.closed:
            raise DeadNodeError(f"EdgeNode '{self._name}' 已关闭")

    def push(self, name: str, value: Any) -> None:
        """异步推送一个可序列化变量到节点 state。"""
        self._check_alive()
        self._backend.push(name, value)

    def push_many(self, mapping: dict[str, Any]) -> None:
        """异步批量推送多个可序列化变量到节点 state。"""
        self._check_alive()
        self._backend.push_many(mapping)

    def pull(self, name: str) -> Any:
        """同步拉取节点 state 中的变量。

        调用前会自动 ``join()`` 所有 pending 操作，确保读到最新状态。
        """
        self._check_alive()
        return self._backend.pull(name)

    def pull_many(self, names: list[str]) -> dict[str, Any]:
        """同步批量拉取多个变量。"""
        self._check_alive()
        return self._backend.pull_many(names)

    def apply(self, target: Any, output: str | None, **kwargs: Any) -> None:
        """让节点执行 target，结果写入 ``state[output]``。

        Args:
            target: 函数、ExecutableUnit 实例或 TaskNode 图。
            output: 结果存储的变量名；``None`` 表示不存储（仅副作用）。
            **kwargs: 额外命名参数，与 state 一起传给 target。
        """
        self._check_alive()
        self._backend.apply(target, output, kwargs)

    def bind(self, target: Any, output: str | None) -> None:
        """绑定默认 target，供后续 ``run()`` 使用。"""
        self._check_alive()
        self._backend.bind(target, output)

    def run(self, **kwargs: Any) -> None:
        """执行已绑定的 target。"""
        self._check_alive()
        self._backend.run(kwargs)

    def join(self) -> None:
        """等待所有 pending 异步操作完成，有异常时抛出。"""
        self._check_alive()
        self._backend.join()

    def close(self) -> None:
        """优雅关闭：等待 pending 完成后释放后端。"""
        if self._backend.closed:
            return
        self._backend.close()

    def kill(self) -> None:
        """强制关闭：立即释放后端，丢弃 pending。"""
        if self._backend.closed:
            return
        self._backend.kill()

    def __enter__(self) -> EdgeNode:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self.kill()
        else:
            self.close()


class ThreadEdgeNode(EdgeNode):
    """基于同进程 worker 线程的 EdgeNode。

    适合本地事件循环、无需序列化的场景；``pull`` 可以返回不可序列化对象。
    """

    def __init__(
        self,
        initializer: Callable[[], Any] | None = None,
        name: str | None = None,
        config: EdgeConfig | None = None,
    ) -> None:
        super().__init__(ThreadEdgeBackend, initializer=initializer, name=name, config=config)


class AsyncioEdgeNode(EdgeNode):
    """基于独立 asyncio event loop 线程的 EdgeNode。"""

    def __init__(
        self,
        initializer: Callable[[], Any] | None = None,
        name: str | None = None,
        config: EdgeConfig | None = None,
    ) -> None:
        super().__init__(AsyncioEdgeBackend, initializer=initializer, name=name, config=config)


class RayEdgeNode(EdgeNode):
    """基于 Ray Actor 的 EdgeNode。

    适合跨进程持有不可序列化对象；``pull`` 只能返回可序列化数据。
    """

    def __init__(
        self,
        initializer: Callable[[], Any] | None = None,
        name: str | None = None,
        config: EdgeConfig | None = None,
        num_cpus: float | Default = Default(),
        num_gpus: float | Default = Default(),
        memory: int | None | Default = Default(),
        max_restarts: int | Default = Default(),
        max_task_retries: int | Default = Default(),
        **kwargs: Any,
    ) -> None:
        resolved = config if config is not None else EdgeConfig()
        update_kwargs = {
            key: value
            for key, value in (
                ("num_cpus", num_cpus),
                ("num_gpus", num_gpus),
                ("memory", memory),
                ("max_restarts", max_restarts),
                ("max_task_retries", max_task_retries),
            )
            if not isinstance(value, Default)
        }
        if kwargs:
            update_kwargs.update(kwargs)
        if update_kwargs:
            resolved.update(**update_kwargs)
        super().__init__(RayEdgeBackend, initializer=initializer, name=name, config=resolved)
