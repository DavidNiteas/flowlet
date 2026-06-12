"""Edge Ray 后端实现。"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from typing import Any

import ray

from .actor import _EdgeActor
from .backend import EdgeBackend
from .config import EdgeConfig
from .errors import DeadNodeError


class RayEdgeBackend(EdgeBackend):
    """基于 Ray Actor 的 Edge 后端。

    每个 EdgeNode 对应一个独立的 Ray Actor；跨进程通信需要序列化，
    因此 state 中的不可序列化对象不能被 pull 回主进程。
    """

    def __init__(
        self,
        initializer: Callable[[], Any] | None = None,
        config: EdgeConfig | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(initializer=initializer, config=config, name=name)
        self._init_ray()
        self._actor = self._create_actor(initializer)
        self._pending: set[ray.ObjectRef] = set()
        self._closed = False
        self._lock = threading.RLock()
        # 等待 Actor 初始化完成，使构造过程同步化。
        ray.get(self._actor.ping.remote())

    def _default_name(self) -> str:
        return "ray_edge"

    def _init_ray(self) -> None:
        if ray.is_initialized():
            return
        ray_init_kwargs = {
            "include_dashboard": self.config.ray_include_dashboard,
            "log_to_driver": self.config.ray_log_to_driver,
        }
        if self.config.ray_dashboard_port is not None:
            ray_init_kwargs["dashboard_port"] = self.config.ray_dashboard_port
        if self.config.ray_runtime_env is not None:
            ray_init_kwargs["runtime_env"] = self.config.ray_runtime_env
        ray.init(**ray_init_kwargs)

    def _create_actor(self, initializer: Callable[[], Any] | None) -> ray.ActorHandle:
        options = {
            "num_cpus": self.config.num_cpus,
            "num_gpus": self.config.num_gpus,
            "max_restarts": self.config.max_restarts,
            "max_task_retries": self.config.max_task_retries,
            "name": f"{self.name}-{uuid.uuid4().hex[:6]}",
        }
        if self.config.memory is not None:
            options["memory"] = self.config.memory
        return _EdgeActor.options(**options).remote(initializer)

    def _check_alive(self) -> None:
        if self._closed:
            raise DeadNodeError(f"EdgeNode '{self.name}' 已关闭")

    def push(self, name: str, value: Any) -> None:
        self._check_alive()
        ref = self._actor.push.remote(name, value)
        with self._lock:
            self._pending.add(ref)

    def push_many(self, mapping: dict[str, Any]) -> None:
        self._check_alive()
        ref = self._actor.push_many.remote(mapping)
        with self._lock:
            self._pending.add(ref)

    def pull(self, name: str) -> Any:
        self._check_alive()
        self.join()
        return ray.get(self._actor.pull.remote(name))

    def pull_many(self, names: list[str]) -> dict[str, Any]:
        self._check_alive()
        self.join()
        return ray.get(self._actor.pull_many.remote(names))

    def apply(
        self,
        target: Any,
        output: str | None,
        kwargs: dict[str, Any],
    ) -> None:
        self._check_alive()
        ref = self._actor.apply.remote(target, output, kwargs)
        with self._lock:
            self._pending.add(ref)

    def bind(self, target: Any, output: str | None) -> None:
        self._check_alive()
        ref = self._actor.bind.remote(target, output)
        with self._lock:
            self._pending.add(ref)

    def run(self, kwargs: dict[str, Any]) -> None:
        self._check_alive()
        ref = self._actor.run.remote(kwargs)
        with self._lock:
            self._pending.add(ref)

    def join(self) -> None:
        with self._lock:
            pending = list(self._pending)
            if not pending:
                return
            self._pending.clear()
        try:
            ray.get(pending)
        finally:
            # 无论成功或失败，都已经消费了这些 refs。
            pass

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.join()
        finally:
            try:
                ray.kill(self._actor)
            except Exception:  # noqa: BLE001
                pass
            self._closed = True

    def kill(self) -> None:
        if self._closed:
            return
        try:
            ray.kill(self._actor)
        except Exception:  # noqa: BLE001
            pass
        with self._lock:
            self._pending.clear()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed
