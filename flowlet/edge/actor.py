"""Edge Ray Actor。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import ray

from .dispatch import _execute_target, _normalize_state


@ray.remote
class _EdgeActor:
    """Ray 后端的有状态 Actor。

    持有 state，并串行处理所有方法调用。target 函数/工作流/图在 Actor
    进程内执行，中间产物可以是不可序列化的 C 对象。
    """

    def __init__(self, initializer: Callable[[], Any] | None) -> None:
        self._state: dict[str, Any] = {}
        self._bound_target: Any = None
        self._bound_output: str | None = None
        self._init_state(initializer)

    def _init_state(self, initializer: Callable[[], Any] | None) -> None:
        if initializer is not None:
            self._state = _normalize_state(initializer())
        else:
            self._state = {}

    def push(self, name: str, value: Any) -> None:
        self._state[name] = value

    def push_many(self, mapping: dict[str, Any]) -> None:
        self._state.update(mapping)

    def pull(self, name: str) -> Any:
        return self._state[name]

    def pull_many(self, names: list[str]) -> dict[str, Any]:
        return {n: self._state[n] for n in names}

    def apply(
        self,
        target: Any,
        output: str | None,
        kwargs: dict[str, Any],
    ) -> None:
        _execute_target(self._state, target, output, kwargs)

    def bind(self, target: Any, output: str | None) -> None:
        self._bound_target = target
        self._bound_output = output

    def run(self, kwargs: dict[str, Any]) -> None:
        _execute_target(self._state, self._bound_target, self._bound_output, kwargs)

    def ping(self) -> str:
        return "pong"
