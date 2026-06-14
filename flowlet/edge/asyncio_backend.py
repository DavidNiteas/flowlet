"""Asyncio Edge backend implemented with CoroutinePool."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import Future, as_completed
from typing import Any

from ..base.coroutine import CoroutinePool
from .backend import EdgeBackend
from .config import EdgeConfig
from .dispatch import _execute_target_async, _normalize_state
from .errors import DeadNodeError


class AsyncioEdgeBackend(EdgeBackend):
    """Edge backend backed by a private asyncio loop thread."""

    def __init__(
        self,
        initializer: Callable[[], Any] | None = None,
        config: EdgeConfig | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(initializer=initializer, config=config, name=name)
        self._pool = CoroutinePool(max_concurrency=self.config.max_concurrent_tasks, name=f"{self.name}-loop")
        self._state: dict[str, Any] = {}
        self._bound_target: Any = None
        self._bound_output: str | None = None
        self._pending: set[Future] = set()
        self._closed = False
        self._state_lock: asyncio.Lock | None = None
        self._pool.submit_awaitable(self._init_state(initializer)).result()

    def _default_name(self) -> str:
        return "asyncio_edge"

    async def _init_state(self, initializer: Callable[[], Any] | None) -> None:
        self._state_lock = asyncio.Lock()
        if initializer is not None:
            value = initializer()
            if hasattr(value, "__await__"):
                value = await value
            self._state = _normalize_state(value)
        else:
            self._state = {}

    def _check_alive(self) -> None:
        if self._closed:
            raise DeadNodeError(f"EdgeNode '{self.name}' 已关闭")

    async def _push(self, name: str, value: Any) -> None:
        async with self._state_lock:
            self._state[name] = value

    async def _push_many(self, mapping: dict[str, Any]) -> None:
        async with self._state_lock:
            self._state.update(mapping)

    async def _pull(self, name: str) -> Any:
        async with self._state_lock:
            return self._state[name]

    async def _pull_many(self, names: list[str]) -> dict[str, Any]:
        async with self._state_lock:
            return {name: self._state[name] for name in names}

    async def _apply(self, target: Any, output: str | None, kwargs: dict[str, Any]) -> None:
        async with self._state_lock:
            await _execute_target_async(self._state, target, output, kwargs)

    async def _bind(self, target: Any, output: str | None) -> None:
        async with self._state_lock:
            self._bound_target = target
            self._bound_output = output

    async def _run(self, kwargs: dict[str, Any]) -> None:
        async with self._state_lock:
            await _execute_target_async(self._state, self._bound_target, self._bound_output, kwargs)

    def _submit(self, coro) -> None:
        future = self._pool.submit_awaitable(coro)
        self._pending.add(future)

    def push(self, name: str, value: Any) -> None:
        self._check_alive()
        self._submit(self._push(name, value))

    def push_many(self, mapping: dict[str, Any]) -> None:
        self._check_alive()
        self._submit(self._push_many(mapping))

    def pull(self, name: str) -> Any:
        self._check_alive()
        self.join()
        return self._pool.submit_awaitable(self._pull(name)).result()

    def pull_many(self, names: list[str]) -> dict[str, Any]:
        self._check_alive()
        self.join()
        return self._pool.submit_awaitable(self._pull_many(names)).result()

    def apply(
        self,
        target: Any,
        output: str | None,
        kwargs: dict[str, Any],
    ) -> None:
        self._check_alive()
        self._submit(self._apply(target, output, kwargs))

    def bind(self, target: Any, output: str | None) -> None:
        self._check_alive()
        self._submit(self._bind(target, output))

    def run(self, kwargs: dict[str, Any]) -> None:
        self._check_alive()
        self._submit(self._run(kwargs))

    def join(self) -> None:
        if not self._pending:
            return
        pending = list(self._pending)
        self._pending.clear()
        try:
            for future in as_completed(pending):
                exc = future.exception()
                if exc is not None:
                    raise exc
        except Exception:
            remaining = {future for future in pending if not future.done()}
            self._pending.update(remaining)
            raise

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.join()
        finally:
            self._pool.close()
            self._closed = True

    def kill(self) -> None:
        if self._closed:
            return
        self._pool.kill()
        for future in list(self._pending):
            if not future.done():
                future.cancel()
        self._pending.clear()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed
