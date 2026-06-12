"""Edge 线程后端实现。"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from concurrent.futures import Future, as_completed
from typing import Any

from .backend import EdgeBackend
from .config import EdgeConfig
from .dispatch import _execute_target, _normalize_state
from .errors import DeadNodeError


class _EdgeWorkerThread:
    """单 worker 线程：按 FIFO 顺序处理命令，天然序列化对 state 的访问。"""

    def __init__(self, initializer: Callable[[], Any] | None) -> None:
        self._state: dict[str, Any] = {}
        self._bound_target: Any = None
        self._bound_output: str | None = None
        self._queue: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True, name="edge-worker")
        self._thread.start()
        self._init_state(initializer)

    def _init_state(self, initializer: Callable[[], Any] | None) -> None:
        if initializer is not None:
            self._state = _normalize_state(initializer())
        else:
            self._state = {}

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                break
            future, op, args, kwargs = item
            try:
                result = self._dispatch(op, *args, **kwargs)
                future.set_result(result)
            except Exception as exc:  # noqa: BLE001
                future.set_exception(exc)

    def _dispatch(self, op: str, *args: Any, **kwargs: Any) -> Any:
        if op == "push":
            self._state[args[0]] = args[1]
            return None
        if op == "push_many":
            self._state.update(args[0])
            return None
        if op == "pull":
            return self._state[args[0]]
        if op == "pull_many":
            return {name: self._state[name] for name in args[0]}
        if op == "apply":
            target, output, apply_kwargs = args
            _execute_target(self._state, target, output, apply_kwargs)
            return None
        if op == "bind":
            self._bound_target, self._bound_output = args
            return None
        if op == "run":
            run_kwargs = args[0]
            _execute_target(self._state, self._bound_target, self._bound_output, run_kwargs)
            return None
        if op == "ping":
            return "pong"
        msg = f"未知命令: {op}"
        raise ValueError(msg)

    def submit(self, op: str, *args: Any, **kwargs: Any) -> Future:
        """提交命令并返回 Future。"""
        future: Future = Future()
        self._queue.put((future, op, args, kwargs))
        return future

    def shutdown(self, wait: bool = True, timeout: float = 1.0) -> None:
        """关闭 worker 线程。"""
        if self._thread is None or not self._thread.is_alive():
            return
        self._queue.put(None)
        if wait:
            self._thread.join()
        else:
            self._thread.join(timeout=timeout)
        self._thread = None


class ThreadEdgeBackend(EdgeBackend):
    """基于同进程 worker 线程的 Edge 后端。

    适合本地事件循环、无需序列化的场景；pull 可以返回不可序列化对象。
    """

    def __init__(
        self,
        initializer: Callable[[], Any] | None = None,
        config: EdgeConfig | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(initializer=initializer, config=config, name=name)
        self._worker = _EdgeWorkerThread(initializer)
        self._pending: set[Future] = set()
        self._closed = False

    def _default_name(self) -> str:
        return "thread_edge"

    def _check_alive(self) -> None:
        if self._closed:
            raise DeadNodeError(f"EdgeNode '{self.name}' 已关闭")

    def push(self, name: str, value: Any) -> None:
        self._check_alive()
        future = self._worker.submit("push", name, value)
        self._pending.add(future)

    def push_many(self, mapping: dict[str, Any]) -> None:
        self._check_alive()
        future = self._worker.submit("push_many", mapping)
        self._pending.add(future)

    def pull(self, name: str) -> Any:
        self._check_alive()
        self.join()
        future = self._worker.submit("pull", name)
        return future.result()

    def pull_many(self, names: list[str]) -> dict[str, Any]:
        self._check_alive()
        self.join()
        future = self._worker.submit("pull_many", names)
        return future.result()

    def apply(
        self,
        target: Any,
        output: str | None,
        kwargs: dict[str, Any],
    ) -> None:
        self._check_alive()
        future = self._worker.submit("apply", target, output, kwargs)
        self._pending.add(future)

    def bind(self, target: Any, output: str | None) -> None:
        self._check_alive()
        future = self._worker.submit("bind", target, output)
        self._pending.add(future)

    def run(self, kwargs: dict[str, Any]) -> None:
        self._check_alive()
        future = self._worker.submit("run", kwargs)
        self._pending.add(future)

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
            # 已经完成的 future 已经消费；未完成的仍留在 pending 中供下次处理。
            remaining = {f for f in pending if not f.done()}
            self._pending.update(remaining)
            raise

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.join()
        finally:
            self._worker.shutdown(wait=True)
            self._closed = True

    def kill(self) -> None:
        if self._closed:
            return
        self._worker.shutdown(wait=False, timeout=1.0)
        for future in list(self._pending):
            if not future.done():
                future.cancel()
        self._pending.clear()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed
