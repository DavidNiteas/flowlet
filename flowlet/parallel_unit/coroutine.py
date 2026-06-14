from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Iterable
from concurrent.futures import Future, as_completed
from typing import Any

from ..base.coroutine import CoroutinePool
from ..base.progress import ProgressManager
from ..executable_unit import Workflow
from .config import ParallelConfig


class CoroutineParallelWorkflow(Workflow):
    """Parallel workflow for asyncio-compatible I/O concurrency."""

    config: ParallelConfig = ParallelConfig()

    def __init__(
        self,
        *args,
        name: str | None = None,
        progress_monitor: ProgressManager | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._pool = CoroutinePool(max_concurrency=self.config.max_concurrent_tasks, name=name or "coroutine-workflow")
        self._shutdown = False
        self._progress_monitor = progress_monitor
        self._name = name

    def __call__(self, func: Callable, *args, **kwargs) -> Future:
        return self.submit(func, *args, **kwargs)

    def submit(self, func: Callable[..., Any], *args: Any, blocking: bool = False, **kwargs: Any) -> Future:
        if self._shutdown:
            raise RuntimeError("Coroutine pool has been shutdown")
        return self._pool.submit(func, *args, blocking=blocking, **kwargs)

    def submit_awaitable(self, awaitable: Awaitable[Any]) -> Future:
        if self._shutdown:
            raise RuntimeError("Coroutine pool has been shutdown")
        return self._pool.submit_awaitable(awaitable)

    def is_ready(self, future: Future) -> bool:
        return future.done()

    def fetch(self, future: Future) -> Any:
        return future.result()

    def submit_many(
        self,
        func: Callable[[Any], Any],
        iterable: Iterable[Any],
        *,
        blocking: bool = False,
    ) -> list[Future]:
        if self._shutdown:
            raise RuntimeError("Coroutine pool has been shutdown")
        return [self.submit(func, item, blocking=blocking) for item in iterable]

    def _generate_task_id(self, func_name: str) -> str:
        name = self._name
        if name is None:
            info = ProgressManager.get_caller_info(3)
            name = info[2] if len(info) > 2 else "CoroutineParallelWorkflow"
        short_uid = uuid.uuid4().hex[:4]
        return f"{name}.{func_name}.{short_uid}"

    def gather(
        self,
        futures: Iterable[Future | Awaitable[Any]],
        description: str = "Gathering results",
    ) -> list[Any]:
        futures_list = [item if isinstance(item, Future) else self.submit_awaitable(item) for item in futures]
        if not futures_list:
            return []

        monitor = self._progress_monitor
        task_id = None
        if monitor is not None:
            func_name = ProgressManager.get_caller_info(2)[1]
            task_id = self._generate_task_id(func_name)
            monitor.register_task(task_id, description, total=len(futures_list))

        results = []
        for i, future in enumerate(self._get_progress_bar(futures_list)):
            results.append(future.result())
            if monitor is not None and task_id is not None:
                monitor.update_progress(task_id, current=i + 1)
        return results

    def map(
        self,
        func: Callable[[Any], Any],
        iterable: Iterable[Any],
        description: str = "Mapping tasks",
        *,
        blocking: bool = False,
    ) -> list[Any]:
        items = list(iterable)
        if not items:
            return []

        if len(items) < self.config.parallel_threshold:
            return [self.fetch(self.submit(func, item, blocking=blocking)) for item in items]

        monitor = self._progress_monitor
        task_id = None
        if monitor is not None:
            func_name = ProgressManager.get_caller_info(2)[1]
            task_id = self._generate_task_id(func_name)
            monitor.register_task(task_id, description, total=len(items))

        futures = self.submit_many(func, items, blocking=blocking)
        results = self.gather(futures)

        if monitor is not None and task_id is not None:
            monitor.update_progress(task_id, current=len(items), status="completed")

        return results

    def wait(self, futures: Iterable[Future], timeout: float | None = None) -> None:
        from concurrent.futures import wait

        wait(futures, timeout=timeout)

    def as_completed(self, futures: Iterable[Future], timeout: float | None = None):
        return as_completed(futures, timeout=timeout)

    def _get_progress_bar(self, iterable: Iterable) -> Iterable:
        monitor = self._progress_monitor
        if monitor is not None and monitor.is_displaying():
            return iterable

        if not self.config.show_progress:
            return iterable

        description = self.config.progress_description
        bar_type = self.config.progress_bar_type

        if bar_type == "rich":
            from rich.progress import track

            return track(iterable, description=description)
        if bar_type == "tqdm":
            from tqdm import tqdm

            return tqdm(iterable, desc=description)
        if bar_type == "jupyter":
            from tqdm.notebook import tqdm as tqdm_notebook

            return tqdm_notebook(iterable, desc=description)
        return iterable

    def shutdown(self, wait: bool = True) -> None:
        if not self._shutdown:
            self._pool.close(wait_tasks=wait)
            self._shutdown = True

    def __enter__(self) -> CoroutineParallelWorkflow:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.shutdown(wait=True)
        else:
            self._pool.kill()
            self._shutdown = True

    def __del__(self) -> None:
        if not getattr(self, "_shutdown", True):
            self.shutdown(wait=False)
