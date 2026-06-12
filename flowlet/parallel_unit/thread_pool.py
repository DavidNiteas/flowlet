from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any

from ..base.progress import ProgressManager
from ..executable_unit import Workflow
from .config import ParallelConfig


class ThreadParallelWorkflow(Workflow):
    """基于ThreadPoolExecutor的线程池工作流。

    使用线程池进行并行处理，共享进程内存空间。
    遵循标准并发执行器接口设计，提供原子操作和批量操作。
    """

    config: ParallelConfig = ParallelConfig()

    def __init__(
        self,
        *args,
        name: str | None = None,
        progress_monitor: ProgressManager | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        # 线程池直接使用max_concurrent_tasks作为最大工作线程数
        self._pool = ThreadPoolExecutor(max_workers=self.config.max_concurrent_tasks)
        self._shutdown = False
        self._progress_monitor = progress_monitor
        self._name = name

    def __call__(self, func: Callable, *args, **kwargs) -> Future:
        """执行单个任务。

        Args:
            func: 要执行的函数
            *args: 函数的位置参数
            **kwargs: 函数的关键字参数

        Returns:
            表示任务的Future对象
        """
        return self.submit(func, *args, **kwargs)

    def submit(self, func: Callable, *args, **kwargs) -> Future:
        """提交单个任务到执行器（原子操作：发送）。

        Args:
            func: 要执行的函数
            *args: 函数的位置参数
            **kwargs: 函数的关键字参数

        Returns:
            表示任务的Future对象
        """
        if self._shutdown:
            raise RuntimeError("Thread pool has been shutdown")

        # 提交任务并返回Future
        return self._pool.submit(func, *args, **kwargs)

    def is_ready(self, future: Future) -> bool:
        """判断任务是否完成（原子操作：判断）。

        Args:
            future: 要检查的Future

        Returns:
            任务是否已完成
        """
        return future.done()

    def fetch(self, future: Future) -> Any:
        """从工作线程中拉取数据（原子操作：拉取）。

        Args:
            future: 要拉取的Future

        Returns:
            任务执行结果
        """
        return future.result()

    def submit_many(self, func: Callable, iterable: Iterable[Any]) -> list[Future]:
        """提交多个任务，返回Futures列表（异步）。

        Args:
            func: 要执行的函数
            iterable: 包含输入数据的可迭代对象

        Returns:
            表示任务的Future对象列表
        """
        if self._shutdown:
            raise RuntimeError("Thread pool has been shutdown")

        items = list(iterable)
        if not items:
            return []

        futures = []
        for item in items:
            future = self.submit(func, item)
            futures.append(future)

        return futures

    def _generate_task_id(self, func_name: str) -> str:
        """生成任务标识，格式为 name.func.short_uid。"""
        name = self._name
        if name is None:
            info = ProgressManager.get_caller_info(3)
            name = info[2] if len(info) > 2 else "ThreadParallelWorkflow"
        short_uid = uuid.uuid4().hex[:4]
        return f"{name}.{func_name}.{short_uid}"

    def gather(
        self,
        futures: Iterable[Future],
        description: str = "Gathering results",
    ) -> list[Any]:
        """批量拉取多个任务的结果。

        Args:
            futures: 要拉取的Future列表
            description: 进度任务描述文本。

        Returns:
            任务执行结果列表
        """
        futures_list = list(futures)
        if not futures_list:
            return []

        monitor = self._progress_monitor
        task_id = None
        if monitor is not None:
            func_name = ProgressManager.get_caller_info(2)[1]
            task_id = self._generate_task_id(func_name)
            monitor.register_task(task_id, description, total=len(futures_list))

        # 收集结果
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
    ) -> list[Any]:
        """将函数应用于可迭代对象的每个元素（同步）。

        相当于批量发送-批量等待-批量拉取的合并策略。

        Args:
            func: 要应用的函数
            iterable: 包含输入数据的可迭代对象
            description: 进度任务描述文本。

        Returns:
            函数应用结果的列表
        """
        items = list(iterable)
        if not items:
            return []

        # 任务数低于阈值时直接串行执行，避免线程池开销
        if len(items) < self.config.parallel_threshold:
            return [func(item) for item in items]

        monitor = self._progress_monitor
        task_id = None
        if monitor is not None:
            func_name = ProgressManager.get_caller_info(2)[1]
            task_id = self._generate_task_id(func_name)
            monitor.register_task(task_id, description, total=len(items))

        futures = self.submit_many(func, items)
        results = self.gather(futures)

        if monitor is not None and task_id is not None:
            monitor.update_progress(task_id, current=len(items), status="completed")

        return results

    def wait(self, futures: Iterable[Future], timeout: float | None = None) -> None:
        """等待所有Future完成（原子操作：等待）。

        Args:
            futures: 要等待的Future对象集合
            timeout: 超时时间（秒）
        """
        from concurrent.futures import wait

        wait(futures, timeout=timeout)

    def as_completed(self, futures: Iterable[Future], timeout: float | None = None):
        """返回一个迭代器，当Future完成时产生Future。

        Args:
            futures: 要监视的Future对象集合
            timeout: 超时时间（秒）

        Yields:
            完成的Future对象
        """
        return as_completed(futures, timeout=timeout)

    def _get_progress_bar(self, iterable: Iterable) -> Iterable:
        """获取进度条迭代器。

        若构造时传入了 progress_monitor 且正在显示，则不额外包装进度条，
        由 ProgressManager 自身负责渲染。

        Args:
            iterable: 要迭代的对象

        Returns:
            带进度条的迭代器
        """
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
        elif bar_type == "tqdm":
            from tqdm import tqdm

            return tqdm(iterable, desc=description)
        elif bar_type == "jupyter":
            from tqdm.notebook import tqdm as tqdm_notebook

            return tqdm_notebook(iterable, desc=description)
        else:
            return iterable

    def shutdown(self, wait: bool = True) -> None:
        """关闭线程池。

        Args:
            wait: 是否等待所有任务完成
        """
        if not self._shutdown:
            self._pool.shutdown(wait=wait)
            self._shutdown = True

    def __enter__(self) -> ThreadParallelWorkflow:
        """上下文管理器入口。"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """上下文管理器出口。"""
        self.shutdown(wait=True)

    def __del__(self) -> None:
        """清理资源。"""
        if not self._shutdown:
            self.shutdown(wait=False)
