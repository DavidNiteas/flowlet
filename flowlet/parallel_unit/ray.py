from __future__ import annotations

import gc
import uuid
from collections.abc import Callable, Iterable
from typing import Any

import ray

from ..base.progress import ProgressManager
from ..executable_unit import Workflow
from .config import ParallelConfig


@ray.remote
def _ray_execute_task(func: Callable, *args, **kwargs) -> Any:
    """Ray远程执行任务函数。

    Args:
        func: 要执行的函数
        *args: 函数的位置参数
        **kwargs: 函数的关键字参数

    Returns:
        函数执行结果
    """
    return func(*args, **kwargs)


class RayPoolCreatorWorkflow(Workflow):
    """Ray并行池创建器工作流。

    专门用于初始化Ray集群，可以独立使用或被RayParallelWorkflow调用。
    """

    config: ParallelConfig = ParallelConfig()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def __call__(self) -> bool:
        """初始化Ray集群。

        Returns:
            是否成功初始化（如果已初始化则返回False）
        """
        if ray.is_initialized():
            return False

        # 构建Ray初始化参数
        ray_init_kwargs = {
            "include_dashboard": self.config.ray_include_dashboard,
            "log_to_driver": self.config.ray_log_to_driver,
        }

        if self.config.ray_dashboard_port is not None:
            ray_init_kwargs["dashboard_port"] = self.config.ray_dashboard_port

        if self.config.ray_runtime_env is not None:
            ray_init_kwargs["runtime_env"] = self.config.ray_runtime_env

        # 初始化Ray
        ray.init(**ray_init_kwargs)
        return True


class RayParallelWorkflow(Workflow):
    """基于Ray的并行工作流。

    使用Ray进行分布式进程池并行处理，支持细粒度资源控制。
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
        self._progress_monitor = progress_monitor
        self._name = name

        # 如果Ray未初始化，自动初始化
        if not ray.is_initialized():
            RayPoolCreatorWorkflow(self.config).bind_input().execute()

    def __call__(self, func: Callable, *args, **kwargs) -> ray.ObjectRef:
        """执行单个任务。

        Args:
            func: 要执行的函数
            *args: 函数的位置参数
            **kwargs: 函数的关键字参数

        Returns:
            表示任务的Ray ObjectRef（Future）
        """
        return self.submit(func, *args, **kwargs)

    def submit(self, func: Callable, *args, **kwargs) -> ray.ObjectRef:
        """提交单个任务到Ray集群（原子操作：发送）。

        Args:
            func: 要执行的函数
            *args: 函数的位置参数
            **kwargs: 函数的关键字参数

        Returns:
            表示任务的Ray ObjectRef（Future）
        """
        # 为任务设置资源需求
        task_options = {
            "num_cpus": self.config.per_task_num_cpus,
            "num_gpus": self.config.per_task_num_gpus,
        }

        if self.config.per_task_memory is not None:
            task_options["memory"] = self.config.per_task_memory

        # 提交任务并返回ObjectRef
        return _ray_execute_task.options(**task_options).remote(func, *args, **kwargs)

    def is_ready(self, future: ray.ObjectRef) -> bool:
        """判断任务是否完成（原子操作：判断）。

        Args:
            future: 要检查的ObjectRef

        Returns:
            任务是否已完成
        """
        ready, _ = ray.wait([future], num_returns=1, timeout=0)
        return len(ready) > 0

    def fetch(self, future: ray.ObjectRef) -> Any:
        """从工作进程中拉取数据（原子操作：拉取）。

        Args:
            future: 要拉取的ObjectRef

        Returns:
            任务执行结果
        """
        return ray.get(future)

    def submit_many(self, func: Callable, iterable: Iterable[Any]) -> list[ray.ObjectRef]:
        """提交多个任务，返回ObjectRefs列表（异步）。

        Args:
            func: 要执行的函数
            iterable: 包含输入数据的可迭代对象

        Returns:
            表示任务的ObjectRef对象列表
        """
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
            name = info[2] if len(info) > 2 else "RayParallelWorkflow"
        short_uid = uuid.uuid4().hex[:4]
        return f"{name}.{func_name}.{short_uid}"

    def map(self, func: Callable[[Any], Any], iterable: Iterable[Any]) -> list[Any]:
        """将函数应用于可迭代对象的每个元素（同步）。

        相当于批量发送-批量等待-批量拉取的合并策略。

        Args:
            func: 要应用的函数
            iterable: 包含输入数据的可迭代对象

        Returns:
            函数应用结果的列表
        """
        items = list(iterable)
        if not items:
            return []

        # 任务数低于阈值时直接串行执行，避免分布式调度开销
        if len(items) < self.config.parallel_threshold:
            return [func(item) for item in items]

        monitor = self._progress_monitor
        task_id = None
        if monitor is not None:
            func_name = ProgressManager.get_caller_info(2)[1]
            task_id = self._generate_task_id(func_name)
            monitor.register_task(task_id, "Mapping tasks", total=len(items))

        futures = self.submit_many(func, items)
        results = self.gather(futures)

        if monitor is not None and task_id is not None:
            monitor.update_progress(task_id, current=len(items), status="completed")

        return results

    def gather(
        self,
        futures: Iterable[ray.ObjectRef],
        description: str = "Gathering results",
    ) -> list[Any]:
        """批量拉取多个任务的结果。

        Args:
            futures: 要拉取的ObjectRef列表
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
        try:
            if self.config.use_concurrent_io:
                # 使用并发IO
                results = ray.get(futures_list)
                if monitor is not None and task_id is not None:
                    monitor.update_progress(task_id, current=len(futures_list))
            else:
                # 顺序拉取
                results = []
                for i, future in enumerate(self._get_progress_bar(futures_list)):
                    results.append(ray.get(future))
                    if monitor is not None and task_id is not None:
                        monitor.update_progress(task_id, current=i + 1)
        except Exception as e:
            raise e

        # 清理引用以帮助垃圾回收
        del futures_list
        gc.collect()

        return results

    def wait(self, futures: Iterable[ray.ObjectRef], timeout: float | None = None) -> None:
        """等待所有ObjectRef完成（原子操作：等待）。

        Args:
            futures: 要等待的ObjectRef对象集合
            timeout: 超时时间（秒）
        """
        ray.wait(list(futures), num_returns=len(list(futures)), timeout=timeout)

    def as_completed(self, futures: Iterable[ray.ObjectRef]):
        """返回一个迭代器，当ObjectRef完成时产生ObjectRef。

        Args:
            futures: 要监视的ObjectRef对象集合

        Yields:
            完成的ObjectRef对象
        """
        futures_list = list(futures)
        while futures_list:
            # 获取一个完成的任务
            ready, futures_list = ray.wait(futures_list, num_returns=1)
            if ready:
                yield ready[0]

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
