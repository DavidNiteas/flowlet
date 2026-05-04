"""进度监视器实现。

支持同进程直接访问和跨进程代理访问两种模式。
可启动/停止 CLI 进度条渲染（自身持有独立线程）。

跨进程代理按后端分为两种：
- :class:`MPProgressProxy`: 使用 ``multiprocessing.Queue``，适用于标准库多进程。
- :class:`RayProgressProxy`: 使用 ``ray.util.queue.Queue``，适用于 Ray 分布式工作进程。

两种代理在类型上即被区分，防止混用导致序列化失败。
"""

from __future__ import annotations

import sys
import threading
import time
import warnings
from abc import ABC, abstractmethod
from contextlib import suppress
from copy import deepcopy
from multiprocessing import Queue as MPQueue
from queue import Empty
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field

from ..config.base_config import BaseConfig

if TYPE_CHECKING:
    pass


class TaskProgress(BaseConfig):
    """单个任务的进度状态。"""

    task_id: str = ""
    description: str = ""
    total: int = 0
    current: int = 0
    status: Literal["pending", "running", "completed", "failed"] = "pending"
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProgressMessage(BaseConfig):
    """IPC 进度消息基类。

    所有跨进程消息都继承此类，通过多态 ``apply`` 方法应用到 ProgressManager。
    """

    def apply(self, monitor: ProgressManager) -> None:
        """将消息应用到指定的 ProgressManager。子类应覆盖此方法。"""
        raise NotImplementedError


class RegisterTaskMessage(ProgressMessage):
    """注册任务消息。"""

    task_id: str
    description: str = ""
    total: int = 0

    def apply(self, monitor: ProgressManager) -> None:
        monitor._register_task_local(self.task_id, self.description, self.total)


class UpdateProgressMessage(ProgressMessage):
    """更新进度消息。

    ``mode="set"`` 时覆盖当前进度，``mode="increment"`` 时累加。
    """

    task_id: str
    current: int = 0
    status: str | None = None
    mode: Literal["set", "increment"] = "set"

    def apply(self, monitor: ProgressManager) -> None:
        if self.mode == "increment":
            monitor._increment_progress_local(
                self.task_id, self.current, self.status
            )
        else:
            monitor._update_progress_local(
                self.task_id, self.current, self.status
            )


class BaseProgress(ABC):
    """进度跟踪抽象基类。

    ProgressManager 和各代理类共享此接口，
    使用方无需关心是在主进程还是子进程中。
    """

    def __init__(self, bar_type: Literal["rich", "tqdm"] = "rich") -> None:
        self._tasks: dict[str, TaskProgress] = {}
        self._lock = threading.RLock()
        self._bar_type = bar_type

        # Display (CLI 进度条)
        self._progress: Any = None
        self._rich_tasks: dict[str, Any] = {}
        self._display_thread: threading.Thread | None = None
        self._display_running = False

    # ---- 通用查询方法 ----

    def get_progress(self, task_id: str) -> TaskProgress | None:
        """获取单个任务的进度状态（深拷贝）。"""
        with self._lock:
            task = self._tasks.get(task_id)
            return deepcopy(task) if task is not None else None

    def get_all_progress(self) -> dict[str, TaskProgress]:
        """获取所有任务的进度状态（深拷贝）。"""
        with self._lock:
            return {k: deepcopy(v) for k, v in self._tasks.items()}

    def remove_task(self, task_id: str) -> None:
        """移除一个任务。"""
        with self._lock:
            self._tasks.pop(task_id, None)
            if task_id in self._rich_tasks:
                rich_task = self._rich_tasks.pop(task_id)
                if self._progress is not None:
                    with suppress(Exception):
                        self._progress.remove_task(rich_task)

    def clear_tasks(self) -> None:
        """清除所有任务。"""
        with self._lock:
            self._tasks.clear()
            if self._progress is not None:
                for rich_task in self._rich_tasks.values():
                    with suppress(Exception):
                        self._progress.remove_task(rich_task)
            self._rich_tasks.clear()

    # ---- 抽象写方法（子类实现） ----

    @abstractmethod
    def register_task(self, task_id: str, description: str, total: int = 0) -> str:
        """注册一个新任务。

        Returns:
            注册的任务标识（即传入的 ``task_id``）。
        """
        raise NotImplementedError

    @abstractmethod
    def update_progress(
        self,
        task_id: str,
        current: int,
        status: str | None = None,
        *,
        mode: Literal["set", "increment"] = "set",
    ) -> None:
        """更新指定任务的进度。

        Args:
            task_id: 任务标识。
            current: 进度值。``mode="set"`` 时直接覆盖，
                ``mode="increment"`` 时累加到现有值。
            status: 可选的状态更新。
            mode: ``"set"``（覆盖，默认）或 ``"increment"``（累加）。
        """
        raise NotImplementedError

    # ---- CLI 进度条渲染（通用） ----

    def start_display(self) -> None:
        """启动 CLI 进度条渲染线程。

        使用 rich.progress.Progress + rich.live.Live，在独立线程中运行。
        若已启动，则忽略。
        """
        if self._display_running:
            return
        self._display_running = True
        self._display_thread = threading.Thread(
            target=self._render_loop,
            daemon=True,
        )
        self._display_thread.start()

    def stop_display(self) -> None:
        """停止 CLI 进度条渲染线程。"""
        if not self._display_running:
            return
        self._display_running = False
        if self._display_thread is not None:
            self._display_thread.join(timeout=2.0)
            self._display_thread = None

    def is_displaying(self) -> bool:
        """检查是否正在显示进度条。"""
        return self._display_running

    # ---- 内部方法 ----

    @staticmethod
    def get_caller_info(k: int) -> tuple[str, ...]:
        """向上追溯 k 级调用者，返回各级函数名的元组。

        返回的元组中，索引 ``0`` 为直接调用者（向上回溯 1 级），
        索引 ``i`` 为向上回溯 ``i + 1`` 级的调用者。

        例如 ``k=2`` 时，返回 ``(直接调用者名称, 二级调用者名称)``。

        Args:
            k: 要追溯的级数，必须 >= 1。

        Returns:
            各级调用者函数名的元组。若某级不存在，用 ``"unknown"`` 填充。
        """

        def _frame_name(i: int) -> str:
            try:
                return sys._getframe(i).f_code.co_name
            except ValueError:
                return "unknown"

        return tuple(map(_frame_name, range(1, k + 1)))

    def _register_task_local(
        self, task_id: str, description: str, total: int = 0
    ) -> str:
        """本地注册任务（不触发 IPC）。

        Returns:
            注册的任务标识（即传入的 ``task_id``）。
        """
        with self._lock:
            self._tasks[task_id] = TaskProgress(
                task_id=task_id,
                description=description,
                total=total,
                current=0,
                status="pending",
            )
            self._sync_to_display(task_id)
        return task_id

    def _update_progress_local(
        self,
        task_id: str,
        current: int,
        status: str | None = None,
    ) -> None:
        """本地更新进度（覆盖式，不触发 IPC）。"""
        with self._lock:
            if task_id not in self._tasks:
                return
            task = self._tasks[task_id]
            task.current = current
            if status is not None:
                task.status = status
            self._sync_to_display(task_id)

    def _increment_progress_local(
        self,
        task_id: str,
        delta: int,
        status: str | None = None,
    ) -> None:
        """本地累加进度（不触发 IPC）。"""
        with self._lock:
            if task_id not in self._tasks:
                return
            task = self._tasks[task_id]
            task.current += delta
            if status is not None:
                task.status = status
            self._sync_to_display(task_id)

    def _sync_to_display(self, task_id: str) -> None:
        """将任务进度同步到 rich 进度条显示。"""
        if self._progress is None:
            return
        task = self._tasks.get(task_id)
        if task is None:
            return
        if task_id not in self._rich_tasks:
            self._rich_tasks[task_id] = self._progress.add_task(
                task.description,
                total=task.total,
            )
        rich_task = self._rich_tasks[task_id]
        try:
            self._progress.update(
                rich_task,
                completed=task.current,
                total=task.total,
            )
        except Exception:
            pass

    def _render_loop(self) -> None:
        """渲染线程：使用 rich Live 持续刷新进度条。"""
        if self._bar_type == "rich":
            try:
                from rich.progress import Progress

                self._progress = Progress()
                self._progress.start()
                try:
                    # 将已有任务同步到进度条
                    with self._lock:
                        for task_id in self._tasks.keys():
                            self._sync_to_display(task_id)
                    while self._display_running:
                        time.sleep(0.1)
                finally:
                    self._progress.stop()
            except ImportError:
                pass
            finally:
                self._progress = None
                self._rich_tasks.clear()
        elif self._bar_type == "tqdm":
            # tqdm 不支持动态添加任务到同一个进度条容器，
            # 故在 tqdm 模式下不启动独立渲染线程，
            # 由调用方在合适的时机使用 tqdm 包装迭代器。
            pass


# ---------------------------------------------------------------------------
# 跨进程代理
# ---------------------------------------------------------------------------


class BaseProgressProxy(BaseProgress, ABC):
    """进度代理抽象基类。

    子进程中的代理实现，维护本地缓存，并通过 IPC 向主进程发送消息。
    写操作同时更新本地缓存并发送 IPC 消息；
    读操作直接读取本地缓存，零 IPC 开销。
    """

    def __init__(
        self,
        tasks_snapshot: dict[str, TaskProgress] | None = None,
        bar_type: Literal["rich", "tqdm"] = "rich",
    ) -> None:
        super().__init__(bar_type)
        if tasks_snapshot is not None:
            self._tasks = tasks_snapshot

    def register_task(self, task_id: str, description: str, total: int = 0) -> str:
        """注册一个新任务（更新本地缓存 + 发送 IPC 消息）。

        Returns:
            注册的任务标识（即传入的 ``task_id``）。
        """
        self._register_task_local(task_id, description, total)
        self._send_message(
            RegisterTaskMessage(task_id=task_id, description=description, total=total)
        )
        return task_id

    def update_progress(
        self,
        task_id: str,
        current: int,
        status: str | None = None,
        *,
        mode: Literal["set", "increment"] = "set",
    ) -> None:
        """更新指定任务的进度（更新本地缓存 + 发送 IPC 消息）。

        Args:
            task_id: 任务标识。
            current: 进度值或累加增量（由 ``mode`` 控制）。
            status: 可选的状态更新。
            mode: ``"set"``（覆盖，默认）或 ``"increment"``（累加）。
        """
        if mode == "increment":
            self._increment_progress_local(task_id, current, status)
        else:
            self._update_progress_local(task_id, current, status)
        self._send_message(
            UpdateProgressMessage(
                task_id=task_id, current=current, status=status, mode=mode
            )
        )

    @abstractmethod
    def _send_message(self, msg: ProgressMessage) -> None:
        """发送 IPC 消息到主进程。子类实现具体的发送逻辑。"""
        raise NotImplementedError


class MPProgressProxy(BaseProgressProxy):
    """Multiprocessing 进度代理。

    使用 ``multiprocessing.Queue`` 进行 IPC，适用于标准库
    :mod:`multiprocessing` 子进程。

    **不可通过 Ray 传递**：``multiprocessing.Queue`` 无法被 Ray 序列化。
    Ray 分布式场景请使用 :class:`RayProgressProxy`。
    """

    def __init__(
        self,
        queue: MPQueue | None = None,
        tasks_snapshot: dict[str, TaskProgress] | None = None,
        bar_type: Literal["rich", "tqdm"] = "rich",
    ) -> None:
        super().__init__(tasks_snapshot, bar_type)
        self._queue = queue

    def _send_message(self, msg: ProgressMessage) -> None:
        if self._queue is not None:
            self._queue.put(msg)

    def __getstate__(self) -> dict[str, Any]:
        """序列化时排除不可序列化的 ``multiprocessing.Queue``。"""
        return {
            "tasks": {k: v.to_dict() for k, v in self._tasks.items()},
            "bar_type": self._bar_type,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        """反序列化时重建基础状态，queue 设为 None。"""
        BaseProgress.__init__(self, state.get("bar_type", "rich"))
        self._tasks = {
            k: TaskProgress.from_dict(v) for k, v in state.get("tasks", {}).items()
        }
        self._queue = None


class RayProgressProxy(BaseProgressProxy):
    """Ray 进度代理。

    使用 ``ray.util.queue.Queue`` 进行 IPC，适用于 Ray 分布式工作进程。

    **可通过 Ray 安全传递**：``ray.util.queue.Queue`` 可被 Ray 的 cloudpickle
    正常序列化/反序列化，且反序列化后仍保持跨进程通信能力。
    """

    def __init__(
        self,
        queue: Any | None = None,
        tasks_snapshot: dict[str, TaskProgress] | None = None,
        bar_type: Literal["rich", "tqdm"] = "rich",
    ) -> None:
        super().__init__(tasks_snapshot, bar_type)
        self._queue = queue

    def _send_message(self, msg: ProgressMessage) -> None:
        if self._queue is not None:
            self._queue.put(msg)

    def __getstate__(self) -> dict[str, Any]:
        """序列化状态。RayQueue 可被 pickle，直接包含在状态中。"""
        return {
            "tasks": {k: v.to_dict() for k, v in self._tasks.items()},
            "bar_type": self._bar_type,
            "queue": self._queue,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        """反序列化时重建基础状态和 queue。"""
        BaseProgress.__init__(self, state.get("bar_type", "rich"))
        self._tasks = {
            k: TaskProgress.from_dict(v) for k, v in state.get("tasks", {}).items()
        }
        self._queue = state.get("queue")


# 向后兼容别名
ProgressProxy = MPProgressProxy
"""向后兼容别名，等同于 :class:`MPProgressProxy`。"""


# ---------------------------------------------------------------------------
# 主进程监视器
# ---------------------------------------------------------------------------


class ProgressManager(BaseProgress):
    """进度监视器（主进程实现）。

    维护一组命名任务的进度状态。支持同进程直接访问和跨进程代理访问。
    可启动独立线程渲染 CLI 进度条。

    同进程模式：
        >>> monitor = ProgressManager()
        >>> monitor.register_task("write", "Writing files", total=10)
        >>> monitor.update_progress("write", current=5)
        >>> prog = monitor.get_progress("write")

    跨进程模式（multiprocessing）：
        >>> monitor = ProgressManager()
        >>> proxy = monitor.get_mp_proxy()
        >>> # 将 proxy 传递给 multiprocessing.Process

    跨进程模式（Ray）：
        >>> monitor = ProgressManager()
        >>> proxy = monitor.get_ray_proxy()
        >>> # 将 proxy 传递给 @ray.remote 函数

    CLI 进度条：
        >>> monitor.start_display()   # 启动渲染线程
        >>> ... # 执行任务
        >>> monitor.stop_display()    # 停止渲染
    """

    def __init__(
        self,
        enable_ipc: bool = False,
        bar_type: Literal["rich", "tqdm"] = "rich",
    ) -> None:
        """初始化 ProgressManager。

        Args:
            enable_ipc: 已废弃，保留此参数仅用于向后兼容。
                Queue 的创建已解耦到代理构造阶段（:meth:`get_mp_proxy` /
                :meth:`get_ray_proxy`），此参数不再影响行为。
            bar_type: 进度条类型，``"rich"`` 或 ``"tqdm"``。
        """
        super().__init__(bar_type)
        if enable_ipc:
            warnings.warn(
                "enable_ipc parameter is deprecated and ignored. "
                "Queue creation is now deferred to proxy construction. "
                "Use get_mp_proxy() or get_ray_proxy() instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        # IPC queues（按需创建）
        self._mp_queue: MPQueue | None = None
        self._ray_queue: Any | None = None
        self._consumer_threads: list[threading.Thread] = []

    def __getstate__(self) -> dict[str, Any]:
        """序列化时排除不可序列化的线程/队列对象。"""
        return {
            "tasks": {k: v.to_dict() for k, v in self._tasks.items()},
            "bar_type": self._bar_type,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        """反序列化时重建基础状态（不包含线程/队列）。"""
        BaseProgress.__init__(self, state.get("bar_type", "rich"))
        self._tasks = {
            k: TaskProgress.from_dict(v) for k, v in state.get("tasks", {}).items()
        }
        self._mp_queue = None
        self._ray_queue = None
        self._consumer_threads = []

    # ---- 实现抽象写方法 ----

    def register_task(self, task_id: str, description: str, total: int = 0) -> str:
        """注册一个新任务。

        Returns:
            注册的任务标识（即传入的 ``task_id``）。
        """
        return self._register_task_local(task_id, description, total)

    def update_progress(
        self,
        task_id: str,
        current: int,
        status: str | None = None,
        *,
        mode: Literal["set", "increment"] = "set",
    ) -> None:
        """更新指定任务的进度。

        Args:
            task_id: 任务标识。
            current: 进度值或累加增量（由 ``mode`` 控制）。
            status: 可选的状态更新。
            mode: ``"set"``（覆盖，默认）或 ``"increment"``（累加）。
        """
        if mode == "increment":
            self._increment_progress_local(task_id, current, status)
        else:
            self._update_progress_local(task_id, current, status)

    # ---- 跨进程代理 ----

    def get_mp_proxy(self) -> MPProgressProxy:
        """获取 multiprocessing 代理对象。

        首次调用时会创建 ``multiprocessing.Queue`` 并启动 IPC 消费线程。
        代理创建时会 snapshot 当前所有任务的进度状态，因此代理的本地
        缓存与主监视器在创建时刻保持一致。
        """
        if self._mp_queue is None:
            self._mp_queue = MPQueue()
            thread = threading.Thread(target=self._consume_mp_loop, daemon=True)
            thread.start()
            self._consumer_threads.append(thread)
        with self._lock:
            tasks_snapshot = {k: deepcopy(v) for k, v in self._tasks.items()}
        return MPProgressProxy(self._mp_queue, tasks_snapshot, self._bar_type)

    def get_ray_proxy(self) -> RayProgressProxy:
        """获取 Ray 代理对象。

        首次调用时会创建 ``ray.util.queue.Queue`` 并启动 IPC 消费线程。
        代理创建时会 snapshot 当前所有任务的进度状态。

        返回的 :class:`RayProgressProxy` 可安全地通过 ``ray.put`` /
        ``ray.get`` 或作为 ``@ray.remote`` 函数的参数传递。
        """
        if self._ray_queue is None:
            try:
                from ray.util.queue import Queue as RayQueue
            except ImportError as exc:
                raise RuntimeError(
                    "Ray is not installed. Install ray to use get_ray_proxy()."
                ) from exc
            self._ray_queue = RayQueue()
            thread = threading.Thread(target=self._consume_ray_loop, daemon=True)
            thread.start()
            self._consumer_threads.append(thread)
        with self._lock:
            tasks_snapshot = {k: deepcopy(v) for k, v in self._tasks.items()}
        return RayProgressProxy(self._ray_queue, tasks_snapshot, self._bar_type)

    def get_proxy(self) -> MPProgressProxy:
        """获取跨进程代理对象（向后兼容，等同于 :meth:`get_mp_proxy`）。"""
        return self.get_mp_proxy()

    # ---- 内部方法 ----

    def _consume_mp_loop(self) -> None:
        """Multiprocessing IPC 消费线程：从 MPQueue 读取消息并应用到本地状态。"""
        while True:
            try:
                msg = self._mp_queue.get(timeout=0.5)
            except Exception:
                continue
            if msg is None:
                break
            if isinstance(msg, ProgressMessage):
                msg.apply(self)

    def _consume_ray_loop(self) -> None:
        """Ray IPC 消费线程：从 Ray Queue 读取消息并应用到本地状态。"""
        while True:
            try:
                msg = self._ray_queue.get(block=True, timeout=0.5)
            except Empty:
                continue
            except Exception:
                continue
            if msg is None:
                break
            if isinstance(msg, ProgressMessage):
                msg.apply(self)
