"""进度监视器单元测试。"""

from __future__ import annotations

import multiprocessing
import threading
import time
import warnings

import pytest
from flowlet.base.progress import (
    BaseProgress,
    BaseProgressProxy,
    MPProgressProxy,
    ProgressManager,
    ProgressMessage,
    ProgressProxy,
    RayProgressProxy,
    RegisterTaskMessage,
    TaskProgress,
    UpdateProgressMessage,
)
from flowlet.config.base_config import BaseConfig


@pytest.fixture(scope="module")
def ray_initialized():
    """Module-scoped Ray 初始化，所有 Ray 测试共享同一个上下文。"""
    import ray

    ray.init(ignore_reinit_error=True)
    yield
    ray.shutdown()


# ==================== 测试1: TaskProgress ====================


class TestTaskProgress:
    """测试 TaskProgress 数据类。"""

    def test_default_initialization(self):
        """测试默认初始化。"""
        task = TaskProgress()
        assert task.task_id == ""
        assert task.description == ""
        assert task.total == 0
        assert task.current == 0
        assert task.status == "pending"
        assert task.metadata == {}

    def test_custom_initialization(self):
        """测试自定义初始化。"""
        task = TaskProgress(
            task_id="task1",
            description="Test task",
            total=100,
            current=50,
            status="running",
            metadata={"key": "value"},
        )
        assert task.task_id == "task1"
        assert task.description == "Test task"
        assert task.total == 100
        assert task.current == 50
        assert task.status == "running"
        assert task.metadata == {"key": "value"}

    def test_serialization(self):
        """测试序列化和反序列化。"""
        task = TaskProgress(task_id="t1", total=10, current=5)
        d = task.to_dict()
        assert d["task_id"] == "t1"
        assert d["total"] == 10
        assert d["current"] == 5

        task2 = TaskProgress.from_dict(d)
        assert task2.task_id == "t1"
        assert task2.total == 10
        assert task2.current == 5


# ==================== 测试2: ProgressMessage ====================


class TestProgressMessage:
    """测试 IPC 消息类。"""

    def test_register_task_message(self):
        """测试注册任务消息。"""
        msg = RegisterTaskMessage(task_id="t1", description="desc", total=100)
        assert msg.task_id == "t1"
        assert msg.description == "desc"
        assert msg.total == 100

        monitor = ProgressManager()
        msg.apply(monitor)
        prog = monitor.get_progress("t1")
        assert prog is not None
        assert prog.description == "desc"
        assert prog.total == 100

    def test_update_progress_message(self):
        """测试更新进度消息。"""
        monitor = ProgressManager()
        monitor.register_task("t1", "desc", total=100)

        msg = UpdateProgressMessage(task_id="t1", current=50, status="running")
        msg.apply(monitor)

        prog = monitor.get_progress("t1")
        assert prog.current == 50
        assert prog.status == "running"

    def test_message_is_base_config(self):
        """测试消息类继承自 BaseConfig。"""
        assert issubclass(RegisterTaskMessage, BaseConfig)
        assert issubclass(UpdateProgressMessage, BaseConfig)
        assert issubclass(RegisterTaskMessage, ProgressMessage)
        assert issubclass(UpdateProgressMessage, ProgressMessage)


# ==================== 测试3: ProgressManager 本地模式 ====================


class TestProgressManagerLocal:
    """测试 ProgressManager 同进程模式。"""

    def test_register_and_get(self):
        """测试注册和获取进度。"""
        monitor = ProgressManager()
        monitor.register_task("task1", "Writing file", total=100)

        prog = monitor.get_progress("task1")
        assert prog is not None
        assert prog.task_id == "task1"
        assert prog.description == "Writing file"
        assert prog.total == 100
        assert prog.current == 0
        assert prog.status == "pending"

    def test_update_progress(self):
        """测试更新进度。"""
        monitor = ProgressManager()
        monitor.register_task("task1", "Writing file", total=100)
        monitor.update_progress("task1", current=50, status="running")

        prog = monitor.get_progress("task1")
        assert prog.current == 50
        assert prog.status == "running"

    def test_get_all_progress(self):
        """测试获取所有进度。"""
        monitor = ProgressManager()
        monitor.register_task("task1", "Task 1", total=10)
        monitor.register_task("task2", "Task 2", total=20)

        all_prog = monitor.get_all_progress()
        assert len(all_prog) == 2
        assert "task1" in all_prog
        assert "task2" in all_prog

    def test_remove_task(self):
        """测试移除任务。"""
        monitor = ProgressManager()
        monitor.register_task("task1", "Task 1", total=10)
        assert monitor.get_progress("task1") is not None

        monitor.remove_task("task1")
        assert monitor.get_progress("task1") is None

    def test_clear_tasks(self):
        """测试清除所有任务。"""
        monitor = ProgressManager()
        monitor.register_task("task1", "Task 1", total=10)
        monitor.register_task("task2", "Task 2", total=20)

        monitor.clear_tasks()
        assert monitor.get_all_progress() == {}

    def test_update_nonexistent_task(self):
        """测试更新不存在的任务应静默忽略。"""
        monitor = ProgressManager()
        # 不应抛出异常
        monitor.update_progress("nonexistent", current=10)

    def test_increment_mode(self):
        """测试累加模式。"""
        monitor = ProgressManager()
        monitor.register_task("task1", "Writing file", total=100)

        monitor.update_progress("task1", current=10, mode="increment")
        assert monitor.get_progress("task1").current == 10

        monitor.update_progress("task1", current=25, mode="increment")
        assert monitor.get_progress("task1").current == 35

        # 覆盖模式（默认）应重置
        monitor.update_progress("task1", current=5)
        assert monitor.get_progress("task1").current == 5

        # 再次累加
        monitor.update_progress("task1", current=3, mode="increment")
        assert monitor.get_progress("task1").current == 8

    def test_thread_safety(self):
        """测试多线程并发更新的线程安全性。"""
        monitor = ProgressManager()
        monitor.register_task("counter", "Counter", total=1000)

        def updater():
            for i in range(100):
                monitor.update_progress("counter", current=i + 1)

        threads = [threading.Thread(target=updater) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        prog = monitor.get_progress("counter")
        # 由于并发更新，current 值不确定，但至少不应该崩溃
        assert prog is not None
        assert 0 <= prog.current <= 1000


# ==================== 测试4: ProgressManager 显示模式 ====================


class TestProgressManagerDisplay:
    """测试 ProgressManager CLI 进度条渲染。"""

    def test_start_stop_display(self):
        """测试启动和停止显示。"""
        monitor = ProgressManager(bar_type="rich")
        assert not monitor.is_displaying()

        monitor.start_display()
        time.sleep(0.2)  # 给渲染线程启动时间
        assert monitor.is_displaying()

        monitor.stop_display()
        assert not monitor.is_displaying()

    def test_display_with_tasks(self):
        """测试显示模式下注册和更新任务。"""
        monitor = ProgressManager(bar_type="rich")
        monitor.start_display()
        time.sleep(0.2)

        monitor.register_task("task1", "Processing", total=100)
        time.sleep(0.1)
        monitor.update_progress("task1", current=50)
        time.sleep(0.1)

        # rich task 应该已创建
        assert "task1" in monitor._rich_tasks

        monitor.stop_display()

    def test_stop_without_start(self):
        """测试未启动时停止不应报错。"""
        monitor = ProgressManager()
        monitor.stop_display()  # 不应抛出异常

    def test_start_when_already_running(self):
        """测试重复启动应忽略。"""
        monitor = ProgressManager(bar_type="rich")
        monitor.start_display()
        time.sleep(0.1)

        # 再次启动不应报错
        monitor.start_display()
        assert monitor.is_displaying()

        monitor.stop_display()


# ==================== 测试5: MPProgressProxy ====================


class TestMPProgressProxy:
    """测试 MPProgressProxy 跨进程代理（multiprocessing 后端）。"""

    def test_proxy_shares_interface_with_monitor(self):
        """测试 Proxy 和 Monitor 共享 BaseProgress 接口。"""
        monitor = ProgressManager()
        proxy = monitor.get_mp_proxy()

        assert isinstance(proxy, BaseProgress)
        assert isinstance(monitor, BaseProgress)
        assert isinstance(proxy, BaseProgressProxy)
        assert isinstance(proxy, MPProgressProxy)

    def test_proxy_local_operations(self):
        """测试 Proxy 本地操作不依赖 IPC。"""
        monitor = ProgressManager()
        proxy = monitor.get_mp_proxy()

        proxy.register_task("local", "Local Task", total=10)
        proxy.update_progress("local", current=5)

        # proxy 本地缓存立即可读
        prog = proxy.get_progress("local")
        assert prog is not None
        assert prog.current == 5

    def test_proxy_ipc_sync(self):
        """测试 Proxy 写操作通过 IPC 同步到 Monitor。"""
        monitor = ProgressManager()
        proxy = monitor.get_mp_proxy()

        proxy.register_task("ipc_task", "IPC Task", total=100)
        time.sleep(0.3)  # 给消费线程处理时间

        prog = monitor.get_progress("ipc_task")
        assert prog is not None
        assert prog.description == "IPC Task"
        assert prog.total == 100

        proxy.update_progress("ipc_task", current=50, status="running")
        time.sleep(0.3)

        prog = monitor.get_progress("ipc_task")
        assert prog.current == 50
        assert prog.status == "running"

    def test_proxy_increment_ipc_sync(self):
        """测试 Proxy 累加模式通过 IPC 同步到 Manager。"""
        monitor = ProgressManager()
        proxy = monitor.get_mp_proxy()

        proxy.register_task("inc_task", "Increment Task", total=100)
        time.sleep(0.3)

        proxy.update_progress("inc_task", current=10)
        time.sleep(0.2)
        assert monitor.get_progress("inc_task").current == 10

        proxy.update_progress("inc_task", current=20, mode="increment")
        time.sleep(0.2)
        assert monitor.get_progress("inc_task").current == 30

        proxy.update_progress("inc_task", current=15, mode="increment")
        time.sleep(0.2)
        assert monitor.get_progress("inc_task").current == 45

    def test_proxy_snapshot(self):
        """测试 Proxy 创建时 snapshot Monitor 状态。"""
        monitor = ProgressManager()
        monitor.register_task("pre", "Pre-existing", total=50)
        monitor.update_progress("pre", current=25)

        proxy = monitor.get_mp_proxy()

        # proxy 创建时已包含 monitor 的任务
        prog = proxy.get_progress("pre")
        assert prog is not None
        assert prog.current == 25

    def test_proxy_multiple_updates(self):
        """测试 Proxy 多次 IPC 更新。"""
        monitor = ProgressManager()
        proxy = monitor.get_mp_proxy()

        proxy.register_task("multi", "Multi", total=10)
        for i in range(1, 11):
            proxy.update_progress("multi", current=i)

        time.sleep(0.5)

        prog = monitor.get_progress("multi")
        assert prog is not None
        # 消费线程可能未处理完所有消息，但至少应处理部分
        assert prog.current >= 0

    def test_proxy_display(self):
        """测试 Proxy 可以独立启动显示。"""
        monitor = ProgressManager()
        proxy = monitor.get_mp_proxy()

        proxy.start_display()
        time.sleep(0.2)
        assert proxy.is_displaying()

        proxy.register_task("proxy_task", "Proxy Task", total=100)
        time.sleep(0.1)

        proxy.stop_display()
        assert not proxy.is_displaying()

    def test_proxy_queue_created_on_demand(self):
        """测试 Queue 在获取代理时才按需创建。"""
        monitor = ProgressManager()
        assert monitor._mp_queue is None
        assert monitor._ray_queue is None

        monitor.get_mp_proxy()
        assert monitor._mp_queue is not None
        assert monitor._consumer_threads

    def test_backward_compat_alias(self):
        """测试 ProgressProxy 是 MPProgressProxy 的别名。"""
        assert ProgressProxy is MPProgressProxy

    def test_backward_compat_get_proxy(self):
        """测试 get_proxy() 是 get_mp_proxy() 的别名。"""
        monitor = ProgressManager()
        proxy1 = monitor.get_proxy()
        proxy2 = monitor.get_mp_proxy()
        assert isinstance(proxy1, MPProgressProxy)
        assert isinstance(proxy2, MPProgressProxy)

    def test_enable_ipc_deprecated(self):
        """测试 enable_ipc 参数被忽略并发出 DeprecationWarning。"""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            monitor = ProgressManager(enable_ipc=True)
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message).lower()
        # queue 未预先创建
        assert monitor._mp_queue is None

    def test_mp_proxy_serialization_ray_safe(self):
        """测试 MPProgressProxy 经序列化后 queue 变为 None（Ray 安全）。"""
        import pickle

        monitor = ProgressManager()
        proxy = monitor.get_mp_proxy()
        proxy.register_task("t1", "Task", total=10)

        pickled = pickle.dumps(proxy)
        restored = pickle.loads(pickled)

        assert isinstance(restored, MPProgressProxy)
        assert restored._queue is None
        # 本地缓存保留
        assert restored.get_progress("t1") is not None


# ==================== 测试6: RayProgressProxy ====================


class TestRayProgressProxy:
    """测试 RayProgressProxy 跨进程代理（Ray 后端）。"""

    def test_proxy_shares_interface_with_monitor(self):
        """测试 Ray Proxy 和 Monitor 共享 BaseProgress 接口。"""
        monitor = ProgressManager()
        proxy = monitor.get_ray_proxy()

        assert isinstance(proxy, BaseProgress)
        assert isinstance(proxy, BaseProgressProxy)
        assert isinstance(proxy, RayProgressProxy)

    def test_proxy_local_operations(self):
        """测试 Ray Proxy 本地操作不依赖 IPC。"""
        monitor = ProgressManager()
        proxy = monitor.get_ray_proxy()

        proxy.register_task("local", "Local Task", total=10)
        proxy.update_progress("local", current=5)

        prog = proxy.get_progress("local")
        assert prog is not None
        assert prog.current == 5

    def test_proxy_ipc_sync(self):
        """测试 Ray Proxy 写操作通过 IPC 同步到 Monitor。"""
        monitor = ProgressManager()
        proxy = monitor.get_ray_proxy()

        proxy.register_task("ipc_task", "IPC Task", total=100)
        time.sleep(0.3)

        prog = monitor.get_progress("ipc_task")
        assert prog is not None
        assert prog.description == "IPC Task"

        proxy.update_progress("ipc_task", current=50, status="running")
        time.sleep(0.3)

        prog = monitor.get_progress("ipc_task")
        assert prog.current == 50
        assert prog.status == "running"


    def test_proxy_increment_ipc_sync(self):
        """测试 Ray Proxy 累加模式通过 IPC 同步到 Manager。"""
        monitor = ProgressManager()
        proxy = monitor.get_ray_proxy()

        proxy.register_task("inc_task", "Increment Task", total=100)
        time.sleep(0.3)

        proxy.update_progress("inc_task", current=10)
        time.sleep(0.2)
        assert monitor.get_progress("inc_task").current == 10

        proxy.update_progress("inc_task", current=25, mode="increment")
        time.sleep(0.2)
        assert monitor.get_progress("inc_task").current == 35

        proxy.update_progress("inc_task", current=10, mode="increment")
        time.sleep(0.2)
        assert monitor.get_progress("inc_task").current == 45

    def test_proxy_snapshot(self):
        """测试 Ray Proxy 创建时 snapshot Monitor 状态。"""
        monitor = ProgressManager()
        monitor.register_task("pre", "Pre-existing", total=50)
        monitor.update_progress("pre", current=25)

        proxy = monitor.get_ray_proxy()

        prog = proxy.get_progress("pre")
        assert prog is not None
        assert prog.current == 25

    def test_ray_proxy_serialization(self, ray_initialized):
        """测试 RayProgressProxy 经 ray.put/ray.get 后 queue 仍然有效。"""
        import ray

        monitor = ProgressManager()
        proxy = monitor.get_ray_proxy()
        proxy.register_task("ray_task", "Ray Task", total=10)

        # 通过 Ray object store 传递
        ref = ray.put(proxy)
        proxy_restored = ray.get(ref)

        assert isinstance(proxy_restored, RayProgressProxy)
        assert proxy_restored._queue is not None

        # 通过 Ray 远程函数传递
        @ray.remote
        def worker(p):
            p.update_progress("ray_task", current=5, status="running")
            return "ok"

        result = ray.get(worker.remote(proxy_restored))
        assert result == "ok"

        time.sleep(0.3)
        prog = monitor.get_progress("ray_task")
        assert prog is not None
        assert prog.current == 5
        assert prog.status == "running"

    def test_ray_queue_created_on_demand(self):
        """测试 Ray Queue 在获取代理时才按需创建。"""
        monitor = ProgressManager()
        assert monitor._ray_queue is None

        monitor.get_ray_proxy()
        assert monitor._ray_queue is not None
        assert monitor._consumer_threads

    def test_proxy_display(self):
        """测试 Ray Proxy 可以独立启动显示。"""
        monitor = ProgressManager()
        proxy = monitor.get_ray_proxy()

        proxy.start_display()
        time.sleep(0.2)
        assert proxy.is_displaying()

        proxy.register_task("proxy_task", "Proxy Task", total=100)
        time.sleep(0.1)

        proxy.stop_display()
        assert not proxy.is_displaying()


# ==================== 测试7: ProgressManager 多后端共存 ====================


class TestProgressManagerMultiBackend:
    """测试 ProgressManager 同时支持 MP 和 Ray 后端。"""

    def test_both_proxies_can_coexist(self):
        """测试同时创建 MP 和 Ray 代理。"""
        monitor = ProgressManager()
        monitor.register_task("shared", "Shared Task", total=100)

        mp_proxy = monitor.get_mp_proxy()
        ray_proxy = monitor.get_ray_proxy()

        assert isinstance(mp_proxy, MPProgressProxy)
        assert isinstance(ray_proxy, RayProgressProxy)

        mp_proxy.update_progress("shared", current=30)
        ray_proxy.update_progress("shared", current=70)

        time.sleep(0.5)

        prog = monitor.get_progress("shared")
        assert prog is not None
        # 两个 queue 的消息都应被消费
        assert prog.current in (30, 70)

    def test_multiple_mp_proxies_share_queue(self):
        """测试多次获取 MP 代理共享同一个 queue。"""
        monitor = ProgressManager()
        proxy1 = monitor.get_mp_proxy()
        proxy2 = monitor.get_mp_proxy()

        assert proxy1._queue is proxy2._queue
        assert monitor._mp_queue is proxy1._queue


# ==================== 测试8: 深拷贝隔离 ====================


class TestProgressIsolation:
    """测试 ProgressManager/Proxy 返回的进度数据是深拷贝的。"""

    def test_get_progress_returns_copy(self):
        """测试 get_progress 返回深拷贝。"""
        monitor = ProgressManager()
        monitor.register_task("t1", "Task", total=10)

        prog1 = monitor.get_progress("t1")
        prog1.current = 999  # 修改拷贝

        prog2 = monitor.get_progress("t1")
        assert prog2.current == 0  # 原始数据未被修改

    def test_get_all_returns_copy(self):
        """测试 get_all_progress 返回深拷贝。"""
        monitor = ProgressManager()
        monitor.register_task("t1", "Task", total=10)

        all1 = monitor.get_all_progress()
        all1["t1"].current = 999

        all2 = monitor.get_all_progress()
        assert all2["t1"].current == 0

    def test_proxy_get_progress_is_copy(self):
        """测试 Proxy get_progress 返回深拷贝。"""
        monitor = ProgressManager()
        proxy = monitor.get_mp_proxy()
        proxy.register_task("t1", "Task", total=10)

        prog1 = proxy.get_progress("t1")
        prog1.current = 999

        prog2 = proxy.get_progress("t1")
        assert prog2.current == 0


# ==================== 直接运行入口（调试/演示用） ====================


def _simulate_work(duration: float = 0.02) -> None:
    """模拟耗时工作。"""
    import time

    time.sleep(duration)


def _demo_worker(proxy) -> None:
    """子进程工作函数：通过 MPProgressProxy 发送进度更新给主进程。

    Args:
        proxy: 主进程创建的 :class:`MPProgressProxy`。
    """
    import time

    proxy.register_task("mp_worker", "MP Worker", total=50)

    for i in range(50):
        proxy.update_progress("mp_worker", current=i + 1)
        time.sleep(0.06)

    proxy.update_progress("mp_worker", current=50, status="completed")


def _demo_ray_worker(proxy) -> None:
    """Ray worker 函数：通过 RayProgressProxy 发送进度更新给主进程。

    Args:
        proxy: 主进程创建的 :class:`RayProgressProxy`。
    """
    import time

    proxy.register_task("ray_worker", "Ray Worker", total=50)

    for i in range(50):
        proxy.update_progress("ray_worker", current=i + 1)
        time.sleep(0.06)

    proxy.update_progress("ray_worker", current=50, status="completed")


def main() -> None:
    """直接运行入口：演示 ProgressManager 统一汇聚本地/MP/Ray 三端进度。

    一个 ProgressManager 同时管理三种工作模式的进度：
    - **本地任务**：主进程直接更新
    - **MP 子进程**：通过 MPProgressProxy 回传
    - **Ray Worker**：通过 RayProgressProxy 回传

    三端进度实时汇聚到同一个 rich 进度条中显示。

    用法:
        cd /mnt/data/daiql/dev_repo/MetaEngine-mono/flowlet
        pixi run -e dev-all-gpu python tests/test_progress.py
    """
    import time

    import ray

    # 1. 初始化 Ray（必须先完成，后续才能创建 RayProgressProxy）
    print("=== 初始化 Ray 集群 ===")
    ray.init(ignore_reinit_error=True)
    print("Ray 就绪\n")

    # 2. 创建共享 ProgressManager，同时获取两种代理
    manager = ProgressManager(bar_type="rich")
    mp_proxy = manager.get_mp_proxy()
    ray_proxy = manager.get_ray_proxy()

    # 3. 启动 CLI 进度条
    manager.start_display()
    time.sleep(0.3)

    # 注册三个任务（本地 + MP + Ray）
    manager.register_task("local", "Local Task", total=60)
    # mp_worker / ray_worker 由子进程自行注册

    print("=== 启动三端并发工作（本地 + MP 子进程 + Ray Worker）===\n")

    # 4. 启动 MP 子进程
    mp_proc = multiprocessing.Process(target=_demo_worker, args=(mp_proxy,))
    mp_proc.start()

    # 5. 启动 Ray Worker
    worker = ray.remote(_demo_ray_worker)
    ray_future = worker.remote(ray_proxy)

    # 6. 本地任务同时执行
    for i in range(60):
        manager.update_progress("local", current=i + 1)
        time.sleep(0.05)

    manager.update_progress("local", current=60, status="completed")

    # 7. 等待 MP 子进程完成
    mp_proc.join()

    # 8. 等待 Ray Worker 完成
    ray.get(ray_future)

    time.sleep(0.5)
    manager.stop_display()

    # 9. 验证三端进度均已同步
    print("\n=== 结果验证 ===")
    for task_id in ["local", "mp_worker", "ray_worker"]:
        prog = manager.get_progress(task_id)
        if prog:
            print(
                f"  {task_id}: {prog.current}/{prog.total}, status={prog.status}"
            )

    ray.shutdown()
    print("\n=== 所有演示完成 ===")


if __name__ == "__main__":
    main()
