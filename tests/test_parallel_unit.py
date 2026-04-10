import os
import time
from concurrent.futures import Future

import pytest
import ray

# 设置环境变量以禁用Ray的警告
os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"

from flowlet.parallel_unit import (
    ParallelConfig,
    RayParallelWorkflow,
    RayPoolCreatorWorkflow,
    ThreadParallelWorkflow,
)


def _ensure_ray_available() -> None:
    """当前环境不允许初始化 Ray 时跳过相关测试。"""
    try:
        ray.init(local_mode=True, include_dashboard=False, log_to_driver=False)
        ray.shutdown()
    except Exception as exc:
        pytest.skip(f"Ray is unavailable in current environment: {exc}")


def _wait_until_ready(workflow, future, timeout: float = 10.0) -> None:
    """等待 Ray 任务完成，避免固定 sleep 带来的脆弱性。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if workflow.is_ready(future):
            return
        time.sleep(0.1)
    pytest.fail(f"Ray future did not become ready within {timeout} seconds")


def _wait_until_ready(workflow, future, timeout: float = 10.0) -> None:
    """等待 Ray 任务进入完成态，避免固定 sleep 带来的脆弱性。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if workflow.is_ready(future):
            return
        time.sleep(0.1)
    pytest.fail(f"Ray future did not become ready within {timeout} seconds")

# ==================== 测试1: ParallelConfig ====================


class TestParallelConfig:
    """测试并行工作流配置类。"""

    def test_default_initialization(self):
        """测试默认初始化。"""
        config = ParallelConfig()
        assert config.max_concurrent_tasks == 4
        assert config.show_progress is False
        assert config.progress_description == "Processing tasks"
        assert config.progress_bar_type == "rich"
        assert config.use_concurrent_io is True
        assert config.per_task_num_cpus == 1.0
        assert config.per_task_num_gpus == 0.0
        assert config.per_task_memory is None
        assert config.ray_runtime_env is None
        assert config.ray_dashboard_port is None
        assert config.ray_include_dashboard is False
        assert config.ray_log_to_driver is True

    def test_custom_initialization(self):
        """测试自定义初始化。"""
        config = ParallelConfig(
            max_concurrent_tasks=8,
            show_progress=True,
            progress_description="Testing tasks",
            progress_bar_type="tqdm",
            use_concurrent_io=False,
            per_task_num_cpus=2.0,
            per_task_num_gpus=0.5,
            per_task_memory=1024 * 1024 * 1024,  # 1GB
            ray_runtime_env={"pip": ["numpy"]},
            ray_dashboard_port=8265,
            ray_include_dashboard=True,
            ray_log_to_driver=False
        )
        assert config.max_concurrent_tasks == 8
        assert config.show_progress is True
        assert config.progress_description == "Testing tasks"
        assert config.progress_bar_type == "tqdm"
        assert config.use_concurrent_io is False
        assert config.per_task_num_cpus == 2.0
        assert config.per_task_num_gpus == 0.5
        assert config.per_task_memory == 1024 * 1024 * 1024
        assert config.ray_runtime_env == {"pip": ["numpy"]}
        assert config.ray_dashboard_port == 8265
        assert config.ray_include_dashboard is True
        assert config.ray_log_to_driver is False

    def test_config_validation(self):
        """测试配置验证。"""
        # 测试max_concurrent_tasks必须大于等于1
        with pytest.raises(Exception):
            ParallelConfig(max_concurrent_tasks=0)

        # 测试per_task_num_cpus必须大于等于0
        with pytest.raises(Exception):
            ParallelConfig(per_task_num_cpus=-1.0)

        # 测试per_task_num_gpus必须大于等于0
        with pytest.raises(Exception):
            ParallelConfig(per_task_num_gpus=-0.5)

        # 测试ray_dashboard_port必须在有效范围内
        with pytest.raises(Exception):
            ParallelConfig(ray_dashboard_port=1023)  # 低于最小值
        with pytest.raises(Exception):
            ParallelConfig(ray_dashboard_port=65536)  # 高于最大值

    def test_config_serialization(self):
        """测试配置序列化和反序列化。"""
        config = ParallelConfig(
            max_concurrent_tasks=8,
            show_progress=True,
            per_task_num_cpus=2.0
        )

        # 序列化为字典
        config_dict = config.to_dict()
        assert config_dict["max_concurrent_tasks"] == 8
        assert config_dict["show_progress"] is True
        assert config_dict["per_task_num_cpus"] == 2.0

        # 从字典创建
        config2 = ParallelConfig.from_dict(config_dict)
        assert config2.max_concurrent_tasks == 8
        assert config2.show_progress is True
        assert config2.per_task_num_cpus == 2.0


# ==================== 测试2: ThreadParallelWorkflow ====================


class TestThreadParallelWorkflow:
    """测试基于线程池的并行工作流。"""

    def test_submit_and_fetch(self):
        """测试提交和获取单个任务。"""
        workflow = ThreadParallelWorkflow()

        # 定义测试函数
        def test_func(x):
            time.sleep(0.1)
            return x * 2

        # 提交任务
        future = workflow.submit(test_func, 42)
        assert isinstance(future, Future)

        # 检查任务是否完成（应该未完成）
        assert not workflow.is_ready(future)

        _wait_until_ready(workflow, future)

        # 获取结果
        result = workflow.fetch(future)
        assert result == 84

        # 关闭工作流
        workflow.shutdown()

    def test_batch_operations(self):
        """测试批量操作。"""
        workflow = ThreadParallelWorkflow()

        # 定义测试函数
        def test_func(x):
            time.sleep(0.1)
            return x * 2

        # 测试submit_many和gather
        inputs = [1, 2, 3, 4, 5]
        futures = workflow.submit_many(test_func, inputs)
        assert len(futures) == 5
        assert all(isinstance(f, Future) for f in futures)

        results = workflow.gather(futures)
        assert results == [2, 4, 6, 8, 10]

        # 测试map
        results2 = workflow.map(test_func, inputs)
        assert results2 == [2, 4, 6, 8, 10]

        # 关闭工作流
        workflow.shutdown()

    def test_context_manager(self):
        """测试上下文管理器接口。"""
        def test_func(x):
            return x * 2

        with ThreadParallelWorkflow() as workflow:
            future = workflow.submit(test_func, 42)
            result = workflow.fetch(future)
            assert result == 84

        # 上下文管理器退出后，工作流应该已关闭
        with pytest.raises(RuntimeError):
            workflow.submit(test_func, 42)

    def test_error_handling(self):
        """测试错误处理。"""
        workflow = ThreadParallelWorkflow()

        def test_func(x):
            return x * 2

        # 提交任务
        future = workflow.submit(test_func, 42)
        assert workflow.fetch(future) == 84

        # 关闭工作流
        workflow.shutdown()

        # 尝试在关闭后提交任务
        with pytest.raises(RuntimeError, match="Thread pool has been shutdown"):
            workflow.submit(test_func, 42)


# ==================== 测试3: RayPoolCreatorWorkflow ====================


class TestRayPoolCreatorWorkflow:
    """测试Ray集群初始化工作流。"""

    def setup_method(self):
        """测试前清理Ray环境。"""
        if ray.is_initialized():
            ray.shutdown()

    def teardown_method(self):
        """测试后清理Ray环境。"""
        if ray.is_initialized():
            ray.shutdown()

    def test_ray_initialization(self):
        """测试Ray集群初始化。"""
        _ensure_ray_available()
        # 确保Ray未初始化
        assert not ray.is_initialized()

        # 创建并执行RayPoolCreatorWorkflow
        workflow = RayPoolCreatorWorkflow()
        result = workflow.bind_input().execute()

        # 验证初始化成功
        assert result is True
        assert ray.is_initialized()

    def test_duplicate_initialization(self):
        """测试重复初始化的行为。"""
        _ensure_ray_available()
        # 第一次初始化
        workflow1 = RayPoolCreatorWorkflow()
        result1 = workflow1.bind_input().execute()
        assert result1 is True
        assert ray.is_initialized()

        # 第二次初始化（应该返回False）
        workflow2 = RayPoolCreatorWorkflow()
        result2 = workflow2.bind_input().execute()
        assert result2 is False
        assert ray.is_initialized()  # Ray应该仍然是初始化状态


# ==================== 测试4: RayParallelWorkflow ====================


class TestRayParallelWorkflow:
    """测试基于Ray的并行工作流。"""

    def setup_method(self):
        """测试前清理Ray环境。"""
        if ray.is_initialized():
            ray.shutdown()

    def teardown_method(self):
        """测试后清理Ray环境。"""
        if ray.is_initialized():
            ray.shutdown()

    def test_submit_and_fetch(self):
        """测试提交和获取单个任务。"""
        _ensure_ray_available()
        workflow = RayParallelWorkflow()

        # 定义测试函数
        def test_func(x):
            time.sleep(0.1)
            return x * 2

        # 提交任务
        future = workflow.submit(test_func, 42)
        assert isinstance(future, ray.ObjectRef)

        # 获取结果
        result = workflow.fetch(future)
        assert result == 84

    def test_batch_operations(self):
        """测试批量操作。"""
        _ensure_ray_available()
        workflow = RayParallelWorkflow()

        # 定义测试函数
        def test_func(x):
            time.sleep(0.1)
            return x * 2

        # 测试submit_many和gather
        inputs = [1, 2, 3, 4, 5]
        futures = workflow.submit_many(test_func, inputs)
        assert len(futures) == 5
        assert all(isinstance(f, ray.ObjectRef) for f in futures)

        results = workflow.gather(futures)
        assert results == [2, 4, 6, 8, 10]

        # 测试map
        results2 = workflow.map(test_func, inputs)
        assert results2 == [2, 4, 6, 8, 10]

    def test_resource_configuration(self):
        """测试资源配置。"""
        _ensure_ray_available()
        # 创建自定义配置
        config = ParallelConfig(
            per_task_num_cpus=1.0,
            per_task_num_gpus=0.0,  # 不使用GPU
            per_task_memory=100 * 1024 * 1024  # 100MB
        )

        workflow = RayParallelWorkflow(config)

        def test_func(x):
            return x * 2

        # 提交任务（应该成功）
        future = workflow.submit(test_func, 42)
        result = workflow.fetch(future)
        assert result == 84
