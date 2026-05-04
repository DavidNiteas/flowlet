from __future__ import annotations

from pydantic import Field

from ..config.base_config import BaseConfig


class ParallelConfig(BaseConfig):
    """并行工作流统一配置类。

    提供统一的并行抽象配置，适用于Ray和线程池两种实现。
    注意：带有"ray_"前缀的参数仅对Ray工作流有效，线程池会自动忽略这些参数。
    """

    # 通用配置
    max_concurrent_tasks: int = Field(
        default=4,
        description="最大并发任务数（线程池直接使用，Ray通过资源限制间接控制）",
        ge=1,
    )

    parallel_threshold: int = Field(
        default=4,
        description="任务数低于此阈值时不启用线程池，直接串行执行以避免线程池开销",
        ge=1,
    )

    # 进度显示配置
    show_progress: bool = Field(
        default=False,
        description="是否显示进度条",
    )

    progress_description: str = Field(
        default="Processing tasks",
        description="进度条描述文本",
    )

    progress_bar_type: str = Field(
        default="rich",
        description="进度条类型，支持: rich, tqdm, jupyter",
        pattern="^(rich|tqdm|jupyter)$",
    )

    # 并发配置
    use_concurrent_io: bool = Field(
        default=True,
        description="在批量操作中是否使用线程并行进行并发IO",
    )

    # Ray特有：每个任务的资源需求
    per_task_num_cpus: float = Field(
        default=1.0,
        description="每个任务需要的CPU核心数（仅Ray工作流有效）",
        ge=0.0,
    )

    per_task_num_gpus: float = Field(
        default=0.0,
        description="每个任务需要的GPU数量（仅Ray工作流有效）",
        ge=0.0,
    )

    per_task_memory: int | None = Field(
        default=None,
        description="每个任务的内存限制（字节，仅Ray工作流有效）",
        ge=0,
    )

    # Ray特有：全局配置
    ray_runtime_env: dict | None = Field(
        default=None,
        description="Ray运行时环境配置（仅Ray工作流有效）",
    )

    ray_dashboard_port: int | None = Field(
        default=None,
        description="Ray仪表板端口（仅Ray工作流有效）",
        ge=1024,
        le=65535,
    )

    ray_include_dashboard: bool = Field(
        default=False,
        description="是否启用Ray仪表板（仅Ray工作流有效）",
    )

    ray_log_to_driver: bool = Field(
        default=True,
        description="是否将日志发送到驱动程序（仅Ray工作流有效）",
    )
