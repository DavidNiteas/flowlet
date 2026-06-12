"""Edge 模式配置。"""

from __future__ import annotations

from pydantic import Field

from ..config.base_config import BaseConfig


class EdgeConfig(BaseConfig):
    """EdgeNode 统一配置。

    Thread 后端与 Ray 后端共享通用字段；带 ``ray_`` 前缀的字段仅在 Ray 后端生效。
    """

    # 通用配置
    max_concurrent_tasks: int = Field(
        default=4,
        description="批量 IO 时本地线程池的最大工作线程数",
        ge=1,
    )

    parallel_threshold: int = Field(
        default=4,
        description="批量操作低于此阈值时直接串行执行，避免线程池开销",
        ge=1,
    )

    use_concurrent_io: bool = Field(
        default=True,
        description="push_many / pull_many 是否使用本地线程并发提交",
    )

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

    # Ray 专用：Actor 资源
    num_cpus: float = Field(
        default=1.0,
        description="Ray Actor 占用的 CPU 数量",
        ge=0.0,
    )

    num_gpus: float = Field(
        default=0.0,
        description="Ray Actor 占用的 GPU 数量",
        ge=0.0,
    )

    memory: int | None = Field(
        default=None,
        description="Ray Actor 内存限制（字节）",
        ge=0,
    )

    max_restarts: int = Field(
        default=0,
        description="Ray Actor 最大重启次数；Edge 模式默认不重试",
        ge=0,
    )

    max_task_retries: int = Field(
        default=0,
        description="Ray Actor 内任务最大重试次数；Edge 模式默认不重试",
        ge=0,
    )

    # Ray 专用：全局初始化
    ray_include_dashboard: bool = Field(
        default=False,
        description="是否启用 Ray 仪表板",
    )

    ray_log_to_driver: bool = Field(
        default=True,
        description="是否将 Ray worker 日志发送到 driver",
    )

    ray_dashboard_port: int | None = Field(
        default=None,
        description="Ray 仪表板端口",
        ge=1024,
        le=65535,
    )

    ray_runtime_env: dict | None = Field(
        default=None,
        description="Ray 运行时环境配置",
    )
