"""flowlet Edge 模式：有状态、命令式、异步执行的节点抽象。

提供 Thread 与 Ray 两种后端，主进程始终面对 ``EdgeNode``。
"""

from __future__ import annotations

from .backend import EdgeBackend
from .config import EdgeConfig
from .errors import BackendError, DeadNodeError, EdgeError, UnsupportedTargetError
from .node import EdgeNode, RayEdgeNode, ThreadEdgeNode
from .ray_backend import RayEdgeBackend
from .thread_backend import ThreadEdgeBackend

__all__ = [
    "EdgeBackend",
    "EdgeConfig",
    "EdgeError",
    "DeadNodeError",
    "BackendError",
    "UnsupportedTargetError",
    "EdgeNode",
    "ThreadEdgeNode",
    "RayEdgeNode",
    "ThreadEdgeBackend",
    "RayEdgeBackend",
]
