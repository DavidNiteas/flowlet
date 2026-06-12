"""Edge 模式异常定义。"""

from __future__ import annotations


class EdgeError(Exception):
    """Edge 模式根异常。"""


class DeadNodeError(EdgeError):
    """对已经 close/kill 的 EdgeNode 发起操作。"""


class BackendError(EdgeError):
    """后端执行过程中发生的错误。"""


class UnsupportedTargetError(EdgeError):
    """apply / bind 接收了不支持的 target 类型。"""
