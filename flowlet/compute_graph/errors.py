"""计算图异常类。"""


class ComputeGraphError(Exception):
    """计算图基类异常。"""


class UnknownSlotError(ComputeGraphError):
    """绑定时引用了不存在的输入槽。"""


class DuplicateNameError(ComputeGraphError):
    """命名输入变量存在冲突（同名但不同对象）。"""


class CyclicDependencyError(ComputeGraphError):
    """bind 时检测到会形成环路。"""


class UnboundInputError(ComputeGraphError):
    """execute 时存在未绑定或未提供的输入。"""
