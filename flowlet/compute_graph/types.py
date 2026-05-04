"""计算图核心数据类型定义。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class InputSlot:
    """输入槽定义。"""

    name: str
    required: bool = True
    default: Any = None


@dataclass(frozen=True)
class OutputSpec:
    """输出规格。"""

    type: Literal["single", "sequence", "mapping", "tuple"]


@dataclass(frozen=True)
class InputField:
    """ExecutableUnit 的输入字段元数据。

    用于在类定义时标注输入参数，使 as_task() 能自动构建 InputSlot。
    """

    name: str
    required: bool = True
    default: Any = None
    description: str = ""


@dataclass(frozen=True)
class OutputField:
    """ExecutableUnit 的输出字段元数据。

    用于在类定义时标注输出规格，使 as_task() 能自动构建 OutputSpec。
    """

    type: Literal["single", "sequence", "mapping", "tuple"]
    description: str = ""


class InputVar:
    """命名输入变量。显式声明需要外部提供的输入。"""

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"InputVar({self.name!r})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, InputVar) and self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)
