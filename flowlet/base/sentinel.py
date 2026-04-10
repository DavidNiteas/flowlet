from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema


class BaseHolder:
    """语义占位符基类。"""

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        return core_schema.is_instance_schema(cls)


class Default(BaseHolder):
    """参数边界哨兵，表示调用方未显式提供值。"""


class Placeholder(BaseHolder):
    """过渡态占位符，表示此处应有值但尚未构建完成。"""


class Emptyholder(BaseHolder):
    """合法空值占位符，表示此处允许为空且当前为空。"""


class Voidholder(BaseHolder):
    """虚空占位符，表示此处必须为空。"""
