from collections.abc import Mapping
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict
from typing_extensions import Self

from ...base.sentinel import Default
from ...base.type_annotation import get_class_from_annotation


class DictConfigBehavior(BaseModel):

    model_config: ClassVar[ConfigDict] = ConfigDict(**{"arbitrary_types_allowed": True})

    def update(self, *args: Any | Self | Default, **kwargs: Any | Self | Default) -> None:
        """更新配置字段。

        Args:
            *args: 可选的配置实例，如果传入与当前实例相同类型的实例，则从该实例复制所有字段值
            **kwargs: 要更新的字段名和值
        """
        for arg in args:
            if isinstance(arg, Default):
                continue
            if isinstance(arg, Mapping):
                for field_name in type(self).model_fields:
                    if field_name in arg:
                        setattr(self, field_name, arg[field_name])
                continue

            for field_name in type(self).model_fields:
                if hasattr(arg, field_name):
                    setattr(self, field_name, getattr(arg, field_name))

        config_kwargs = {k: v for k, v in kwargs.items() if isinstance(v, type(self))}
        param_dict = {k: v for k, v in kwargs.items() if not isinstance(v, Default)}

        for key in type(self).model_fields.keys():
            if key in config_kwargs:
                setattr(self, key, config_kwargs[key])
            elif key in param_dict:
                setattr(self, key, param_dict[key])

    def to_dict(self):
        """将配置转换为字典。

        Returns:
            配置的字典表示
        """
        return self.model_dump()

    @classmethod
    def from_dict(
        cls,
        params: dict,
    ) -> Self:
        """从字典创建配置实例。

        Args:
            params: 配置参数字典

        Returns:
            配置实例
        """
        input_params = {}
        for param_name, param_meta in cls.model_fields.items():
            if param_name in params:
                param_cls = get_class_from_annotation(param_meta.annotation)
                if isinstance(param_cls, cls):
                    input_params[param_name] = param_cls.from_dict(params[param_name])
                else:
                    input_params[param_name] = params[param_name]
        return cls(**input_params)

    @classmethod
    def from_other_config(cls, other: Any) -> Self:
        """从其他配置对象构建当前类型。"""
        new_instance = cls()
        new_instance.update(other)
        return new_instance
