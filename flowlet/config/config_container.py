from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..base.sentinel import Default
from .base_config import BaseConfig


class BaseConfigContainer(BaseConfig):
    def update(
        self,
        *args: Any | BaseConfig | BaseConfigContainer | Default,
        **kwargs: Any | BaseConfig | BaseConfigContainer | Default,
    ) -> None:
        """更新配置字段。

        Args:
            *args: 可选的配置实例。支持以下类型：
                - 与当前实例完全同类型的实例：复制所有字段值
                - BaseConfig 子类/父类实例：仅复制同名字段值
                - Mapping（如 dict）：按 key 匹配复制
            **kwargs: 要更新的字段名和值，参数池中的 BaseConfig 子类会被透传到下级 Config
        """
        for arg in args:
            if isinstance(arg, Default):
                continue

            if isinstance(arg, type(self)):
                # 完全同类型：复制所有字段
                for field_name in type(self).model_fields:
                    if hasattr(arg, field_name):
                        setattr(self, field_name, getattr(arg, field_name))
                return

            if isinstance(arg, BaseConfig):
                # BaseConfig 子类/父类：复制同名字段
                for field_name in type(self).model_fields:
                    if field_name in type(arg).model_fields and hasattr(arg, field_name):
                        setattr(self, field_name, getattr(arg, field_name))
                return

            if isinstance(arg, Mapping):
                # Mapping：按 key 复制
                for field_name in type(self).model_fields:
                    if field_name in arg:
                        setattr(self, field_name, arg[field_name])
                return

        config_kwargs = {k: v for k, v in kwargs.items() if isinstance(v, BaseConfig)}
        param_dict = {k: v for k, v in kwargs.items() if not isinstance(v, Default | BaseConfig)}

        for key in type(self).model_fields.keys():
            config = getattr(self, key)
            if isinstance(config, BaseConfig):
                if key in config_kwargs:
                    setattr(self, key, config_kwargs[key])
                elif isinstance(config, BaseConfigContainer):
                    config.update(*args, **kwargs)
                elif param_dict:
                    config.update(**param_dict)
            elif key in param_dict:
                setattr(self, key, param_dict[key])
