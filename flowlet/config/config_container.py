from __future__ import annotations

from typing import Any

from ..base.sentinel import Default
from .base_config import BaseConfig


class BaseConfigContainer(BaseConfig):

    def update(
        self,
        *args: Any | BaseConfig | BaseConfigContainer | Default,
        **kwargs: Any | BaseConfig | BaseConfigContainer | Default
    ) -> None:
        """更新配置字段。

        Args:
            *args: 可选的配置实例，如果传入与当前实例相同类型的实例，则从该实例复制所有字段值
            **kwargs: 要更新的字段名和值，参数池中的 BaseConfig 子类会被透传到下级 Config
        """
        for arg in args:
            if isinstance(arg, type(self)):
                for field_name in type(self).model_fields:
                    if hasattr(arg, field_name):
                        setattr(self, field_name, getattr(arg, field_name))
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
