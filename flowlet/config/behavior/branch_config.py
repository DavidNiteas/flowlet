from typing import ClassVar, Union

from pydantic import Field
from typing_extensions import Self

from ...base.type_annotation import get_class_from_annotation
from .dict_config import DictConfigBehavior


class BranchConfigBehavior(DictConfigBehavior):
    configs_type: ClassVar[dict[str, Union[DictConfigBehavior, "BranchConfigBehavior"]]] = {}

    method_name: str
    configs: dict[str, Union[DictConfigBehavior, "BranchConfigBehavior"]] = Field(
        default={},
        description="分支配置项",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for config_name, config_type in self.configs_type.items():
            if config_name not in self.configs:
                self.configs[config_name] = config_type()
            if not isinstance(self.configs[config_name], config_type):
                if isinstance(self.configs[config_name], dict):
                    self.configs[config_name] = config_type.from_dict(self.configs[config_name])
                else:
                    self.configs[config_name] = config_type()

    @classmethod
    def from_dict(cls, params: dict) -> Self:
        input_params = {}
        if "method_name" in params:
            input_params["method_name"] = params["method_name"]

        configs_params = params.get("configs", {})
        configs = {}
        for config_name, config_type in cls.configs_type.items():
            if config_name in configs_params:
                configs[config_name] = config_type.from_dict(configs_params[config_name])
            else:
                configs[config_name] = config_type()
        input_params["configs"] = configs

        for meta_name, meta_info in cls.model_fields.items():
            if meta_name not in input_params and meta_name != "configs":
                if meta_name in params:
                    param_cls = get_class_from_annotation(meta_info.annotation)
                    if isinstance(param_cls, cls):
                        input_params[meta_name] = param_cls.from_dict(params[meta_name])
                    else:
                        input_params[meta_name] = params[meta_name]
        return cls(**input_params)

    @property
    def config(self) -> Union[DictConfigBehavior, "BranchConfigBehavior"]:
        return self.configs[self.method_name]

    @config.setter
    def config(self, value: Union[DictConfigBehavior, "BranchConfigBehavior"]):
        self.configs[self.method_name] = value

    def __getitem__(self, key: str):
        if key == "method_name":
            return self.method_name
        elif key == "config":
            return self.config
        else:
            return self.configs[key]

    def __setitem__(self, key: str, value: Union[DictConfigBehavior, "BranchConfigBehavior", str]):
        if key == "method_name":
            self.method_name = value
        elif key == "config":
            self.configs[self.method_name] = value
        else:
            self.configs[key] = value

    def to_dict(self):
        """将配置转换为字典。

        Returns:
            配置的字典表示
        """
        # 直接构建字典，确保所有字段都被正确序列化
        result = {}
        # 添加所有非configs字段
        for field_name in type(self).model_fields:
            if field_name != "configs":
                value = getattr(self, field_name)
                # 确保嵌套的配置对象也被序列化
                if hasattr(value, "to_dict"):
                    result[field_name] = value.to_dict()
                else:
                    result[field_name] = value
        # 确保configs中的每个配置对象都被正确序列化
        configs_dict = {}
        for config_name, config_obj in self.configs.items():
            if hasattr(config_obj, "to_dict"):
                configs_dict[config_name] = config_obj.to_dict()
            else:
                configs_dict[config_name] = config_obj
        result["configs"] = configs_dict
        return result

    def model_dump(self, **kwargs):
        """重写model_dump方法，确保使用我们的to_dict方法。

        Returns:
            配置的字典表示
        """
        return self.to_dict()
