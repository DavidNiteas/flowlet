from pathlib import Path

import toml
from typing_extensions import Self

from .dict_config import DictConfigBehavior


class TomlConfigBehavior(DictConfigBehavior):
    def to_toml_string(self) -> str:
        """将配置转换为TOML字符串。

        Returns:
            TOML格式的字符串
        """
        return toml.dumps(self.model_dump())

    @classmethod
    def from_toml_string(cls, toml_string: str) -> Self:
        """从TOML字符串创建配置实例。

        Args:
            toml_string: TOML格式的字符串

        Returns:
            配置实例
        """
        return cls.from_dict(toml.loads(toml_string))

    def to_toml(self, path: Path) -> None:
        """将配置保存为TOML文件。

        Args:
            path: 文件路径
        """
        with open(path, "w", encoding="utf-8") as f:
            toml.dump(self.model_dump(), f)

    @classmethod
    def from_toml(cls, path: Path) -> Self:
        """从TOML文件加载配置。

        Args:
            path: 文件路径

        Returns:
            配置实例
        """
        return cls.from_dict(toml.load(path))
