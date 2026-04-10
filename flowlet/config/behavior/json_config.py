import json
from pathlib import Path

from typing_extensions import Self

from .dict_config import DictConfigBehavior


class JsonConfigBehavior(DictConfigBehavior):

    def to_json_string(self) -> str:
        """将配置转换为JSON字符串。

        Returns:
            JSON格式的字符串
        """
        return json.dumps(self.model_dump(), ensure_ascii=False, indent=2)

    @classmethod
    def from_json_string(cls, json_string: str) -> Self:
        """从JSON字符串创建配置实例。

        Args:
            json_string: JSON格式的字符串

        Returns:
            配置实例
        """
        return cls.from_dict(json.loads(json_string))

    def to_json(self, path: Path) -> None:
        """将配置保存为JSON文件。

        Args:
            path: 文件路径
        """
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.model_dump(), f, ensure_ascii=False, indent=2)

    @classmethod
    def from_json(cls, path: Path) -> Self:
        """从JSON文件加载配置。

        Args:
            path: 文件路径

        Returns:
            配置实例
        """
        with open(path, encoding="utf-8") as f:
            return cls.from_dict(json.load(f))
