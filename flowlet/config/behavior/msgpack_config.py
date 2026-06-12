from pathlib import Path

import msgpack
from typing_extensions import Self

from .dict_config import DictConfigBehavior


class MsgpackConfigBehavior(DictConfigBehavior):
    def to_msgpack_bytes(self) -> bytes:
        """将配置转换为MsgPack字节。

        Returns:
            MsgPack格式的字节
        """
        return msgpack.packb(self.model_dump())

    @classmethod
    def from_msgpack_bytes(cls, msgpack_bytes: bytes) -> Self:
        """从MsgPack字节创建配置实例。

        Args:
            msgpack_bytes: MsgPack格式的字节

        Returns:
            配置实例
        """
        return cls.from_dict(msgpack.unpackb(msgpack_bytes))

    def to_msgpack(self, path: Path) -> None:
        """将配置保存为MsgPack文件。

        Args:
            path: 文件路径
        """
        with open(path, "wb") as f:
            msgpack.pack(self.model_dump(), f)

    @classmethod
    def from_msgpack(cls, path: Path) -> Self:
        """从MsgPack文件加载配置。

        Args:
            path: 文件路径

        Returns:
            配置实例
        """
        with open(path, "rb") as f:
            return cls.from_dict(msgpack.unpack(f))
