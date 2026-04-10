from __future__ import annotations

from collections.abc import Iterable
from typing import Any, ClassVar, Generic, TypeVar

from typing_extensions import Self

from .base_config import BaseConfig

ConfigT = TypeVar("ConfigT", bound=BaseConfig)
KeyT = TypeVar("KeyT")


class ConfigSequence(list[ConfigT], Generic[ConfigT]):
    """BaseConfig 序列容器，负责批量包装 Config。"""

    item_type: ClassVar[type[BaseConfig] | None] = None

    @classmethod
    def _build_item(cls, other: Any) -> ConfigT:
        item_type = cls.item_type
        if item_type is None:
            raise TypeError(f"{cls.__name__} must define item_type before calling from_other_sequence")
        if isinstance(other, item_type):
            return other

        from_other_context = getattr(item_type, "from_other_context", None)
        if callable(from_other_context):
            return from_other_context(other)

        from_other_config = getattr(item_type, "from_other_config", None)
        if callable(from_other_config):
            return from_other_config(other)

        raise TypeError(f"{item_type.__name__} does not support building from {type(other).__name__}")

    @classmethod
    def from_other_sequence(
        cls,
        sequence: Iterable[Any],
    ) -> Self:
        result = cls()
        for item in sequence:
            result.append(cls._build_item(item))
        return result

    @classmethod
    def from_map(
        cls,
        config_map: dict[Any, ConfigT],
    ) -> Self:
        return cls.from_other_sequence(config_map.values())


class ConfigMap(dict[KeyT, ConfigT], Generic[KeyT, ConfigT]):
    """BaseConfig 映射容器，只负责 key/value 组织。"""

    @classmethod
    def get_key(cls, config: ConfigT) -> KeyT:
        raise NotImplementedError

    def add(self, config: ConfigT) -> None:
        self[type(self).get_key(config)] = config

    @classmethod
    def from_sequence(
        cls,
        sequence: Iterable[ConfigT],
    ) -> Self:
        result = cls()
        for config in sequence:
            result.add(config)
        return result
