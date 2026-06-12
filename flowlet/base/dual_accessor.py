"""Dual accessor descriptors.

Provides :class:`dual_property` and :class:`dual_method`, which support both
instance and class access patterns via the descriptor protocol.

Originally from ``MassDataModule.data_module.base_module.nested_tree_module.tools``,
ported to ``flowlet`` for broader reuse (e.g. :meth:`ExecutableUnit.run`).
"""

from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import Any, Generic, TypeVar, overload

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema

R = TypeVar("R")


class dual_property(Generic[R]):
    """Descriptor that supports both instance and class access.

    Usage::

        @dual_property
        def foo(self) -> R: ...          # instance mode

        @foo.classmode
        def foo(cls) -> R: ...           # class mode
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        return core_schema.is_instance_schema(cls)

    def __init__(self, fget: Callable[..., R]) -> None:
        self.fget = fget
        self.fget_cls: Callable[..., R] | None = None
        self._name: str = getattr(fget, "__name__", repr(fget))

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name

    def classmode(self, fget_cls: Callable[..., R]) -> dual_property[R]:
        """Register a class-mode getter; returns *self* for chaining."""
        self.fget_cls = fget_cls
        return self

    @property
    def __isabstractmethod__(self) -> bool:
        return bool(
            getattr(self.fget, "__isabstractmethod__", False) or getattr(self.fget_cls, "__isabstractmethod__", False)
        )

    @overload
    def __get__(self, obj: None, objtype: type) -> R: ...
    @overload
    def __get__(self, obj: object, objtype: type) -> R: ...
    def __get__(self, obj: Any, objtype: type | None = None) -> R:
        if obj is None:
            if self.fget_cls is None:
                raise AttributeError(
                    f"dual_property '{self._name}' has no class-mode getter. "
                    f"Did you forget to decorate with @{self._name}.classmode?"
                )
            return self.fget_cls(objtype)
        return self.fget(obj)


class dual_method(Generic[R]):
    """Descriptor that supports both instance and class calls.

    Usage::

        @dual_method
        def bar(self, x: int) -> R: ...          # instance mode

        @bar.classmode
        def bar(cls, x: int) -> R: ...           # class mode (optional)
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        return core_schema.is_instance_schema(cls)

    def __init__(self, instance_func: Callable[..., R]) -> None:
        self.instance_func = instance_func
        self.class_func: Callable[..., R] | None = None
        self._name: str = getattr(instance_func, "__name__", repr(instance_func))

    def __set_name__(self, owner: type, name: str) -> None:
        self._name = name

    def classmode(self, class_func: Callable[..., R]) -> dual_method[R]:
        """Register a class-mode function; returns *self* for chaining."""
        self.class_func = class_func
        return self

    @property
    def __isabstractmethod__(self) -> bool:
        return bool(
            getattr(self.instance_func, "__isabstractmethod__", False)
            or getattr(self.class_func, "__isabstractmethod__", False)
        )

    @overload
    def __get__(self, obj: None, objtype: type) -> Callable[..., R]: ...
    @overload
    def __get__(self, obj: object, objtype: type) -> Callable[..., R]: ...
    def __get__(self, obj: Any, objtype: type | None = None) -> Callable[..., R]:
        if obj is None:
            if self.class_func is None:
                # No classmode provided – return the descriptor itself to keep
                # the original behaviour (e.g. introspection).
                return self  # type: ignore[return-value]
            return partial(self.class_func, objtype)
        return partial(self.instance_func, obj)
