from collections.abc import Mapping, Sequence, Set
from typing import Any, Literal, TypeVar, overload

from pydantic.fields import FieldInfo

QryConfigType = TypeVar('ConfigType')
DefConfigType = TypeVar('DefConfigType')

@overload
def extract_config_from_container(
    config_container: Sequence[Any | QryConfigType] | Set[Any | QryConfigType],
    config_type: type[QryConfigType],
    raise_err: Literal[True] = True,
    default_config: DefConfigType = None,
) -> QryConfigType:
    ...

@overload
def extract_config_from_container(
    config_container: Sequence[Any | QryConfigType] | Set[Any | QryConfigType],
    config_type: type[QryConfigType],
    raise_err: Literal[False] = False,
    default_config: DefConfigType = None,
) -> QryConfigType | DefConfigType:
    ...

def extract_config_from_container(
    config_container: Sequence[Any | QryConfigType] | Set[Any | QryConfigType],
    config_type: type[QryConfigType],
    raise_err: bool = True,
    default_config: DefConfigType = None,
) -> QryConfigType | DefConfigType:
    for item in config_container:
        if isinstance(item, config_type):
            return item
    if raise_err:
        raise ValueError(f"No config of type {config_type.__name__} found in container")
    return default_config

@overload
def extract_config_from_mapping(
    config_mapping: Mapping[str, Any | QryConfigType],
    config_type: type[QryConfigType],
    key: str | None = None,
    raise_err: Literal[True] = True,
    default_config: DefConfigType | None = None,
) -> QryConfigType:
    ...

@overload
def extract_config_from_mapping(
    config_mapping: Mapping[str, Any | QryConfigType],
    config_type: type[QryConfigType],
    key: str | None = None,
    raise_err: Literal[False] = False,
    default_config: DefConfigType = None,
) -> QryConfigType | DefConfigType:
    ...

def extract_config_from_mapping(
    config_mapping: Mapping[str, Any | QryConfigType],
    config_type: type[QryConfigType],
    key: str | None = None,
    raise_err: bool = True,
    default_config: DefConfigType = None,
) -> QryConfigType | DefConfigType:
    if key is not None:
        if key in config_mapping:
            item = config_mapping[key]
            if isinstance(item, config_type):
                return item
            if raise_err:
                raise ValueError(f"Item at key '{key}' is not of type {config_type.__name__}")
            return default_config
        if raise_err:
            raise ValueError(f"Key '{key}' not found in mapping")
        return default_config

    for item in config_mapping.values():
        if isinstance(item, config_type):
            return item
    if raise_err:
        raise ValueError(f"No config of type {config_type.__name__} found in mapping")
    return default_config

@overload
def extract_config_from_instance(
    obj: Any,
    config_type: type[QryConfigType],
    key: str | None = None,
    raise_err: Literal[True] = True,
    default_config: DefConfigType = None,
) -> QryConfigType:
    ...

@overload
def extract_config_from_instance(
    obj: Any,
    config_type: type[QryConfigType],
    key: str | None = None,
    raise_err: Literal[False] = False,
    default_config: DefConfigType = None,
) -> QryConfigType | DefConfigType:
    ...

def extract_config_from_instance(
    obj: Any,
    config_type: type[QryConfigType],
    key: str | None = None,
    raise_err: bool = True,
    default_config: DefConfigType = None,
) -> QryConfigType | DefConfigType:
    if key is not None:
        if hasattr(obj, key):
            item = getattr(obj, key)
            if isinstance(item, config_type):
                return item
            if raise_err:
                raise ValueError(f"Attribute '{key}' is not of type {config_type.__name__}")
            return default_config
        if raise_err:
            raise ValueError(f"Attribute '{key}' not found in instance")
        return default_config

    for attr_name in dir(obj):
        if not attr_name.startswith('__') and not attr_name.endswith('__'):
            try:
                item = getattr(obj, attr_name)
                if isinstance(item, config_type):
                    return item
            except Exception:
                pass
    if raise_err:
        raise ValueError(f"No config of type {config_type.__name__} found in instance")
    return default_config

@overload
def extract_config_from_FieldInfo(
    field_info: FieldInfo,
    config_type: type[QryConfigType],
    key: str | None = None,
    raise_err: Literal[True] = True,
    default_config: DefConfigType = None,
) -> QryConfigType:
    ...

@overload
def extract_config_from_FieldInfo(
    field_info: FieldInfo,
    config_type: type[QryConfigType],
    key: str | None = None,
    raise_err: Literal[False] = False,
    default_config: DefConfigType = None,
) -> QryConfigType | DefConfigType:
    ...

def extract_config_from_FieldInfo(
    field_info: FieldInfo,
    config_type: type[QryConfigType],
    key: str | None = None,
    raise_err: bool = True,
    default_config: DefConfigType = None,
) -> QryConfigType | DefConfigType:
    if field_info.json_schema_extra is None:
        if raise_err:
            raise ValueError("FieldInfo does not have json_schema_extra")
        return default_config
    return extract_config_from_mapping(field_info.json_schema_extra, config_type, key, raise_err, default_config)

