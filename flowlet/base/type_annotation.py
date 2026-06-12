import types
from enum import Enum, auto
from types import NoneType
from typing import Any, ForwardRef, TypeVar, Union, get_args, get_origin


class TypeKind(Enum):
    BARE_CLASS = auto()  # 裸类：int, str, list, MyClass（就是类本身）
    GENERIC_ALIAS = auto()  # 泛型：list[str], dict[int, str]（Python 3.9+）
    UNION_TYPE = auto()  # 联合：str | int, Optional[str]（Python 3.10+）
    TYPING_FORM = auto()  # typing 特殊形式：Any, NoReturn, TypeVar, ForwardRef
    INSTANCE = auto()  # 实例：123, "hello", []
    UNKNOWN = auto()  # 未知（如模块、函数等不好分类的）


def classify_object(obj) -> TypeKind:
    """
    三分类判断：
    - 裸类（BARE_CLASS）：纯净的类对象，可用于实例化
    - 注解形式（GENERIC_ALIAS/UNION_TYPE/TYPING_FORM）：描述类型的元对象，不能实例化
    - 实例（INSTANCE）：已经创建的数据值
    """

    # 1. 检查是否是泛型别名（list[str]）- 必须在检查类之前，因为 Python 3.9+ 中有些泛型也是类
    if isinstance(obj, types.GenericAlias):
        return TypeKind.GENERIC_ALIAS

    # 2. 检查是否是联合类型（str | int）
    if isinstance(obj, types.UnionType):
        return TypeKind.UNION_TYPE

    # 3. 检查 typing 模块的特殊形式（包括 Any, NoReturn 等）
    # 注意：这必须在检查 BARE_CLASS 之前，因为在 Python 3.11+ 中 typing.Any 的 isinstance(Any, type) 为 True

    # 检查是否有 __origin__ 或 __args__（typing.List[str], Union[str, int] 等）
    if hasattr(obj, "__origin__") or hasattr(obj, "__args__"):
        # 但有 origin 的不一定是 typing 形式，可能是 GenericAlias（已处理）
        return TypeKind.TYPING_FORM

    # 补漏：typing._SpecialForm（Any, NoReturn 等没有 __origin__ 的）
    if type(obj).__module__ == "typing":
        return TypeKind.TYPING_FORM

    # 4. 检查是否是 TypeVar 或 ForwardRef（它们没有 __origin__）
    if isinstance(obj, (TypeVar, ForwardRef)):
        return TypeKind.TYPING_FORM

    # 5. 检查是否是类（type 的子类）
    # 注意：这必须在 typing 检查之后
    if isinstance(obj, type):
        # 纯类，没有泛型包装
        return TypeKind.BARE_CLASS

    # 6. 剩下的：如果是可调用对象但不是类，视为实例（包括函数、模块等）
    # 严格区分：只要不是类型系统的一部分，就是实例值
    return TypeKind.INSTANCE


# =============================================================================
# 基于 TypeKind 的辅助函数
# =============================================================================


def _is_union_typing_form(obj) -> bool:
    """检查是否是 typing.Union 或 typing.Optional 等 Union 形式的 typing 对象"""
    if classify_object(obj) is not TypeKind.TYPING_FORM:
        return False
    origin = get_origin(obj)
    return origin is Union


def _is_any_type(obj) -> bool:
    """检查是否是 typing.Any"""
    return classify_object(obj) is TypeKind.TYPING_FORM and obj is Any


def _get_origin_and_args(annotation: Any) -> tuple[type | None, tuple[Any, ...]]:
    """
    统一获取类型的 origin 和 args，基于 TypeKind 分类处理。

    Returns:
        (origin, args): origin 为 None 表示不是泛型/联合类型
    """
    kind = classify_object(annotation)

    if kind is TypeKind.BARE_CLASS:
        return (annotation, ())

    if kind is TypeKind.GENERIC_ALIAS:
        origin = get_origin(annotation)
        args = get_args(annotation)
        return (origin, args)

    if kind is TypeKind.UNION_TYPE:
        return (Union, get_args(annotation))

    if kind is TypeKind.TYPING_FORM:
        if _is_any_type(annotation):
            return (None, ())
        origin = get_origin(annotation)
        args = get_args(annotation)
        # 对于 typing.Union, origin 是 Union；对于 typing.List[str], origin 是 list
        return (origin, args)

    # INSTANCE 和 UNKNOWN 无法解析
    return (None, ())


def _iter_type_components(annotation: Any):
    """
    迭代遍历类型注解中的所有组成类型（用于 Union 展开）。
    基于 TypeKind 判断是否需要展开。

    Yields:
        类型注解的组成部分
    """
    kind = classify_object(annotation)

    if kind is TypeKind.UNION_TYPE or _is_union_typing_form(annotation):
        # Union 类型：展开参数
        for arg in get_args(annotation):
            yield from _iter_type_components(arg)
    elif kind in (TypeKind.GENERIC_ALIAS, TypeKind.TYPING_FORM):
        # 泛型：返回自身，但也需要处理其参数
        yield annotation
    elif kind is TypeKind.BARE_CLASS:
        yield annotation
    # INSTANCE 和 UNKNOWN 不产生有效类型


# =============================================================================
# 主要 API 函数（基于 TypeKind 重写，保持接口不变）
# =============================================================================


def get_class_from_annotation(annotation) -> type | None:
    """
    把注解中的『具体类』提取出来：
      list[int]   -> list
      int | str   -> int   （Union 取第一个非-None 的类）
      Union[int, None] -> int
      普通类       -> 自身
      全 None      -> None
    """
    if annotation is None:
        return None

    kind = classify_object(annotation)

    # 处理 Union 类型：取第一个非 None 的类型
    if kind is TypeKind.UNION_TYPE or _is_union_typing_form(annotation):
        for arg in get_args(annotation):
            if arg is not NoneType:
                return get_class_from_annotation(arg)
        return None  # 全是 None

    # 处理泛型：返回 origin
    if kind is TypeKind.GENERIC_ALIAS:
        return get_origin(annotation)

    # 处理 typing 形式（非 Union）
    if kind is TypeKind.TYPING_FORM:
        if _is_any_type(annotation):
            return None
        origin = get_origin(annotation)
        if origin is not None:
            return origin
        # 无法解析的 typing 形式（如 TypeVar）返回 None
        return None

    # 裸类直接返回
    if kind is TypeKind.BARE_CLASS:
        return annotation

    # 其他情况返回 None
    return None


def unwrap_type(annotation) -> list[type]:
    """
    将任何类型对象展开为基础类型的扁平列表。

    - int → [int]
    - list[str] → [list]
    - str | int → [str, int]
    - list[str] | dict[int, bool] → [list, dict]
    """
    if annotation is None:
        raise TypeError(f"Unsupported type object: {annotation} (type: {type(annotation).__name__})")

    kind = classify_object(annotation)

    # Union 类型：递归展开所有参数
    if kind is TypeKind.UNION_TYPE or _is_union_typing_form(annotation):
        result = []
        for arg in get_args(annotation):
            result.extend(unwrap_type(arg))
        return result

    # 泛型类型：提取 origin 并展开
    if kind is TypeKind.GENERIC_ALIAS:
        origin = get_origin(annotation)
        if origin is not None:
            return unwrap_type(origin)
        raise TypeError(f"Unsupported type object: {annotation} (type: {type(annotation).__name__})")

    # typing 形式（非 Union）
    if kind is TypeKind.TYPING_FORM:
        if _is_any_type(annotation):
            raise TypeError(f"Unsupported type object: {annotation} (type: {type(annotation).__name__})")
        origin = get_origin(annotation)
        if origin is not None:
            return unwrap_type(origin)
        raise TypeError(f"Unsupported type object: {annotation} (type: {type(annotation).__name__})")

    # 裸类：叶子节点
    if kind is TypeKind.BARE_CLASS:
        return [annotation]

    raise TypeError(f"Unsupported type object: {annotation} (type: {type(annotation).__name__})")


def is_subclass_in_annotation(annotation: Any, target_class: type) -> bool:
    """判断FieldInfo的annotation中是否包含target_class的子类或自身

    Args:
        annotation: FieldInfo的annotation属性
        target_class: 目标类

    Returns:
        bool: 如果annotation中包含target_class的子类或自身，返回True，否则返回False
    """
    # 处理 None 和 Any 类型
    if annotation is None or _is_any_type(annotation):
        return False

    # 使用迭代而非递归方式处理类型检查
    types_to_check = [annotation]
    checked_types = set()

    while types_to_check:
        current_type = types_to_check.pop()
        type_id = id(current_type)

        # 避免重复检查同一类型
        if type_id in checked_types:
            continue
        checked_types.add(type_id)

        kind = classify_object(current_type)

        # 处理 Union 类型：将参数加入待检查列表
        if kind is TypeKind.UNION_TYPE or _is_union_typing_form(current_type):
            types_to_check.extend(get_args(current_type))
            continue

        # 处理泛型类型：检查 origin
        if kind in (TypeKind.GENERIC_ALIAS, TypeKind.TYPING_FORM):
            origin, args = _get_origin_and_args(current_type)

            # 检查 origin 是否匹配
            if isinstance(origin, type):
                try:
                    if issubclass(origin, target_class) or origin == target_class:
                        return True
                except TypeError:
                    pass

            # 将泛型参数加入待检查列表
            if args:
                types_to_check.extend(args)
            continue

        # 处理裸类：直接检查
        if kind is TypeKind.BARE_CLASS:
            try:
                if issubclass(current_type, target_class) or current_type == target_class:
                    return True
            except TypeError:
                pass
            continue

        # INSTANCE 和 UNKNOWN 不产生有效类型检查

    return False


T = TypeVar("T", bound=type)


def extract_target_subclass_from_annotation(annotation: Any, target_class: T) -> T | None:
    """从annotation中抽取target_class的子类或自身

    Args:
        annotation: FieldInfo的annotation属性
        target_class: 目标类

    Returns:
        Type[T] | None: 如果annotation中包含target_class的子类或自身，返回第一个找到的类，否则返回None
    """
    # 处理 None 和 Any 类型
    if annotation is None or _is_any_type(annotation):
        return None

    # 使用迭代而非递归方式处理类型检查
    types_to_check = [annotation]
    checked_types = set()

    while types_to_check:
        current_type = types_to_check.pop()
        type_id = id(current_type)

        # 避免重复检查同一类型
        if type_id in checked_types:
            continue
        checked_types.add(type_id)

        kind = classify_object(current_type)

        # 处理 Union 类型：将参数加入待检查列表
        if kind is TypeKind.UNION_TYPE or _is_union_typing_form(current_type):
            types_to_check.extend(get_args(current_type))
            continue

        # 处理泛型类型：检查 origin
        if kind in (TypeKind.GENERIC_ALIAS, TypeKind.TYPING_FORM):
            origin, args = _get_origin_and_args(current_type)

            # 检查 origin 是否匹配
            if isinstance(origin, type):
                try:
                    if issubclass(origin, target_class) or origin == target_class:
                        return origin
                except TypeError:
                    pass

            # 将泛型参数加入待检查列表
            if args:
                types_to_check.extend(args)
            continue

        # 处理裸类：直接检查
        if kind is TypeKind.BARE_CLASS:
            try:
                if issubclass(current_type, target_class) or current_type == target_class:
                    return current_type
            except TypeError:
                pass
            continue

        # INSTANCE 和 UNKNOWN 不产生有效类型检查

    return None


def is_instance_in_annotation(instance: Any, annotation: Any) -> bool:
    """判断某个实例是否是annotation所标注的类的实例或者子类实例
    类似于isinstance，但支持复杂的类型注解

    Args:
        instance: 要检查的实例
        annotation: 类型注解

    Returns:
        bool: 如果实例是annotation标注的类的实例或其子类的实例，返回True，否则返回False
    """
    # 处理 None 和 Any 类型
    if annotation is None:
        return instance is None
    if _is_any_type(annotation):
        return True

    # 使用迭代而非递归方式处理类型检查
    types_to_check = [annotation]
    checked_types = set()

    while types_to_check:
        current_type = types_to_check.pop()
        type_id = id(current_type)

        # 避免重复检查同一类型
        if type_id in checked_types:
            continue
        checked_types.add(type_id)

        kind = classify_object(current_type)

        # 处理 Union 类型：将参数加入待检查列表
        if kind is TypeKind.UNION_TYPE or _is_union_typing_form(current_type):
            types_to_check.extend(get_args(current_type))
            continue

        # 处理泛型类型
        if kind in (TypeKind.GENERIC_ALIAS, TypeKind.TYPING_FORM):
            origin, args = _get_origin_and_args(current_type)

            if origin is None:
                continue

            # 检查集合类型
            if origin in (list, set, tuple):
                if not isinstance(instance, origin):
                    # 继续检查其他类型（可能是 Union 的其它分支）
                    pass
                else:
                    # 获取泛型参数
                    if not args or (origin is tuple and len(args) != len(instance)):
                        pass
                    else:
                        # 检查集合中的每个元素
                        if origin is tuple:
                            # 对于tuple，需要检查每个位置的元素类型
                            for item, item_type in zip(instance, args):
                                if not is_instance_in_annotation(item, item_type):
                                    break
                            else:
                                # 所有元素都匹配
                                return True
                        else:
                            # 对于list和set，检查所有元素都匹配类型
                            elem_type = args[0]
                            if not instance or all(is_instance_in_annotation(elem, elem_type) for elem in instance):
                                return True

            # 检查字典类型
            elif origin is dict:
                if not isinstance(instance, dict):
                    # 实例不是字典，不检查 args，直接继续
                    pass
                elif len(args) != 2:
                    pass
                else:
                    key_type, value_type = args
                    if all(
                        is_instance_in_annotation(k, key_type) and is_instance_in_annotation(v, value_type)
                        for k, v in instance.items()
                    ):
                        return True
                # 对于 dict 类型，不检查 args 作为替代类型（它们是元素类型）
                continue

            # 处理普通泛型类
            elif isinstance(origin, type):
                try:
                    if isinstance(instance, origin):
                        return True
                except TypeError:
                    pass

            # 将泛型参数加入待检查列表（用于嵌套泛型）
            if args:
                types_to_check.extend(args)
            continue

        # 处理裸类：直接 isinstance 检查
        if kind is TypeKind.BARE_CLASS:
            try:
                if isinstance(instance, current_type):
                    return True
            except TypeError:
                pass
            continue

        # INSTANCE 和 UNKNOWN 不产生有效类型检查

    return False
