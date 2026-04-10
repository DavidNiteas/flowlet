from types import NoneType, UnionType
from typing import Any, TypeVar, Union, get_args, get_origin


def get_class_from_annotation(annotation) -> type | None:
    """
    把注解中的『具体类』提取出来：
      list[int]   -> list
      int | str   -> int   （Union 取第一个非-None 的类）
      Union[int, None] -> int
      普通类       -> 自身
      全 None      -> None
    """
    # 1. 处理 UnionType / Union
    if isinstance(annotation, UnionType) or getattr(annotation, "__origin__", None) is Union:
        for arg in get_args(annotation):
            if arg is not NoneType:
                # 递归处理可能嵌套的泛型
                return get_class_from_annotation(arg)
        return None  # 全是 None

    # 2. 普通泛型 list[int] / dict[...]
    if (origin := get_origin(annotation)) is not None:
        return origin

    # 3. 已经是裸类（或 None）
    return annotation if annotation is not None else None

def is_subclass_in_annotation(
    annotation: Any,
    target_class: type
) -> bool:
    """判断FieldInfo的annotation中是否包含target_class的子类或自身

    Args:
        annotation: FieldInfo的annotation属性
        target_class: 目标类

    Returns:
        bool: 如果annotation中包含target_class的子类或自身，返回True，否则返回False
    """
    # 处理None和Any类型
    if annotation is None or annotation is Any:
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

        # 尝试获取原始类型（处理泛型）
        origin = get_origin(current_type)
        if origin is not None:
            # 检查原始类型是否匹配
            if isinstance(origin, type):
                try:
                    if issubclass(origin, target_class) or origin == target_class:
                        return True
                except TypeError:
                    pass

            # 获取泛型参数并添加到待检查列表
            args = get_args(current_type)
            if args:
                types_to_check.extend(args)

        # 处理普通类型
        elif isinstance(current_type, type):
            try:
                if issubclass(current_type, target_class) or current_type == target_class:
                    return True
            except TypeError:
                pass

        # 尝试获取可能的参数（处理Union/Optional等）
        else:
            try:
                args = get_args(current_type)
                if args:
                    types_to_check.extend(args)
            except TypeError:
                pass

    return False


T = TypeVar('T', bound=type)

def extract_target_subclass_from_annotation(
    annotation: Any,
    target_class: T
) -> T | None:
    """从annotation中抽取target_class的子类或自身

    Args:
        annotation: FieldInfo的annotation属性
        target_class: 目标类

    Returns:
        Type[T] | None: 如果annotation中包含target_class的子类或自身，返回第一个找到的类，否则返回None
    """
    # 处理None和Any类型
    if annotation is None or annotation is Any:
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

        # 尝试获取原始类型（处理泛型）
        origin = get_origin(current_type)
        if origin is not None:
            # 检查原始类型是否匹配
            if isinstance(origin, type):
                try:
                    if issubclass(origin, target_class) or origin == target_class:
                        return origin
                except TypeError:
                    pass

            # 获取泛型参数并添加到待检查列表
            args = get_args(current_type)
            if args:
                types_to_check.extend(args)

        # 处理普通类型
        elif isinstance(current_type, type):
            try:
                if issubclass(current_type, target_class) or current_type == target_class:
                    return current_type
            except TypeError:
                pass

        # 尝试获取可能的参数（处理Union/Optional等）
        else:
            try:
                args = get_args(current_type)
                if args:
                    types_to_check.extend(args)
            except TypeError:
                pass

    return None

def is_instance_in_annotation(
    instance: Any,
    annotation: Any
) -> bool:
    """判断某个实例是否是annotation所标注的类的实例或者子类实例
    类似于isinstance，但支持复杂的类型注解

    Args:
        instance: 要检查的实例
        annotation: 类型注解

    Returns:
        bool: 如果实例是annotation标注的类的实例或其子类的实例，返回True，否则返回False
    """
    # 处理None和Any类型
    if annotation is None:
        return instance is None
    if annotation is Any:
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

        # 尝试获取原始类型（处理泛型）
        origin = get_origin(current_type)
        if origin is not None:
            # 检查原始类型是否为集合类型
            if origin is list or origin is set or origin is tuple:
                # 如果实例不是对应类型，继续检查其他类型
                if not isinstance(instance, origin):
                    continue

                # 获取泛型参数
                args = get_args(current_type)
                if not args or origin is tuple and len(args) != len(instance):
                    continue

                # 检查集合中的每个元素
                if origin is tuple:
                    # 对于tuple，需要检查每个位置的元素类型
                    for i, (item, item_type) in enumerate(zip(instance, args)):
                        if not is_instance_in_annotation(item, item_type):
                            break
                    else:
                        # 所有元素都匹配
                        return True
                else:
                    # 对于list和set，检查第一个元素类型，然后假设所有元素都符合
                    elem_type = args[0]
                    # 检查集合是否为空或者所有元素都匹配类型
                    if not instance or all(is_instance_in_annotation(elem, elem_type) for elem in instance):
                        return True
            elif origin is dict:
                # 检查字典类型
                if not isinstance(instance, dict):
                    continue

                # 获取键值类型参数
                args = get_args(current_type)
                if len(args) != 2:
                    continue

                key_type, value_type = args
                # 检查所有键值对
                if all(
                    is_instance_in_annotation(k, key_type)
                    and is_instance_in_annotation(v, value_type)
                    for k, v in instance.items()
                ):
                    return True
            elif isinstance(origin, type):
                # 处理普通泛型类
                try:
                    if isinstance(instance, origin):
                        return True
                except TypeError:
                    pass

            # 获取泛型参数并添加到待检查列表（处理Union、Optional等）
            args = get_args(current_type)
            if args:
                types_to_check.extend(args)
        # 处理普通类型
        elif isinstance(current_type, type):
            try:
                if isinstance(instance, current_type):
                    return True
            except TypeError:
                pass
        # 尝试获取可能的参数（处理Union/Optional等）
        else:
            try:
                args = get_args(current_type)
                if args:
                    types_to_check.extend(args)
            except TypeError:
                pass

    return False
