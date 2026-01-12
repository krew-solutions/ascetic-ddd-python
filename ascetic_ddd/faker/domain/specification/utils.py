import dataclasses
import typing

from ..identity.interfaces import IIdentity
from ..values.empty import empty


__all__ = ("object_pattern_match", "object_pattern_to_key", "object_pattern_to_criteria",)


def object_pattern_match(object_pattern: IIdentity, tested_value: IIdentity) -> bool:
    if object_pattern is empty:
        return True

    if dataclasses.is_dataclass(object_pattern):
        for field in dataclasses.fields(object_pattern):
            search_item = getattr(object_pattern, field.name)
            if search_item is empty:
                continue
            tested_item = getattr(tested_value, field.name)
            """
            if dataclasses.is_dataclass(search_item) and not object_pattern_match(search_item, tested_item):
                return False
            """
            if search_item != tested_item:
                return False
        return True
    raise ValueError("Unknown type %r", object_pattern)


def object_pattern_to_key(object_pattern: IIdentity, value: IIdentity) -> typing.Hashable:
    result = list()
    if object_pattern is empty:
        return frozenset()
    # if dataclasses.is_dataclass(value):
    if dataclasses.is_dataclass(object_pattern):
        for field in dataclasses.fields(object_pattern):
            search_item = getattr(object_pattern, field.name)
            if search_item is empty:
                continue
            value_item = getattr(value, field.name)
            """
            if dataclasses.is_dataclass(search_item):
                attr_val = object_pattern_to_key(search_item, value_item)
            """
            result.append((field.name, value_item))
        return frozenset(result)
    return frozenset()


def object_pattern_to_criteria(object_pattern: IIdentity) -> dict:
    result = dict()
    if object_pattern is empty:
        return result
    elif dataclasses.is_dataclass(object_pattern):
        for field in dataclasses.fields(object_pattern):
            search_item = getattr(object_pattern, field.name)
            if search_item is empty:
                continue
            value_item = getattr(object_pattern, field.name)
            """
            if dataclasses.is_dataclass(search_item):
                attr_val = object_pattern_to_criteria(search_item, value_item)
            """
            result[field.name] = value_item
    return result
