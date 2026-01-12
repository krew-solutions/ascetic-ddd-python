import typing

__all__ = ("classproperty", "setterproperty",)


class classproperty:
    """Class property decorator."""

    def __init__(self, getter: typing.Callable) -> None:
        self.getter = getter

    def __get__(self, instance, owner):
        return self.getter(owner)


class setterproperty:
    def __init__(self, func, doc=None):
        self.func = func
        self.__doc__ = doc if doc is not None else func.__doc__

    def __set__(self, obj, value):
        return self.func(obj, value)
