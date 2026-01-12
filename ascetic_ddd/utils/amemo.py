from collections.abc import Awaitable, Callable
from functools import wraps
from typing import ParamSpec, TypeVar

__all__ = ("amemo",)

T = TypeVar("T")
P = ParamSpec("P")


def amemo(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
    _cache = {}

    @wraps(func)
    async def _deco(*args, **kwargs):
        key = (tuple(args), tuple(kwargs.items()))
        if key not in _cache:
            _cache[key] = await func(*args, **kwargs)
        return _cache[key]

    return _deco
