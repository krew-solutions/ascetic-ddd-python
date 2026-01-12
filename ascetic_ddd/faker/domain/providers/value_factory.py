import asyncio
import inspect
import math
import os
import typing
import operator
from hypothesis import strategies

from ascetic_ddd.faker.domain.providers.interfaces import IValueFactory


__all__ = (
    "IterableFactory",
    "HypothesisStrategyFactory",
    "CallableFactory",
    "CountableFactory",
    "SequenceFactory",
    "RangeFactory",
    "TemplateFactory",
    "prepare_value_factory",
)

from ..session.interfaces import ISession

T = typing.TypeVar("T", covariant=True)


def prepare_value_factory(value_factory):
    if value_factory is not None:
        if isinstance(value_factory, strategies.SearchStrategy):
            value_factory = HypothesisStrategyFactory(value_factory)
        elif isinstance(value_factory, typing.Iterable) and not isinstance(value_factory, (str, bytes)):
            value_factory = IterableFactory(value_factory)
        elif callable(value_factory):
            # Check if already wrapped
            if not isinstance(value_factory, CallableFactory):
                value_factory = CallableFactory(value_factory)
    return value_factory


class IterableFactory(typing.Generic[T]):

    def __init__(self, values: typing.Iterable[T]):
        self._values = iter(values)

    async def __call__(self, session: ISession, position: typing.Optional[int] = None) -> T:
        try:
            return next(self._values)
        except StopIteration as e:
            raise StopAsyncIteration from e


class HypothesisStrategyFactory(typing.Generic[T]):

    def __init__(self, strategy: strategies.SearchStrategy[T]):
        self._strategy = strategy

    async def __call__(self, session: ISession, position: typing.Optional[int] = None) -> T:
        return self._strategy.example()


class CallableFactory(typing.Generic[T]):
    """
    Обёртка для callable с любым числом параметров (0, 1 или 2).
    Автоматически определяет сигнатуру и async.
    """

    def __init__(self, callable_: typing.Callable):
        self._callable = callable_
        signature = inspect.signature(callable_)
        self._num_params = len(signature.parameters)
        self._is_async = (
            asyncio.iscoroutinefunction(callable_) or
            asyncio.iscoroutinefunction(getattr(callable_, '__call__', None))
        )

    async def __call__(self, session: ISession, position: typing.Optional[int] = None) -> T:
        if self._num_params == 0:
            result = self._callable()
        elif self._num_params == 1:
            result = self._callable(session)
        else:
            result = self._callable(session, position)
        if self._is_async or asyncio.iscoroutine(result):
            result = await result
        return result


class CountableFactory(typing.Generic[T]):

    def __init__(self, base: str):
        self._count = 0
        self._pid = os.getpid()
        self._base = base

    async def __call__(self, session: ISession, position: typing.Optional[int] = None) -> T:
        result = "%s_%s_%s" % (self._base, os.getpid(), ++self._count)
        self._count += 1
        return result


class SequenceFactory(typing.Generic[T]):

    def __init__(self, lower: T, delta: typing.Any):
        self._lower = lower
        self._delta = delta
        self._op = operator.add

    async def __call__(self, session: ISession, position: typing.Optional[int] = None) -> T:
        return self._op(self._lower, self._delta * position)


class RangeFactory(typing.Generic[T]):

    def __init__(self, lower: T, upper: T):
        self._lower = lower
        self._upper = upper
        self._range = upper - lower

    async def __call__(self, session: ISession, position: typing.Optional[int] = None) -> T:
        degree = 1 if position < 2 else math.ceil(math.log2(position))
        base = 2 ** degree
        value = self._lower + self._range * (position % base) / base
        assert self._lower <= value < self._upper
        return value


class TemplateFactory(typing.Generic[T]):

    def __init__(self, delegate: IValueFactory[typing.Any], template: T):
        assert isinstance(template, str) and "%s" in template
        self._template = template
        self._delegate = delegate

    async def __call__(self, session: ISession, position: typing.Optional[int] = None) -> T:
        return self._template % (await self._delegate(session, position),)
