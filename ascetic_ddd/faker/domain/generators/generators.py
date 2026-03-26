import asyncio
import inspect
import math
import os
import typing
import operator
from hypothesis import strategies

from ascetic_ddd.faker.domain.generators.interfaces import IInputGenerator
from ascetic_ddd.faker.domain.query.operators import IQueryOperator, EqOperator, IsNullOperator
from ascetic_ddd.session.interfaces import ISession


__all__ = (
    "IterableGenerator",
    "HypothesisStrategyGenerator",
    "CallableGenerator",
    "CountableGenerator",
    "SequenceGenerator",
    "RangeGenerator",
    "TemplateGenerator",
    "RequiredGenerator",
    "prepare_input_generator",
)

T = typing.TypeVar("T")


class _SupportsRangeOps(typing.Protocol):
    def __sub__(self, __other: typing.Any) -> typing.Any: ...
    def __add__(self, __other: typing.Any) -> typing.Any: ...
    def __le__(self, __other: typing.Any) -> bool: ...
    def __lt__(self, __other: typing.Any) -> bool: ...


_RangeT = typing.TypeVar("_RangeT", bound=_SupportsRangeOps)


def prepare_input_generator(input_generator):
    if input_generator is not None:
        if isinstance(input_generator, strategies.SearchStrategy):
            input_generator = HypothesisStrategyGenerator(input_generator)
        elif isinstance(input_generator, typing.Iterable) and not isinstance(input_generator, (str, bytes)):
            input_generator = IterableGenerator(input_generator)
        elif callable(input_generator):
            # Check if already wrapped
            if not isinstance(input_generator, CallableGenerator):
                input_generator = CallableGenerator(input_generator)
        input_generator = RequiredGenerator(input_generator)  # pyright: ignore[reportArgumentType]
    return input_generator


class IterableGenerator(typing.Generic[T]):

    def __init__(self, values: typing.Iterable[T]):
        self._source = values
        self._values = iter(self._source)

    async def __call__(self, session: ISession, query: IQueryOperator | None = None, position: int = -1) -> T:
        try:
            return next(self._values)
        except StopIteration as e:
            self._values = iter(self._source)
            return await self.__call__(session=session, query=query, position=position)


class HypothesisStrategyGenerator(typing.Generic[T]):
    """
    Do we actually need it?
    self._strategy.example() is a regular function.
    CallableGenerator can be used instead.
    """

    def __init__(self, strategy: strategies.SearchStrategy[T]):
        self._strategy = strategy

    async def __call__(self, session: ISession, query: IQueryOperator | None = None, position: int = -1) -> T:
        return self._strategy.example()


class CallableGenerator(typing.Generic[T]):
    """
    Wrapper for a callable with any number of parameters (0, 1, or 2).
    Automatically detects signature and async.
    """

    def __init__(self, callable_: typing.Callable):
        self._callable = callable_
        signature = inspect.signature(callable_)
        self._num_params = len(signature.parameters)
        self._is_async = (
            inspect.iscoroutinefunction(callable_) or
            inspect.iscoroutinefunction(getattr(callable_, '__call__', None))
        )

    async def __call__(self, session: ISession, query: IQueryOperator | None = None, position: int = -1) -> T:
        if self._num_params == 0:
            result = self._callable()
        elif self._num_params == 1:
            result = self._callable(session)
        elif self._num_params == 2:
            result = self._callable(session, query)
        else:
            result = self._callable(session, query, position)
        if self._is_async or asyncio.iscoroutine(result):
            result = await result
        return result


class CountableGenerator:

    def __init__(self, base: str):
        self._count = 0
        self._pid = os.getpid()
        self._base = base

    async def __call__(self, session: ISession, query: IQueryOperator | None = None, position: int = -1) -> str:
        result = "%s_%s_%s" % (self._base, os.getpid(), ++self._count)
        self._count += 1
        return result


class SequenceGenerator(typing.Generic[T]):

    def __init__(self, lower: T, delta: typing.Any):
        self._lower = lower
        self._delta = delta
        self._op = operator.add

    async def __call__(self, session: ISession, query: IQueryOperator | None = None, position: int = -1) -> T:
        return self._op(self._lower, self._delta * position)


class RangeGenerator(typing.Generic[_RangeT]):

    def __init__(self, lower: _RangeT, upper: _RangeT):
        self._lower = lower
        self._upper = upper
        self._range = upper - lower

    async def __call__(self, session: ISession, query: IQueryOperator | None = None, position: int = -1) -> _RangeT:
        assert position >= 0
        degree = 1 if position < 2 else math.ceil(math.log2(position))
        base = 2 ** degree
        value = self._lower + self._range * (position % base) / base
        assert self._lower <= value < self._upper
        return value


class RequiredGenerator(typing.Generic[T]):

    def __init__(self, delegate: IInputGenerator[T]):
        self._delegate = delegate

    async def __call__(self, session: ISession, query: IQueryOperator | None = None, position: int = -1) -> T:
        if isinstance(query, EqOperator):
            return query.value
        return await self._delegate(session, query, position)


class TemplateGenerator:

    def __init__(self, delegate: IInputGenerator[typing.Any], template: str):
        assert "%s" in template
        self._template = template
        self._delegate = delegate

    async def __call__(self, session: ISession, query: IQueryOperator | None = None, position: int = -1) -> str:
        return self._template % (await self._delegate(session, query, position),)
