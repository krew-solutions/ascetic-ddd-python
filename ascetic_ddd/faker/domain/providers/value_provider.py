import typing

from ascetic_ddd.faker.domain.providers._mixins import BaseDistributorProvider
from ascetic_ddd.faker.domain.providers.interfaces import IValueProvider, IValueGenerator
from ascetic_ddd.faker.domain.providers.value_generators import prepare_value_generator
from ascetic_ddd.faker.domain.values.empty import empty, Empty
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.session.interfaces import ISession

__all__ = ('ValueProvider',)

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


class ValueProvider(
    BaseDistributorProvider[T_Input, T_Output],
    IValueProvider[T_Input, T_Output],
    typing.Generic[T_Input, T_Output]
):
    _distributor: IM2ODistributor[T_Output]
    _input_value: T_Input | Empty = empty
    _output_result: T_Output | Empty = empty
    _value_generator: IValueGenerator[T_Input] | None = None
    _result_factory: typing.Callable[[T_Input], T_Output]
    _result_exporter: typing.Callable[[T_Output], T_Input]
    _provider_name: str | None = None

    def __init__(
            self,
            distributor: IM2ODistributor,
            value_generator: IValueGenerator[T_Input] | None = None,
            result_factory: typing.Callable[[T_Input], T_Output] | None = None,
            result_exporter: typing.Callable[[T_Output], T_Input] | None = None,
    ):
        self._value_generator = prepare_value_generator(value_generator)
        self._distributor = distributor
        if result_factory is not None:
            def result_factory(result):
                return result
        self._result_factory = result_factory

        if result_exporter is not None:
            def result_exporter(value):
                return value
        self._result_exporter = result_exporter
        super().__init__()

    async def create(self, session: ISession) -> T_Output:
        return self._output_result

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return
        try:
            self._output_result = await self._distributor.next(session)
            value = self._result_exporter(self._output_result)
            self.set(value)
        except StopAsyncIteration as e:
            value = await self._value_generator(session, e.args[0] if e.args else None)
            self._output_result = self._result_factory(value)
            await self._distributor.append(session, self._output_result)
            self.set(value)
