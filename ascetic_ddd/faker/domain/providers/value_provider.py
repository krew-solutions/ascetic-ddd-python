import typing

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor, IM2ODistributor
from ascetic_ddd.faker.domain.providers._mixins import BaseDistributionProvider
from ascetic_ddd.faker.domain.providers.interfaces import IValueProvider, IValueGenerator
from ascetic_ddd.faker.domain.providers.value_generators import prepare_value_generator
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.values.empty import empty

__all__ = ('ValueProvider',)

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


class ValueProvider(
    BaseDistributionProvider[T_Input, T_Output],
    IValueProvider[T_Input, T_Output],
    typing.Generic[T_Input, T_Output]
):
    _value_generator: IValueGenerator[T_Input] | None
    _result_factory: typing.Callable[[T_Input], T_Output]  # T_Output of each nested Provider.
    _result_exporter: typing.Callable[[T_Output], T_Input]

    def __init__(
            self,
            distributor: IM2ODistributor,
            value_generator: IValueGenerator[T_Input] | None = None,
            result_factory: typing.Callable[[T_Input], T_Output] | None = None,
            result_exporter: typing.Callable[[T_Output], T_Input] | None = None,
    ):
        self._value_generator = prepare_value_generator(value_generator) if value_generator is not None else None

        if result_factory is None:
            def result_factory(result):
                return result

        self._result_factory = result_factory

        if result_exporter is None:
            def result_exporter(value):
                return value

        self._result_exporter = result_exporter
        super().__init__(distributor=distributor)

    async def create(self, session: ISession) -> T_Output:
        return self._output_result

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return

        if self._input_value is not empty:
            self._output_result = self._result_factory(self._input_value)
            # await cursor.append(session, self._output_result)
            return

        try:
            result = await self._distributor.next(session)
            value = self._result_exporter(result)
            self.set(value)
            # self.set() could reset self._output_result
            self._output_result = result
        except ICursor as cursor:
            if self._value_generator is None:
                self._output_result = self._result_factory(None)
            else:
                value = await self._value_generator(session, cursor.position)
                result = self._result_factory(value)
                await cursor.append(session, result)
                self.set(value)
                # self.set() could reset self._output_result
                self._output_result = result
