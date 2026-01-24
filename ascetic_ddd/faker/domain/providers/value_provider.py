import typing

from ascetic_ddd.faker.domain.distributors.m2o import DummyDistributor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor, IM2ODistributor
from ascetic_ddd.faker.domain.providers._mixins import BaseDistributionProvider
from ascetic_ddd.faker.domain.providers.interfaces import IValueProvider
from ascetic_ddd.faker.domain.generators.interfaces import IInputGenerator
from ascetic_ddd.faker.domain.generators.generators import prepare_input_generator
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.values.empty import empty

__all__ = ('ValueProvider',)

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


class ValueProvider(
    BaseDistributionProvider[T_Input, T_Output],
    IValueProvider[T_Input, T_Output],
    typing.Generic[T_Input, T_Output]
):
    _input_generator: IInputGenerator[T_Input] | None = None
    _output_factory: typing.Callable[[T_Input], T_Output] = None  # T_Output of each nested Provider.
    _output_exporter: typing.Callable[[T_Output], T_Input] = None

    def __init__(
            self,
            distributor: IM2ODistributor | None,
            input_generator: IInputGenerator[T_Input] | None = None,
            output_factory: typing.Callable[[T_Input], T_Output] | None = None,
            output_exporter: typing.Callable[[T_Output], T_Input] | None = None,
    ):
        if distributor is None:
            distributor = DummyDistributor()

        if self._input_generator is None and input_generator is not None:
            self._input_generator = prepare_input_generator(input_generator)

        if self._output_factory is None:
            if output_factory is None:

                def output_factory(result):
                    return result

            self._output_factory = output_factory

        if self._output_exporter is None:
            if output_exporter is None:

                def output_exporter(value):
                    return value

            self._output_exporter = output_exporter

        super().__init__(distributor=distributor)

    async def create(self, session: ISession) -> T_Output:
        return self._output

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return

        if self._input is not empty:
            self._output = self._output_factory(self._input)
            # await cursor.append(session, self._output)
            return

        try:
            result = await self._distributor.next(session, self._make_specification())
            value = self._output_exporter(result)
            self.set(value)
            # self.set() could reset self._output
            self._output = result
        except ICursor as cursor:
            if self._input_generator is None:
                self._output = self._output_factory(None)
            else:
                value = await self._input_generator(session, cursor.position)
                result = self._output_factory(value)
                await cursor.append(session, result)
                self.set(value)
                # self.set() could reset self._output
                self._output = result

    def _make_specification(self) -> ISpecification | None:
        return None
