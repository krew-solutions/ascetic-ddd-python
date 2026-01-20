import typing

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor, IM2ODistributor
from ascetic_ddd.faker.domain.providers._mixins import BaseCompositeDistributionProvider
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.faker.domain.values.empty import empty
from ascetic_ddd.faker.domain.specification.object_pattern_specification import ObjectPatternSpecification
from ascetic_ddd.faker.domain.session.interfaces import ISession

__all__ = (
    'CompositeValueProvider',
)


T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


class CompositeValueProvider(
    BaseCompositeDistributionProvider,
    typing.Generic[T_Input, T_Output]
):
    _result_factory: typing.Callable[[...], T_Output] = None  # T_Output of each nested Provider.
    _result_exporter: typing.Callable[[T_Output], T_Input] = None

    def __init__(
            self,
            distributor: IM2ODistributor[T_Input],
            result_factory: typing.Callable[[...], T_Output] | None = None,  # T_Output of each nested Provider.
            result_exporter: typing.Callable[[T_Output], T_Input] | None = None,
    ):

        if self._result_factory is None:
            if result_factory is None:

                def result_factory(**kwargs):
                    return kwargs

            self._result_factory = result_factory

        if self._result_exporter is None:
            if result_exporter is None:

                def result_exporter(value):
                    return value

            self._result_exporter = result_exporter

        super().__init__(distributor=distributor)
        self.on_init()

    def on_init(self):
        pass

    async def create(self, session: ISession) -> T_Output:
        return self._output_result

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            if self._output_result is empty:
                self._output_result = await self._default_factory(session)
            return

        if self._input_value is empty:
            specification = EmptySpecification()
        else:
            specification = ObjectPatternSpecification(self._input_value, self._result_exporter)

        await self.do_populate(session)
        cursors = {}
        for attr, provider in self._providers.items():
            try:
                await provider.populate(session)
            except ICursor as cursor:
                cursors[attr] = cursor

        try:
            result = await self._distributor.next(session, specification)
            if result is not None:
                value = self._result_exporter(result)
                self.set(value)
            else:
                self.set(None)
            # self.set() could reset self._output_result
            self._output_result = result
        except ICursor as cursor:
            result = await self._default_factory(session, cursor.position)
            value = self._result_exporter(result)
            self.set(value)
            # self.set() could reset self._output_result
            self._output_result = result
            if not self.is_transient():
                await cursor.append(session, self._output_result)
            # infinite recursion
            # await self.populate(session)

    async def _default_factory(self, session: ISession, position: typing.Optional[int] = None):
        data = dict()
        for attr, provider in self._providers.items():
            data[attr] = await provider.create(session)
        return self._result_factory(**data)

    async def do_populate(self, session: ISession) -> None:
        pass
