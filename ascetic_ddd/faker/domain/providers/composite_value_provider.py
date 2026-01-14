import typing

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor
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

    async def create(self, session: ISession) -> T_Output:
        return self._output_result

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return
        if self._input_value is empty:
            specification = EmptySpecification()
        else:
            specification = ObjectPatternSpecification(self._input_value, self._result_exporter)

        await self.do_populate(session)
        for provider in self._providers.values():
            await provider.populate(session)

        try:
            self._output_result = await self._distributor.next(session, specification)
            value = self._result_exporter(self._output_result)
            self.set(value)
        except ICursor as cursor:
            if self.is_complete():
                result = await self._default_factory(session, e.args[0] if e.args else None)
                self._output_result = self._result_factory(result)
                await cursor.append(session, self._output_result)
                value = self._result_exporter(self._output_result)
                self.set(value)

    async def do_populate(self, session: ISession) -> None:
        pass
