import typing

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor
from ascetic_ddd.faker.domain.providers._mixins import BaseDistributionProvider
from ascetic_ddd.faker.domain.providers.interfaces import IValueProvider
from ascetic_ddd.faker.domain.session.interfaces import ISession

__all__ = ('ValueProvider',)

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


class ValueProvider(
    BaseDistributionProvider[T_Input, T_Output],
    IValueProvider[T_Input, T_Output],
    typing.Generic[T_Input, T_Output]
):

    async def create(self, session: ISession) -> T_Output:
        return self._output_result

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return
        try:
            self._output_result = await self._distributor.next(session)
            value = self._result_exporter(self._output_result)
            self.set(value)
        except ICursor as cursor:
            value = await self._value_generator(session, e.args[0] if e.args else None)
            self._output_result = self._result_factory(value)
            await cursor.append(session, self._output_result)
            self.set(value)
