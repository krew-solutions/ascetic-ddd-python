import typing

from ascetic_ddd.faker.domain.providers._mixins import BaseProvider
from ascetic_ddd.faker.domain.providers.interfaces import IValueProvider
from ascetic_ddd.faker.domain.values.empty import empty, Empty
from ascetic_ddd.faker.domain.session.interfaces import ISession

__all__ = ('PredefinedProvider',)

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


class PredefinedProvider(BaseProvider[T_Input, T_Output], IValueProvider[T_Input, T_Output]):
    _default_input_value: T_Input | Empty = empty
    _provider_name: str | None = None

    def __init__(
            self,
            default_value: T_Input | Empty = empty,
            result_factory: typing.Callable[[T_Input], T_Output] | None = None,
    ):
        super().__init__()

        self._default_input_value = default_value

        if result_factory is None:
            def result_factory(result):
                return result

        self._result_factory = result_factory

    async def create(self, session: ISession) -> T_Output:
        return self._result_factory(self._input_value)

    async def populate(self, session: ISession) -> None:
        if self._input_value is empty:
            self.set(self._default_input_value)
