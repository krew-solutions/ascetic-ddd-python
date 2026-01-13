import typing
import functools
from abc import ABCMeta
from collections.abc import Callable

from ascetic_ddd.faker.domain.providers._mixins import ObservableMixin, CloneableMixin
from ascetic_ddd.faker.domain.providers.interfaces import IValueProvider, IShunt, ICompositeValueProvider
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.values.empty import empty, Empty
from ascetic_ddd.faker.domain.distributors.interfaces import IDistributor
from ascetic_ddd.faker.domain.specification.object_pattern_specification import ObjectPatternSpecification
from ascetic_ddd.faker.domain.session.interfaces import ISession

__all__ = (
    'CompositeValueProvider',
)

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


class CompositeValueProvider(
    ObservableMixin,
    CloneableMixin,
    ICompositeValueProvider[T_Input, T_Output],
    typing.Generic[T_Input, T_Output],
    metaclass=ABCMeta
):
    _distributor: IDistributor[T_Input]
    _criteria: ISpecification | None = None
    _value_generator: Callable[[...], T_Input]
    _result_factory: typing.Callable[[...], T_Output]  # T_Output of each nested Provider.
    _result_exporter: typing.Callable[[T_Output], T_Input]
    _input_value: T_Input | Empty = empty
    _output_result: T_Output | Empty = empty
    _provider_name: str | None = None

    def __init__(
            self,
            distributor: IDistributor[T_Input],
            result_factory: typing.Callable[[...], T_Output] | None = None,  # T_Output of each nested Provider.
            result_exporter: typing.Callable[[T_Output], T_Input] | None = None,
    ):
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

    def is_complete(self) -> bool:
        return (
            self._output_result is not empty or
            any(provider.is_complete() for provider in self._providers.values())
        )

    def do_empty(self, clone: typing.Self, shunt: IShunt):
        clone._input_value = empty
        clone._output_result = empty
        for attr, provider in self._providers.items():
            setattr(clone, attr, provider.empty(shunt))

    def reset(self) -> None:
        self._input_value = empty
        self._output_result = empty
        self.notify('input_value', self._input_value)
        for provider in self._providers.values():
            provider.reset()

    async def create(self, session: ISession) -> T_Output:
        return self._output_result

    def set(self, value: T_Input) -> None:
        self._input_value = value
        self.notify('input_value', value)
        for attr, val in value.items():
            """
            Вложенная композиция поддерживается автоматически.
            """
            getattr(self, attr).set(val)

    def get(self) -> T_Input:
        value = dict()
        for attr, provider in self._providers.items():
            val = provider.get()
            if val is not empty:
                value[attr] = val
        return value

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return
        if self._input_value is empty:
            specification = EmptySpecification()
        else:
            specification = ObjectPatternSpecification(self._input_value, self._result_exporter)

        for provider in self._providers.values():
            await provider.populate(session)

        try:
            self._output_result = await self._distributor.next(session, specification)
            value = self._result_exporter(self._output_result)
            self.set(value)
        except StopAsyncIteration as e:
            if self.is_complete():
                result = await self._default_factory(session, e.args[0] if e.args else None)
                self._output_result = self._result_factory(result)
                await self._distributor.append(session, self._output_result)
                value = self._result_exporter(self._output_result)
                self.set(value)

    async def _default_factory(self, session: ISession, position: typing.Optional[int] = None):
        data = dict()
        for attr, provider in self._providers.items():
            data[attr] = await provider.create(session)
        return self._result_factory(**data)

    async def append(self, session: ISession, value: T_Output):
        await self._distributor.append(session, value)

    async def setup(self, session: ISession):
        await self._distributor.setup(session)
        for provider in self._providers.values():
            await provider.setup(session)

    async def cleanup(self, session: ISession):
        await self._distributor.cleanup(session)
        for provider in self._providers.values():
            await provider.cleanup(session)

    @classmethod
    @property
    @functools.cache
    def _provider_attrs(cls) -> list[str]:
        attrs = list()
        for cls_ in cls.mro():  # Use self.__dict__ or self.__reduce__() instead?
            if hasattr(cls_, '__annotations__'):
                for key in cls_.__annotations__.keys():
                    if not key.startswith('_') and key not in attrs:
                        attrs.append(key)
        return attrs

    @property
    def _providers(self) -> dict[str, IValueProvider[typing.Any]]:
        return {i: getattr(self, i) for i in self._provider_attrs}

    @property
    def provider_name(self):
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value):
        self._provider_name = value
        self._distributor.provider_name = value
        for attr, provider in self._providers.items():
            provider.provider_name = "%s.%s" % (value, attr)
