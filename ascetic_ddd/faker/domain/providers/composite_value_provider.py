import typing

from ascetic_ddd.faker.domain.distributors.m2o import DummyDistributor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor, IM2ODistributor
from ascetic_ddd.faker.domain.providers._mixins import BaseCompositeDistributionProvider
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.specification.object_pattern_resolvable_specification import ObjectPatternResolvableSpecification
from ascetic_ddd.faker.domain.values.empty import empty
from ascetic_ddd.seedwork.domain.session.interfaces import ISession

__all__ = (
    'CompositeValueProvider',
)


T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


class CompositeValueProvider(
    BaseCompositeDistributionProvider,
    typing.Generic[T_Input, T_Output]
):
    _output_factory: typing.Callable[[...], T_Output] = None  # T_Output of each nested Provider.
    _output_exporter: typing.Callable[[T_Output], T_Input] = None
    _specification_factory: typing.Callable[..., ISpecification]

    def __init__(
            self,
            distributor: IM2ODistributor[T_Input] | None = None,
            output_factory: typing.Callable[[...], T_Output] | None = None,  # T_Output of each nested Provider.
            output_exporter: typing.Callable[[T_Output], T_Input] | None = None,
            specification_factory: typing.Callable[..., ISpecification] = ObjectPatternResolvableSpecification,
    ):
        if distributor is None:
            distributor = DummyDistributor()

        if self._output_factory is None:
            if output_factory is None:

                def output_factory(**kwargs):
                    return kwargs

            self._output_factory = output_factory

        if self._output_exporter is None:
            if output_exporter is None:

                def output_exporter(value):
                    return value

            self._output_exporter = output_exporter

        self._specification_factory = specification_factory
        super().__init__(distributor=distributor)
        self.on_init()

    def on_init(self):
        pass

    async def create(self, session: ISession) -> T_Output:
        return self._output

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            if self._output is empty:
                self._output = await self._default_factory(session)
            return

        if self._input is empty:
            specification = EmptySpecification()
        else:
            specification = self._specification_factory(self._input, self._output_exporter)

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
                value = self._output_exporter(result)
                self.set(value)
            else:
                self.set(None)
            # self.set() could reset self._output
            self._output = result
        except ICursor as cursor:
            result = await self._default_factory(session, cursor.position)
            value = self._output_exporter(result)
            self.set(value)
            # self.set() could reset self._output
            self._output = result
            if not self.is_transient():
                await cursor.append(session, self._output)
            # infinite recursion
            # await self.populate(session)

    async def _default_factory(self, session: ISession, position: typing.Optional[int] = None):
        data = dict()
        for attr, provider in self._providers.items():
            data[attr] = await provider.create(session)
        return self._output_factory(**data)

    async def do_populate(self, session: ISession) -> None:
        pass
