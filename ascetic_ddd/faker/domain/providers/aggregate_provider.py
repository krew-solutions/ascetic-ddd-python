import typing
from abc import ABCMeta

from ascetic_ddd.seedwork.domain.identity.interfaces import IAccessible
from ascetic_ddd.faker.domain.providers._mixins import BaseCompositeProvider, ObservableMixin
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.providers.interfaces import IEntityProvider
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.values.empty import empty
from ascetic_ddd.observable.interfaces import IObservable


__all__ = ('IAggregateRepository', 'AggregateProvider',)

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


class IAggregateRepository(IObservable, typing.Protocol[T_Output]):

    async def insert(self, session: ISession, agg: T_Output):
        ...

    async def get(self, session: ISession, id_: IAccessible[typing.Any]) -> T_Output | None:
        ...

    async def update(self, session: ISession, agg: T_Output):
        ...

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[T_Output]:
        ...

    async def setup(self, session: ISession):
        ...

    async def cleanup(self, session: ISession):
        ...


class AggregateProvider(
    BaseCompositeProvider[T_Input, T_Output],
    IEntityProvider[T_Input, T_Output],
    typing.Generic[T_Input, T_Output],
    metaclass=ABCMeta
):
    _id_attr: str
    _repository: IAggregateRepository[T_Output]
    _output_factory: typing.Callable[[...], T_Output] = None  # T_Output of each nested Provider.
    _output_exporter: typing.Callable[[T_Output], T_Input] = None

    _aspect_mapping = {
        "repository": "_repository",
        **ObservableMixin._aspect_mapping
    }

    def __init__(
            self,
            repository: IAggregateRepository,
            # distributor_factory: IM2ODistributorFactory,
            output_factory: typing.Callable[[...], T_Output] | None = None,  # T_Output of each nested Provider.
            output_exporter: typing.Callable[[T_Output], T_Input] | None = None,
    ):
        self._repository = repository

        if self._output_factory is None:
            if output_factory is None:

                def output_factory(**result):
                    return result

            self._output_factory = output_factory

        if self._output_exporter is None:
            if output_exporter is None:

                def output_exporter(value):
                    return value

            self._output_exporter = output_exporter

        super().__init__()
        self.on_init()

    def on_init(self):
        pass

    async def create(self, session: ISession) -> T_Output:
        if self._output is not empty:
            return self._output
        result = await self._default_factory(session)
        if self.id_provider.is_complete() and not self.id_provider.is_transient():
            # id_ здесь может быть еще неизвестен, т.к. агрегат не создан.
            # А может быть и известен, если его id_ реиспользуется как FK.
            id_ = await self.id_provider.create(session)
            # Skip repository lookup if id contains empty fields (auto-increment PKs)
            saved_result = await self._repository.get(session, id_)
        else:
            saved_result = None

        if saved_result is not None:
            result = saved_result
            state = self._output_exporter(result)
            self.set(state)
            await self.populate(session)
        else:
            await self._repository.insert(session, result)
            state = self._output_exporter(result)
            self.id_provider.set(state.get(self._id_attr))
            await self.id_provider.populate(session)
            # await self.id_provider.append(session, getattr(result, self._id_attr))
        # self.set() could reset self._output
        self._output = result

        # Create dependent entities AFTER this aggregate is created (they need its ID for FK)
        if self._dependent_providers:
            dependency_id = self.id_provider.get()
            for attr, dep_provider in self._dependent_providers.items():
                dep_provider.set_dependency_id(dependency_id)
                await dep_provider.populate(session)
                await dep_provider.create(session)

        return result

    async def populate(self, session: ISession) -> None:
        # Prevent diamond problem (cycles in FK)
        # See also https://github.com/mikeboers/C3Linearize
        if self.is_complete():
            return
        await self.do_populate(session)
        for attr, provider in self._providers.items():
            await provider.populate(session)

    async def do_populate(self, session: ISession) -> None:
        pass

    async def _default_factory(self, session: ISession, position: typing.Optional[int] = None):
        data = dict()
        for attr, provider in self._providers.items():
            data[attr] = await provider.create(session)
        return self._output_factory(**data)

    @property
    def id_provider(self):
        return getattr(self, self._id_attr)

    async def setup(self, session: ISession):
        await self._repository.setup(session)
        await super().setup(session)

    async def cleanup(self, session: ISession):
        await self._repository.cleanup(session)
        await super().cleanup(session)
