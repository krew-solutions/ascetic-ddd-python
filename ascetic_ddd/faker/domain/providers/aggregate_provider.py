import typing
from abc import ABCMeta, abstractmethod

from ascetic_ddd.faker.domain.distributors.m2o import IM2ODistributorFactory
from ascetic_ddd.faker.domain.providers._mixins import BaseCompositeProvider
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.providers.interfaces import IEntityProvider
from ascetic_ddd.faker.domain.session.interfaces import ISession


__all__ = ('IAggregateRepository', 'AggregateProvider',)


T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


class IAggregateRepository(typing.Protocol[T_Output], metaclass=ABCMeta):
    @abstractmethod
    async def insert(self, session: ISession, agg: T_Output):
        raise NotImplementedError

    @abstractmethod
    async def get(self, session: ISession, id_: typing.Any) -> T_Output | None:
        raise NotImplementedError

    @abstractmethod
    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[T_Output]:
        raise NotImplementedError

    @abstractmethod
    async def setup(self, session: ISession):
        raise NotImplementedError

    @abstractmethod
    async def cleanup(self, session: ISession):
        raise NotImplementedError


class AggregateProvider(
    BaseCompositeProvider[T_Input, T_Output],
    IEntityProvider[T_Input, T_Output],
    typing.Generic[T_Input, T_Output],
    metaclass=ABCMeta
):
    _id_attr: str
    _repository: IAggregateRepository[T_Output]
    _result_factory: typing.Callable[[...], T_Output]  # T_Output of each nested Provider.
    _result_exporter: typing.Callable[[T_Output], T_Input]

    def __init__(
            self,
            repository: IAggregateRepository,
            # distributor_factory: IM2ODistributorFactory,
            result_factory: typing.Callable[[...], T_Output] | None = None,  # T_Output of each nested Provider.
            result_exporter: typing.Callable[[T_Output], T_Input] | None = None,
    ):
        super().__init__()
        self._repository = repository

        if result_factory is not None:
            def result_factory(result):
                return result
        self._result_factory = result_factory

        if result_exporter is not None:
            def result_exporter(value):
                return value
        self._result_exporter = result_exporter

        self.on_init()

    def on_init(self):
        pass

    async def create(self, session: ISession) -> T_Output:
        if self._output_result is not None:
            return self._output_result
        result = self._default_factory(session)
        if self.id_provider.is_complete():
            # id_ здесь может быть еще неизвестен, т.к. агрегат не создан.
            # А может быть и известен, если его id_ реиспользуется как FK.
            id_ = await getattr(self, self._id_attr).create(session)
            saved_result = await self._repository.get(session, id_)
        else:
            saved_result = None
        if saved_result is not None:
            result = saved_result
            state = self._result_exporter(result)
            # Тут можно пооптимизировать.
            # Действительно ли нам нужно бояться повторного вызова метода populate()?
            self.reset()
            self.set(state)
            await self.populate(session)
        else:
            await self._repository.insert(session, result)
            state = self._result_exporter(result)
            self.id_provider.set(state.get(self._id_attr))
            await self.id_provider.populate(session)
            # await self.id_provider.append(session, getattr(result, self._id_attr))
        self._output_result = result
        return result

    async def populate(self, session: ISession) -> None:
        # Prevent diamond problem (cycles in FK)
        # See also https://github.com/mikeboers/C3Linearize
        if self.is_complete():
            return
        await self.do_populate(session)
        for attr, provider in self._providers.items():
            try:
                await provider.populate(session)
            except StopAsyncIteration:
                if attr == self._id_attr:
                    continue
                else:
                    raise

    async def do_populate(self, session: ISession) -> None:
        pass

    async def _default_factory(self, session: ISession, position: typing.Optional[int] = None):
        data = dict()
        for attr, provider in self._providers.items():
            data[attr] = await provider.create(session)
        return self._result_factory(**data)

    @property
    def id_provider(self):
        return getattr(self, self._id_attr)

    async def setup(self, session: ISession):
        await self._repository.setup(session)
        await super().setup(session)

    async def cleanup(self, session: ISession):
        await self._repository.cleanup(session)
        await super().cleanup(session)
