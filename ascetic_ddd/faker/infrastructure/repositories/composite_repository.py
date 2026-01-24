import typing
from collections.abc import Callable, Hashable

from ascetic_ddd.faker.domain.providers.aggregate_provider import IAggregateRepository
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.infrastructure.distributors.m2o.interfaces import IPgExternalSource
from ascetic_ddd.seedwork.domain.identity.interfaces import IAccessible
from ascetic_ddd.disposable.interfaces import IDisposable


__all__ = ('CompositeRepository', 'CompositeAutoPkRepository',)


T = typing.TypeVar("T", covariant=True)


class CompositeRepository(typing.Generic[T]):
    _external_repository: IAggregateRepository[T]
    _internal_repository: IAggregateRepository[T] | IPgExternalSource

    def __init__(
            self,
            external_repository: IAggregateRepository[T],
            internal_repository: IAggregateRepository[T],
    ):
        self._external_repository = external_repository
        self._internal_repository = internal_repository

    @property
    def table(self) -> str:
        return self._internal_repository.table

    async def insert(self, session: ISession, agg: T):
        await self._internal_repository.insert(session, agg)  # Lock it first.
        await self._external_repository.insert(session, agg)

    async def update(self, session: ISession, agg: T):
        await self._internal_repository.update(session, agg)
        await self._external_repository.update(session, agg)

    async def get(self, session: ISession, id_: IAccessible[typing.Any]) -> T | None:
        return await self._internal_repository.get(session, id_)

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[T]:
        return await self._internal_repository.find(session, specification)

    async def setup(self, session: ISession):
        await self._internal_repository.setup(session)

    async def cleanup(self, session: ISession):
        await self._internal_repository.cleanup(session)

    # IObservable delegation to internal_repository

    def attach(self, aspect: Hashable, observer: Callable, id_: Hashable | None = None) -> IDisposable:
        return self._internal_repository.attach(aspect, observer, id_)

    def detach(self, aspect: Hashable, observer: Callable, id_: Hashable | None = None):
        return self._internal_repository.detach(aspect, observer, id_)

    def notify(self, aspect: Hashable, *args, **kwargs):
        return self._internal_repository.notify(aspect, *args, **kwargs)

    async def anotify(self, aspect: Hashable, *args, **kwargs):
        return await self._internal_repository.anotify(aspect, *args, **kwargs)


class CompositeAutoPkRepository(CompositeRepository[T], typing.Generic[T]):

    async def insert(self, session: ISession, agg: T):
        await self._external_repository.insert(session, agg)  # But ID can be undefined!
        await self._internal_repository.insert(session, agg)
