import typing

from ascetic_ddd.faker.domain.providers.aggregate_provider import IAggregateRepository
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.seedwork.domain.identity.interfaces import IAccessible


__all__ = ('CompositeRepository', 'CompositeAutoPkRepository',)


T = typing.TypeVar("T", covariant=True)


class CompositeRepository(typing.Generic[T]):
    _external_repository: IAggregateRepository[T]
    _internal_repository: IAggregateRepository[T]

    def __init__(
            self,
            external_repository: IAggregateRepository[T],
            internal_repository: IAggregateRepository[T],
    ):
        self._external_repository = external_repository
        self._internal_repository = internal_repository

    async def insert(self, session: ISession, agg: T):
        await self._internal_repository.insert(session, agg)  # Lock it first.
        await self._external_repository.insert(session, agg)

    async def get(self, session: ISession, id_: IAccessible[typing.Any]) -> T | None:
        return await self._internal_repository.get(session, id_)

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[T]:
        return await self._internal_repository.find(session, specification)

    async def setup(self, session: ISession):
        await self._internal_repository.setup(session)

    async def cleanup(self, session: ISession):
        await self._internal_repository.cleanup(session)


class CompositeAutoPkRepository(CompositeRepository[T], typing.Generic[T]):

    async def insert(self, session: ISession, agg: T):
        await self._external_repository.insert(session, agg)  # But ID can be undefined!
        await self._internal_repository.insert(session, agg)
