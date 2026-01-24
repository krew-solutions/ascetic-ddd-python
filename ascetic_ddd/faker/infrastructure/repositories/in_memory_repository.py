import typing

from ascetic_ddd.seedwork.domain.identity.interfaces import IAccessible
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.seedwork.domain.utils.data import hashable
from ascetic_ddd.observable.observable import Observable

__all__ = ('InMemoryRepository',)


T = typing.TypeVar("T", covariant=True)


class InMemoryRepository(Observable, typing.Generic[T]):
    _id_attr: str = None
    _aggregates: dict[T]
    _agg_exporter: typing.Callable[[T], dict]

    def __init__(self, agg_exporter: typing.Callable[[T], dict], id_attr: str = None):
        super().__init__()
        self._agg_exporter = agg_exporter
        self._id_attr = id_attr
        self._aggregates = dict()

    async def insert(self, session: ISession, agg: T):
        state = self._agg_exporter(agg)
        id_ = self._id(state)
        self._aggregates[hashable(id_)] = agg
        await self.anotify('inserted', session, agg)

    async def update(self, session: ISession, agg: T):
        state = self._agg_exporter(agg)
        id_ = self._id(state)
        self._aggregates[hashable(id_)] = agg
        await self.anotify('updated', session, agg)

    async def get(self, session: ISession, id_: IAccessible[typing.Any] | typing.Any) -> T | None:
        key = id_.value if hasattr(id_, 'value') else id_
        return self._aggregates.get(hashable(key))

    async def find(self, session: ISession, specification: ISpecification) -> typing.AsyncIterable[T]:
        """Add index support by extending the class"""
        for id_, agg in self._aggregates.items():
            if await specification.is_satisfied_by(session, agg):
                yield agg

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass

    def _id(self, state: dict) -> typing.Any:
        if self._id_attr is not None:
            return state.get(self._id_attr)
        return next(iter(state.values()))

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self
