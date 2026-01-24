import typing

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.observable.observable import Observable
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification

__all__ = ('DummyDistributor',)

T = typing.TypeVar("T", covariant=True)


class DummyDistributor(Observable, IM2ODistributor[T], typing.Generic[T]):
    _provider_name: str | None = None

    async def next(
            self,
            session: ISession,
            specification: ISpecification[T] | None = None,
    ) -> T:
        raise Cursor(
            position=None,
            callback=self._append,
        )

    async def _append(self, session: ISession, value: T, position: int | None):
        await self.anotify('value', session, value)

    async def append(self, session: ISession, value: T):
        await self._append(session, value, None)

    @property
    def provider_name(self):
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value):
        if self._provider_name is None:
            self._provider_name = value

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    def bind_external_source(self, external_source: typing.Any) -> None:
        """DummyDistributor не использует external_source."""
        pass
