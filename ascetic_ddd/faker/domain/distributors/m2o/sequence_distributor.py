import typing
from collections import defaultdict

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.observable.observable import Observable
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification

__all__ = ('SequenceDistributor',)

T = typing.TypeVar("T", covariant=True)


class SequenceDistributor(Observable, IM2ODistributor[T], typing.Generic[T]):
    _sequences: dict[ISpecification, int]
    _provider_name: str | None = None
    _default_spec: ISpecification

    def __init__(self):
        self._sequences = defaultdict(int)
        self._default_spec = EmptySpecification()
        super().__init__()

    async def next(
            self,
            session: ISession,
            specification: ISpecification[T] | None = None,
    ) -> T:
        if specification is None:
            specification = self._default_spec
        position = self._sequences[specification]
        self._sequences[specification] += 1
        raise Cursor(
            position=position,
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
        """SequenceDistributor не использует external_source."""
        pass
