import random
import typing
from typing import Hashable, Callable

from ascetic_ddd.disposable import IDisposable
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification

__all__ = ('NullableDistributor',)


T = typing.TypeVar("T", covariant=True)


class NullableDistributor(IM2ODistributor[T], typing.Generic[T]):
    _delegate: IM2ODistributor[T]
    _null_weight: float
    _default_spec: ISpecification

    def __init__(
            self,
            delegate: IM2ODistributor[T],
            null_weight: float = 0
    ):
        self._delegate = delegate
        self._null_weight = null_weight
        self._default_spec = EmptySpecification()

    async def next(
            self,
            session: ISession,
            specification: ISpecification[T] | None = None,
    ) -> T | None:
        if specification is None:
            specification = self._default_spec
        # if isinstance(specification, EmptySpecification) and self._null_weight > 0 and self._is_null():
        if self._null_weight > 0 and self._is_null():
            return None
        return await self._delegate.next(session, specification)

    @property
    def provider_name(self):
        return self._delegate.provider_name

    @provider_name.setter
    def provider_name(self, value):
        self._delegate.provider_name = value

    def _is_null(self) -> bool:
        return random.random() < self._null_weight

    def attach(self, aspect: Hashable, observer: Callable, id_: Hashable | None = None) -> IDisposable:
        return self._delegate.attach(aspect, observer, id_)

    def detach(self, aspect, observer, id_: Hashable | None = None):
        return self._delegate.detach(aspect, observer, id_)

    def notify(self, aspect, *args, **kwargs):
        return self._delegate.notify(aspect, *args, **kwargs)

    async def anotify(self, aspect: Hashable, *args, **kwargs):
        return await self._delegate.anotify(aspect, *args, **kwargs)

    async def append(self, session: ISession, value: T):
        await self._delegate.append(session, value)

    async def setup(self, session: ISession):
        await self._delegate.setup(session)

    async def cleanup(self, session: ISession):
        await self._delegate.cleanup(session)

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    def bind_external_source(self, external_source: typing.Any) -> None:
        """Делегирует привязку внешнего источника данных."""
        self._delegate.bind_external_source(external_source)
