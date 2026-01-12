import typing
from abc import ABCMeta, abstractmethod

from ascetic_ddd.observable.interfaces import IObservable
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification

__all__ = (
    'IDistributor',
    'IDistributorFactory',
)


T = typing.TypeVar("T", covariant=True)


class IDistributor(IObservable, typing.Generic[T], metaclass=ABCMeta):

    @abstractmethod
    async def next(
            self,
            session: ISession,  # To get Redis connect from it.
            specification: ISpecification[T] | None = None,
    ) -> T:
        """
        Returns next value from distribution.
        Raises StopAsyncIteration(num) when scale is reached, signaling caller to create new value.
        num is sequence position (for SequenceDistributor) or None.
        """
        raise NotImplementedError

    @abstractmethod
    async def append(self, session: ISession, value: T):
        raise NotImplementedError

    @property
    @abstractmethod
    def provider_name(self):
        raise NotImplementedError

    @provider_name.setter
    @abstractmethod
    def provider_name(self, value):
        raise NotImplementedError

    @abstractmethod
    async def setup(self, session: ISession):
        raise NotImplementedError

    @abstractmethod
    async def cleanup(self, session: ISession):
        raise NotImplementedError

    @abstractmethod
    def __copy__(self):
        raise NotImplementedError

    @abstractmethod
    def __deepcopy__(self, memodict={}):
        raise NotImplementedError


class IDistributorFactory(typing.Protocol[T], metaclass=ABCMeta):

    @abstractmethod
    def __call__(
        self,
        weights: list[float] | None = None,
        scale: float | None = None,
        null_weight: float = 0,
    ) -> IDistributor[T]:
        raise NotImplementedError
