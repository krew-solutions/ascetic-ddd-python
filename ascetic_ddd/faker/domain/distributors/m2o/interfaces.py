import typing
from abc import ABCMeta, abstractmethod

from ascetic_ddd.observable.interfaces import IObservable
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification

__all__ = (
    'IM2ODistributor',
    'IM2ODistributorFactory',
    'ICursor',
)


T = typing.TypeVar("T", covariant=True)


class IRepository(IObservable, typing.Protocol[T], metaclass=ABCMeta):
    pass


class IM2ODistributor(IObservable, typing.Generic[T], metaclass=ABCMeta):

    @abstractmethod
    async def next(
            self,
            session: ISession,  # To get Redis connect from it.
            specification: ISpecification[T] | None = None,
    ) -> T:
        """
        Returns next value from distribution.
        Raises ICursor(num) when mean is reached, signaling caller to create new value.
        num is sequence position (for SequenceDistributor) or None.
        """
        raise NotImplementedError

    @abstractmethod
    async def append(self, session: ISession, value: T):
        """
        Appends value to the distributor.
        """
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


class ICursor(typing.Generic[T], StopAsyncIteration, metaclass=ABCMeta):
    """
    Заинтересованные декораторы должны перехватывать Cursor и создавать свой, если им нужно добавить объект к себе.
    Например, если WeightedDistributor станет декоратором для SequenceDistributor.
    """
    position: int

    @abstractmethod
    async def append(self, session: ISession, value: T):
        raise NotImplementedError


class IM2ODistributorFactory(typing.Protocol[T], metaclass=ABCMeta):

    @abstractmethod
    def __call__(
        self,
        weights: list[float] | None = None,
        skew: float | None = None,
        mean: float | None = None,
        null_weight: float = 0,
        sequence: bool = False
    ) -> IM2ODistributor[T]:
        """
        Фабрика для Distributor.

        Args:
            weights: If a weights sequence is specified, selections are made according to the relative weights.
            skew: Параметр перекоса (1.0 = равномерно, 2.0+ = перекос к началу). Default = 2.0
            mean: Среднее количество использований каждого значения. Use mean = 1 for unique.
            null_weight: Вероятность вернуть None (0-1)
            sequence: Pass sequence number to value generator.
        """
        raise NotImplementedError
