import typing
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Hashable

from hypothesis import strategies

from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.observable.interfaces import IObservable


__all__ = (
    'IDiscreteValueProvider',
    'IReferenceProvider',
    'IEntityProvider',
    'IRelativeProvider',
    'INameable',
    'IShunt',
    'ICloneable',
    'IProbabilityDistributionDiscreteValueProvider',
    'IValueGenerator',
    'IValueAnyGenerator',
)

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")
T_Cloneable = typing.TypeVar("T_Cloneable")


class INameable(typing.Protocol, metaclass=ABCMeta):

    @property
    @abstractmethod
    def provider_name(self) -> str:
        raise NotImplementedError

    @provider_name.setter
    @abstractmethod
    def provider_name(self, value: str):
        raise NotImplementedError


class IShunt(metaclass=ABCMeta):

    def __getitem__(self, key: typing.Hashable) -> typing.Any:
        raise NotImplementedError

    def __setitem__(self, key: typing.Hashable, value: typing.Any):
        raise NotImplementedError

    def __contains__(self, key: typing.Hashable):
        raise NotImplementedError


class ICloneable(typing.Protocol, metaclass=ABCMeta):

    @abstractmethod
    def empty(self, shunt: IShunt | None = None) -> typing.Self:
        # For older python: def empty(self: T_Cloneable, shunt: IShunt | None = None) -> T_Cloneable:
        raise NotImplementedError

    @abstractmethod
    def do_empty(self, clone: typing.Self, shunt: IShunt):
        raise NotImplementedError


class ISetupable(typing.Protocol, metaclass=ABCMeta):

    @abstractmethod
    async def setup(self, session: ISession):
        raise NotImplementedError

    @abstractmethod
    async def cleanup(self, session: ISession):
        raise NotImplementedError


class IProvidable(typing.Protocol, metaclass=ABCMeta):

    @abstractmethod
    def reset(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def populate(self, session: ISession) -> None:
        raise NotImplementedError


class IMutable(typing.Protocol[T_Input, T_Output], metaclass=ABCMeta):

    @abstractmethod
    async def create(self, session: ISession) -> T_Output:
        raise NotImplementedError

    @abstractmethod
    def set(self, value: T_Input) -> None:
        raise NotImplementedError


class IDiscreteValueProvider(
    IMutable[T_Input, T_Output], IProvidable, IObservable, INameable, ICloneable,
    ISetupable, typing.Protocol[T_Input, T_Output], metaclass=ABCMeta
):
    pass


class IProbabilityDistributionDiscreteValueProvider(
    IDiscreteValueProvider[T_Input, T_Output], typing.Protocol[T_Input, T_Output], metaclass=ABCMeta
):

    @abstractmethod
    def is_complete(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def append(self, session: ISession, value: T_Output):
        raise NotImplementedError


class IEntityProvider(IMutable[T_Input, T_Output], IProvidable, IObservable, INameable, ICloneable,
                      ISetupable, typing.Protocol[T_Input, T_Output], metaclass=ABCMeta):

    @abstractmethod
    def on_init(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def id_provider(self) -> IDiscreteValueProvider[T] | IProbabilityDistributionDiscreteValueProvider[T]:
        raise NotImplementedError


class IReferenceProvider(IDiscreteValueProvider[T_Input, T_Output],
                         typing.Protocol[T_Input, T_Output], metaclass=ABCMeta):

    @property
    @abstractmethod
    def aggregate_provider(self) -> IEntityProvider[T_Input, T_Output]:
        raise NotImplementedError

    @aggregate_provider.setter
    @abstractmethod
    def aggregate_provider(
            self,
            aggregate_provider: IEntityProvider[T_Input, T_Output] | Callable[[], IEntityProvider[T_Input, T_Output]]
    ) -> None:
        raise NotImplementedError


class IRelativeProvider(typing.Protocol, metaclass=ABCMeta):

    @abstractmethod
    def set_scope(self, scope: Hashable) -> None:
        raise NotImplementedError


class IValueGenerator(typing.Protocol[T_Input]):
    """
    Фабрика значений для дистрибьюторов.
    Принимает session и опциональный position (номер в последовательности).
    """

    async def __call__(self, session: ISession, position: int | None = None) -> T_Input:
        ...


IValueAnyGenerator: typing.TypeAlias = (
    IValueGenerator[T_Input] | typing.Iterable[T_Input] | strategies.SearchStrategy[T_Input] | Callable
)
