import typing
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Hashable

from hypothesis import strategies

from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.observable.interfaces import IObservable


__all__ = (
    'IValueProvider',
    'IReferenceProvider',
    'IEntityProvider',
    'IRelativeProvider',
    'INameable',
    'IShunt',
    'ICloneable',
    'ISetupable',
    'IValueGenerator',
    'IValueAnyGenerator',
    'ICompositeValueProvider',
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

    @abstractmethod
    def is_complete(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def is_transient(self) -> bool:
        raise NotImplementedError


class IMutable(typing.Protocol[T_Input, T_Output], metaclass=ABCMeta):

    @abstractmethod
    async def create(self, session: ISession) -> T_Output:
        raise NotImplementedError

    @abstractmethod
    def set(self, value: T_Input) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self) -> T_Input:
        raise NotImplementedError

    @abstractmethod
    async def append(self, session: ISession, value: T_Output):
        raise NotImplementedError


class IValueProvider(
    IMutable[T_Input, T_Output], IProvidable, IObservable, INameable, ICloneable,
    ISetupable, typing.Protocol[T_Input, T_Output], metaclass=ABCMeta
):
    """
    Architecture:
    IValueProvider = f(input_value | None) = result,
    where
    result : T <- Distributor[T] <- (
        <- result : result ∈ Sᴛ ∧ P(specification) ~ 𝒟(S)  # select from a set with given probability distribution and Specification
        or
        <- result <- result_factory(input_value)
            <- input_value <- (
                set(value)
                or
                ValueGenerator(position | None) <- position | None
            )
        ),
    where
        ":" means instance of type,
        "<-" means "from",
        "∈" means belongs,
        "Sᴛ" or "{x : T}" means set of type "T",
        "∧" means satisfies the condition P(),
        "~ 𝒟(S)" means according to the probability distribution,
        "Σx" means composition of "x",
        "⊆" means subset of a composition.
    """
    pass


class ICompositeMutable(typing.Protocol[T_Input, T_Output], metaclass=ABCMeta):
    """
    Структура Provider не совпадает со структурой агрегата, если агрегат приводится в требуемое состояние многоходово
    (см. агрегат Specialist at grade project).
    Это подсказка на вопрос о том, должен ли Distributor хранить сырые значения провайдера или готовый агрегат.

    В method self.set(...) технически невозможно установить в качестве значения итоговый тип,
    т.к. для валидного его состояния банально может не хватать данных (Auto Increment PK, FK).
    """

    @abstractmethod
    async def create(self, session: ISession) -> T_Output:
        raise NotImplementedError

    @abstractmethod
    def set(self, value: T_Input) -> None:
        """
        Не используем **kwargs, т.к. иначе придется инспектировать сигнатуру каждого вложенного сеттера
        (композиция может быть вложенной).
        Ну и в принципе здесь можно принимать Specification вторым аргументом.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self) -> T_Input:
        raise NotImplementedError

    @abstractmethod
    async def append(self, session: ISession, value: T_Output):
        raise NotImplementedError


class ICompositeValueProvider(
    IMutable[T_Input, T_Output], IProvidable, IObservable, INameable, ICloneable,
    ISetupable, typing.Protocol[T_Input, T_Output], metaclass=ABCMeta
):
    """
    Architecture:
    ICompositeValueProvider = f(Σ input_value | None) = result,
    where
    result : T <- Distributor[T] <- (
        <- result : result ∈ Sᴛ ∧ P(specification) ~ 𝒟(S)  # select from a set with given probability distribution and Specification
        or
        <- result <- result_factory(Σ leaf_result)
            <- Σ IValueProvider(∈ Σ input_value) | ICompositeValueProvider(⊆ Σ input_value)
    ),
    where
        ":" means instance of type,
        "<-" means "from",
        "∈" means belongs,
        "Sᴛ" or "{x : T}" means set of type "T",
        "∧" means satisfies the condition P(),
        "~ 𝒟(S)" means according to the probability distribution,
        "Σx" means composition of "x",
        "⊆" means subset of a composition.
    """
    pass


class IEntityProvider(ICompositeMutable[T_Input, T_Output], IProvidable, IObservable, INameable, ICloneable,
                      ISetupable, typing.Protocol[T_Input, T_Output], metaclass=ABCMeta):

    @abstractmethod
    def on_init(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def id_provider(self) -> IValueProvider[T_Input, T_Output]:
        raise NotImplementedError


T_Id_Output = typing.TypeVar("T_Id_Output")


class IReferenceProvider(IValueProvider[T_Input, T_Id_Output],
                         typing.Protocol[T_Input, T_Output, T_Id_Output], metaclass=ABCMeta):

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


class IDependentMutable(typing.Protocol[T_Input, T_Output], metaclass=ABCMeta):

    @abstractmethod
    async def create(self, session: ISession) -> list[T_Output]:
        raise NotImplementedError

    @abstractmethod
    def set(self, value: list[T_Input]) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self) -> list[T_Input]:
        raise NotImplementedError


class IDependentProvider(
    IDependentMutable[T_Input, T_Id_Output], IProvidable, IObservable, INameable, ICloneable,
    ISetupable, typing.Protocol[T_Input, T_Output, T_Id_Output], metaclass=ABCMeta
):

    @property
    @abstractmethod
    def aggregate_providers(self) -> list[IEntityProvider[T_Input, T_Output]]:
        raise NotImplementedError

    @aggregate_providers.setter
    @abstractmethod
    def aggregate_providers(
            self,
            aggregate_provider: list[IEntityProvider[T_Input, T_Output] |
                                     Callable[[], IEntityProvider[T_Input, T_Output]]]
    ) -> None:
        raise NotImplementedError


class IRelativeProvider(typing.Protocol, metaclass=ABCMeta):

    @abstractmethod
    def set_scope(self, scope: Hashable) -> None:
        raise NotImplementedError


class IValueGenerator(typing.Protocol[T_Input], metaclass=ABCMeta):
    """
    Фабрика значений для дистрибьюторов.
    Принимает session и опциональный position (номер в последовательности).
    """

    async def __call__(self, session: ISession, position: int | None = None) -> T_Input:
        ...


IValueAnyGenerator: typing.TypeAlias = (
    IValueGenerator[T_Input] | typing.Iterable[T_Input] | strategies.SearchStrategy[T_Input] | Callable
)
