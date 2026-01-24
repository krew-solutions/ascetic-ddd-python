import typing
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Hashable

from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.observable.interfaces import IObservable


__all__ = (
    'INameable',
    'ICloningShunt',
    'ICloneable',
    'ISetupable',
    'IProvidable',
    'IInputOutput',
    'IValueProvider',
    'IRelativeValueProvider',
    'ICompositeInputOutput',
    'ICompositeValueProvider',
    'IEntityProvider',
    'IReferenceProvider',
    'IDependentInputOutput',
    'IDependentProvider',
)

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")
T_Cloneable = typing.TypeVar("T_Cloneable")
T_Id_Output = typing.TypeVar("T_Id_Output")


class INameable(metaclass=ABCMeta):

    @property
    @abstractmethod
    def provider_name(self) -> str:
        raise NotImplementedError

    @provider_name.setter
    @abstractmethod
    def provider_name(self, value: str):
        raise NotImplementedError


class ICloningShunt(metaclass=ABCMeta):

    @abstractmethod
    def __getitem__(self, key: typing.Hashable) -> typing.Any:
        raise NotImplementedError

    @abstractmethod
    def __setitem__(self, key: typing.Hashable, value: typing.Any):
        raise NotImplementedError

    @abstractmethod
    def __contains__(self, key: typing.Hashable):
        raise NotImplementedError


class ICloneable(metaclass=ABCMeta):

    @abstractmethod
    def empty(self, shunt: ICloningShunt | None = None) -> typing.Self:
        # For older python: def empty(self: T_Cloneable, shunt: IShunt | None = None) -> T_Cloneable:
        raise NotImplementedError

    @abstractmethod
    def do_empty(self, clone: typing.Self, shunt: ICloningShunt):
        raise NotImplementedError


class ISetupable(metaclass=ABCMeta):

    @abstractmethod
    async def setup(self, session: ISession):
        raise NotImplementedError

    @abstractmethod
    async def cleanup(self, session: ISession):
        raise NotImplementedError


class IProvidable(metaclass=ABCMeta):

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


class IInputOutput(typing.Generic[T_Input, T_Output], metaclass=ABCMeta):

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
    IInputOutput[T_Input, T_Output], IProvidable, IObservable, INameable, ICloneable,
    ISetupable, typing.Generic[T_Input, T_Output], metaclass=ABCMeta
):
    """
    Immutable.
    Architecture:
    IValueProvider = f(input | None) = result,
    where
    result : T <- Distributor[T] <- (
        <- result : result ∈ Sᴛ ∧ P(specification) ~ 𝒟(S)  # select from a set with given probability distribution and Specification
        or
        <- result <- output_factory(input)
            <- input <- (
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


class IRelativeValueProvider(IValueProvider[T_Input, T_Output], typing.Generic[T_Input, T_Output], metaclass=ABCMeta):

    @abstractmethod
    def set_scope(self, scope: Hashable) -> None:
        raise NotImplementedError


class ICompositeInputOutput(typing.Generic[T_Input, T_Output], metaclass=ABCMeta):
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
    IInputOutput[T_Input, T_Output], IProvidable, IObservable, INameable, ICloneable,
    ISetupable, typing.Generic[T_Input, T_Output], metaclass=ABCMeta
):
    """
    Immutable. Composite ValueObject.
    Architecture:
    ICompositeValueProvider = f(Σ input | None) = result,
    where
    result : T <- Distributor[T] <- (
        <- result : result ∈ Sᴛ ∧ P(specification) ~ 𝒟(S)  # select from a set with given probability distribution and Specification
        or
        <- result <- output_factory(Σ leaf_result)
            <- Σ IValueProvider(∈ Σ input) | ICompositeValueProvider(⊆ Σ input)
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


class IEntityProvider(
    ICompositeInputOutput[T_Input, T_Output], IProvidable, IObservable, INameable, ICloneable,
    ISetupable, typing.Generic[T_Input, T_Output], metaclass=ABCMeta
):
    """
    Mutable. Saved as part of aggregate.
    """

    @abstractmethod
    def on_init(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def id_provider(self) -> IValueProvider[T_Input, T_Output]:
        raise NotImplementedError


class IReferenceProvider(
    IValueProvider[T_Input, T_Id_Output],
    typing.Generic[T_Input, T_Output, T_Id_Output], metaclass=ABCMeta
):

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


class IDependentInputOutput(typing.Generic[T_Input, T_Output], metaclass=ABCMeta):

    @abstractmethod
    async def create(self, session: ISession) -> list[T_Output]:
        raise NotImplementedError

    @abstractmethod
    def set(self, value: list[T_Input], weights: list[float] | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def get(self) -> list[T_Input]:
        raise NotImplementedError


class IDependentProvider(
    IDependentInputOutput[T_Input, T_Id_Output], IProvidable, IObservable, INameable, ICloneable,
    ISetupable, typing.Generic[T_Input, T_Output, T_Id_Output], metaclass=ABCMeta
):
    """
    Я думал над тем, чтоб разбить providers на m2o и o2m, но это было бы неуместно потому,
    что, например, для генерации значения зарплаты мы можем использовать IO2MDistributor,
    но это не o2m, это, по сути, m2o.

    Вместо m2o и o2m можно было бы использовать термины belongs и has,
    но они неуместны по отношению к простым значениями. Не может User принадлежать status.
    """

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

    @abstractmethod
    def set_dependency_id(self, dependency_id: typing.Any) -> None:
        raise NotImplementedError
