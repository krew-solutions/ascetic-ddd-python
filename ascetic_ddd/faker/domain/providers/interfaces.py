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
    'ICompositeValueProvider',
    'IEntityProvider',
    'IAggregateProvider',
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
    """
    Структура EntityProvider не совпадает со структурой агрегата,
    если агрегат приводится в требуемое состояние многоходово
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
        Для CompositeProvider не используем **kwargs,
        т.к. иначе придется инспектировать сигнатуру каждого вложенного вызываемого сеттера
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


class IValueProvider(
    IInputOutput[T_Input, T_Output], IProvidable, IObservable, INameable, ICloneable,
    ISetupable, typing.Generic[T_Input, T_Output], metaclass=ABCMeta
):
    pass


class IRelativeValueProvider(IValueProvider[T_Input, T_Output], typing.Generic[T_Input, T_Output], metaclass=ABCMeta):

    @abstractmethod
    def set_scope(self, scope: Hashable) -> None:
        raise NotImplementedError


class ICompositeValueProvider(
    IValueProvider[T_Input, T_Output], typing.Generic[T_Input, T_Output], metaclass=ABCMeta
):
    @property
    @abstractmethod
    def providers(self) -> dict[str, IValueProvider[typing.Any, typing.Any]]:
        raise NotImplementedError

    @property
    @abstractmethod
    def dependent_providers(self) -> dict[str, 'IDependentProvider[typing.Any, typing.Any, typing.Any]']:
        raise NotImplementedError


class IEntityProvider(
    ICompositeValueProvider[T_Input, T_Output], typing.Generic[T_Input, T_Output], metaclass=ABCMeta
):
    @abstractmethod
    def on_init(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def id_provider(self) -> IValueProvider[T_Input, T_Output]:
        raise NotImplementedError


class IAggregateProvider(
    IEntityProvider[T_Input, T_Output], typing.Generic[T_Input, T_Output], metaclass=ABCMeta
):
    # TODO: Add repository here, move id_provider here
    pass


class IReferenceProvider(
    IValueProvider[T_Input, T_Id_Output],
    typing.Generic[T_Input, T_Output, T_Id_Output], metaclass=ABCMeta
):

    @property
    @abstractmethod
    def aggregate_provider(self) -> IAggregateProvider[T_Input, T_Output]:
        raise NotImplementedError

    @aggregate_provider.setter
    @abstractmethod
    def aggregate_provider(
            self,
            aggregate_provider: IAggregateProvider[T_Input, T_Output] | Callable[[], IAggregateProvider[T_Input, T_Output]]
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
    def aggregate_providers(self) -> list[IAggregateProvider[T_Input, T_Output]]:
        raise NotImplementedError

    @aggregate_providers.setter
    @abstractmethod
    def aggregate_providers(
            self,
            aggregate_provider: list[IAggregateProvider[T_Input, T_Output] |
                                     Callable[[], IAggregateProvider[T_Input, T_Output]]]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_dependency_id(self, dependency_id: typing.Any) -> None:
        raise NotImplementedError
