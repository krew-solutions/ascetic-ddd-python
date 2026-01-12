import copy
import typing
import abc
from collections.abc import Hashable, Callable

from ascetic_ddd.disposable import IDisposable
from ascetic_ddd.faker.domain.distributors.interfaces import IDistributor
from ascetic_ddd.faker.domain.providers.interfaces import IDiscreteValueProvider, INameable, IShunt, ICloneable
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.values.empty import empty, Empty

__all__ = (
    'ObservableMixin',
    'NameableMixin',
    'CloneableMixin',
    'Shunt',
    'BaseProvider',
    'BaseDistributorProvider',
)

from ascetic_ddd.observable.interfaces import IObservable

from ascetic_ddd.observable.observable import Observable

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")
T_Cloneable = typing.TypeVar("T_Cloneable")


class ObservableMixin(Observable, IObservable, metaclass=abc.ABCMeta):

    _aspect_mapping = {
        "distributor": "_distributor"
    }

    def _split_aspect(self, aspect: typing.Hashable) -> tuple[str | None, typing.Hashable]:
        if isinstance(aspect, str) and "." in aspect:
            attr, inner_aspect = aspect.split('.', maxsplit=1)
            attr = self._aspect_mapping.get(attr, attr)
            return attr, inner_aspect
        return None, aspect

    def attach(self, aspect: Hashable, observer: Callable, id_: Hashable | None = None) -> IDisposable:
        attr, inner_aspect = self._split_aspect(aspect)
        if attr is not None:
            return getattr(self, attr).attach(inner_aspect, observer, id_)
        else:
            return super().attach(inner_aspect, observer, id_)

    def detach(self, aspect: Hashable, observer: Callable, id_: Hashable | None = None):
        attr, inner_aspect = self._split_aspect(aspect)
        if attr is not None:
            return getattr(self, attr).detach(inner_aspect, observer, id_)
        else:
            super().detach(inner_aspect, observer, id_)

    def notify(self, aspect: Hashable, *args, **kwargs):
        attr, inner_aspect = self._split_aspect(aspect)
        if attr is not None:
            return getattr(self, attr).notify(inner_aspect, *args, **kwargs)
        else:
            super().notify(inner_aspect, *args, **kwargs)

    async def anotify(self, aspect: Hashable, *args, **kwargs):
        attr, inner_aspect = self._split_aspect(aspect)
        if attr is not None:
            return await getattr(self, attr).anotify(inner_aspect, *args, **kwargs)
        else:
            await super().anotify(inner_aspect, *args, **kwargs)


class NameableMixin(INameable, metaclass=abc.ABCMeta):
    _provider_name: str | None = None

    @property
    def provider_name(self) -> str:
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value: str):
        if self._provider_name is None:
            self._provider_name = value


class Shunt(IShunt):

    def __init__(self):
        self._data = {}

    def __getitem__(self, key: typing.Hashable) -> typing.Any:
        return self._data[key]

    def __setitem__(self, key: typing.Hashable, value: typing.Any):
        self._data[key] = value

    def __contains__(self, key: typing.Hashable):
        return key in self._data


class CloneableMixin(ICloneable):

    def empty(self, shunt: IShunt | None = None) -> typing.Self:
        if shunt is None:
            shunt = Shunt()
        if self in shunt:
            return shunt[self]
        c = copy.copy(self)
        self.do_empty(c, shunt)
        shunt[self] = c
        return c

    def do_empty(self, clone: typing.Self, shunt: IShunt):
        pass


class BaseProvider(
    NameableMixin,
    ObservableMixin,
    CloneableMixin,
    IDiscreteValueProvider[T_Input, T_Output],
    typing.Generic[T_Input, T_Output],
    metaclass=abc.ABCMeta
):
    _value: T_Input | Empty = empty

    def reset(self) -> None:
        self._value = empty
        self.notify('value', self._value)

    def set(self, value: T) -> None:
        self._value = value
        self.notify('value', self._value)

    def do_empty(self, clone: typing.Self, shunt: IShunt):
        clone._value = empty

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass


class BaseDistributorProvider(BaseProvider[T_Input, T_Output], typing.Generic[T_Input, T_Output],
                              metaclass=abc.ABCMeta):
    _distributor: IDistributor[T_Input]

    @property
    def provider_name(self) -> str:
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value: str):
        self._provider_name = value
        self._distributor.provider_name = value

    async def setup(self, session: ISession):
        await self._distributor.setup(session)
        await super().setup(session)

    async def cleanup(self, session: ISession):
        await self._distributor.cleanup(session)
        await super().cleanup(session)
