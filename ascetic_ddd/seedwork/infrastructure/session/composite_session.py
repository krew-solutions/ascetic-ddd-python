import typing
from collections.abc import Callable, Hashable

from ascetic_ddd.seedwork.domain.session.interfaces import ISessionPool, ISession

__all__ = (
    "CompositeSessionPool",
    "CompositeSession",
)

T = typing.TypeVar("T", covariant=True)


class CompositeAsyncContextManager(typing.Generic[T]):
    _delegates: typing.Iterable[typing.AsyncContextManager]
    _entered_delegates: typing.Iterable[T]
    _factory: Callable[[typing.Iterable[T]], T]

    def __init__(self,
                 delegates: typing.Iterable[typing.AsyncContextManager],
                 factory: Callable[[typing.Iterable[T]], T]):
        self._delegates = delegates
        self._factory = factory

    async def __aenter__(self) -> T:
        delegates = []
        for i in self._delegates:
            delegates.append(await i.__aenter__())
        return self._factory(delegates)

    async def __aexit__(self, exc_type, exc, tb):
        for i in self._delegates:
            await i.__aexit__(exc_type, exc, tb)


class CompositeSessionPool(ISessionPool):
    _delegates: typing.Iterable[ISessionPool]

    def __init__(self, *delegates: ISessionPool) -> None:
        self._delegates = delegates

    def session(self) -> typing.AsyncContextManager[ISession]:
        delegates = tuple(delegate.session() for delegate in self._delegates)
        return CompositeAsyncContextManager[ISession](delegates, CompositeSession)

    def __getitem__(self, item):
        return list(self._delegates)[item]

    def _split_aspect(self, aspect: typing.Hashable) -> tuple[str | None, typing.Hashable]:
        if isinstance(aspect, str) and "." in aspect:
            item, inner_aspect = aspect.split('.', maxsplit=1)
            return item, inner_aspect
        return None, aspect

    def attach(self, aspect, observer, id_: Hashable | None = None):
        item, inner_aspect = self._split_aspect(aspect)
        if item is not None:
            return self[item].attach(inner_aspect, observer, id_)

    def detach(self, aspect, observer, id_: Hashable | None = None):
        item, inner_aspect = self._split_aspect(aspect)
        if item is not None:
            return self[item].detach(inner_aspect, observer, id_)

    def notify(self, aspect, *args, **kwargs):
        item, inner_aspect = self._split_aspect(aspect)
        if item is not None:
            return self[item].notify(inner_aspect, *args, **kwargs)

    async def anotify(self, aspect, *args, **kwargs):
        item, inner_aspect = self._split_aspect(aspect)
        if item is not None:
            return await self[item].anotify(inner_aspect, *args, **kwargs)


class CompositeSession(ISession):
    _delegates: typing.Iterable[ISession]
    _parent: typing.Optional["CompositeSession"]

    def __init__(self, delegates: typing.Iterable[ISession],  parent: typing.Optional["CompositeSession"] = None):
        self._delegates = delegates
        self._parent = parent

    def atomic(self) -> typing.AsyncContextManager[ISession]:
        delegates = tuple(delegate.atomic() for delegate in self._delegates)

        def _factory(_delegates: typing.Iterable[ISession]):
            return CompositeTransactionSession(_delegates, self)

        return CompositeAsyncContextManager[ISession](delegates, _factory)

    def __getattr__(self, item):
        for delegate in self._delegates:
            if hasattr(delegate, item):
                return getattr(delegate, item)
        raise AttributeError

    def __getitem__(self, item):
        return list(self._delegates)[item]

    def _split_aspect(self, aspect: typing.Hashable) -> tuple[str | None, typing.Hashable]:
        if isinstance(aspect, str) and "." in aspect:
            item, inner_aspect = aspect.split('.', maxsplit=1)
            return item, inner_aspect
        return None, aspect

    def attach(self, aspect, observer, id_: Hashable | None = None):
        item, inner_aspect = self._split_aspect(aspect)
        if item is not None:
            return self[item].attach(inner_aspect, observer, id_)

    def detach(self, aspect, observer, id_: Hashable | None = None):
        item, inner_aspect = self._split_aspect(aspect)
        if item is not None:
            return self[item].detach(inner_aspect, observer, id_)

    def notify(self, aspect, *args, **kwargs):
        item, inner_aspect = self._split_aspect(aspect)
        if item is not None:
            return self[item].notify(inner_aspect, *args, **kwargs)

    async def anotify(self, aspect, *args, **kwargs):
        item, inner_aspect = self._split_aspect(aspect)
        if item is not None:
            return await self[item].anotify(inner_aspect, *args, **kwargs)


class CompositeTransactionSession(CompositeSession):
    pass
