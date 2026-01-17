import datetime
import os
import typing

from ascetic_ddd.faker.infrastructure.dump.interfaces import IFileDump

__all__ = ("CompositeDump",)


class CompositeDump(IFileDump):
    _delegates: typing.Iterable[IFileDump]

    def __init__(self, delegates: typing.Iterable[IFileDump]):
        self._delegates = delegates

    async def exists(self, name: str) -> bool:
        return all(await delegate.exists(name) for delegate in self._delegates)

    @property
    def ttl(self) -> datetime.timedelta:
        return min(delegate.ttl for delegate in self._delegates)

    async def dump(self, name: str):
        for delegate in self._delegates:
            await delegate.dump(name)

    async def load(self, name: str):
        for delegate in self._delegates:
            await delegate.load(name)

    def make_filepath(self, name: str) -> str:
        return os.path.commonprefix([delegate.make_filepath(name) for delegate in self._delegates])
