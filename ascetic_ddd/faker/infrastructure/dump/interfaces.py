import datetime
import typing
from abc import ABCMeta, abstractmethod

__all__ = ("IDump", "IFileDump",)


class IFileDump(typing.Protocol, metaclass=ABCMeta):

    @abstractmethod
    async def exists(self, name: str) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def ttl(self) -> datetime.timedelta:
        raise NotImplementedError

    @abstractmethod
    async def dump(self, name: str):
        raise NotImplementedError

    @abstractmethod
    async def load(self, name: str):
        raise NotImplementedError

    @abstractmethod
    def make_filepath(self, name: str) -> str:
        raise NotImplementedError


class IDump(typing.Protocol, metaclass=ABCMeta):

    @abstractmethod
    async def dump(self, out: typing.IO[bytes]):
        raise NotImplementedError

    @abstractmethod
    async def load(self, in_: typing.IO[bytes]):
        raise NotImplementedError
