import datetime
import typing

__all__ = ("IDump", "IFileDump",)


class IFileDump(typing.Protocol):

    async def exists(self, name: str) -> bool:
        ...

    @property
    def ttl(self) -> datetime.timedelta:
        ...

    async def dump(self, name: str):
        ...

    async def load(self, name: str):
        ...

    def make_filepath(self, name: str) -> str:
        ...


class IDump(typing.Protocol):

    async def dump(self, out: typing.IO[bytes]):
        ...

    async def load(self, in_: typing.IO[bytes]):
        ...
