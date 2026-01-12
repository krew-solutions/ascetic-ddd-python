from abc import ABCMeta, abstractmethod

__all__ = ("IDisposable",)


class IDisposable(metaclass=ABCMeta):
    @abstractmethod
    async def dispose(self):
        raise NotImplementedError

    @abstractmethod
    def __add__(self, other: "IDisposable") -> "IDisposable":
        raise NotImplementedError
