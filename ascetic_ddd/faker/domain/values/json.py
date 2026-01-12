import typing

from .utils import hashable


__all__ = ('Json', )


class Json:

    def __init__(self, obj: typing.Any):
        self.obj = obj

    def __hash__(self):
        return hash(hashable(self.obj))
