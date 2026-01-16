import typing

from ascetic_ddd.seedwork.domain.utils.data import freeze


__all__ = ('Json', )


class Json:

    def __init__(self, obj: typing.Any):
        self.obj = obj

    def __hash__(self):
        return hash(freeze(self.obj))
