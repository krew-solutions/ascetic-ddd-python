import typing
from abc import ABCMeta

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor
from ascetic_ddd.faker.domain.providers._mixins import BaseCompositeProvider
from ascetic_ddd.faker.domain.providers.interfaces import IEntityProvider
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.values.empty import empty

T_Input = typing.TypeVar("T_Input")
T_Output = typing.TypeVar("T_Output")


__all__ = ('EntityProvider',)


class EntityProvider(
    BaseCompositeProvider[T_Input, T_Output],
    IEntityProvider[T_Input, T_Output],
    typing.Generic[T_Input, T_Output],
    metaclass=ABCMeta
):
    _id_attr: str
    _output_factory: typing.Callable[[...], T_Output] = None
    _output_exporter: typing.Callable[[T_Output], T_Input] = None

    def __init__(
            self,
            output_factory: typing.Callable[[...], T_Output] | None = None,  # T_Output of each nested Provider.
            output_exporter: typing.Callable[[T_Output], T_Input] | None = None,
    ):

        if self._output_factory is None:
            if output_factory is None:

                def output_factory(**kwargs):
                    return kwargs

            self._output_factory = output_factory

        if self._output_exporter is None:
            if output_exporter is None:

                def output_exporter(value):
                    return value

            self._output_exporter = output_exporter

        super().__init__()
        self.on_init()

    def on_init(self):
        pass

    async def create(self, session: ISession) -> T_Output:
        if self._output is empty:
            self._output = await self._default_factory(session)
        return self._output

    async def _default_factory(self, session: ISession, position: typing.Optional[int] = None):
        data = dict()
        for attr, provider in self._providers.items():
            data[attr] = await provider.create(session)
        return self._output_factory(**data)

    @property
    def id_provider(self):
        return getattr(self, self._id_attr)

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return
        await self.do_populate(session)
        for attr, provider in self._providers.items():
            try:
                await provider.populate(session)
            except ICursor:
                if attr == self._id_attr:
                    continue
                else:
                    raise

    async def do_populate(self, session: ISession) -> None:
        pass
