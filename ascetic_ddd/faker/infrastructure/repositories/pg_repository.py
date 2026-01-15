import typing

from ascetic_ddd.faker.infrastructure.utils.dict import flatten_dict
from ascetic_ddd.seedwork.infrastructure.utils.pg import escape
from ascetic_ddd.seedwork.domain.identity.interfaces import IAccessible
from ascetic_ddd.faker.infrastructure.session.pg_session import extract_external_connection
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification

__all__ = ('PgRepository',)


T = typing.TypeVar("T", covariant=True)


class PgRepository(typing.Generic[T]):
    _extract_connection = staticmethod(extract_external_connection)
    _table: str
    _id_attr: str
    _agg_exporter: typing.Callable[[T], dict]

    def __init__(self):
        pass

    async def insert(self, session: ISession, agg: T):
        state = self._agg_exporter(agg)
        flat_state = flatten_dict(state)
        sql = """
            INSERT INTO %s (%s)
            VALUES (%s)
        """ % (
            self._table,
            ", ".join(escape(key) for key in flat_state.keys()),
            ", ".join("%%(%s)s" % key for key in flat_state.keys())
        )
        async with self._extract_connection(session).cursor() as acursor:
            try:
                await acursor.execute(sql, flat_state)
            except Exception:
                raise

    async def get(self, session: ISession, id_: IAccessible[typing.Any]) -> T | None:
        raise NotImplementedError

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[T]:
        raise NotImplementedError

    def _id(self, state: dict) -> typing.Any:
        if self._id_attr is not None:
            return state.get(self._id_attr)
        return next(iter(state.values()))

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass
