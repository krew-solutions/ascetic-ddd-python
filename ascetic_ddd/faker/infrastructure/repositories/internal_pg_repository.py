import json
import typing

from functools import partial, wraps
from psycopg.types.json import Jsonb

from ascetic_ddd.seedwork.infrastructure.utils.pg import escape
from ascetic_ddd.seedwork.infrastructure.utils import serializer
from ascetic_ddd.seedwork.domain.identity.interfaces import IAccessible
from ascetic_ddd.faker.infrastructure.session.pg_session import extract_internal_connection
from ascetic_ddd.faker.infrastructure.specification.pg_specification_visitor import PgSpecificationVisitor
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.infrastructure.utils.json import JSONEncoder
from ascetic_ddd.observable.observable import Observable

__all__ = ('InternalPgRepository',)

T = typing.TypeVar("T", covariant=True)


class InternalPgRepository(Observable, typing.Generic[T]):
    _extract_connection = staticmethod(extract_internal_connection)
    _table: str
    _id_attr: str | None
    _agg_exporter: typing.Callable[[T], dict]
    _initialized: bool

    @staticmethod
    def check_init(func):

        @wraps(func)
        async def _deco(self, session: ISession, *args, **kwargs):
            if not self._initialized:
                await self.setup(session)
            return await func(self, session, *args, **kwargs)

        return _deco

    def __init__(
            self,
            table: str,
            agg_exporter: typing.Callable[[T], dict],
            id_attr: str = None,
            initialized: bool = False
    ):
        super().__init__()
        self._table = escape(table)
        self._agg_exporter = agg_exporter
        self._id_attr = id_attr
        self._initialized = initialized

    @property
    def table(self) -> str:
        return self._table

    @check_init
    async def insert(self, session: ISession, agg: T):
        sql = """
            INSERT INTO %(table)s (value_id, value, object)
            VALUES (%%s, %%s, %%s)
        """ % {
            'table': self._table,
        }
        state = self._agg_exporter(agg)
        params = (
            self._encode(self._id(state)),
            self._encode(state),
            self._serialize(agg),
        )

        async with self._extract_connection(session).cursor() as acursor:
            try:
                await acursor.execute(sql, params)
            except Exception:
                raise

        await self.anotify('inserted', session, agg)

    @check_init
    async def get(self, session: ISession, id_: IAccessible[typing.Any] | typing.Any) -> T | None:
        """
        TODO: Pass Specification() instead of id_?
        """
        sql = """
            SELECT object FROM %(table)s WHERE value_id = %%s
        """ % {
            'table': self._table,
        }
        key = id_.value if hasattr(id_, 'value') else id_
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (self._encode(key),))
            row = await acursor.fetchone()
            return row and self._deserialize(row[0])

    async def update(self, session: ISession, agg: T):
        state = self._agg_exporter(agg)
        sql = """
            UPDATE %(table)s SET value = %%s, object = %%s WHERE value_id = %%s
        """ % {
            'table': self._table,
        }
        params = (
            self._encode(state),
            self._serialize(agg),
            self._encode(self._id(state)),
        )

        async with self._extract_connection(session).cursor() as acursor:
            try:
                await acursor.execute(sql, params)
            except Exception:
                raise

        await self.anotify('updated', session, agg)

    @check_init
    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[T]:
        visitor = PgSpecificationVisitor()
        specification.accept(visitor)
        sql = """
            SELECT object FROM %(table)s WHERE %(criteria)s
        """ % {
            'table': self._table,
            'criteria': visitor.sql
        }
        params = tuple(self._encode(i) if isinstance(i, (list, tuple, dict)) else i for i in visitor.params)
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, params)
            return [self._deserialize(row[0]) for row in await acursor.fetchone()]

    def _id(self, state: dict) -> typing.Any:
        if self._id_attr is not None:
            return state.get(self._id_attr)
        return next(iter(state.values()))

    @staticmethod
    def _encode(obj):
        dumps = partial(json.dumps, cls=JSONEncoder)
        return Jsonb(obj, dumps)

    _serialize = staticmethod(serializer.serialize)
    _deserialize = staticmethod(serializer.deserialize)

    async def _is_initialized(self, session: ISession) -> bool:
        sql = """SELECT to_regclass(%s)"""
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (self._table, ))
            regclass = (await acursor.fetchone())[0]
        return regclass is not None

    async def _setup(self, session: ISession):
        sql = """
            CREATE TABLE IF NOT EXISTS %(table)s (
                position serial NOT NULL PRIMARY KEY,  -- for ordering to reuse the table in a distributor.
                value_id JSONB NOT NULL UNIQUE,
                value JSONB NOT NULL,
                object TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS %(index_name)s ON %(table)s USING GIN(value jsonb_path_ops);
        """ % {
            "table": self._table,
            "index_name": escape("gin_%s" % self._table[:(63 - 4)]),
        }
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql)

    async def setup(self, session: ISession):
        if not self._initialized:  # Fixes diamond problem
            if not (await self._is_initialized(session)):
                await self._setup(session)
            self._initialized = True

    async def cleanup(self, session: ISession):
        # FIXME: diamond problem
        self._initialized = False
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute("DROP TABLE IF EXISTS %s" % self._table)
