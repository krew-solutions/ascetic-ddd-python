import functools
import json
import os
import socket
import threading
import typing
import dataclasses

from psycopg.types.json import Jsonb

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.infrastructure.session.pg_session import extract_internal_connection
from ascetic_ddd.faker.infrastructure.specification.pg_specification_visitor import PgSpecificationVisitor
from ascetic_ddd.faker.infrastructure.utils.json import JSONEncoder
from ascetic_ddd.observable.observable import Observable
from ascetic_ddd.seedwork.infrastructure.utils import serializer
from ascetic_ddd.seedwork.infrastructure.utils.pg import escape

__all__ = ('PgSkewDistributor',)

T = typing.TypeVar("T", covariant=True)


class Tables:
    values: str | None
    params: str | None

    def set(self, name: str):
        max_prefix_len = 12
        max_pg_name_len = 63
        max_name_len = max_pg_name_len - max_prefix_len
        if len(name) > max_name_len:
            name = name[-max_name_len:]
        self.values = escape("values_for_%s" % name)
        self.params = escape("params_for_%s" % name)


class PgSkewDistributor(Observable, IM2ODistributor[T], typing.Generic[T]):
    """
    Дистрибьютор со степенным распределением в PostgreSQL.

    Один параметр skew вместо списка весов:
    - skew = 1.0 — равномерное распределение
    - skew = 2.0 — умеренный перекос (первые 20% получают ~60% вызовов)
    - skew = 3.0 — сильный перекос (первые 10% получают ~70% вызовов)

    Преимущества перед PgDistributor:
    - Один параметр вместо списка весов
    - Проще SQL (нет таблицы weights, нет cumulative weights)
    - O(1) выбор позиции

    Ограничение: при динамическом создании значений ранние значения
    получают больше вызовов, т.к. доступны дольше. Для генератора фейковых данных приемлемо.
    """
    _extract_connection = staticmethod(extract_internal_connection)
    _initialized: bool = False
    _mean: float = 50
    _skew: float = 2.0
    _tables: Tables
    _default_key: str = str(frozenset())
    _provider_name: str | None = None

    def __init__(
            self,
            skew: float = 2.0,
            mean: float | None = None,
            initialized: bool = False
    ):
        self._tables = Tables()
        self._skew = skew
        if mean is not None:
            self._mean = mean
        self._initialized = initialized
        super().__init__()

    async def next(
            self,
            session: ISession,
            specification: ISpecification[T] | None = None,
    ) -> T:
        if specification is None:
            specification = EmptySpecification()

        if not self._initialized:
            await self.setup(session)

        if self._mean is None:
            self._mean = await self._get_param(session, 'mean')

        if self._mean == 1:
            raise Cursor(
                position=None,
                callback=self._append,
            )

        value, should_create_new = await self._get_next_value(session, specification)
        if should_create_new:
            raise Cursor(
                position=None,
                callback=self._append,
            )
        if value is None:
            value = await self._get_value(session, specification, 0)

        return value

    async def _append(self, session: ISession, value: T, position: int | None):
        sql = """
            INSERT INTO %(values_table)s (value, object)
            VALUES (%%s, %%s)
            ON CONFLICT DO NOTHING;
        """ % {
            'values_table': self._tables.values,
        }
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (self._encode(value), self._serialize(value)))
        await self.anotify('value', session, value)

    @property
    def provider_name(self):
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value):
        if self._provider_name is None:
            self._provider_name = value
            self._tables.set(value)

    async def setup(self, session: ISession):
        if not self._initialized:
            if not (await self._is_initialized(session)):
                await self._setup(session)
            self._initialized = True

    async def cleanup(self, session: ISession):
        self._initialized = False
        for name, t in vars(self._tables).items():
            async with self._extract_connection(session).cursor() as acursor:
                await acursor.execute("DROP TABLE IF EXISTS %s" % t)

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    async def _setup(self, session: ISession):
        await self._create_values_table(session)
        await self._create_params_table(session)
        await self._set_param(session, 'mean', self._mean)
        await self._set_param(session, 'skew', self._skew)

    async def _is_initialized(self, session: ISession) -> bool:
        sql = """SELECT to_regclass(%s)"""
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (self._tables.values,))
            regclass = (await acursor.fetchone())[0]
        return regclass is not None

    # values_table  #####################################################################################

    async def _create_values_table(self, session: ISession):
        sql = """
            CREATE TABLE IF NOT EXISTS %(values_table)s (
                id serial NOT NULL PRIMARY KEY,
                value JSONB NOT NULL,
                object TEXT NOT NULL,
                UNIQUE (value)
            );
            CREATE INDEX IF NOT EXISTS %(index_name)s ON %(values_table)s USING GIN(value jsonb_path_ops);
        """ % {
            "values_table": self._tables.values,
            "index_name": escape("gin_%s" % self.provider_name[:(63 - 4)]),
        }
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql)

    async def _get_next_value(self, session: ISession, specification: ISpecification[T]):
        """
        Выбор значения со степенным распределением:
        idx = floor(total_values * (1 - random())^skew)

        При skew=1: равномерное распределение
        При skew=2: первые 50% получают ~75% вызовов
        При skew=3: первые 33% получают ~70% вызовов

        Вероятностный подход для создания новых значений: с вероятностью 1/mean.
        Работает корректно per-specification (WHERE условие учитывается).
        """
        visitor = PgSpecificationVisitor()
        specification.accept(visitor)

        sql = """
            WITH params AS (
                SELECT
                    (SELECT value::decimal FROM %(params_table)s WHERE key = 'mean' LIMIT 1) AS expected_mean,
                    (SELECT value::decimal FROM %(params_table)s WHERE key = 'skew' LIMIT 1) AS skew
            ),
            value_stats AS (
                SELECT COUNT(*) AS total_values
                FROM %(values_table)s
                %(where)s
            ),
            target AS (
                SELECT
                    -- Степенное распределение: idx = floor(n * (1 - random())^skew)
                    -- skew=1: равномерное, skew=2+: перекос к началу
                    LEAST(
                        FLOOR(vs.total_values * POWER(1 - RANDOM(), p.skew))::integer,
                        GREATEST(vs.total_values - 1, 0)
                    ) AS pos,
                    vs.total_values,
                    p.expected_mean
                FROM value_stats vs
                CROSS JOIN params p
            )
            SELECT
                (
                    SELECT object
                    FROM %(values_table)s
                    %(where)s
                    ORDER BY id
                    OFFSET t.pos
                    LIMIT 1
                ) AS object,
                -- Вероятностный подход: создаём новое с вероятностью 1/mean
                (t.total_values = 0 OR RANDOM() < 1.0 / GREATEST(t.expected_mean, 1)) AS should_create_new,
                t.total_values
            FROM target t
        """ % {
            'params_table': self._tables.params,
            'values_table': self._tables.values,
            'where': "WHERE %s" % visitor.sql if visitor.sql else "",
        }

        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, visitor.params + visitor.params)
            row = await acursor.fetchone()
            if not row or not row[0]:
                return (None, True)
            # row[0] = object, row[1] = should_create_new, row[2] = total_values
            should_create_new = row[1] if row[2] and row[2] > 0 else True
            return (self._deserialize(row[0]), should_create_new)

    async def _get_value(self, session: ISession, specification: ISpecification[T], offset: int) -> T:
        visitor = PgSpecificationVisitor()
        specification.accept(visitor)

        sql = """
            SELECT object FROM %(values_table)s
            %(where)s
            ORDER BY id
            OFFSET %%s LIMIT 1
        """ % {
            'values_table': self._tables.values,
            'where': "WHERE %s" % visitor.sql if visitor.sql else "",
        }

        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, visitor.params + (offset,))
            return self._deserialize((await acursor.fetchone())[0])

    # params_table ###################################################################################

    async def _create_params_table(self, session: ISession):
        sql = """
            CREATE TABLE IF NOT EXISTS %s (
                key VARCHAR(40) NOT NULL PRIMARY KEY,
                value JSONB NOT NULL,
                object TEXT NOT NULL
            )
        """ % (
            self._tables.params,
        )
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql)

    async def _set_param(self, session: ISession, key: str, value: typing.Any):
        sql = """
            INSERT INTO %s (key, value, object) VALUES (%%s, %%s, %%s)
        """ % (
            self._tables.params,
        )
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (key, self._encode(value), self._serialize(value)))

    async def _get_param(self, session: ISession, key: str) -> typing.Any:
        sql = """
            SELECT object FROM %s WHERE key = %%s;
        """ % (
            self._tables.params,
        )
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (key,))
            return self._deserialize((await acursor.fetchone())[0])

    @staticmethod
    def get_thread_id():
        return '{0}.{1}.{2}'.format(
            socket.gethostname(), os.getpid(), threading.get_ident()
        )

    @staticmethod
    def _encode(obj):
        if dataclasses.is_dataclass(obj):
            obj = dataclasses.asdict(obj)
        dumps = functools.partial(json.dumps, cls=JSONEncoder)
        return Jsonb(obj, dumps)

    _serialize = staticmethod(serializer.serialize)
    _deserialize = staticmethod(serializer.deserialize)
