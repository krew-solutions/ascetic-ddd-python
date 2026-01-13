import functools
import json
import os
import socket
import threading
import typing
import dataclasses

from psycopg.types.json import Jsonb

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IDistributor
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.infrastructure.session.pg_session import extract_internal_connection
from ascetic_ddd.faker.infrastructure.specification.pg_specification_visitor import PgSpecificationVisitor
from ascetic_ddd.faker.infrastructure.utils.json import JSONEncoder
from ascetic_ddd.observable.observable import Observable
from ascetic_ddd.seedwork.infrastructure.utils import serializer
from ascetic_ddd.seedwork.infrastructure.utils.pg import escape


__all__ = ('PgWeightedDistributor',)


T = typing.TypeVar("T", covariant=True)


class Tables:
    values: str | None
    params: str | None
    weights: str | None

    def set(self, name: str):
        max_prefix_len = 12
        max_pg_name_len = 63
        max_name_len = max_pg_name_len - max_prefix_len
        if len(name) > max_name_len:
            name = name[-max_name_len:]
        self.values = escape("values_for_%s" % name)
        self.params = escape("params_for_%s" % name)
        self.weights = escape("weights_for_%s" % name)


class PgWeightedDistributor(Observable, IDistributor[T], typing.Generic[T]):
    """
    Дистрибьютор с взвешенным распределением в PostgreSQL.

    Ограничение: при динамическом создании значений ранние значения
    получают больше вызовов, т.к. доступны дольше. Это даёт ~85% vs 70% для первой
    партиции вместо точного соответствия весам. Для генератора фейковых данных приемлемо.
    """
    _extract_connection = staticmethod(extract_internal_connection)
    _initialized: bool = False
    _scale: float = 50
    _tables: Tables
    _weights: list[float]
    _default_key: str = str(frozenset())
    _provider_name: str | None = None

    def __init__(
            self,
            weights: typing.Iterable[float] = tuple(),
            scale: float | None = None,
            initialized: bool = False
    ):
        self._tables = Tables()
        self._weights = list(weights)
        if scale is not None:
            self._scale = scale
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

        if self._scale is None:
            self._scale = await self._get_param(session, 'scale')

        if self._scale == 1:
            raise StopAsyncIteration(None)

        value, should_create_new = await self._get_next_value(session, specification)
        if should_create_new:
            raise StopAsyncIteration(None)
        if value is None:
            value = await self._get_value(session, specification, 0)

        return value

    async def append(self, session: ISession, value: T):
        sql = """
            INSERT INTO %(values_table)s (value, object)
            VALUES (%%s, %%s)
            ON CONFLICT DO NOTHING;
        """ % {
            'values_table': self._tables.values,
        }
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (self._encode(value), self._serialize(value)))
        # logging.debug("Append: %s", value)
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
        if not self._initialized:  # Fixes diamond problem
            if not (await self._is_initialized(session)):
                await self._setup(session)
            self._initialized = True

    async def cleanup(self, session: ISession):
        # FIXME: diamond problem
        self._initialized = False
        for name, t in vars(self._tables).items():
            async with self._extract_connection(session).cursor() as acursor:
                await acursor.execute("DROP TABLE IF EXISTS %s" % t)

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    async def _setup(self, session: ISession):
        await self._create_weights_table(session)
        await self._populate_weights(session, self._weights)
        await self._create_values_table(session)
        await self._create_params_table(session)
        await self._set_param(session, 'scale', self._scale)

    async def _is_initialized(self, session: ISession) -> bool:
        sql = """SELECT to_regclass(%s)"""
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (self._tables.weights, ))
            regclass = (await acursor.fetchone())[0]
        return regclass is not None

    # values_table  #####################################################################################

    async def _create_values_table(self, session: ISession):
        sql = """
            CREATE TABLE IF NOT EXISTS %(values_table)s (
                id serial NOT NULL PRIMARY KEY,
                value JSONB NOT NULL,
                -- criteria JSONB NOT NULL,  -- Save the whole agg here? It is not a way, since
                -- two dependent aggregates (Endorser and Specialist) can be created in a multi-stage
                -- interdependent way and we need to sync the repo and the distributor.
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

    async def _get_next_value(self, session: ISession, specification: ISpecification, scale: float = None):
        # TODO: https://dataschool.com/learn-sql/random-sequences/
        """
        Оптимизированный выбор значения без блокирующих счётчиков:
        1. Выбор партиции по кумулятивным весам — O(w)
        2. Выбор позиции внутри партиции со slope bias — O(1)
        3. Получение значения по позиции — O(log n) с индексом
        4. Вероятностное решение о создании нового значения — без счётчиков
        """
        visitor = PgSpecificationVisitor()
        specification.accept(visitor)
        # logging.debug('SQL: %s; Params: %s', visitor.sql, visitor.params)

        # Используем ЛЕВУЮ партицию (LAG) и смещаем к КОНЦУ — это компенсирует то, что ранние
        # значения получают больше вызовов (доступны дольше при динамическом создании).
        # Для weights=[0.7, 0.2, 0.07, 0.03]:
        #   partition 0: первая → local_skew=1.0 (равномерно)
        #   partition 1: ratio=3.5 → local_skew≈2.81 (смещение к концу, ближе к partition 0)
        #   partition 2: ratio=2.86 → local_skew≈2.52
        #   partition 3: ratio=2.33 → local_skew≈2.22
        # Вероятностный подход: создаём новое значение с вероятностью 1/scale.
        # Это работает корректно с любым WHERE условием (per-specification).
        sql = """
            WITH params AS (
                SELECT value::decimal AS expected_scale
                FROM %(params_table)s
                WHERE key = 'scale'
                LIMIT 1
            ),
            value_stats AS (
                SELECT COUNT(*) AS total_values
                FROM %(values_table)s
                %(where)s
            ),
            -- Кумулятивные веса для выбора партиции
            cumulative_weights AS (
                SELECT
                    row_number() OVER (ORDER BY id) AS partition_idx,
                    weight,
                    SUM(weight) OVER (ORDER BY id) AS cum_weight,
                    SUM(weight) OVER () AS total_weight,
                    LAG(weight) OVER (ORDER BY id) AS prev_weight,
                    COUNT(*) OVER () AS num_partitions
                FROM %(weights_table)s
            ),
            -- Один RANDOM() для выбора партиции (иначе каждая строка получит свой RANDOM)
            partition_rand AS (SELECT RANDOM() AS r),
            -- Выбор партиции по весам (кумулятивное распределение)
            selected_partition AS (
                SELECT
                    partition_idx,
                    num_partitions,
                    -- Вычисляем локальный наклон из соотношения весов соседних партиций
                    -- ratio > 1 → смещение к концу партиции (ближе к предыдущей)
                    -- ratio = 1 → равномерное распределение
                    CASE
                        WHEN prev_weight > 0 AND prev_weight IS NOT NULL AND weight > 0
                        THEN GREATEST(1.0, LOG(2, prev_weight / weight) + 1)
                        ELSE 1.0
                    END AS local_skew
                FROM cumulative_weights, partition_rand
                WHERE cum_weight >= total_weight * r
                ORDER BY partition_idx
                LIMIT 1
            ),
            -- Вычисляем границы партиции и позицию со slope bias
            target AS (
                SELECT
                    -- end = floor(partition_idx * total / num_partitions)
                    -- size = ceil(total / num_partitions)
                    -- pos = end - 1 - floor(size * (1 - random())^local_skew)
                    -- Смещение к КОНЦУ партиции (ближе к предыдущей)
                    GREATEST(0,
                        FLOOR(sp.partition_idx * vs.total_values::decimal / sp.num_partitions)::integer - 1 -
                        LEAST(
                            FLOOR(
                                CEIL(vs.total_values::decimal / sp.num_partitions) *
                                POWER(1 - RANDOM(), sp.local_skew)
                            )::integer,
                            GREATEST(CEIL(vs.total_values::decimal / sp.num_partitions)::integer - 1, 0)
                        )
                    ) AS pos,
                    vs.total_values,
                    p.expected_scale
                FROM selected_partition sp
                CROSS JOIN value_stats vs
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
                -- Вероятностный подход: создаём новое с вероятностью 1/scale
                -- Работает корректно per-specification (WHERE учитывается в total_values)
                (t.total_values = 0 OR RANDOM() < 1.0 / GREATEST(t.expected_scale, 1)) AS should_create_new,
                t.total_values
            FROM target t
        """ % {
            'params_table': self._tables.params,
            'values_table': self._tables.values,
            'weights_table': self._tables.weights,
            'where': "WHERE %s" % visitor.sql if visitor.sql else "",
        }

        async with self._extract_connection(session).cursor() as acursor:
            # visitor.params передаётся дважды: для value_stats и для финального SELECT
            await acursor.execute(sql, visitor.params + visitor.params)
            row = await acursor.fetchone()
            if not row or not row[0]:
                return (None, True)
            # row[0] = object, row[1] = should_create_new, row[2] = total_values
            should_create_new = row[1] if row[2] and row[2] > 0 else True
            return (self._deserialize(row[0]), should_create_new)

    async def _get_value(self, session: ISession, specification: ISpecification, offset: int) -> T:
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

    # weights_table  #######################################################################################

    async def _create_weights_table(self, session: ISession):
        sql = """
            CREATE TABLE IF NOT EXISTS %s (
                id serial NOT NULL PRIMARY KEY,
                weight NUMERIC(6, 5) NOT NULL -- REAL, DOUBLE PRECISION
            )
        """ % (
            self._tables.weights,
        )
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql)

    async def _populate_weights(self, session: ISession, weights: list[float]):
        if not weights:
            return
        sql = """
            INSERT INTO %s (weight)
            VALUES %s;
        """ % (
            self._tables.weights,
            ", ".join(["(%s)"] * len(weights))
        )
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, weights)

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


class AppendOnlyPgWeightedDistributor(PgWeightedDistributor[T], typing.Generic[T]):

    async def _create_values_table(self, session: ISession):
        sql = """
            CREATE TABLE IF NOT EXISTS %(values_table)s (
                id serial NOT NULL PRIMARY KEY,
                value JSONB NOT NULL,
                -- criteria JSONB NOT NULL,  -- Save the whole agg here? It is not a way, since
                -- two dependent aggregates (Endorser and Specialist) can be created in a multi-stage
                -- interdependent way and we need to sync the repo and the distributor.
                object TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS %(index_name)s ON %(values_table)s USING GIN(value jsonb_path_ops);
        """ % {
            "values_table": self._tables.values,
            "index_name": escape("gin_%s" % self.provider_name[:(63 - 4)]),
        }
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql)

    async def append(self, session: ISession, value: T):
        sql = """
            INSERT INTO %(values_table)s (value, object)
            VALUES (%%s, %%s)
            ON CONFLICT DO NOTHING;
        """ % {
            'values_table': self._tables.values,
        }
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (self._encode(value), self._serialize(value)))
        # logging.debug("Append: %s", value)
        await self.anotify('value', session, value)
