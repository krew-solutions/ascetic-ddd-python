import functools
import json
import math
import os
import random
import socket
import threading
import typing
import dataclasses
from abc import abstractmethod
from typing import Hashable, Callable

from psycopg.types.json import Jsonb

from ascetic_ddd.disposable import IDisposable
from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.infrastructure.distributors.m2o.interfaces import IPgExternalSource
from ascetic_ddd.faker.infrastructure.session.pg_session import extract_internal_connection
from ascetic_ddd.faker.infrastructure.specification.pg_specification_visitor import PgSpecificationVisitor
from ascetic_ddd.faker.infrastructure.utils.json import JSONEncoder
from ascetic_ddd.seedwork.infrastructure.utils import serializer
from ascetic_ddd.seedwork.infrastructure.utils.pg import escape


__all__ = ('BasePgDistributor', 'PgWeightedDistributor')


T = typing.TypeVar("T", covariant=True)


class BasePgDistributor(IM2ODistributor[T], typing.Generic[T]):
    """
    Базовый класс для PostgreSQL дистрибьюторов.

    Ограничение: при динамическом создании значений ранние значения
    получают больше вызовов, т.к. доступны дольше. Для генератора фейковых данных приемлемо.
    """
    _extract_connection = staticmethod(extract_internal_connection)
    _initialized: bool = False
    _mean: float = 50
    _values_table: str | None = None
    _default_spec: ISpecification
    _provider_name: str | None = None
    _external_source: IPgExternalSource | None = None
    _delegate: IM2ODistributor[T]

    def __init__(
            self,
            delegate: IM2ODistributor[T],
            mean: float | None = None,
            initialized: bool = False
    ):
        self._delegate = delegate
        if mean is not None:
            self._mean = mean
        self._initialized = initialized
        self._external_source = None
        self._default_spec = EmptySpecification()
        super().__init__()

    def bind_external_source(self, external_source: typing.Any) -> None:
        """Привязывает внешний источник данных (repository) и обновляет таблицу."""
        if not isinstance(external_source, IPgExternalSource):
            raise TypeError("Expected IPgExternalSource, got %s" % type(external_source))
        self._external_source = external_source
        self._values_table = external_source.table

    async def next(
            self,
            session: ISession,
            specification: ISpecification[T] | None = None,
    ) -> T:
        if specification is None:
            specification = self._default_spec

        if not self._initialized:
            await self.setup(session)

        # Резолвим вложенные constraints (если есть)
        if hasattr(specification, 'resolve_nested'):
            await specification.resolve_nested(session)

        value, should_create_new = await self._get_next_value(session, specification)
        if should_create_new:
            try:
                value = await self._delegate.next(session)
            except Cursor as cursor:
                raise Cursor(
                    position=None,
                    callback=self._append,
                    delegate=cursor
                )
        return value

    @abstractmethod
    async def _get_next_value(self, session: ISession, specification: ISpecification[T]) -> tuple[T | None, bool]:
        raise NotImplementedError

    async def _append(self, session: ISession, value: T, position: int | None):
        if self._external_source:
            return
        sql = """
            INSERT INTO %(values_table)s (value, object)
            VALUES (%%s, %%s)
            ON CONFLICT DO NOTHING;
        """ % {
            'values_table': self._values_table,
        }
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (self._encode(value), self._serialize(value)))
        # logging.debug("Append: %s", value)
        await self.anotify('value', session, value)

    async def append(self, session: ISession, value: T):
        await self._append(session, value, None)

    @property
    def provider_name(self):
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value):
        if self._provider_name is None:
            self._provider_name = value
            if self._values_table is None:
                self._values_table = escape("values_for_%s" % value[-(63 - 11):])

    def attach(self, aspect: Hashable, observer: Callable, id_: Hashable | None = None) -> IDisposable:
        return self._delegate.attach(aspect, observer, id_)

    def detach(self, aspect, observer, id_: Hashable | None = None):
        return self._delegate.detach(aspect, observer, id_)

    def notify(self, aspect, *args, **kwargs):
        return self._delegate.notify(aspect, *args, **kwargs)

    async def anotify(self, aspect: Hashable, *args, **kwargs):
        return await self._delegate.anotify(aspect, *args, **kwargs)

    async def setup(self, session: ISession):
        if not self._initialized:  # Fixes diamond problem
            if not (await self._is_initialized(session)):
                await self._setup(session)
            self._initialized = True

    async def cleanup(self, session: ISession):
        # FIXME: diamond problem
        self._initialized = False
        if self._external_source:
            return
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute("DROP TABLE IF EXISTS %s" % self._values_table)

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    async def _setup(self, session: ISession):
        if self._external_source:
            return
        sql = """
            CREATE TABLE IF NOT EXISTS %(values_table)s (
                position serial NOT NULL PRIMARY KEY,
                value JSONB NOT NULL,
                object TEXT NOT NULL,
                UNIQUE (value)
            );
            CREATE INDEX IF NOT EXISTS %(index_name)s ON %(values_table)s USING GIN(value jsonb_path_ops);
        """ % {
            "values_table": self._values_table,
            "index_name": escape("gin_%s" % self.provider_name[:(63 - 4)]),
        }
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql)

    async def _is_initialized(self, session: ISession) -> bool:
        if self._external_source:
            return True  # external source table is managed by repository
        sql = """SELECT to_regclass(%s)"""
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (self._values_table,))
            regclass = (await acursor.fetchone())[0]
        return regclass is not None

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


# =============================================================================
# PgWeightedDistributor
# =============================================================================

class PgWeightedDistributor(BasePgDistributor[T], typing.Generic[T]):
    """
    Дистрибьютор с взвешенным распределением в PostgreSQL.

    Ограничение: при динамическом создании значений ранние значения
    получают больше вызовов, т.к. доступны дольше. Это даёт ~85% vs 70% для первой
    партиции вместо точного соответствия весам. Для генератора фейковых данных приемлемо.
    """
    _weights: list[float]

    def __init__(
            self,
            delegate: IM2ODistributor[T],
            weights: typing.Iterable[float] = tuple(),
            mean: float | None = None,
            initialized: bool = False
    ):
        self._weights = list(weights)
        super().__init__(delegate=delegate, mean=mean, initialized=initialized)

    def _compute_partition(self) -> tuple[int, float, int]:
        """
        Вычисляет партицию в Python.

        Returns:
            (partition_idx, local_skew, num_partitions)

        Используем ЛЕВУЮ партицию (LAG) и смещаем к КОНЦУ — это компенсирует то, что ранние
        значения получают больше вызовов (доступны дольше при динамическом создании).
        Для weights=[0.7, 0.2, 0.07, 0.03]:
          partition 0: первая → local_skew=1.0 (равномерно)
          partition 1: ratio=3.5 → local_skew≈2.81 (смещение к концу, ближе к partition 0)
          partition 2: ratio=2.86 → local_skew≈2.52
          partition 3: ratio=2.33 → local_skew≈2.22
        """
        num_partitions = len(self._weights)
        if num_partitions == 0:
            return (0, 1.0, 1)

        # Выбор партиции по весам — O(w)
        partition_idx = random.choices(range(num_partitions), weights=self._weights, k=1)[0]

        # Вычисляем локальный наклон из соотношения весов соседних партиций
        if partition_idx > 0:
            prev_weight = self._weights[partition_idx - 1]
            curr_weight = self._weights[partition_idx]
            if curr_weight > 0:
                ratio = prev_weight / curr_weight
                local_skew = max(1.0, math.log2(ratio) + 1)
            else:
                local_skew = 2.0
        else:
            local_skew = 1.0

        return (partition_idx, local_skew, num_partitions)

    async def _get_next_value(self, session: ISession, specification: ISpecification[T]) -> tuple[T | None, bool]:
        """
        Оптимизированный выбор значения:
        1. Выбор партиции по весам — O(w) в Python
        2. Выбор позиции внутри партиции со slope bias — O(1) в SQL
        3. Получение значения по позиции — O(log n) с индексом
        4. Вероятностное решение о создании нового значения
        """
        visitor = PgSpecificationVisitor()
        specification.accept(visitor)

        partition_idx, local_skew, num_partitions = self._compute_partition()

        sql = """
            WITH filtered AS (
                SELECT position, object FROM %(values_table)s %(where)s
            ),
            stats AS (
                SELECT COUNT(*) AS n FROM filtered
            ),
            target AS (
                SELECT
                    -- end = floor((partition_idx + 1) * total / num_partitions)
                    -- size = ceil(total / num_partitions)
                    -- pos = end - 1 - floor(size * (1 - random())^local_skew)
                    -- Смещение к КОНЦУ партиции (ближе к предыдущей)
                    GREATEST(0,
                        FLOOR((%(partition_idx)s + 1) * n::decimal / %(num_partitions)s)::integer - 1 -
                        LEAST(
                            FLOOR(CEIL(n::decimal / %(num_partitions)s) * POWER(1 - RANDOM(), %(local_skew)s))::integer,
                            GREATEST(CEIL(n::decimal / %(num_partitions)s)::integer - 1, 0)
                        )
                    ) AS pos,
                    n
                FROM stats
            )
            SELECT
                (SELECT object FROM filtered ORDER BY position OFFSET t.pos LIMIT 1),
                -- Вероятностный подход: создаём новое с вероятностью 1/mean
                (t.n = 0 OR RANDOM() < 1.0 / %(expected_mean)s),
                t.n
            FROM target t
        """ % {
            'values_table': self._values_table,
            'where': "WHERE %s" % visitor.sql if visitor.sql else "",
            'partition_idx': partition_idx,
            'num_partitions': num_partitions,
            'local_skew': local_skew,
            'expected_mean': max(self._mean, 1),
        }

        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, visitor.params)
            row = await acursor.fetchone()
            if not row or not row[0]:
                return (None, True)
            should_create_new = row[1] if row[2] and row[2] > 0 else True
            return (self._deserialize(row[0]), should_create_new)
