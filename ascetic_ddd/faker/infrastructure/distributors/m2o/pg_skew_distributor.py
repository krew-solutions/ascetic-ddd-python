import typing

from ascetic_ddd.faker.domain.distributors.m2o import IM2ODistributor
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.infrastructure.distributors.m2o.pg_weighted_distributor import BasePgDistributor
from ascetic_ddd.faker.infrastructure.specification.pg_specification_visitor import PgSpecificationVisitor


__all__ = ('PgSkewDistributor',)


T = typing.TypeVar("T", covariant=True)


class PgSkewDistributor(BasePgDistributor[T], typing.Generic[T]):
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
    _skew: float = 2.0

    def __init__(
            self,
            delegate: IM2ODistributor[T],
            skew: float = 2.0,
            mean: float | None = None,
            initialized: bool = False
    ):
        self._skew = skew
        super().__init__(delegate=delegate, mean=mean, initialized=initialized)

    async def _get_next_value(self, session: ISession, specification: ISpecification[T]) -> tuple[T | None, bool]:
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
            WITH filtered AS (
                SELECT position, object FROM %(values_table)s %(where)s
            ),
            stats AS (
                SELECT COUNT(*) AS n FROM filtered
            ),
            target AS (
                SELECT
                    -- Степенное распределение: idx = floor(n * (1 - random())^skew)
                    -- skew=1: равномерное, skew=2+: перекос к началу
                    LEAST(FLOOR(n * POWER(1 - RANDOM(), %(skew)s))::integer, GREATEST(n - 1, 0)) AS pos,
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
            'skew': self._skew,
            'expected_mean': max(self._mean, 1),
        }

        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, visitor.params)
            row = await acursor.fetchone()
            if not row or not row[0]:
                return (None, True)
            should_create_new = row[1] if row[2] and row[2] > 0 else True
            return (self._deserialize(row[0]), should_create_new)
