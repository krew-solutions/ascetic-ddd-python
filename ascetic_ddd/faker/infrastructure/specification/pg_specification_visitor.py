import dataclasses
import functools
import json
import typing

from psycopg.types.json import Jsonb

from ascetic_ddd.faker.infrastructure.utils.json import JSONEncoder
from ascetic_ddd.seedwork.infrastructure.utils.pg import escape
from ascetic_ddd.faker.domain.specification.interfaces import ISpecificationVisitor

__all__ = ("PgSpecificationVisitor",)


class PgSpecificationVisitor(ISpecificationVisitor):
    _target_value_expr: str
    _sql: str
    _params: typing.Tuple[typing.Any, ...]

    __slots__ = ("_target_value_expr", "_sql", "_params",)

    def __init__(self, target_value_expr: str = "value"):
        self._target_value_expr = target_value_expr
        self._sql = ""
        self._params = tuple()

    @property
    def sql(self) -> str:
        return self._sql

    @property
    def params(self) -> typing.Tuple[typing.Any, ...]:
        return self._params

    def visit_jsonpath_specification(self, jsonpath: str, params: typing.Tuple[typing.Any, ...]):
        self._sql += "jsonb_path_match(%s, '%s')" % (self._target_value_expr, jsonpath)  # jsonb_path_match_tz?
        self._params += params

    def visit_object_pattern_specification(
            self,
            object_pattern: dict,
            aggregate_provider_accessor: typing.Callable[[], typing.Any] | None = None
    ):
        if not object_pattern:
            return

        # object_pattern может быть не dict — используем простой @>
        # (скалярное значение или композитный объект без metadata о провайдерах)
        if not isinstance(object_pattern, dict):
            self._sql += "%s @> %%s" % self._target_value_expr
            self._params += (self._encode(object_pattern),)
            return

        # Разделяем на простые и вложенные constraints
        simple_constraints = {}
        nested_constraints = {}

        for key, value in object_pattern.items():
            if isinstance(value, dict):
                nested_constraints[key] = value
            else:
                simple_constraints[key] = value

        conditions = []

        # Простые constraints: value @> '{"status": "active"}'
        if simple_constraints:
            conditions.append("%s @> %%s" % self._target_value_expr)
            self._params += (self._encode(simple_constraints),)

        # Вложенные constraints (dict) — смотрим на тип провайдера:
        # - IReferenceProvider → FK на другой агрегат → subquery
        # - ICompositeValueProvider или другой → композитный Value Object / Entity → простой @>
        if nested_constraints and aggregate_provider_accessor is not None:
            from ascetic_ddd.faker.domain.providers.interfaces import IReferenceProvider

            aggregate_provider = aggregate_provider_accessor()
            providers = aggregate_provider._providers

            for key, nested_pattern in nested_constraints.items():
                nested_provider = providers.get(key)
                if isinstance(nested_provider, IReferenceProvider):
                    # FK на другой агрегат — получаем таблицу и строим subquery
                    related_agg_provider = nested_provider.aggregate_provider
                    if hasattr(related_agg_provider, '_repository'):
                        related_table = related_agg_provider._repository.table

                        # Рекурсивно строим subquery для вложенного pattern
                        subquery_sql, subquery_params = self._build_subquery(
                            key,
                            related_table,
                            nested_pattern,
                            lambda rap=related_agg_provider: rap
                        )
                        conditions.append(subquery_sql)
                        self._params += subquery_params
                else:
                    # Композитный Value Object / Entity — используем простой @>
                    conditions.append("%s @> %%s" % self._target_value_expr)
                    self._params += (self._encode({key: nested_pattern}),)
        elif nested_constraints:
            # Нет aggregate_provider_accessor — fallback на простой @>
            conditions.append("%s @> %%s" % self._target_value_expr)
            self._params += (self._encode(nested_constraints),)

        if conditions:
            self._sql += " AND ".join(conditions)

    def _build_subquery(
            self,
            fk_key: str,
            related_table: str,
            nested_pattern: dict,
            related_aggregate_provider_accessor: typing.Callable[[], typing.Any]
    ) -> tuple[str, tuple]:
        """
        Строит EXISTS subquery для вложенного constraint.

        Использует EXISTS вместо IN для лучшей работы с индексами:
        - rt.value @> '{"status": "active"}' — использует GIN index
        - rt.value_id = main.value->'fk_id' — использует B-tree index (UNIQUE constraint)

        Args:
            fk_key: имя FK атрибута (например, 'fk_id')
            related_table: таблица связанного агрегата
            nested_pattern: вложенный pattern для фильтрации
            related_aggregate_provider_accessor: accessor для связанного AggregateProvider

        Returns:
            (sql, params) — SQL условие и параметры
        """
        # Рекурсивно обрабатываем вложенный pattern
        nested_visitor = PgSpecificationVisitor(target_value_expr="rt.value")
        nested_visitor.visit_object_pattern_specification(
            nested_pattern,
            related_aggregate_provider_accessor
        )

        if nested_visitor.sql:
            # EXISTS (SELECT 1 FROM related_table rt WHERE rt.value @> ... AND rt.value_id = main.value->'fk_id')
            # fk_key безопасен — это имя атрибута из кода провайдера
            sql = "EXISTS (SELECT 1 FROM %s rt WHERE %s AND rt.value_id = %s->'%s')" % (
                related_table,
                nested_visitor.sql,
                self._target_value_expr,
                fk_key,
            )
            return (sql, nested_visitor.params)
        else:
            return ("TRUE", tuple())

    def visit_scope_specification(self, scope: typing.Hashable):
        pass

    def visit_empty_specification(self):
        pass

    @staticmethod
    def _encode(obj):
        if dataclasses.is_dataclass(obj):
            obj = dataclasses.asdict(obj)
        dumps = functools.partial(json.dumps, cls=JSONEncoder)
        return Jsonb(obj, dumps)
