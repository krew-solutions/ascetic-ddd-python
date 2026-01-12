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
    _target_agg_expr: str
    _sql: str
    _params: typing.Tuple[typing.Any, ...]

    __slots__ = ("_target_obj_expr", "_sql", "_params",)

    def __init__(self, target_value_expr: str = "value", target_obj_expr: str = "a.value"):
        self._target_value_expr = target_value_expr
        self._target_agg_expr = escape(target_obj_expr)
        self._sql = ""
        self._params = tuple()

    @property
    def sql(self) -> str:
        return self._sql

    @property
    def params(self) -> typing.Tuple[typing.Any, ...]:
        return self._params

    def visit_jsonpath_specification(self, jsonpath: str, params: typing.Tuple[typing.Any, ...]):
        # TODO: Set target
        self._sql += "jsonb_path_match(%s, '%s')" % (self._target_value_expr, jsonpath)  # jsonb_path_match_tz?
        self._params += params

    def visit_object_pattern_specification(self, object_pattern: dict):
        criteria = object_pattern
        if criteria:
            self._sql += "%s @> %%s" % self._target_value_expr
            self._params += (self._encode(criteria),)

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
