import dataclasses
import json
import typing
from abc import ABCMeta, abstractmethod
from functools import partial

from psycopg.types.json import Jsonb

from ascetic_ddd.faker.domain.values.json import Json
from ascetic_ddd.faker.infrastructure.utils.dataclasses import IDataclass
from ascetic_ddd.faker.infrastructure.utils.json import JSONEncoder
from ascetic_ddd.seedwork.infrastructure.utils.pg import escape
from ascetic_ddd.seedwork.domain.identity.interfaces import IAccessible
from ascetic_ddd.faker.infrastructure.session.pg_session import extract_external_connection
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.observable.observable import Observable

__all__ = ('PgRepository', 'IAggregateState', 'DataclassState',)


T = typing.TypeVar("T", covariant=True)


class PgRepository(Observable, typing.Generic[T]):
    _extract_connection = staticmethod(extract_external_connection)
    _table: str
    _id_attr: str
    _agg_exporter: typing.Callable[[T], 'IAggregateState']
    _agg_factory: typing.Callable[[dict], T]

    def __init__(self):
        super().__init__()

    async def insert(self, session: ISession, agg: T):
        state = self._agg_exporter(agg)

        params = state.kwargs()
        if state.is_auto_increment_pk():
            del params[state.pk_field()]

        sql = """
            INSERT INTO %s (%s)
            VALUES (%s) RETURNING %s
        """ % (
            self._table,
            ", ".join(escape(key) for key in params.keys()),
            ", ".join("%%(%s)s" % key for key in params.keys()),
            escape(state.pk_field())
        )

        async with self._extract_connection(session).cursor() as acursor:
            try:
                await acursor.execute(sql, params)
            except Exception:
                raise
            else:
                if state.is_auto_increment_pk():
                    state.pk_setter()(await acursor.fetchone()[0])

        await self.anotify('inserted', session, agg)

    async def get(self, session: ISession, id_: IAccessible[typing.Any]) -> T | None:
        raise NotImplementedError

    async def update(self, session: ISession, agg: T):
        raise NotImplementedError

    async def find(self, session: ISession, specification: ISpecification) -> typing.Iterable[T]:
        raise NotImplementedError

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass


class IAggregateState(metaclass=ABCMeta):
    @abstractmethod
    def fields(self) -> tuple[str, ...]:
        ...

    @abstractmethod
    def args(self) -> tuple[typing.Any, ...]:
        ...

    @abstractmethod
    def kwargs(self) -> dict[str, typing.Any]:
        ...

    @abstractmethod
    def is_auto_increment_pk(self) -> bool:
        ...

    @abstractmethod
    def pk_field(self) -> str:
        ...

    @abstractmethod
    def pk_param(self) -> typing.Any:
        ...

    @abstractmethod
    def pk_setter(self) -> typing.Callable[[typing.Any], None]:
        ...


class DataclassState(IAggregateState):

    def __init__(self, agg: IDataclass, id_attr: str | None = None, pk_field: str | None = None):
        self._agg = agg
        self._state = self._export_state(agg)

        if id_attr is None:
            id_attr = self._default_id_attr(agg)
        self._id_attr = id_attr

        if pk_field is None:
            pk_field = id_attr.split('.', maxsplit=2)[0]
        self._pk_field = pk_field

        self._pk_param = self._state.get(self._pk_field)

        self._is_auto_increment_pk = self._state.get(self._pk_field) is None

    @staticmethod
    def _default_id_attr(agg: IDataclass):
        id_attr = ''
        obj = agg
        while dataclasses.is_dataclass(obj):
            attr_name = next(iter(dataclasses.fields(obj))).name
            if id_attr:
                id_attr += '.'
            id_attr += attr_name
            obj = getattr(obj, attr_name)
        return id_attr

    def fields(self) -> tuple[str, ...]:
        return tuple(self._state.keys())

    def args(self) -> tuple[typing.Any, ...]:
        return tuple(self._state.values())

    def kwargs(self) -> dict[str, typing.Any]:
        return self._state.copy()

    def is_auto_increment_pk(self) -> bool:
        return self._is_auto_increment_pk

    def pk_field(self) -> str:
        return self._pk_field

    def pk_param(self) -> typing.Any:
        return self._pk_param

    def pk_setter(self) -> typing.Callable[[typing.Any], None]:
        return partial(self._set_id, agg=self._agg)

    async def _set_id(self, agg: T, val):
        if self._id_attr is None:
            return
        path = self._id_attr.split(".")
        obj = agg
        for step in path[:-1]:
            obj = getattr(obj, step)
        setattr(obj, path[-1], val)

    def _get_id(self, agg: IDataclass):
        if self._id_attr is None:
            return
        path = self._id_attr.split(".")
        val = agg
        for step in path:
            val = getattr(val, step)
        return val

    def _export_state(self, agg: IDataclass) -> dict:
        source = self._agg_to_state(agg)
        target = dict()
        for key, value in source.items():
            if dataclasses.is_dataclass(value):
                target.update(self._agg_to_state(value))
            else:
                target[key] = value
        return target

    def _agg_to_state(self, agg: IDataclass) -> dict:
        target = dict()
        for field in dataclasses.fields(agg):
            value = getattr(agg, field.name)
            if type(agg).__annotations__[field.name] in (Json, Json | None):
                if value is not None:
                    target[field.name] = self._encode(value)
                else:
                    target[field.name] = value
            elif isinstance(value, tuple):
                target[field.name] = list(value)
            else:
                target[field.name] = value
        return target

    @staticmethod
    def _encode(obj):
        # if dataclasses.is_dataclass(obj):
        #     obj = dataclasses.asdict(obj)
        dumps = partial(json.dumps, cls=JSONEncoder)
        return Jsonb(obj, dumps)