import dataclasses
import os
import json
import uuid
import typing

from ascetic_ddd.faker.domain.values.json import Json
from ascetic_ddd.faker.infrastructure.session.rest_session import extract_request
from ascetic_ddd.seedwork.domain.identity.interfaces import IAccessible
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.values.empty import empty
from ascetic_ddd.faker.infrastructure.utils.json import JSONEncoder
from ascetic_ddd.faker.infrastructure.utils.dataclasses import IDataclass
from ascetic_ddd.observable.observable import Observable

__all__ = ('RestRepository',)


T = typing.TypeVar("T", covariant=True, bound=IDataclass)


class RestRepository(Observable, typing.Generic[T]):
    _extract_request = staticmethod(extract_request)
    _base_url: str
    _path: str
    _id_attr: str | None

    def __init__(self, base_url: str, path: str | None = None):
        super().__init__()
        self._base_url = base_url
        if path is not None:
            self._path = path

    async def do_prepare_request(self, session: ISession, url: str):
        pass

    async def insert(self, session: ISession, agg: T):
        params = self.agg_to_params(agg)
        # Changed as self._encode method is not working with our API
        # Used default json serializer from requests package instead of self._encode
        url = self.url(agg)
        await self.do_prepare_request(session, url)
        response = await self._extract_request(session).post(url, data=params)
        await self.do_handle_insert_response(agg, session, response)
        await self._set_pk(agg, response)
        await self.anotify('inserted', session, agg)

    async def do_handle_insert_response(self, session: ISession, agg: T, response):
        pass

    async def _set_pk(self, agg: T, response):
        if self._id_attr is None:
            return
        path = self._id_attr.split(".")
        obj = agg
        resp = (await response.json())
        for step in path[:-1]:
            obj = getattr(obj, step)
            resp = resp.get(step)
        setattr(obj, path[-1], uuid.UUID(resp.get(path[-1])))

    def _get_pk(self, agg: T):
        if self._id_attr is None:
            return
        path = self._id_attr.split(".")
        val = agg
        for step in path:
            val = getattr(val, step)
        return val

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

    def url(self, agg: T) -> str:
        pk = self._get_pk(agg)
        if pk is None:
            return os.path.join(self._base_url, self._path)
        return os.path.join(self._base_url, self._path, str(pk))

    def agg_to_params(self, agg: IDataclass) -> dict:
        params = dict()
        for field in dataclasses.fields(agg):
            value = getattr(agg, field.name)
            if value is empty:
                continue
            elif type(agg).__annotations__[field.name] in (Json, Json | None):
                if value is not None:
                    params[field.name] = self._encode(value)
                else:
                    params[field.name] = value
            elif dataclasses.is_dataclass(value):
                params[field.name] = self.agg_to_params(value)
            else:
                params[field.name] = value
        return params

    @staticmethod
    def _encode(obj):
        return json.dumps(obj, cls=JSONEncoder)
