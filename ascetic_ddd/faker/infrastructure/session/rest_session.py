import dataclasses
import typing
import socket
import aiohttp
from contextlib import asynccontextmanager
from time import perf_counter

from aiohttp.client import ClientSession

from ascetic_ddd.observable.observable import Observable
from ascetic_ddd.seedwork.domain.session.interfaces import ISessionPool, ISession
from ascetic_ddd.faker.infrastructure.session.interfaces import IRestSession

__all__ = (
    "RestSession",
    "RestSessionPool",
    "extract_request",
)

_HOST = socket.gethostname()


def extract_request(session: ISession) -> ClientSession:
    return typing.cast(IRestSession, session).request


class RestSessionPool(Observable, ISessionPool):

    def __init__(self) -> None:
        super().__init__()

    @asynccontextmanager
    async def session(self) -> typing.AsyncIterator[ISession]:
        session = RestSession()
        await self.anotify(
            aspect='session_started',
            session=session
        )
        try:
            yield session
        finally:
            await self.anotify(
                aspect='session_ended',
                session=session
            )


class RestSession(Observable, IRestSession):
    # _client_session: httpx.AsyncClient
    _client_session: ClientSession
    _parent: typing.Optional["RestSession"]

    @dataclasses.dataclass(kw_only=True)
    class RequestViewModel:
        time_start: float
        label: str
        status: int | None
        response_time: float | None

        def __str__(self):
            return self.label + "." + str(self.status)

    def __init__(self, parent: typing.Optional["RestSession"] = None, client_session: ClientSession | None = None):
        super().__init__()
        self._parent = parent

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(self._on_request_start)
        trace_config.on_request_end.append(self._on_request_end)
        self._client_session = client_session or ClientSession(trace_configs=[trace_config])

    async def _on_request_start(self, session, context, params):
        prefix = "performance-testing.%(hostname)s.%(method)s.%(host)s.%(path)s"
        data = {
            "method": params.method,
            "hostname": _HOST,
            "host": params.url.host,
            "path": params.url.path,
        }
        context._request_view = self.RequestViewModel(
            time_start=perf_counter(),  # asyncio.get_event_loop().time()
            label=prefix % data,
            status=None,
            response_time=None,
        )

        await self.anotify(
            aspect='request_started',
            session=self,
            sender=context,
            request_view=context._request_view,
        )

    async def _on_request_end(self, session, context, params):
        request_view = context._request_view

        # response_time = asyncio.get_event_loop().time() - request_view.time_start
        response_time = perf_counter() - request_view.time_start
        request_view.status = params.response.status
        request_view.response_time = response_time

        await self.anotify(
            aspect='request_ended',
            session=self,
            sender=context,
            request_view=request_view,
        )

    @asynccontextmanager
    async def atomic(self) -> typing.AsyncIterator[ISession]:
        async with self._client_session as client_session:
            session = RestTransactionSession(self, client_session)
            await self.anotify(
                aspect='session_started',
                session=session
            )
            try:
                yield session
            finally:
                await self.anotify(
                    aspect='session_ended',
                    session=session
                )

    @property
    # def request(self) -> httpx.AsyncClient:
    def request(self) -> ClientSession:
        return self._client_session


class RestTransactionSession(RestSession):
    pass
