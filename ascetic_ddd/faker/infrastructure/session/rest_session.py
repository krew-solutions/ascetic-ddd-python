import typing
import socket
import aiohttp
from contextlib import asynccontextmanager
from time import perf_counter

from aiohttp.client import ClientSession

from ascetic_ddd.observable.observable import Observable
from ascetic_ddd.faker.domain.session.interfaces import ISessionPool, ISession
from ascetic_ddd.faker.infrastructure.session.interfaces import IRestSession
from ascetic_ddd.faker.domain.utils.stats import Collector

__all__ = (
    "RestSession",
    "RestSessionPool",
    "extract_request",
)

_HOST = socket.gethostname()


def extract_request(session: ISession) -> ClientSession:
    return typing.cast(IRestSession, session).request


class RestSessionPool(Observable, ISessionPool):
    response_time: float
    stats: Collector

    def __init__(self) -> None:
        self.response_time = 0.0
        self.stats = Collector()
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
            self.response_time += session.response_time
            self.stats.update(session.stats)
            await self.anotify(
                aspect='session_ended',
                session=session
            )


class RestSession(Observable, IRestSession):
    # _client_session: httpx.AsyncClient
    _client_session: ClientSession
    _parent: typing.Optional["RestSession"]
    response_time: float
    stats: Collector

    def __init__(self, parent: typing.Optional["RestSession"] = None, client_session: ClientSession | None = None):
        super().__init__()
        self._parent = parent
        self.response_time = 0.0
        self.stats = Collector()

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(self._on_request_start)
        trace_config.on_request_end.append(self._on_request_end)
        self._client_session = client_session or ClientSession(trace_configs=[trace_config])

    class RequestViewModel:
        time_start: float
        label: str
        data: dict
        status: int
        response_time: float

        def __str__(self):
            return self.label + "." + str(self.status)

    async def _on_request_start(self, session, context, params):
        request = self.RequestViewModel()
        # self._request.time_start = asyncio.get_event_loop().time()
        request.time_start = perf_counter()
        prefix = "performance-testing.%(hostname)s.%(method)s.%(host)s.%(path)s"
        data = {
            "method": params.method,
            "hostname": _HOST,
            "host": params.url.host,
            "path": params.url.path,
        }
        label = prefix % data
        request.label = label
        request.data = data
        context._request = request

    async def _on_request_end(self, session, context, params):
        # response_time = asyncio.get_event_loop().time() - self._time_start
        request = context._request
        response_time = perf_counter() - request.time_start
        self.response_time += response_time
        if self._parent:
            self._parent.response_time += response_time

        request.status = params.response.status
        request.response_time = response_time
        self.stats.append("%s.%s" % (request.label, str(request.status)), response_time)

        await self.anotify(
            aspect='request_complete',
            request=request,
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
                self.response_time += session.response_time
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
