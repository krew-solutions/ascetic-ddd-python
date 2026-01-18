from urllib.parse import urlparse
try:
    from aiodogstatsd import Client
except ImportError:

    class Client:
        pass


__all__ = ("RestStatsdObserver", "make_statsd_client")


async def make_statsd_client(address="udp://127.0.0.1:8125", **kw):
    res = urlparse(address)
    client = Client(host=res.hostname, port=res.port, **kw)
    await client.connect()
    return client


class RestStatsdObserver:
    _client: Client

    def __init__(self, client: Client):
        self._client = client

    async def request_ended(self, aspect, request_view, **kwargs):
        """
        https://gr1n.github.io/aiodogstatsd/usage/
        """
        self._client.timing(request_view.label, value=request_view.response_time.total_seconds())
        self._client.increment(
            request_view.label + "." + str(request_view.status)
        )

    async def session_ended(self, aspect, session, **kwargs):
        pass
        # client = self._client
        # await client.close()
