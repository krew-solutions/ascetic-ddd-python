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

    async def request_complete(self, aspect, request):
        """
        https://gr1n.github.io/aiodogstatsd/usage/
        """
        client = self._client
        client.timing(request.label, value=request.response_time.total_seconds())
        client.increment(
            request.label + "." + str(request.status)
        )

    async def session_ended(self, aspect, session):
        pass
        # client = self._client
        # await client.close()
