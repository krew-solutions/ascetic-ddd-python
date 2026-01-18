
from ascetic_ddd.faker.domain.utils.stats import Collector

__all__ = ("RestStatsObserver",)


class RestStatsObserver:
    _stats: Collector

    def __init__(self, stats: Collector):
        self._stats = stats

    @property
    def stats(self) -> Collector:
        return self._stats

    async def request_ended(self, aspect, request_view, **kwargs):
        self.stats.append("%s.%s" % (request_view.label, str(request_view.status)), request_view.response_time)
