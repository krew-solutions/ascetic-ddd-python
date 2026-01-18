import os
import logging

__all__ = ("PgLoggingObserver",)


class PgLoggingObserver:

    def __init__(self, logger: logging.Logger):
        self._logger = logger

    async def __call__(self, aspect, query, params, response_time, **kwargs):
        self._logger.debug("pid: %s; time: %s, sql: %s; params: %r", os.getpid(), response_time, query, params)
