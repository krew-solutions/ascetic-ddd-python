import json
import typing
from abc import ABCMeta, abstractmethod

from ...domain.aggregate import (
    EventMeta,
    EventMetaExporter,
    IPersistentDomainEventExporter,
    PersistentDomainEvent,
)
from ..session import ISession
from .json import JSONEncoder

__all__ = ("EventInsertQuery", "IEventInsertQuery")


@abstractmethod
class IEventInsertQuery(IPersistentDomainEventExporter, metaclass=ABCMeta):

    @abstractmethod
    def set_stream_type(self, value: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def evaluate(self, session: ISession) -> None:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def make(cls, event: PersistentDomainEvent) -> "IEventInsertQuery":
        raise NotImplementedError


class EventInsertQuery(IEventInsertQuery, metaclass=ABCMeta):
    # TODO: add occurred_at column to table for partitioning reason? created_at with default = NOW()
    _sql = """
        INSERT INTO event_log
        (stream_type, stream_id, stream_position, event_type, event_version, payload, metadata)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s)
    """

    def __init__(self) -> None:
        self.data = {}
        self._params: list[typing.Any] = [None] * 7
        self._metadata = {}
        super().__init__()

    def set_stream_type(self, value: str) -> None:
        self._params[0] = value

    def set_stream_id(self, value: int) -> None:
        self._params[1] = value

    def set_aggregate_version(self, value: int) -> None:
        self._params[2] = value

    def set_event_type(self, value: str) -> None:
        self._params[3] = value

    def set_event_version(self, value: int) -> None:
        self._params[4] = value

    def set_event_meta(self, meta: EventMeta) -> None:
        exporter = EventMetaExporter()
        meta.export(exporter)
        self._params[6] = self._encode(exporter.data)

    async def evaluate(self, session: ISession) -> None:
        self._params[5] = self._encode(self.data)
        async with session.connection.cursor() as acursor:
            await acursor.execute(self._sql, self._params)

    @staticmethod
    def _encode(obj):
        return json.dumps(obj, cls=JSONEncoder)
