from dataclasses import dataclass

__all__ = ("StreamId",)


@dataclass(frozen=True)
class StreamId:
    # tenant_id: int
    stream_type: str
    stream_id: str
