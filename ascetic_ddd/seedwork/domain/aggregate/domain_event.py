from dataclasses import dataclass

__all__ = ("DomainEvent",)


@dataclass(frozen=True, kw_only=True)
class DomainEvent:
    pass
