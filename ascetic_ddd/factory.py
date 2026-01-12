from .event_bus import IEventBus, InMemoryEventBus
from .utils.amemo import amemo

__all__ = (
    "ascetic_ddd_factory",
    "BuildingBlocksFactory",
)


class BuildingBlocksFactory:
    @amemo
    async def make_in_memory_event_bus(self) -> IEventBus[str, dict]:
        return InMemoryEventBus[str, dict]()


ascetic_ddd_factory = BuildingBlocksFactory()
