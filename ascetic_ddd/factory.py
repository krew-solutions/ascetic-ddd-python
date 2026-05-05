from ascetic_ddd.bus import IBus, InMemoryBus
from ascetic_ddd.session.interfaces import ISession
from ascetic_ddd.utils.amemo import amemo

__all__ = (
    "ascetic_ddd_factory",
    "BuildingBlocksFactory",
)


class BuildingBlocksFactory:
    @amemo
    async def make_in_memory_bus(self) -> IBus[ISession]:
        return InMemoryBus[ISession]()


ascetic_ddd_factory = BuildingBlocksFactory()
