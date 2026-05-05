"""Activity type resolver - bidirectional mapping between names and activity classes.

Used to serialize and deserialize RoutingSlip across distributed services.
Activity classes are not directly JSON-serializable, so each side of the wire
needs a registry that translates a name (the JSON value) into the activity
class to instantiate.
"""

from abc import ABCMeta, abstractmethod
from typing import Protocol, runtime_checkable

from ascetic_ddd.saga.activity import Activity


__all__ = (
    'ActivityTypeResolver',
    'MapBasedResolver',
    'NamedActivity',
)


@runtime_checkable
class NamedActivity(Protocol):
    """Optional protocol for activities exposing their canonical type name.

    Activities implementing this protocol enable fallback name resolution
    in MapBasedResolver.get_name() when the activity class has not been
    explicitly registered. They also serve as self-documentation of the
    name used on the wire.

    Typical usage in combination with Activity:

        class MyActivity(Activity):
            def type_name(self) -> str:
                return "MyActivity"
    """

    def type_name(self) -> str:
        ...


class ActivityTypeResolver(metaclass=ABCMeta):
    """Abstract bidirectional mapping between activity type names and classes.

    Used by routing_slip_serialization to translate between activity classes
    (which are not JSON-serializable) and their canonical names (which are).
    Dependency injection is preferred over a global registry: each resolver
    is independent, preventing state pollution across tests and services.
    """

    @abstractmethod
    def resolve(self, type_name: str) -> type[Activity]:
        """Return the activity class registered under the given name.

        Args:
            type_name: Name under which the activity class was registered.

        Returns:
            The activity class.

        Raises:
            KeyError: If no activity class is registered under this name.
        """
        ...

    @abstractmethod
    def get_name(self, activity_type: type[Activity]) -> str:
        """Return the registered name for the given activity class.

        Args:
            activity_type: The activity class to look up.

        Returns:
            The registered name. If the class is not registered but
            implements NamedActivity, returns the result of type_name().

        Raises:
            KeyError: If the activity class is neither registered nor
                implements NamedActivity.
        """
        ...


class MapBasedResolver(ActivityTypeResolver):
    """Simple in-memory implementation of ActivityTypeResolver.

    Maintains a pair of maps (name -> type, type -> name) for O(1) lookup
    in both directions. Not thread-safe; register all activity types at
    startup, before serialization begins.
    """

    def __init__(self) -> None:
        self._name_to_type: dict[str, type[Activity]] = {}
        self._type_to_name: dict[type[Activity], str] = {}

    def register(self, name: str, activity_type: type[Activity]) -> None:
        """Register an activity class under the given name.

        Re-registering the same name overwrites the previous entry.

        Args:
            name: Canonical name for the activity class (used as JSON value).
            activity_type: The activity class to register.
        """
        self._name_to_type[name] = activity_type
        self._type_to_name[activity_type] = name

    def resolve(self, type_name: str) -> type[Activity]:
        try:
            return self._name_to_type[type_name]
        except KeyError:
            raise KeyError("activity type not registered: %s" % type_name) from None

    def get_name(self, activity_type: type[Activity]) -> str:
        name = self._type_to_name.get(activity_type)
        if name is not None:
            return name
        # Fallback: instantiate the activity and ask it for its name.
        # Allows serialization of activities that implement NamedActivity
        # even without explicit registration. Deserialization still requires
        # registration (no name -> type mapping exists otherwise).
        instance = activity_type()
        if isinstance(instance, NamedActivity):
            return instance.type_name()
        raise KeyError(
            "activity type not registered: %s" % activity_type.__name__
        )
