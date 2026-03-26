import typing
from collections.abc import Callable

from ascetic_ddd.option import Option, Some, Nothing
from ascetic_ddd.faker.domain.aop.providers.interfaces import IProvider
from ascetic_ddd.faker.domain.query import parse_query, query_to_dict
from ascetic_ddd.faker.domain.query.operators import (
    IQueryOperator, EqOperator, RelOperator, MergeConflict, IsNullOperator,
)
from ascetic_ddd.faker.domain.providers.exceptions import DiamondUpdateConflict
from ascetic_ddd.session.interfaces import ISession

__all__ = ('ReferenceProvider',)

IdT = typing.TypeVar('IdT')


class ReferenceProvider(typing.Generic[IdT]):
    """Provides FK ID by populating referenced aggregate and extracting its ID.

    Intended to be wrapped by DistributedProvider for distributor-based selection:

        DistributedProvider(
            ReferenceProvider(aggregate_provider=tenant_provider, id_attr='id'),
            distributor=fk_distributor,
        )

    Supports require() formats:
    - {'$eq': 27} — scalar FK value
    - {'tenant_id': ..., 'local_id': ...} — composite FK
    - {'$rel': {'status': {'$eq': 'active'}}} — constraints on related aggregate

    Args:
        aggregate_provider: Referenced aggregate provider, or callable factory
            for lazy resolution (cyclic dependencies).
        id_attr: Field name to extract ID from aggregate state.
    """

    def __init__(
            self,
            aggregate_provider: 'IProvider[typing.Any] | Callable[[], IProvider[typing.Any]]',
            id_attr: str = 'id',
    ) -> None:
        self._id_attr = id_attr
        self._output: Option[IdT | None] = Nothing()
        self._criteria: IQueryOperator | None = None
        self._is_transient: bool = False
        # Lazy (callable factory) or eager (provider instance) resolution.
        # Provider instances have populate attribute; plain callables do not.
        if callable(aggregate_provider) and not hasattr(aggregate_provider, 'populate'):
            self._aggregate_provider_factory: Callable[[], IProvider[typing.Any]] | None = aggregate_provider
            self._aggregate_provider: IProvider[typing.Any] | None = None
        else:
            self._aggregate_provider_factory = None
            self._aggregate_provider = typing.cast(IProvider[typing.Any], aggregate_provider)

    @property
    def aggregate_provider(self) -> 'IProvider[typing.Any]':
        if self._aggregate_provider is None:
            assert self._aggregate_provider_factory is not None
            self._aggregate_provider = self._aggregate_provider_factory()
        return self._aggregate_provider

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return
        agg = self.aggregate_provider
        await agg.populate(session)
        id_value = self._extract_id(agg)
        self._output = Some(id_value)
        self._is_transient = False

    def _extract_id(self, agg_provider: 'IProvider[typing.Any]') -> typing.Any:
        output = agg_provider.output()
        if hasattr(output, self._id_attr):
            return getattr(output, self._id_attr)
        state = agg_provider.state()
        if isinstance(state, dict) and self._id_attr in state:
            return state[self._id_attr]
        return state

    def output(self) -> IdT:
        return self._output.unwrap()  # type: ignore[return-value]

    def require(self, criteria: dict[str, typing.Any]) -> None:
        new_criteria = parse_query(criteria)

        # Null FK — no reference
        if isinstance(new_criteria, EqOperator) and new_criteria.value is None:
            self._output = Some(None)
            self._is_transient = False
            return
        elif isinstance(new_criteria, IsNullOperator) and new_criteria.value:
            self._output = Some(None)
            self._is_transient = False
            return

        old_criteria = self._criteria

        if self._criteria is not None:
            try:
                self._criteria = self._criteria + new_criteria
            except MergeConflict as e:
                raise DiamondUpdateConflict(
                    e.existing_value, e.new_value, 'ReferenceProvider'
                )
        else:
            self._criteria = new_criteria

        if self._criteria != old_criteria:
            self._output = Nothing()
            self._propagate_to_aggregate(new_criteria)

    def _propagate_to_aggregate(self, criteria: IQueryOperator) -> None:
        """Propagate constraints to aggregate_provider.

        - $rel criteria -> propagate body to aggregate_provider
        - ID criteria -> propagate to aggregate_provider's ID field
        """
        if isinstance(criteria, RelOperator):
            self.aggregate_provider.require(query_to_dict(criteria.query))
        else:
            self.aggregate_provider.require(
                {self._id_attr: query_to_dict(criteria)}
            )

    def state(self) -> typing.Any:
        return self._output.unwrap()

    def reset(self, visited: set[int] | None = None) -> None:
        if visited is None:
            visited = set()
        if id(self) in visited:
            return
        visited.add(id(self))
        self._output = Nothing()
        self._criteria = None
        self._is_transient = False
        if self._aggregate_provider is not None:
            self._aggregate_provider.reset(visited)
        # Reset lazy accessor for next resolution
        if self._aggregate_provider_factory is not None:
            self._aggregate_provider = None

    def is_complete(self) -> bool:
        return self._output.is_some()

    def is_transient(self) -> bool:
        return self._is_transient

    async def setup(self, session: ISession, visited: set[int] | None = None) -> None:
        if visited is None:
            visited = set()
        if id(self) in visited:
            return
        visited.add(id(self))
        # Don't force-resolve lazy factory during setup — avoids infinite
        # recursion for self-referential structures (parent_id → same faker).
        if self._aggregate_provider is not None:
            await self._aggregate_provider.setup(session, visited)

    async def cleanup(self, session: ISession, visited: set[int] | None = None) -> None:
        if visited is None:
            visited = set()
        if id(self) in visited:
            return
        visited.add(id(self))
        if self._aggregate_provider is not None:
            await self._aggregate_provider.cleanup(session, visited)
