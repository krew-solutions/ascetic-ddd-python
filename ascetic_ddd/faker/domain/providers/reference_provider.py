import typing
from collections.abc import Callable
from abc import ABCMeta, abstractmethod
from functools import cached_property

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor, IM2ODistributor
from ascetic_ddd.faker.domain.providers._mixins import BaseDistributionProvider
from ascetic_ddd.faker.domain.providers.exceptions import DiamondUpdateConflict
from ascetic_ddd.faker.domain.providers.interfaces import (
    IReferenceProvider, ICloningShunt, ISetupable, IAggregateProvider,
)
from ascetic_ddd.faker.domain.query.operators import (
    IQueryOperator, EqOperator, RelOperator, MergeConflict, IsNullOperator
)
from ascetic_ddd.faker.domain.query.parser import parse_query
from ascetic_ddd.faker.domain.query.visitors import query_to_dict
from ascetic_ddd.option.option import Some, Nothing
from ascetic_ddd.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
from ascetic_ddd.faker.domain.specification.query_lookup_specification import QueryLookupSpecification
from ascetic_ddd.faker.domain.providers.events import CriteriaRequiredEvent, OutputPopulatedEvent

__all__ = ('ReferenceProvider',)

IdInputT = typing.TypeVar("IdInputT")
IdOutputT = typing.TypeVar("IdOutputT")
AggInputT = typing.TypeVar("AggInputT", bound=dict)
AggOutputT = typing.TypeVar("AggOutputT", bound=object)


class IAggregateProviderAccessor(
    ISetupable,
    typing.Generic[AggInputT, AggOutputT, IdInputT, IdOutputT],
    metaclass=ABCMeta
):
    # TODO: Make resolve() explicitly?
    # TODO: Use Monad, Deferred or Future?

    @abstractmethod
    def __call__(
            self,
            reference_provider: IReferenceProvider[IdInputT, IdOutputT, AggInputT, AggOutputT]
    ) -> IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT]:
        raise NotImplementedError

    @abstractmethod
    def reset(self, visited: set | None = None):
        raise NotImplementedError

    @abstractmethod
    def clone(self, shunt: ICloningShunt | None = None) -> typing.Self:
        raise NotImplementedError


class ReferenceProvider(
    BaseDistributionProvider[IdInputT, IdOutputT],
    IReferenceProvider[IdInputT, IdOutputT, AggInputT, AggOutputT],
    typing.Generic[IdInputT, IdOutputT, AggInputT, AggOutputT]
):
    _aggregate_provider_accessor: IAggregateProviderAccessor[AggInputT, AggOutputT, IdInputT, IdOutputT]

    def __init__(
            self,
            distributor: IM2ODistributor[IdOutputT],
            aggregate_provider: (IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT] |
                                 Callable[[], IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT]]),
    ):
        self.aggregate_provider = aggregate_provider
        super().__init__(distributor=distributor)

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return

        try:
            result = await self._distributor.next(session, self._make_specification())
            if result.is_some():
                id_output = result.unwrap()
                id_input = self.export(id_output)
                # id_input = self.aggregate_provider.id_provider.export(id_output)
                self.aggregate_provider.id_provider.require({'$eq': id_input})
                # self.aggregate_provider.require({self._id_attr: {'$eq': id_input}})
                await self.aggregate_provider.populate(session)
                self._set_input(id_input)
                await self._set_output(session, id_output, is_distributed=True)
            else:
                # Alternative to "if isinstance(new_criteria, EqOperator) and new_criteria.value is None"
                # self._criteria = None
                self._set_input(None)
                await self._set_output(session, None, is_distributed=True)
        except ICursor as cursor:
            if self._criteria is not None:
                # Propagate constraints to aggregate_provider (already done in require())
                pass
            await self.aggregate_provider.populate(session)
            id_output = self.aggregate_provider.id_provider.output()
            await cursor.append(session, id_output)
            self._set_input(self.aggregate_provider.id_provider.state())
            # self.require() could reset self._output
            await self._set_output(session, id_output)

    def _make_specification(self) -> ISpecification[IdOutputT]:
        # Create specification with aggregate_provider_accessor for lazy lookup and subqueries
        if self._criteria is not None:
            return QueryLookupSpecification[IdOutputT](
                self._criteria,
                self.export,
                aggregate_provider_accessor=lambda: self.aggregate_provider,
            )
        return EmptySpecification[IdOutputT]()

    def require(self, criteria: dict[str, typing.Any]) -> None:
        """
        Set reference provider value using query format.

        Supports all formats:
        - {'$eq': 27} - scalar PK
        - {'tenant_id': {'$eq': 15}, 'local_id': {'$eq': 27}} - composite PK
        - {'$rel': {'is_active': {'$eq': True}}} - related aggregate criteria

        Non-$rel criteria are ID-level constraints for the distributor.
        $rel criteria are propagated to aggregate_provider.
        """
        new_criteria = parse_query(criteria)

        # Null FK — no reference. Don't propagate to aggregate.
        if isinstance(new_criteria, EqOperator) and new_criteria.value is None:
            self._set_input(None)
            self._output = Some(None)
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
                raise DiamondUpdateConflict(e.existing_value, e.new_value, self.provider_name) from e
        else:
            self._criteria = new_criteria
        # Only reset output if input actually changed
        if self._criteria != old_criteria:
            self._input = Nothing()
            self._output = Nothing()
            self._propagate_to_aggregate(new_criteria)
            self._on_required.notify(CriteriaRequiredEvent(new_criteria))

    def export(self, output: IdOutputT) -> IdInputT:
        return self.aggregate_provider.id_provider.export(output)

    def _propagate_to_aggregate(self, criteria: IQueryOperator) -> None:
        """
        Propagate constraints to aggregate_provider.

        - $rel criteria → propagate to aggregate_provider (field-level constraints)
        - ID criteria → propagate to id_provider directly

        infinite recursion prevention:
        We only propagate once during require(), not recursively.
        """
        if isinstance(criteria, RelOperator):
            self.aggregate_provider.require(query_to_dict(criteria.query))
        else:
            self.aggregate_provider.id_provider.require(query_to_dict(criteria))

    @property
    def aggregate_provider(self) -> IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT]:
        return self._aggregate_provider_accessor(self)

    @aggregate_provider.setter
    def aggregate_provider(
            self,
            aggregate_provider: (IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT] |
                                 Callable[[], IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT]])
    ) -> None:
        if callable(aggregate_provider):
            self._aggregate_provider_accessor = LazyAggregateProviderAccessor[
                AggInputT, AggOutputT, IdInputT, IdOutputT
            ](aggregate_provider)

        else:
            self._aggregate_provider_accessor = AggregateProviderAccessor[
                AggInputT, AggOutputT, IdInputT, IdOutputT
            ](aggregate_provider)

    def _do_clone(self, clone: typing.Self, shunt: ICloningShunt):
        clone._aggregate_provider_accessor = self._aggregate_provider_accessor.clone(shunt)
        super()._do_clone(clone, shunt)

    def _do_reset(self, visited: set) -> None:
        self._aggregate_provider_accessor.reset(visited)
        super()._do_reset(visited)

    @cached_property
    def _id_attr(self) -> str:
        return self.aggregate_provider.id_provider.provider_name.rsplit(".", 1)[-1]

    async def setup(self, session: ISession):
        await super().setup(session)
        await self._aggregate_provider_accessor.setup(session)

    async def cleanup(self, session: ISession):
        await super().cleanup(session)
        await self._aggregate_provider_accessor.cleanup(session)


class AggregateProviderAccessor(
    IAggregateProviderAccessor[AggInputT, AggOutputT, IdInputT, IdOutputT],
    typing.Generic[AggInputT, AggOutputT, IdInputT, IdOutputT]
):
    _aggregate_provider: IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT]
    _subscribed: bool

    def __init__(
            self,
            aggregate_provider: IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT]
    ):
        self._aggregate_provider = aggregate_provider
        self._subscribed = False

    def __call__(
            self,
            reference_provider: IReferenceProvider[IdInputT, IdOutputT, AggInputT, AggOutputT]
    ) -> IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT]:

        if not self._subscribed:
            async def _observer(event: OutputPopulatedEvent[IdOutputT]):
                if not event.is_transient:
                    await reference_provider.append(event.session, event.output)

            self._aggregate_provider.id_provider.on_populated.attach(_observer, id(reference_provider))
            self._subscribed = True

        return self._aggregate_provider

    def reset(self, visited: set | None = None):
        self._aggregate_provider.reset(visited)

    def clone(self, shunt: ICloningShunt | None = None):
        return AggregateProviderAccessor[
            AggInputT, AggOutputT, IdInputT, IdOutputT
        ](self._aggregate_provider.clone(shunt))

    async def setup(self, session: ISession):
        await self._aggregate_provider.setup(session)

    async def cleanup(self, session: ISession):
        await self._aggregate_provider.cleanup(session)


class LazyAggregateProviderAccessor(
    IAggregateProviderAccessor[AggInputT, AggOutputT, IdInputT, IdOutputT],
    typing.Generic[AggInputT, AggOutputT, IdInputT, IdOutputT]
):
    _aggregate_provider: IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT] | None = None
    _aggregate_provider_factory: Callable[[], IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT]]

    def __init__(
            self,
            aggregate_provider_factory: Callable[[], IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT]]
    ):
        self._aggregate_provider_factory = aggregate_provider_factory

    def __call__(
            self,
            reference_provider: IReferenceProvider[IdInputT, IdOutputT, AggInputT, AggOutputT]
    ) -> IAggregateProvider[AggInputT, AggOutputT, IdInputT, IdOutputT]:
        if self._aggregate_provider is None:
            self._aggregate_provider = self._aggregate_provider_factory()

            async def _observer(event: OutputPopulatedEvent[IdOutputT]):
                if not event.is_transient:
                    await reference_provider.append(event.session, event.output)

            self._aggregate_provider.id_provider.on_populated.attach(_observer, id(reference_provider))

        return self._aggregate_provider

    def reset(self, visited: set | None = None):
        self._aggregate_provider = None

    def clone(self, shunt: ICloningShunt | None = None):
        return LazyAggregateProviderAccessor[
            AggInputT, AggOutputT, IdInputT, IdOutputT
        ](self._aggregate_provider_factory)

    async def setup(self, session: ISession):
        if self._aggregate_provider is not None:
            await self._aggregate_provider.setup(session)

    async def cleanup(self, session: ISession):
        if self._aggregate_provider is not None:
            await self._aggregate_provider.cleanup(session)
