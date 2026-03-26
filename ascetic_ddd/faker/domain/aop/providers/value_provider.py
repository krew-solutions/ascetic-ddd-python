import typing

from ascetic_ddd.option import Option, Some, Nothing
from ascetic_ddd.faker.domain.query import parse_query
from ascetic_ddd.faker.domain.query.operators import (
    IQueryOperator, EqOperator, MergeConflict, IsNullOperator,
)
from ascetic_ddd.faker.domain.providers.exceptions import DiamondUpdateConflict
from ascetic_ddd.session.interfaces import ISession

__all__ = ('ValueProvider',)

T = typing.TypeVar('T')
T_co = typing.TypeVar('T_co', covariant=True)


class IInputGenerator(typing.Protocol[T_co]):

    async def __call__(self, session: ISession, position: int) -> T_co:
        ...


class ValueProvider(typing.Generic[T]):
    """Leaf provider that generates a single value.

    Value is produced from:
    1. Explicit $eq criteria (set via require())
    2. input_generator callback (creates new values)
    3. None (transient placeholder)
    """

    def __init__(
            self,
            input_generator: IInputGenerator[T] | None = None,
    ) -> None:
        self._input_generator = input_generator
        self._output: Option[T | None] = Nothing()
        self._criteria: IQueryOperator | None = None
        self._is_transient: bool = False

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return
        # If $eq criteria is set, use the value directly
        if self._criteria is not None:
            if isinstance(self._criteria, EqOperator):
                self._output = Some(self._criteria.value)
                self._is_transient = False
                return
            elif isinstance(self._criteria, IsNullOperator) and self._criteria.value:
                self._output = Some(None)
                self._is_transient = False
                return
        # Generate via input_generator
        if self._input_generator is not None:
            value = await self._input_generator(session, -1)
            self._output = Some(value)
            self._is_transient = self._criteria is None
            return
        # No generator — transient None
        self._output = Some(None)
        self._is_transient = True

    def output(self) -> T:
        return self._output.unwrap()  # type: ignore[return-value]

    def require(self, criteria: dict[str, typing.Any]) -> None:
        new_criteria = parse_query(criteria)
        if self._criteria is not None:
            try:
                self._criteria = self._criteria + new_criteria
            except MergeConflict as e:
                raise DiamondUpdateConflict(
                    e.existing_value, e.new_value, 'ValueProvider'
                )
        else:
            self._criteria = new_criteria
        self._output = Nothing()

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

    def is_complete(self) -> bool:
        return self._output.is_some()

    def is_transient(self) -> bool:
        return self._is_transient

    async def setup(self, session: ISession, visited: set[int] | None = None) -> None:
        pass

    async def cleanup(self, session: ISession, visited: set[int] | None = None) -> None:
        pass
