import typing

from ascetic_ddd.faker.domain.query import parse_query
from ascetic_ddd.faker.domain.query.operators import EqOperator, IsNullOperator
from ascetic_ddd.session.interfaces import ISession

__all__ = ('ValueFactory',)

T = typing.TypeVar('T')
T_co = typing.TypeVar('T_co', covariant=True)


class IInputGenerator(typing.Protocol[T_co]):

    async def __call__(self, session: ISession, position: int) -> T_co:
        ...


class ValueFactory(typing.Generic[T]):
    """Stateless leaf creator.

    Produces a value from:
    1. Explicit $eq in criteria
    2. input_generator callback
    3. None (transient placeholder)
    """

    def __init__(
            self,
            input_generator: IInputGenerator[T] | None = None,
    ) -> None:
        self._input_generator = input_generator

    async def create(
            self,
            session: ISession,
            criteria: dict[str, typing.Any] | None = None,
    ) -> T | None:
        if criteria is not None:
            parsed = parse_query(criteria)
            if isinstance(parsed, EqOperator):
                return parsed.value
            elif isinstance(parsed, IsNullOperator) and parsed.value:
                return None
        if self._input_generator is not None:
            return await self._input_generator(session, -1)
        return None

    async def setup(self, session: ISession) -> None:
        pass

    async def cleanup(self, session: ISession) -> None:
        pass
