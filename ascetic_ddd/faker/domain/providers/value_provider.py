import typing

from ascetic_ddd.faker.domain.distributors.m2o import DummyDistributor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import ICursor, IM2ODistributor
from ascetic_ddd.faker.domain.providers._mixins import BaseDistributionProvider
from ascetic_ddd.faker.domain.providers.interfaces import IValueProvider
from ascetic_ddd.faker.domain.query.operators import EqOperator, IsNullOperator
from ascetic_ddd.faker.domain.generators.interfaces import IInputGenerator
from ascetic_ddd.faker.domain.generators.generators import prepare_input_generator
from ascetic_ddd.faker.domain.specification.query_lookup_specification import QueryLookupSpecification
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification
__all__ = ('ValueProvider',)

InputT = typing.TypeVar("InputT")
OutputT = typing.TypeVar("OutputT")


class ValueProvider(
    BaseDistributionProvider[InputT, OutputT],
    IValueProvider[InputT, OutputT],
    typing.Generic[InputT, OutputT]
):
    """
    Immutable output - simple ValueObject.
    Architecture:
    IValueProvider = f(input | None) = result,
    where
    result : T <- Distributor[T] <- (
        <- result : result ∈ Sᴛ ∧ P(specification) ~ 𝒟(S)  # select from a set with given probability distribution and Specification
        or
        <- result <- output_factory(input)
            <- input <- (
                require({'$eq': value})
                or
                ValueGenerator(criteria, position | None) <- position | None
            )
        ),
    where
        ":" means instance of type,
        "<-" means "from",
        "∈" means belongs,
        "Sᴛ" or "{x : T}" means set of type "T",
        "∧" means satisfies the condition P(),
        "~ 𝒟(S)" means according to the probability distribution,
        "Σx" means composition of "x",
        "⊆" means subset of a composition.
    """
    _input_generator: IInputGenerator[InputT] | None = None
    _output_factory: typing.Callable[[InputT | None], OutputT] = None  # type: ignore[assignment]  # OutputT of each nested Provider.
    _output_exporter: typing.Callable[[OutputT], InputT] = None  # type: ignore[assignment]

    def __init__(
            self,
            distributor: IM2ODistributor | None,
            input_generator: IInputGenerator[InputT] | None = None,
            output_factory: typing.Callable[[InputT], OutputT] | None = None,
            output_exporter: typing.Callable[[OutputT], InputT] | None = None,
    ):
        if distributor is None:
            distributor = DummyDistributor()

        if self._input_generator is None and input_generator is not None:
            self._input_generator = prepare_input_generator(input_generator)

        if self._output_factory is None:
            if output_factory is None:

                def output_factory(result):
                    return result

            self._output_factory = output_factory  # pyright: ignore[reportAttributeAccessIssue]

        if self._output_exporter is None:
            if output_exporter is None:

                def output_exporter(value):
                    return value

            self._output_exporter = output_exporter

        super().__init__(distributor=distributor)

    async def populate(self, session: ISession) -> None:
        if self._output.is_nothing():
            if isinstance(self._criteria, EqOperator):
                # Extract value from EqOperator
                self._set_input(self._criteria.value)
            elif isinstance(self._criteria, IsNullOperator) and self._criteria.value:
                self._set_input(None)
            if self._input.is_some():
                await self._set_output(session, self._output_factory(typing.cast(InputT, self._input.unwrap())))
                # await cursor.append(session, self._output.unwrap())
            else:
                try:
                    # EqOperator would pollute the BaseDistributor index, must not pass it here.
                    output = (await self._distributor.next(session, self._make_specification())).unwrap()
                    self._set_input(self.export(output))
                    await self._set_output(session, output, is_distributed=True)
                except ICursor as cursor:
                    if self._input_generator is None:
                        self._set_input(None)
                        self._is_transient = True
                        await self._set_output(session, self._output_factory(None))
                    else:
                        self._set_input(await self._input_generator(session, self._criteria, cursor.position))
                        await self._set_output(session, self._output_factory(typing.cast(InputT, self._input.unwrap())))
                        await cursor.append(session, self._output.unwrap())

    def export(self, output: OutputT) -> InputT:
        return self._output_exporter(output)

    def _make_specification(self) -> ISpecification[OutputT]:
        return EmptySpecification[OutputT]()
