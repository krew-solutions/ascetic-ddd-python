import typing

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.distributors.m2o.weighted_distributor import WeightedDistributor
from ascetic_ddd.faker.domain.distributors.m2o.nullable_distributor import NullableDistributor
from ascetic_ddd.faker.domain.distributors.m2o.dummy_distributor import DummyDistributor
from ascetic_ddd.faker.domain.distributors.m2o.sequence_distributor import SequenceDistributor
from ascetic_ddd.faker.domain.distributors.m2o.skew_distributor import SkewDistributor

__all__ = ('distributor_factory',)


T = typing.TypeVar("T", covariant=True)


def distributor_factory(
    weights: list[float] | None = None,
    skew: float | None = None,
    scale: float | None = None,
    null_weight: float = 0,
    sequence: bool = False
) -> IM2ODistributor[T]:
    """
    Фабрика для Distributor.

    Args:
        weights: If a weights sequence is specified, selections are made according to the relative weights.
        skew: Параметр перекоса (1.0 = равномерно, 2.0+ = перекос к началу). Default = 2.0
        scale: Среднее количество использований каждого значения. Use scale = 1 for unique.
        null_weight: Вероятность вернуть None (0-1)
        sequence: Pass sequence number to value generator.
    """
    if weights is not None:
        dist = WeightedDistributor[T](weights, scale)
    elif skew is not None:
        dist = SkewDistributor[T](skew=skew, scale=scale)
    elif sequence:
        dist = SequenceDistributor[T]()
    else:
        dist = DummyDistributor()
    if null_weight:
        dist = NullableDistributor[T](dist, null_weight)
    return dist
