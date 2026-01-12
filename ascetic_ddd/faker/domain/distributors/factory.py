import typing

from ascetic_ddd.faker.domain.distributors import WeightedDistributor, NullableDistributor
from ascetic_ddd.faker.domain.distributors.interfaces import IDistributor
from ascetic_ddd.faker.domain.distributors.sequence_distributor import SequenceDistributor
from ascetic_ddd.faker.domain.distributors.skew_distributor import SkewDistributor

__all__ = ('distributor_factory', 'skew_distributor_factory')


T = typing.TypeVar("T", covariant=True)


def distributor_factory(
    weights: list[float] | None = None,
    scale: float | None = None,
    null_weight: float = 0,
) -> IDistributor[T]:
    if weights is not None:
        dist = WeightedDistributor[T](weights, scale)
    else:
        dist = SequenceDistributor[T]()
    if null_weight:
        dist = NullableDistributor[T](dist, null_weight)
    return dist


def skew_distributor_factory(
    skew: float = 2.0,
    scale: float | None = None,
    null_weight: float = 0,
) -> IDistributor[T]:
    """
    Фабрика для SkewDistributor.

    Args:
        skew: Параметр перекоса (1.0 = равномерно, 2.0+ = перекос к началу)
        scale: Среднее количество использований каждого значения
        null_weight: Вероятность вернуть None (0-1)
    """
    dist = SkewDistributor[T](skew=skew, scale=scale)
    if null_weight:
        dist = NullableDistributor[T](dist, null_weight)
    return dist
