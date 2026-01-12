import typing

from ascetic_ddd.faker.domain.distributors import NullableDistributor
from ascetic_ddd.faker.domain.distributors.interfaces import IDistributor
from ascetic_ddd.faker.infrastructure.distributors.pg_sequence_distributor import PgSequenceDistributor
from ascetic_ddd.faker.infrastructure.distributors.pg_skew_distributor import PgSkewDistributor
from ascetic_ddd.faker.infrastructure.distributors import PgWeightedDistributor

__all__ = ('pg_distributor_factory', 'pg_skew_distributor_factory')


T = typing.TypeVar("T", covariant=True)


def pg_distributor_factory(
    weights: list[float] | None = None,
    scale: float | None = None,
    null_weight: float = 0,
) -> IDistributor[T]:
    if weights is not None:
        dist = PgWeightedDistributor[T](weights, scale)
    else:
        dist = PgSequenceDistributor[T]()
    if null_weight:
        dist = NullableDistributor[T](dist, null_weight)
    return dist


def pg_skew_distributor_factory(
    skew: float = 2.0,
    scale: float | None = None,
    null_weight: float = 0,
) -> IDistributor[T]:
    """
    Фабрика для PgSkewDistributor.

    Args:
        skew: Параметр перекоса (1.0 = равномерно, 2.0+ = перекос к началу)
        scale: Среднее количество использований каждого значения
        null_weight: Вероятность вернуть None (0-1)
    """
    dist = PgSkewDistributor[T](skew=skew, scale=scale)
    if null_weight:
        dist = NullableDistributor[T](dist, null_weight)
    return dist
