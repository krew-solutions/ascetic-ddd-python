import random
import typing
from typing import Callable, Protocol, runtime_checkable

from ascetic_ddd.faker.domain.distributors.o2m.interfaces import IO2MDistributor

__all__ = ('DistributionDistributor',)


@runtime_checkable
class ScipyDistribution(Protocol):
    """Protocol для scipy.stats распределений."""
    def rvs(self, size: int | None = None) -> float: ...
    def mean(self) -> float: ...


class DistributionDistributor(IO2MDistributor):
    """
    Универсальный O2M дистрибьютор с произвольным статистическим распределением.

    Принимает распределение как стратегию:
    - scipy.stats distribution (рекомендуется)
    - callable (функция-генератор)

    Примеры scipy.stats распределений:
    - stats.expon() — экспоненциальное (много мелких, мало крупных)
    - stats.pareto(b=2.0) — Парето (правило 80/20)
    - stats.lognorm(s=1.0) — логнормальное (размеры компаний, доходы)
    - stats.gamma(a=2.0) — гамма
    - stats.weibull_min(c=1.5) — Вейбулла
    - stats.zipf(a=2.0) — Zipf (частоты, популярность)

    Примеры:
        from scipy import stats

        # Экспоненциальное распределение
        dist = DistributionDistributor(
            distribution=stats.expon(),
            target_mean=50,
        )

        # Парето (80/20)
        dist = DistributionDistributor(
            distribution=stats.pareto(b=2.0),
            target_mean=50,
        )

        # С callable
        dist = DistributionDistributor(
            sampler=lambda: random.expovariate(1),
            sampler_mean=1.0,
            target_mean=50,
        )

        devices_count = dist.distribute()  # среднее = 50
    """
    _distribution: ScipyDistribution | None
    _sampler: Callable[[], float] | None
    _sampler_mean: float | None
    _target_mean: float

    def __init__(
            self,
            distribution: ScipyDistribution | None = None,
            sampler: Callable[[], float] | None = None,
            sampler_mean: float | None = None,
            target_mean: float | None = None,
    ):
        """
        Args:
            distribution: scipy.stats distribution object (например stats.expon())
            sampler: Callable, возвращающий случайное значение (альтернатива distribution)
            sampler_mean: Среднее значение sampler (обязательно если используется sampler)
            target_mean: Целевое среднее количество items на owner
        """
        if distribution is None and sampler is None:
            raise ValueError("Необходимо указать distribution или sampler")

        if distribution is not None and sampler is not None:
            raise ValueError("Укажите только distribution или sampler, не оба")

        if sampler is not None and sampler_mean is None:
            raise ValueError("При использовании sampler необходимо указать sampler_mean")

        self._distribution = distribution
        self._sampler = sampler
        self._sampler_mean = sampler_mean
        self._target_mean = target_mean if target_mean is not None else 50.0

        # Вычисляем среднее распределения для нормализации
        if self._distribution is not None:
            try:
                self._dist_mean = float(self._distribution.mean())
            except (TypeError, ValueError):
                # Некоторые распределения не имеют конечного среднего
                self._dist_mean = 1.0
        else:
            self._dist_mean = self._sampler_mean if self._sampler_mean else 1.0

    def distribute(self) -> int:
        """
        Возвращает количество items из распределения.

        Returns:
            Случайное количество items. Среднее по всем вызовам = target_mean.
        """
        # Генерируем значение из распределения
        if self._distribution is not None:
            raw_value = float(self._distribution.rvs())
        else:
            raw_value = self._sampler()

        # Нормализуем: raw_value / dist_mean * target_mean
        if self._dist_mean > 0:
            normalized = raw_value / self._dist_mean * self._target_mean
        else:
            normalized = raw_value

        # Возвращаем неотрицательное целое
        return max(0, round(normalized))

    def reset(self) -> None:
        """Для совместимости. Stateless — ничего не делает."""
        pass

    @classmethod
    def exponential(cls, target_mean: float = 50.0) -> 'DistributionDistributor':
        """
        Создаёт дистрибьютор с экспоненциальным распределением.

        Много мелких значений, мало крупных. Среднее = target_mean.
        """
        return cls(
            sampler=lambda: random.expovariate(1),
            sampler_mean=1.0,
            target_mean=target_mean,
        )

    @classmethod
    def pareto(cls, alpha: float = 2.0, target_mean: float = 50.0) -> 'DistributionDistributor':
        """
        Создаёт дистрибьютор с распределением Парето.

        Правило 80/20. alpha определяет степень неравенства:
        - alpha=1.16: 80% items у 20% owners
        - alpha=2.0: умеренное неравенство
        - alpha>3: более равномерное

        Args:
            alpha: Параметр формы (больше = равномернее)
            target_mean: Целевое среднее
        """
        if alpha <= 1:
            raise ValueError("alpha должен быть > 1 для конечного среднего")

        # Среднее Парето = alpha / (alpha - 1) для x_m = 1
        pareto_mean = alpha / (alpha - 1)

        return cls(
            sampler=lambda: random.paretovariate(alpha),
            sampler_mean=pareto_mean,
            target_mean=target_mean,
        )

    @classmethod
    def lognormal(cls, sigma: float = 1.0, target_mean: float = 50.0) -> 'DistributionDistributor':
        """
        Создаёт дистрибьютор с логнормальным распределением.

        Хорошо моделирует размеры компаний, доходы, и т.п.

        Args:
            sigma: Параметр формы (больше = больше разброс)
            target_mean: Целевое среднее
        """
        import math
        # Для lognormal(0, sigma): среднее = exp(sigma^2 / 2)
        lognorm_mean = math.exp(sigma ** 2 / 2)

        return cls(
            sampler=lambda: random.lognormvariate(0, sigma),
            sampler_mean=lognorm_mean,
            target_mean=target_mean,
        )

    @classmethod
    def gamma(cls, shape: float = 2.0, target_mean: float = 50.0) -> 'DistributionDistributor':
        """
        Создаёт дистрибьютор с гамма-распределением.

        Args:
            shape: Параметр формы (k или alpha)
            target_mean: Целевое среднее
        """
        # Среднее гамма = shape * theta, используем theta=1
        gamma_mean = shape

        return cls(
            sampler=lambda: random.gammavariate(shape, 1.0),
            sampler_mean=gamma_mean,
            target_mean=target_mean,
        )

    @classmethod
    def weibull(cls, shape: float = 1.5, target_mean: float = 50.0) -> 'DistributionDistributor':
        """
        Создаёт дистрибьютор с распределением Вейбулла.

        Args:
            shape: Параметр формы (k)
            target_mean: Целевое среднее
        """
        import math
        # Среднее Weibull = lambda * Gamma(1 + 1/k), используем lambda=1
        weibull_mean = math.gamma(1 + 1 / shape)

        return cls(
            sampler=lambda: random.weibullvariate(1.0, shape),
            sampler_mean=weibull_mean,
            target_mean=target_mean,
        )

    @classmethod
    def uniform(cls, target_mean: float = 50.0, spread: float = 0.5) -> 'DistributionDistributor':
        """
        Создаёт дистрибьютор с равномерным распределением.

        Args:
            target_mean: Целевое среднее
            spread: Разброс (0.5 = от 0.5*target_mean до 1.5*target_mean)
        """
        low = target_mean * (1 - spread)
        high = target_mean * (1 + spread)
        uniform_mean = (low + high) / 2

        return cls(
            sampler=lambda: random.uniform(low, high),
            sampler_mean=uniform_mean,
            target_mean=target_mean,
        )
