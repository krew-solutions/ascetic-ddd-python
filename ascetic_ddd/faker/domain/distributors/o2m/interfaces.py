from abc import ABCMeta, abstractmethod

__all__ = ('IO2MDistributor',)


class IO2MDistributor(metaclass=ABCMeta):
    """
    Интерфейс O2M дистрибьютора.

    В отличие от M2O (который для каждого child выбирает parent),
    O2M для каждого parent определяет сколько children создать.

    Stateless: каждый вызов distribute() независим.
    Подходит для многопоточного использования.

    Пример:
        dist = SkewDistributor(skew=2.0, mean=50)

        for _ in range(companies_count):
            company = create_company()
            devices_count = dist.distribute()  # среднее = 50
            create_devices(company, devices_count)
    """

    @abstractmethod
    def distribute(self) -> int:
        """
        Возвращает количество items для текущего owner.

        Returns:
            Случайное количество items. Среднее по всем вызовам = mean.
        """
        raise NotImplementedError
