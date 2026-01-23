# Генератор фейковых данных DDD-приложения

# Зачем?

На показатели нагрузочного тестирования существенное влияние оказывает селективность индексов БД.
Один и тот же объем данных при различной селективности индексов может дать существенно отличающиеся результаты.

Я попытался найти коробочное решение, позволяющее воспроизвести селективность индексов базы данных целевой системы,
но эти попытки остались безуспешными.
Вот что ответил мне Claud:

> Problems with existing solutions:
> 1. No distribution control — Faker generates uniformly, real data has skew (Zipf, Pareto)
> 2. No M2O/O2M relationships — hard to generate "20% of companies have 80% of orders"
> 3. Stateless — each call is independent, can't reuse created entities
> 4. No specifications — can't request "a company from Moscow with active status"

> But limitations remain:
> 1. Fixed quantity — size=3, not "from 1 to 100 with exponential distribution"
> 2. No reuse — each SubFactory creates a new object, can't "pick an existing company with 80% probability"
> 3. No distribution — can't say "20% of companies get 80% of orders"

Другая проблема заключается в том, что сгенерированные данные должны соответствовать инвариантам бизнес-логики.
Бизнес-логика реализуется доменным слоем приложения.
Таким образом, генерация валидных данных подразумевает под собой либо полное воспроизводство бизнес-логики генератором
фейковых данных, либо реиспользование доменных моделей генератором фейковых данных.

Поскольку агрегат доменной модели инкапсулирован, и зачастую требуется вызвать несколько его методов,
чтоб привести его в требуемое состояние,
при этом сохранение агрегата зачастую происходит в несколько SQL-запросов (особенно Event Sourced Aggregate),
а доступ к внутреннему состоянию инкапсулированного агрегата извне закрыт,
то наиболее удобным вариантом является реиспользование доменных моделей генератором фейковых данных.

Другой вариант подразумевает использование CQRS-Commands приложения вместо прямого доступа к доменной модели
приложения.
Обращаться к CQRS-Commands можно как In-Process (минуя сетевые Hexagonal Adapters),
так и Out-Of-Process (через сетевой интерфейс приложения).
В таком случае генератор фейковых становится удобным не только для генерации фейковых данных для нагрузочного тестирования,
но и для In-Process Component (Service) Tests, а так же для Out-of-Process Component (Service) Tests.
А именно на этом уровне обычно делаются Acceptance Tests для Service, зачастую с использованием
BDD (Behavior-driven development) и ATDD (Acceptance Test-Driven Development).

Подробнее о пирамиде тестирования микросервисов смотрите в
[Testing Strategies in a Microservice Architecture](https://martinfowler.com/articles/microservice-testing/).

Данный пакет проекта может так же использоваться для генерации \*csv, \*.jsonl фидов (feeds) для нагрузочных движков в формате Command Log. Подробней об этом будет позже.


# Распределение для distributor

Как снять распределение с БД действующего проекта?


## Снятие weights для большого диапазона

```sql
SELECT array_agg(weight ORDER BY part)
FROM (
  SELECT
      ntile(4) OVER (ORDER BY c DESC) AS part,
      SUM(c) OVER (PARTITION BY ntile(4) OVER (ORDER BY c DESC)) /
      SUM(c) OVER () AS weight
  FROM (
      SELECT company_id, COUNT(*) AS c
      FROM employees
      WHERE company_id IS NOT NULL
      GROUP BY company_id
  ) AS per_company
) AS t
GROUP BY part;
```


## Снятие skew

Skew вычисляется через log-log линейную регрессию (степенной закон Ципфа).

Математическое обоснование:
- SkewDistributor использует формулу: `idx = floor(n * (1 - random())^skew)`
- Это даёт плотность вероятности: `p(x) ∝ x^(1/skew - 1)`
- Закон Ципфа: `freq(rank) ∝ rank^(-alpha)`
- Сравнивая показатели: `-alpha = 1/skew - 1`

Формулы преобразования:
- `alpha = 1 - 1/skew = (skew - 1) / skew`
- `skew = 1 / (1 - alpha)`

```sql
WITH ranked AS (
    SELECT
        company_id,
        COUNT(*) AS freq,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank
    FROM employees
    WHERE company_id IS NOT NULL
    GROUP BY company_id
),
log_data AS (
    SELECT
        LN(rank::float) AS log_rank,
        LN(freq::float) AS log_freq
    FROM ranked
    WHERE rank <= (SELECT COUNT(*) * 0.9 FROM ranked)  -- отбросить хвост
)
SELECT
    1.0 / (1.0 + REGR_SLOPE(log_freq, log_rank)) AS skew,
    -REGR_SLOPE(log_freq, log_rank) AS alpha,
    REGR_R2(log_freq, log_rank) AS r_squared
FROM log_data;
```

Примечание: `slope < 0` для Zipf-данных, поэтому `1 + slope = 1 - alpha`.

Интерпретация:
- `alpha ≈ 0` → `skew ≈ 1.0` — равномерное распределение
- `alpha ≈ 0.5` → `skew ≈ 2.0` — умеренный перекос
- `alpha ≈ 0.67` → `skew ≈ 3.0` — сильный перекос
- `alpha → 1` → `skew → ∞` — экстремальный перекос (всё в одно значение)
- `r_squared` — качество подгонки (0-1), чем ближе к 1, тем лучше данные описываются степенным законом


## Снятие weights для фиксированного диапазона (выбор из списка)

```sql
SELECT json_agg(val), json_agg(p) FROM (
  SELECT
      status AS val,
      ROUND(COUNT(id)::decimal / SUM(COUNT(id)) OVER (), 5) AS p
  FROM employees
  WHERE status IS NOT NULL
  GROUP BY status
  ORDER BY COUNT(id) DESC
) AS result;
```


## Снятие mean (среднего значения)

```sql
SELECT ROUND(COUNT(*)::decimal / GREATEST(COUNT(DISTINCT "company_id"), 1), 5) AS scale
FROM employees
WHERE "company_id" IS NOT NULL;
```


## Снятие null_weight

```sql
SELECT
  CASE WHEN company_id IS NULL THEN 'NULL' ELSE 'NOT NULL' END AS val,
  ROUND(COUNT(id)::decimal / SUM(COUNT(id)) OVER (), 5) AS p
FROM employees
GROUP BY 1
ORDER BY val DESC;
```


# Пример использования

Рассмотрим пример с multi-tenant приложением: Tenant, Author и Book.
Book имеет композитный ключ (TenantId, InternalBookId).


## Доменные модели

```python
import dataclasses

from psycopg_pool import AsyncConnectionPool

from ascetic_ddd.faker.domain.distributors.m2o.factory import distributor_factory
from ascetic_ddd.faker.domain.providers.interfaces import (
    IValueProvider, ICompositeValueProvider, IEntityProvider, IReferenceProvider
)
from ascetic_ddd.faker.domain.providers.aggregate_provider import AggregateProvider
from ascetic_ddd.faker.domain.providers.reference_provider import ReferenceProvider
from ascetic_ddd.faker.domain.providers.composite_value_provider import CompositeValueProvider
from ascetic_ddd.faker.domain.providers.value_provider import ValueProvider
from ascetic_ddd.faker.infrastructure.repositories.composite_repository import CompositeAutoPkRepository
from ascetic_ddd.faker.infrastructure.repositories.internal_pg_repository import InternalPgRepository
from ascetic_ddd.faker.infrastructure.repositories.pg_repository import PgRepository
from ascetic_ddd.faker.infrastructure.session.pg_session import InternalPgSessionPool, ExternalPgSessionPool
from ascetic_ddd.seedwork.infrastructure.session.composite_session import CompositeSessionPool

from faker import Faker
fake = Faker()

######################## Domain Model ######################################

########### Tenant aggregate #################

@dataclasses.dataclass()
class TenantId:
    value: int | None


@dataclasses.dataclass()
class TenantName:
    value: str


class Tenant:
    
    def __init__(self, id: TenantId, name: TenantName):
        self._id = id
        self._name = name
    
    def export(self, exporter: dict):
        exporter['id'] = self._id.value
        exporter['name'] = self._name.value


########### Author Aggregate #################


@dataclasses.dataclass()
class InternalAuthorId:
    value: int | None


class AuthorId:
    tenant_id: TenantId
    author_id: InternalAuthorId

    @property
    def value(self):
        return {
            'tenant_id': self.tenant_id.value,
            'author_id': self.author_id.value,
        }


@dataclasses.dataclass()
class AuthorName:
    value: str


class Author:

    def __init__(self, id: AuthorId, name: AuthorName):
        self._id = id
        self._name = name

    def export(self, exporter: dict):
        exporter['id'] = self._id.value
        exporter['name'] = self._name.value


########### Book aggregate #################

@dataclasses.dataclass()
class InternalBookId:
    value: int | None


@dataclasses.dataclass(frozen=True, kw_only=True)
class BookId:
    tenant_id: TenantId
    book_id: InternalBookId

    @property
    def value(self):
        return {
            'tenant_id': self.tenant_id.value,
            'book_id': self.book_id.value,
        }


@dataclasses.dataclass()
class BookTitle:
    value: str


class Book:

    def __init__(self, id: BookId, author_id: AuthorId, title: BookTitle):
        self._id = id
        self._author_id = author_id
        self._title = title
    
    def export(self, exporter: dict):
        exporter['id'] = self._id.value
        exporter['_author_id'] = self._author_id.value
        exporter['title'] = self._title.value


######################## Providers ######################################


class TenantProvider(AggregateProvider[dict, Tenant]):
    _id_attr = 'id'

    id: IValueProvider[int, TenantId]
    name: IValueProvider[str, TenantName]

    def __init__(self, repository):
        self.id = ValueProvider[int, TenantId](
            distributor=distributor_factory(),  # Receive from DB
            output_factory=TenantId,
            output_exporter=lambda x: x.value,
        )
        self.name = ValueProvider[str, TenantName](
            distributor=distributor_factory(sequence=True),
            output_factory=TenantName,
            input_generator=lambda session, position: "Tenant %s" % position,
        )
        super().__init__(
            repository=repository,
            output_factory=Tenant,
            output_exporter=self._export,
        )

    @staticmethod
    def _export(agg: Tenant) -> dict:
        exporter = dict()
        agg.export(exporter)
        return exporter


class AuthorIdProvider(CompositeValueProvider[dict, TenantId]):
    author_id: IValueProvider[int, AuthorId]
    tenant_id: IReferenceProvider[dict, Tenant, TenantId]

    def __init__(self, tenant_provider: TenantProvider):
        self.author_id = ValueProvider[int, AuthorId](
            distributor=distributor_factory(),  # Receive from DB
            output_factory=InternalAuthorId,
            output_exporter=lambda x: x.value,
        )
        # Ссылка на Tenant с распределением skew=2.0 (перекос к началу)
        # mean=10 означает в среднем 10 авторов на tenant
        self.tenant_id = ReferenceProvider[dict, Tenant, TenantId](
            distributor=distributor_factory(skew=2.0, mean=10),
            aggregate_provider=tenant_provider
        )

        super().__init__(
            output_factory=AuthorId,
            output_exporter=lambda result: result.value
        )


class AuthorProvider(AggregateProvider[dict, Author]):
    _id_attr = 'id'
    id: ICompositeValueProvider[dict, AuthorId]
    name: IValueProvider[str, AuthorName]

    def __init__(self, repository, tenant_provider: TenantProvider):
        self.id = AuthorIdProvider(tenant_provider=tenant_provider)
        self.name = ValueProvider[str, AuthorName](
            input_generator=lambda session, position: "%s %s" % (fake.first_name(), fake.last_name()),
        )
        super().__init__(
            repository=repository,
            output_factory=Author,
            output_exporter=self._export,
        )

    @staticmethod
    def _export(agg: Author) -> dict:
        exporter = dict()
        agg.export(exporter)
        return exporter


class BookIdProvider(CompositeValueProvider[dict, TenantId]):
    book_id: IValueProvider[int, BookId]
    tenant_id: IReferenceProvider[dict, Tenant, TenantId]

    def __init__(self, tenant_provider: TenantProvider):
        self.book_id = ValueProvider[int, BookId](
            distributor=distributor_factory(),  # Receive from DB
            output_factory=InternalBookId,
            output_exporter=lambda x: x.value,
        )
        self.tenant_id = ReferenceProvider[dict, Tenant, TenantId](
            distributor=distributor_factory(weights=[0.7, 0.2, 0.07, 0.03], mean=50),
            aggregate_provider=tenant_provider
        )

        super().__init__(
            output_factory=AuthorId,
            output_exporter=lambda result: result.value
        )


class BookProvider(AggregateProvider[dict, Book]):
    _id_attr = 'id'
    id: BookIdProvider
    author_id: IReferenceProvider[dict, Author, AuthorId]
    title: IValueProvider[str, BookTitle]

    def __init__(self, repository, tenant_provider: TenantProvider, author_provider: AuthorProvider):
        self.id = BookIdProvider(tenant_provider=tenant_provider)
        # Ссылка на Author с распределением weights (20% авторов пишут 70% книг)
        # mean=50 означает в среднем 50 книг на автора
        self.author_id = ReferenceProvider[dict, Author, AuthorId](
            distributor=distributor_factory(weights=[0.7, 0.2, 0.07, 0.03], mean=50),
            aggregate_provider=author_provider,
        )
        self.title = ValueProvider[str, BookTitle](
            distributor=distributor_factory(),
            input_generator=lambda session, position: fake.sentence(nb_words=3).replace('.', ''),
        )
        super().__init__(
            repository=repository,
            output_factory=Book,
            output_exporter=self._export,
        )

    async def do_populate(self, session, specification=None):
        # Берём tenant_id из id для согласованности
        await self.id.populate(session)
        self.author_id.set({'tenant_id': self.id.tenant_id.get(),})
        await super().do_populate(session)

    @staticmethod
    def _export(agg: Book) -> dict:
        exporter = dict()
        agg.export(exporter)
        return exporter


######################## Использование ######################################


tenant_repository = CompositeAutoPkRepository(
    external_repository=PgRepository(),  # Use real Repository instead
    internal_repository=InternalPgRepository(
        table='tenants',
        agg_exporter=TenantProvider._export
    )
)


author_repository = CompositeAutoPkRepository(
    external_repository=PgRepository(),  # Use real Repository instead
    internal_repository=InternalPgRepository(
        table='authors',
        agg_exporter=AuthorProvider._export
    )
)


book_repository = CompositeAutoPkRepository(
    external_repository=PgRepository(),  # Use real Repository instead
    internal_repository=InternalPgRepository(
        table='books',
        agg_exporter=BookProvider._export
    )
)

# Создаём провайдеры
tenant_provider = TenantProvider(tenant_repository)
author_provider = AuthorProvider(author_repository, tenant_provider)
book_provider = BookProvider(book_repository, tenant_provider, author_provider)

async def generate_data():

    internal_pg_pool = AsyncConnectionPool('internal_postgresql_url', max_size=4, open=False)
    await internal_pg_pool.open()
    internal_session_pool = InternalPgSessionPool(internal_pg_pool)

    external_pg_pool = AsyncConnectionPool('internal_postgresql_url', max_size=4, open=False)
    await external_pg_pool.open()
    external_session_pool = ExternalPgSessionPool(external_pg_pool)

    session_pool = CompositeSessionPool(external_session_pool, internal_session_pool)

    # Генерируем 1000 книг
    for _ in range(1000):
        with session_pool.session() as session, session.atomic() as ts_session:
            book_provider.reset()
            await book_provider.populate(ts_session)
            book = await book_provider.create(ts_session)
            print(f"Created: {book._title} by {book._author_id}")
```


## Параметры distributor

| Параметр | Описание |
|----------|----------|
| `weights` | Список весов партиций, например `[0.7, 0.2, 0.07, 0.03]` — 70% попадут в первую партицию |
| `skew` | Параметр перекоса: 1.0 = равномерно, 2.0+ = перекос к началу |
| `mean` | Среднее количество использований каждого значения. `mean=1` для уникальных значений |
| `null_weight` | Вероятность вернуть None (0-1) |
| `sequence` | Передавать порядковый номер в генератор значений |
