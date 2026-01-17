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

Внимание!
Данный пакет проекта пока еще не прошел отладку и оптимизацию, и представляет собой просто черновик моих мыслей.


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


## Снятие weights для фиксированного диапазона (выбор из списка)

```sql
SELECT json_agg(result.val), json_agg(result.p) FROM (
    SELECT tc.val, round((tc.c / SUM(tc.c) OVER ())::decimal, 5) AS p, tc.c AS c, SUM(tc.c) OVER () AS s FROM (
        SELECT
            status AS val, COUNT(id) AS c
        FROM employees
        WHERE status IS NOT NULL
        GROUP BY status ORDER BY c DESC
    ) AS tc
) AS result;
```


## Снятие mean (среднего значения)

```sql
SELECT ROUND(total_count::decimal / GREATEST(distinct_count, 1), 5) AS scale FROM (
    SELECT
        COUNT(*) AS total_count, COUNT(DISTINCT "company_id") AS distinct_count
    FROM employees
    WHERE "company_id" IS NOT NULL
) AS subquery;
```


## Снятие null_weight

```sql
SELECT tc.val, round((tc.c / SUM(tc.c) OVER ())::decimal, 5) AS p, tc.c AS c, SUM(tc.c) OVER () AS s FROM (
    SELECT
        CASE WHEN company_id IS NULL THEN 'NULL' ELSE 'NOT NULL' END AS val, COUNT(device_id) AS c
    FROM api_device
    GROUP BY val
    ORDER BY val DESC
) AS tc;
```