# JSONPath to SQL Compiler

Компилятор JSONPath выражений в SQL запросы для нормализованных реляционных баз данных.

## Описание

Библиотека позволяет компилировать JSONPath выражения в SQL запросы (SQLAlchemy) для работы с нормализованными таблицами, где вложенные сущности хранятся в отдельных таблицах с отношениями one-to-many, many-to-one, etc.

## Архитектура

### Visitor Pattern

Компилятор использует паттерн Visitor для обхода AST дерева jsonpath2:

```
JSONPath → jsonpath2.Path.parse_str() → AST → Visitor → SQLAlchemy Query
```

### Основные компоненты

1. **SchemaMetadata** - метаданные схемы БД:
   - `tables` - словарь SQLAlchemy Table объектов
   - `relationships` - связи между таблицами
   - `root_table` - корневая таблица (точка входа)

2. **RelationshipMetadata** - метаданные связи:
   - `target_table` - целевая таблица
   - `foreign_key` - внешний ключ
   - `target_primary_key` - первичный ключ целевой таблицы
   - `relationship_type` - тип связи (one-to-many, many-to-one, etc)

3. **CompilationContext** - контекст компиляции:
   - Отслеживает текущую таблицу
   - Собирает JOIN-ы
   - Собирает WHERE условия
   - Собирает SELECT колонки

4. **Visitors**:
   - `RootNodeVisitor` - обработка $ (корневого узла)
   - `SubscriptNodeVisitor` - обработка [...] (subscript узлов)
   - `ObjectIndexSubscriptVisitor` - обработка .field или ['field']
   - `FilterSubscriptVisitor` - обработка [?(...)] (фильтров)
   - `WildcardSubscriptVisitor` - обработка [*] (wildcard)

## Поддерживаемые возможности

### ✅ Реализовано

1. **Навигация по полям**: `$.users` → `SELECT * FROM users`
2. **Навигация по связям**: `$.users.orders` → `SELECT * FROM users JOIN orders ON ...`
3. **Фильтры**: `$[?(@.age > 18)]` → `WHERE age > 18`
4. **Вложенная навигация**: `$.users.orders.items.product` → множественные JOIN-ы
5. **Операторы сравнения**: `=`, `!=`, `>`, `<`, `>=`, `<=`
6. **Wildcard**: `$[*]` → `SELECT * (все колонки)`

### ❌ Не реализовано (TODO)

1. **Array subscripts**: `$[0]`, `$[0:5]` - индексы и срезы массивов
2. **Recursive descent**: `$..field` - рекурсивный поиск
3. **Логические операторы**: `&&`, `||` в фильтрах
4. **Функции**: `length()`, `keys()`, `values()`
5. **Агрегации**: `SUM()`, `COUNT()`, `AVG()`
6. **Many-to-many** отношения через промежуточные таблицы
7. **Subqueries** для сложных фильтров

## Использование

### Пример 1: Базовая настройка

```python
from sqlalchemy import Column, Float, Integer, String, MetaData, Table
from ascetic_ddd.jsonpath2_ext.infrastructure.jsonpath2_to_sqlalchemy_sql import (
    JSONPathToSQLCompiler,
    SchemaMetadata,
    RelationshipMetadata,
)

# Определяем таблицы
metadata = MetaData()

users = Table(
    "users", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("age", Integer),
)

orders = Table(
    "orders", metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", Integer),
    Column("total", Float),
)

# Определяем связи
relationships = {
    "users": {
        "orders": RelationshipMetadata(
            target_table="orders",
            foreign_key="user_id",
            target_primary_key="id",
        ),
    },
}

# Создаём схему
schema = SchemaMetadata(
    tables={"users": users, "orders": orders},
    relationships=relationships,
    root_table="users",
)

# Компилируем JSONPath в SQL
compiler = JSONPathToSQLCompiler(schema)
query = compiler.compile("$.orders[?(@.total > 100)]")

print(query)
# SELECT orders.id, orders.user_id, orders.total
# FROM users
# JOIN orders ON users.id = orders.user_id
# WHERE orders.total > 100
```

### Пример 2: Работа с вложенными связями

```python
# Схема: users -> orders -> order_items -> products

relationships = {
    "users": {
        "orders": RelationshipMetadata(
            target_table="orders",
            foreign_key="user_id",
        ),
    },
    "orders": {
        "items": RelationshipMetadata(
            target_table="order_items",
            foreign_key="order_id",
        ),
    },
    "order_items": {
        "product": RelationshipMetadata(
            target_table="products",
            foreign_key="product_id",
        ),
    },
}

# Компиляция глубокой навигации
query = compiler.compile("$.orders.items.product[?(@.price < 100)]")

# Результат:
# SELECT products.*
# FROM users
# JOIN orders ON users.id = orders.user_id
# JOIN order_items ON orders.id = order_items.order_id
# JOIN products ON order_items.product_id = products.id
# WHERE products.price < 100
```

### Пример 3: Фильтры

```python
# Простой фильтр
query = compiler.compile("$[?(@.age > 18)]")
# WHERE age > 18

# Фильтр на связанной таблице
query = compiler.compile("$.orders[?(@.status = 'completed')]")
# JOIN orders ... WHERE orders.status = 'completed'
```

## Запуск примера

```bash
python -m ascetic_ddd.jsonpath2_ext.examples.jsonpath2_to_raw_sql_example
```

## Расширение функциональности

### Добавление нового типа узла

1. Создать новый Visitor класс:

```python
class MyCustomNodeVisitor(NodeVisitor):
    def visit(self, node: MyCustomNode, context: CompilationContext):
        # Ваша логика компиляции
        pass
```

2. Зарегистрировать в `_node_visitors`:

```python
_node_visitors[MyCustomNode] = MyCustomNodeVisitor()
```

### Добавление нового оператора

Расширить `FilterSubscriptVisitor._compile_binary_operator()`:

```python
elif expr.token == "LIKE":
    return left_column.like(right_value)
```

## Ограничения

1. **Требуется полная схема БД** - компилятор должен знать все таблицы и связи заранее
2. **Нет автоматического определения связей** - связи нужно описывать вручную
3. **Ограниченная поддержка JSONPath** - не все возможности JSONPath поддерживаются
4. **Нет оптимизации запросов** - генерируются простые JOIN-ы без учёта индексов

## Альтернативные подходы

Если вам не подходит этот компилятор, рассмотрите:

1. **JSONB в PostgreSQL** - храните данные как JSONB и используйте встроенный JSONPath
2. **GraphQL** - используйте Hasura или PostGraphile для автоматической генерации
3. **ORM query builders** - SQLAlchemy, Django ORM с path-based синтаксисом

