# JSONPath RFC 9535 Extensions

Расширения для библиотеки `jsonpath-rfc9535` с поддержкой параметризованных выражений. Полностью совместимо со стандартом **RFC 9535 (JSONPath)**.

## Преимущества

✅ **RFC 9535 Compliant** - полная поддержка официального стандарта JSONPath
✅ **Современная библиотека** - `jsonpath-rfc9535` активно поддерживается
✅ **Стандартный синтаксис** - `$[?@.age > 18]` вместо проприетарных вариантов
✅ **Параметризация** - C-style placeholders для переиспользования выражений

## Структура

```
jsonpath_rfc9535_ext/
├── domain/
│   ├── jsonpath_parameterized_parser.py  # Парсер с поддержкой placeholders
│   └── tests/
│       └── test_jsonpath_parameterized_parser.py  # 24 теста
├── infrastructure/
│   ├── jsonpath_to_raw_sql.py  # SQL конвертер
│   └── jsonpath_to_sqlalchemy_sql.py  # SQLAlchemy конвертер
├── examples/
│   ├── jsonpath_parameterized_example.py  # Примеры использования
│   ├── jsonpath_to_raw_sql_example.py  # Примеры SQL
│   └── jsonpath_to_sqlalchemy_sql_example.py  # Примеры SQLAlchemy
└── README.md
```

## Возможности

### Параметризованные выражения

Поддержка C-style placeholders:
- **Позиционные**: `%s`, `%d`, `%f`
- **Именованные**: `%(name)s`, `%(age)d`, `%(price)f`

### Логические операторы (RFC 9535)

- **AND**: `&&`
- **OR**: `||`
- **NOT**: `!`

### Переиспользование

Создайте выражение один раз, используйте много раз с разными параметрами.

## Использование

### Базовый пример

```python
from ascetic_ddd.jsonpath_rfc9535_ext.domain.jsonpath_parameterized_parser import parse

# Создать параметризованное выражение
expr = parse("$[?@.age > %d]")

# Данные
users = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
    {"name": "Charlie", "age": 35},
]

# Выполнить с разными параметрами
results = expr.find(users, (27,))  # Вернет Alice и Charlie
results = expr.find(users, (32,))  # Вернет только Charlie
```

### Именованные параметры

```python
expr = parse("$[?@.name == %(name)s]")
result = expr.find_one(users, {"name": "Alice"})
```

### Логические операторы

```python
# AND
expr = parse("$[?@.age > %d && @.active == %s]")
results = expr.find(users, (26, True))

# OR
expr = parse("$[?@.age < %d || @.age > %d]")
results = expr.find(users, (25, 35))

# NOT
expr = parse("$[?!(@.active == %s)]")
results = expr.find(users, (True,))
```

### Работа с вложенными данными

```python
data = {
    "users": [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ]
}

expr = parse("$.users[?@.age > %d]")
results = expr.find(data, (27,))
```

## Примеры

```bash
# Запустить примеры
python -m ascetic_ddd.jsonpath_rfc9535_ext.examples.jsonpath_parameterized_example
```

## Тестирование

```bash
# Запустить тесты (24 теста)
python -m unittest discover -s ascetic_ddd/jsonpath_rfc9535_ext/domain/tests -p "test_*.py" -v
```

## API

### parse(template: str, env: JSONPathEnvironment = None) -> ParametrizedExpression

Создает параметризованное выражение.

**Параметры:**
- `template` - JSONPath выражение с placeholders
- `env` - (опционально) JSONPath environment

**Возвращает:**
- `ParametrizedExpression` - параметризованное выражение

### ParametrizedExpression

Класс для работы с параметризованными JSONPath выражениями.

#### Методы

**find(data, params) -> list**
- Найти все совпадения
- `params`: tuple для позиционных, dict для именованных параметров
- Возвращает список значений

**find_one(data, params) -> Any**
- Найти первое совпадение
- Возвращает значение или `None`

**finditer(data, params) -> Iterator**
- Итератор по совпадениям
- Возвращает генератор значений

**placeholders -> list**
- Информация о placeholders в выражении

## Преимущества

### 1. Производительность

Выражение парсится один раз, параметры подставляются при каждом вызове:

```python
expr = parse("$[?@.age > %d]")  # Парсится один раз

# Многократное использование
for min_age in [18, 25, 30, 40]:
    results = expr.find(users, (min_age,))  # Только подстановка параметров
```

### 2. Безопасность

Автоматическое экранирование строк:

```python
expr = parse("$[?@.name == %s]")
results = expr.find(users, ("O'Brien",))  # Автоматическое экранирование
```

### 3. Удобство

C-style синтаксис placeholders, знакомый всем разработчикам:

```python
# Позиционные
expr = parse("$[?@.age > %d && @.active == %s]")
results = expr.find(users, (25, True))

# Именованные
expr = parse("$[?@.age > %(min_age)d && @.active == %(active)s]")
results = expr.find(users, {"min_age": 25, "active": True})
```

### 4. Совместимость со стандартом

Полная поддержка **RFC 9535** - официального стандарта JSONPath:

- ✅ Стандартные операторы: `==`, `!=`, `<`, `>`, `<=`, `>=`
- ✅ Логические операторы: `&&`, `||`, `!`
- ✅ Синтаксис `@` для текущего элемента
- ✅ Все селекторы и функции из стандарта

## SQL Компиляторы

Компиляторы для преобразования JSONPath в SQL запросы.

### Возможности SQL компиляторов

| Возможность | Raw SQL | SQLAlchemy |
|-------------|---------|------------|
| Простые фильтры (`@.field > value`) | ✓ | ✓ |
| Логические операторы (`&&`, `\|\|`, `!`) | ✓ | ✓ |
| Скобки для группировки | ✓ | ✓ |
| Навигация по связям (`$.orders[*]`) | ✓ | ✓ |
| **Nested Paths** (`@.orders.total`) | ✓ | ✓ |
| **Nested Wildcards** (`@.items[*][?...]`) | ✓ | ✓ |

### Nested Paths vs Nested Wildcards

**Nested Paths** - доступ к полям через связи (использует JOIN):
```
$[?@.orders.total > 100]
→ SELECT users.* FROM users
  JOIN orders ON users.id = orders.user_id
  WHERE orders.total > 100
```

**Nested Wildcards** - фильтрация по дочерней коллекции (использует EXISTS):
```
$[?@.orders[*][?@.total > 100]]
→ SELECT users.* FROM users
  WHERE EXISTS (SELECT 1 FROM orders
                WHERE orders.user_id = users.id
                AND orders.total > 100)
```

### Когда использовать что

| Сценарий | Паттерн | SQL |
|----------|---------|-----|
| Фильтр по полю связи | `@.orders.total > 100` | JOIN |
| "Хотя бы один" в коллекции | `@.orders[*][?@.total > 100]` | EXISTS |
| Несколько условий на связь | `@.orders.total > 100 && @.orders.status == 'done'` | JOIN |

### Пример SQL компилятора

```python
from ascetic_ddd.jsonpath_rfc9535_ext.infrastructure.jsonpath_to_raw_sql import (
    JSONPathToSQLCompiler, SchemaDef, TableDef, ColumnDef, RelationshipDef, RelationType
)

# Определить схему
users_table = TableDef(name="users", columns={...}, primary_key="id")
orders_table = TableDef(name="orders", columns={...}, primary_key="id")

relationships = {
    "users": {
        "orders": RelationshipDef(
            target_table="orders",
            foreign_key="user_id",
            relationship_type=RelationType.ONE_TO_MANY,
        ),
    },
}

schema = SchemaDef(
    tables={"users": users_table, "orders": orders_table},
    relationships=relationships,
    root_table="users",
)

compiler = JSONPathToSQLCompiler(schema)

# Nested path - использует JOIN
sql = compiler.compile("$[?@.orders.total > 100]")

# Nested wildcard - использует EXISTS
sql = compiler.compile("$[?@.orders[*][?@.total > 100]]")
```

## Установка зависимостей

```bash
pip install jsonpath-rfc9535
```

