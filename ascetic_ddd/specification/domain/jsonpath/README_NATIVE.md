# Нативный JSONPath Parser (Без Внешних Зависимостей)

## Описание

Полностью самодостаточный парсер JSONPath выражений, который **не требует внешних библиотек**. Напрямую преобразует RFC 9535 совместимые JSONPath выражения в Specification AST.

## Ключевые преимущества

✅ **Нет внешних зависимостей** - работает на чистом Python
✅ **RFC 9535 совместимость** - поддержка стандартных операторов (`==`, `&&`, `||`, `!`)
✅ **Скобочки** - группировка логических выражений (`$[?(@.age >= 18 && @.age <= 65) && @.active == true]`)
✅ **Полный контроль** - прозрачная логика парсинга
✅ **Легковесность** - минимум кода, только необходимое
✅ **Простота поддержки** - весь код в одном файле
✅ **Полная функциональность** - все логические операторы включая NOT
✅ **Вложенные wildcards** ✨ - фильтрация по вложенным коллекциям
✅ **Вложенные пути** ✨ - доступ к вложенным полям (`$.a.b.c[?@.x > 1]`)

## Использование

```python
from ascetic_ddd.specification.domain.jsonpath.jsonpath_native_parser import parse

# Создать спецификацию
spec = parse("$[?(@.age > %d)]")


# Создать контекст
class DictContext:
    def __init__(self, data):
        self._data = data

    def get(self, key):
        return self._data[key]


user = DictContext({"age": 30})

# Проверить соответствие
result = spec.match(user, (25,))  # True
```

## Архитектура

### Компоненты

1. **Lexer** - Токенизация JSONPath выражений
   - Распознает операторы, идентификаторы, литералы
   - Обрабатывает плейсхолдеры

2. **Parser** - Преобразование токенов в AST
   - Рекурсивный парсер выражений
   - Прямое создание Specification узлов

3. **Placeholder Binding** - Привязка параметров
   - Поддержка позиционных и именованных параметров
   - Типизированные плейсхолдеры (%s, %d, %f)

### Процесс парсинга

```
JSONPath Template
      ↓
[Lexer] Tokenization
      ↓
Token Stream
      ↓
[Parser] Expression Parsing
      ↓
Specification AST
      ↓
[Binding] Placeholder Values
      ↓
Bound AST
      ↓
[Evaluation] EvaluateVisitor
      ↓
Boolean Result
```

## RFC 9535 Соответствие

Полная поддержка стандарта RFC 9535:

### ✅ Операторы сравнения
- `==` - Равенство (RFC 9535: двойной знак)
- `!=` - Неравенство
- `>` - Больше
- `<` - Меньше
- `>=` - Больше или равно
- `<=` - Меньше или равно

### ✅ Логические операторы
- `&&` - Логическое AND (RFC 9535)
- `||` - Логическое OR (RFC 9535)
- `!` - Логическое NOT (RFC 9535)

### Параметризация
```python
# Позиционные
parse("$[?@.age > %d]")            # Целое число
parse("$[?@.name == %s]")          # Строка (RFC 9535: ==)
parse("$[?@.price > %f]")          # Число с плавающей точкой

# Именованные
parse("$[?@.age > %(min_age)d]")
parse("$[?@.name == %(name)s]")    # RFC 9535: ==

# Логические операторы (RFC 9535)
parse("$[?@.age > %d && @.active == %s]")   # AND
parse("$[?@.age < %d || @.age > %d]")       # OR
parse("$[?!(@.active == %s)]")              # NOT
```

### Коллекции с Wildcard
```python
spec = parse("$.items[*][?(@.price > %f)]")

from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext

item1 = DictContext({"name": "Laptop", "price": 999.99})
item2 = DictContext({"name": "Mouse", "price": 29.99})

collection = CollectionContext([item1, item2])
store = DictContext({"items": collection})

# Проверяет, есть ли хотя бы один товар с price > 500
spec.match(store, (500.0,))  # True
```

### Вложенные Wildcards ✨
```python
# Вложенные коллекции: категории -> товары
spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

# Создаём структуру данных
item1 = DictContext({"name": "Laptop", "price": 999.0})
items = CollectionContext([item1])
category = DictContext({"name": "Electronics", "items": items})

categories = CollectionContext([category])
store = DictContext({"categories": categories})

# Есть ли категория с товаром дороже 500?
spec.match(store, (500.0,))  # True
```

## Сравнение версий

| Характеристика | Нативный | RFC 9535 | JSONPath2 | Lambda Filter |
|----------------|----------|----------|-----------|---------------|
| Внешние зависимости | ❌ Нет | jsonpath-rfc9535 | jsonpath2 | ❌ Нет |
| Размер кода | ~500 строк | ~550 строк | ~670 строк | ~600 строк |
| Синтаксис | JSONPath | JSONPath | JSONPath | Python lambda |
| RFC 9535 compliance | ✅ Полное | ✅ Полное | ⚠️ Частичное | N/A |
| Операторы (==, !=, >, <, >=, <=) | ✅ | ✅ | ✅ | ✅ |
| Логические операторы (&&, \|\|, !) | ✅ | ✅ | ✅ (авто) | ✅ (and, or, not) |
| Скобочки | ✅ | ✅ | ✅ (авто) | ✅ |
| Параметризация | ✅ | ✅ | ✅ | ✓ |
| Wildcard коллекции | ✅ | ✅ | ✅ | ❌ |
| Вложенные wildcards | ✅ | ✅ | ✅ | ✓ |
| Вложенные пути | ✅ | ✅ | ✅ | ✅ |
| Скорость парсинга | Быстрая | Быстрая | Быстрая | Быстрая |
| Контроль над AST | 🟢 Полный | Частичный | Частичный | 🟢 Полный |
| Расширяемость | 🟢 Высокая | Ограничена | Ограничена | 🟢 Высокая |

## Поддерживаемые возможности

Текущая реализация поддерживает:
- Простые фильтры: `$[?@.field op value]`
- Логические выражения: `$[?@.a > 1 && @.b == 2]`, `$[?@.a < 1 || @.a > 10]`
- Отрицание: `$[?!(@.active == true)]`
- Wildcard коллекции: `$.collection[*][?@.field op value]`
- Вложенные wildcards: `$.categories[*][?@.items[*][?@.price > 100]]` ✨
- Вложенные пути: `$.a.b.c[?@.x > 1]`, `$[?@.a.b.c > 1]` ✨ **NEW!**

Не поддерживается (пока):
- Функции JSONPath (len, min, max и т.д.)
- Индексы массивов: `$.items[0]`, `$.items[1:5]`

## Тестирование

```bash
# Запустить тесты нативного парсера
python -m unittest ascetic_ddd.specification.domain.jsonpath.test_jsonpath_parser_native -v

# Все тесты
python -m unittest discover -s ascetic_ddd/specification -p "test_*.py" -v
```

## Полный пример использования

Запустите интерактивный пример с 11 демонстрациями:

```bash
python -m ascetic_ddd.specification.domain.jsonpath.example_usage_native
```

Пример демонстрирует:
- Все операторы сравнения (`==`, `!=`, `>`, `<`, `>=`, `<=`)
- Позиционные и именованные плейсхолдеры
- Логические операторы RFC 9535 (`&&`, `||`, `!`)
- Wildcard коллекции
- Работу лексера (токенизация)
- Переиспользование спецификаций
- Boolean значения

См. файл [example_usage_native.py](examples/jsonpath_native_example.py) для полного кода.

## Примеры

### Базовое использование

```python
from ascetic_ddd.specification.domain.jsonpath.jsonpath_native_parser import parse

# Простое сравнение
spec = parse("$[?@.age > %d]")
user = DictContext({"age": 30})
spec.match(user, (25,))  # True

# Строковое сравнение (RFC 9535: ==)
spec = parse("$[?@.status == %s]")
task = DictContext({"status": "done"})
spec.match(task, ("done",))  # True

# Именованные параметры
spec = parse("$[?@.score >= %(min_score)d]")
student = DictContext({"score": 85})
spec.match(student, {"min_score": 80})  # True

# Логические операторы (RFC 9535)
spec = parse("$[?@.age > %d && @.active == %s]")
user = DictContext({"age": 30, "active": True})
spec.match(user, (25, True))  # True

# NOT оператор (RFC 9535)
spec = parse("$[?!(@.deleted == %s)]")
item = DictContext({"deleted": False})
spec.match(item, (True,))  # True
```

### Работа с коллекциями

```python
from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext

spec = parse("$.users[*][?(@.age >= %d)]")

user1 = DictContext({"name": "Alice", "age": 30})
user2 = DictContext({"name": "Bob", "age": 25})

users = CollectionContext([user1, user2])
root = DictContext({"users": users})

# Есть ли хотя бы один пользователь с age >= 28?
spec.match(root, (28,))  # True (Alice)
```

### Вложенные Wildcards ✨ NEW!

```python
from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext

# Вложенные wildcards: фильтрация по вложенным коллекциям
spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

# Создаём структуру: категории -> товары
item1 = DictContext({"name": "Laptop", "price": 999.0})
item2 = DictContext({"name": "Mouse", "price": 29.0})
items1 = CollectionContext([item1, item2])
category1 = DictContext({"name": "Electronics", "items": items1})

item3 = DictContext({"name": "Shirt", "price": 49.0})
items2 = CollectionContext([item3])
category2 = DictContext({"name": "Clothing", "items": items2})

categories = CollectionContext([category1, category2])
store = DictContext({"categories": categories})

# Есть ли категория, в которой есть товар дороже 500?
spec.match(store, (500.0,))  # True (category1 имеет Laptop)
```

**Вложенные wildcards с логикой:**

```python
# Вложенный wildcard с AND оператором
spec = parse("$.categories[*][?@.items[*][?@.price > %f && @.price < %f]]")

# Есть ли категория с товаром в диапазоне 500-1000?
spec.match(store, (500.0, 1000.0))  # True (Laptop: 999)

# Есть ли категория с товаром в диапазоне 1000-2000?
spec.match(store, (1000.0, 2000.0))  # False
```

**Множественные совпадения:**

```python
# Проверка на несколько категорий с дорогими товарами
spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

# Category 1 с дорогим товаром
item1 = DictContext({"name": "Laptop", "price": 999.0})
items1 = CollectionContext([item1])
category1 = DictContext({"name": "Electronics", "items": items1})

# Category 2 с дорогим товаром
item2 = DictContext({"name": "Designer Jeans", "price": 299.0})
items2 = CollectionContext([item2])
category2 = DictContext({"name": "Clothing", "items": items2})

categories = CollectionContext([category1, category2])
store = DictContext({"categories": categories})

# Обе категории имеют товары дороже 200
spec.match(store, (200.0,))  # True
```

### Вложенные Пути ✨ NEW!

```python
# Создать специальный контекст для вложенных структур
class NestedDictContext:
    def __init__(self, data):
        self._data = data

    def get(self, key):
        value = self._data[key]
        # Автоматически оборачиваем вложенные dict
        if isinstance(value, dict):
            return NestedDictContext(value)
        return value

# Простой вложенный путь: $.store.products[*][?@.price > 500]
spec = parse("$.store.products[*][?@.price > %f]")

product1 = DictContext({"name": "Laptop", "price": 999.0})
product2 = DictContext({"name": "Mouse", "price": 29.0})
products = CollectionContext([product1, product2])

data = NestedDictContext({
    "store": {
        "name": "MyStore",
        "products": products
    }
})

spec.match(data, (500.0,))  # True (Laptop > 500)
```

**Глубоко вложенные пути:**

```python
# Глубокая вложенность: $.company.department.team.members[*][?@.age > 28]
spec = parse("$.company.department.team.members[*][?@.age > %d]")

member1 = DictContext({"name": "Alice", "age": 30})
member2 = DictContext({"name": "Bob", "age": 25})
members = CollectionContext([member1, member2])

data = NestedDictContext({
    "company": {
        "department": {
            "team": {
                "members": members
            }
        }
    }
})

spec.match(data, (28,))  # True (Alice > 28)
```

**Вложенные пути в фильтрах:**

```python
# Фильтр на вложенном поле: $[?@.user.profile.age > 25]
spec = parse("$[?@.user.profile.age > %d]")

data = NestedDictContext({
    "user": {
        "profile": {
            "age": 30
        }
    }
})

spec.match(data, (25,))  # True
```

**Комбинация вложенных путей и логики:**

```python
# $.store.products[*][?@.price > 500 && @.stock > 5]
spec = parse("$.store.products[*][?@.price > %f && @.stock > %d]")

product = DictContext({"name": "Monitor", "price": 599.0, "stock": 10})
products = CollectionContext([product])

data = NestedDictContext({
    "store": {
        "products": products
    }
})

spec.match(data, (500.0, 5))  # True
```

## Внутреннее устройство

### Токены

Лексер распознает следующие типы токенов:

```python
DOLLAR      # $
AT          # @
DOT         # .
LBRACKET    # [
RBRACKET    # ]
LPAREN      # (
RPAREN      # )
QUESTION    # ?
WILDCARD    # *
AND         # && (RFC 9535)
OR          # || (RFC 9535)
NOT         # ! (RFC 9535)
EQ          # == (RFC 9535: двойной знак)
NE/GT/LT/GTE/LTE  # Операторы сравнения
NUMBER      # 123, 45.67
STRING      # "text", 'text'
PLACEHOLDER # %d, %s, %(name)d
IDENTIFIER  # age, name, status
```

### AST узлы

Парсер создает следующие Specification узлы:

- `GlobalScope()` - корневой контекст
- `Item()` - текущий элемент коллекции (@)
- `Field(parent, name)` - доступ к полю
- `Value(val)` - литеральное значение
- `Equal/NotEqual/GreaterThan/...` - операторы сравнения
- `And(left, right)` - логическое И (&&)
- `Or(left, right)` - логическое ИЛИ (||)
- `Not(operand)` - логическое НЕ (!)
- `Wildcard(parent, predicate)` - фильтрация коллекций

