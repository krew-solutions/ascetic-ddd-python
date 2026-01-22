# JSONPath RFC 9535 Specification Parser

Парсер JSONPath выражений для Specification Pattern с использованием библиотеки **jsonpath-rfc9535**.

## Описание

Эта реализация использует библиотеку `jsonpath-rfc9535` для парсинга JSONPath выражений и преобразует их в AST узлы Specification Pattern. **Полностью соответствует стандарту RFC 9535** и поддерживает параметризацию в стиле C-форматирования строк.

## Ключевые особенности

✅ **Полное соответствие RFC 9535** - использует официальный стандарт JSONPath
✅ **Параметризация** - поддержка плейсхолдеров (%s, %d, %f, %(name)s)
✅ **Стандартные операторы** - `==`, `!=`, `>`, `<`, `>=`, `<=`
✅ **Логические операторы RFC 9535** - `&&` (AND), `||` (OR), `!` (NOT)
✅ **Скобочки** - группировка логических выражений (`$[?(@.age >= 18 && @.age <= 65) && @.active == true]`)
✅ **Коллекции с wildcard** - фильтрация элементов коллекций
✅ **Вложенные wildcards** ✨ - фильтрация по вложенным коллекциям (`$.categories[*][?@.items[*][?@.price > 100]]`)
✅ **Вложенные пути** ✨ - доступ к вложенным полям (`$[?@.profile.age > 25]`, `$[?@.company.department.manager.level > 5]`)
✅ **Переиспользование** - одна спецификация с разными параметрами
✅ **Строгое соответствие стандарту** - гарантированная совместимость

## Использование

```python
from ascetic_ddd.specification.domain.jsonpath.jsonpath_rfc9535_parser import parse

# Создать спецификацию
spec = parse("$[?@.age > %d]")


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

## Соответствие RFC 9535

Эта реализация **полностью соответствует** стандарту RFC 9535:

### ✅ Операторы сравнения (RFC 9535)
- `==` - Равенство (**двойной знак**, как в стандарте)
- `!=` - Неравенство
- `>` - Больше
- `<` - Меньше
- `>=` - Больше или равно
- `<=` - Меньше или равно

### ✅ Логические операторы (RFC 9535)
- `&&` - Логическое AND (как в стандарте)
- `||` - Логическое OR (как в стандарте)
- `!` - Логическое NOT (как в стандарте)

### ✅ Референсы
- `@` - Текущий узел (current node)
- `$` - Корневой узел (root node)

## Сравнение с другими версиями

| Характеристика | RFC 9535 | JSONPath2 | Lambda Filter | Нативный |
|----------------|----------|-----------|---------------|----------|
| Внешние зависимости | jsonpath-rfc9535 | jsonpath2 | ❌ Нет | ❌ Нет |
| Размер кода | ~550 строк | ~670 строк | ~600 строк | ~500 строк |
| Синтаксис | JSONPath | JSONPath | Python lambda | JSONPath |
| Соответствие RFC 9535 | ✅ **Полное** | ⚠️ Частичное | N/A | ✅ **Полное** |
| Оператор равенства | `==` ✅ | `=` (авто-конверт) | `==` | `==` ✅ |
| Логические операторы | `&&`, `||` ✅ | `&&`, `||` (авто) | `and`, `or` | `&&`, `||` ✅ |
| NOT оператор | `!` ✅ | `!` (авто) | `not` | `!` ✅ |
| Скобочки | ✅ | ✅ (авто) | ✅ | ✅ |
| Параметризация | ✓ | ✓ | ✓ | ✓ |
| Wildcard коллекции | ✓ | ✓ | ❌ | ✓ |
| Вложенные wildcards | ✅ | ✅ | ✓ | ✅ |
| Вложенные пути | ✅ | ✅ | ✅ | ✅ |
| Стабильность | Высокая | Высокая | Высокая | Средняя |

## Поддерживаемые возможности

### Операторы сравнения

```python
# RFC 9535 использует == для равенства (двойной знак)
parse("$[?@.age == %d]")           # Равно
parse("$[?@.age != %d]")           # Не равно
parse("$[?@.age > %d]")            # Больше
parse("$[?@.age < %d]")            # Меньше
parse("$[?@.age >= %d]")           # Больше или равно
parse("$[?@.age <= %d]")           # Меньше или равно
```

### Логические операторы

```python
# RFC 9535 использует && для AND
parse("$[?@.age > %d && @.active == %s]")

# RFC 9535 использует || для OR
parse("$[?@.age < %d || @.age > %d]")

# RFC 9535 использует ! для NOT
parse("$[?!(@.active == %s)]")

# Сложные выражения
parse("$[?(@.age >= %d && @.age <= %d) && @.status == %s]")
```

### Параметризация

```python
# Позиционные параметры
parse("$[?@.age > %d]")            # Целое число
parse("$[?@.name == %s]")          # Строка
parse("$[?@.price > %f]")          # Число с плавающей точкой

# Именованные параметры
parse("$[?@.age > %(min_age)d]")
parse("$[?@.name == %(name)s]")
parse("$[?@.price > %(min_price)f]")

# Множественные параметры
parse("$[?@.age >= %(min_age)d && @.age <= %(max_age)d]")
```

### Коллекции с Wildcard

```python
spec = parse("$.items[*][?@.price > %f]")

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

## Примеры использования

### Базовое использование

```python
from ascetic_ddd.specification.domain.jsonpath.jsonpath_rfc9535_parser import parse

# Простое сравнение (RFC 9535: ==)
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
```

### Логические операторы (RFC 9535)

```python
# AND оператор (RFC 9535: &&)
spec = parse("$[?@.age > %d && @.active == %s]")
user = DictContext({"age": 30, "active": True})
spec.match(user, (25, True))  # True

# OR оператор (RFC 9535: ||)
spec = parse("$[?@.age < %d || @.age > %d]")
user_young = DictContext({"age": 15})
spec.match(user_young, (18, 65))  # True

# NOT оператор (RFC 9535: !)
spec = parse("$[?!(@.active == %s)]")
user_inactive = DictContext({"active": False})
spec.match(user_inactive, (True,))  # True
```

### Работа с коллекциями

```python
from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext

spec = parse("$.users[*][?@.age >= %d]")

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

# Добавим дорогой товар во вторую категорию
item4 = DictContext({"name": "Designer Jeans", "price": 299.0})
items2 = CollectionContext([item3, item4])
category2 = DictContext({"name": "Clothing", "items": items2})

categories = CollectionContext([category1, category2])
store = DictContext({"categories": categories})

# Теперь обе категории имеют товары дороже 200
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

# Простой вложенный путь: $[?@.profile.age > 25]
spec = parse("$[?@.profile.age > %d]")

data = NestedDictContext({
    "profile": {
        "age": 30,
        "name": "Alice"
    }
})

spec.match(data, (25,))  # True
```

**Глубоко вложенные пути:**

```python
# Глубокая вложенность: $[?@.company.department.manager.level > 5]
spec = parse("$[?@.company.department.manager.level > %d]")

data = NestedDictContext({
    "company": {
        "department": {
            "manager": {
                "level": 7,
                "name": "Alice"
            }
        }
    }
})

spec.match(data, (5,))  # True
```

**Вложенные пути с логическими операторами:**

```python
# $[?@.profile.age > 25 && @.profile.active == true]
spec = parse("$[?@.profile.age > %d && @.profile.active == %s]")

data = NestedDictContext({
    "profile": {
        "age": 30,
        "active": True
    }
})

spec.match(data, (25, True))  # True
```

**Вложенные пути со скобочками:**

```python
# Скобочки для приоритета операций
spec = parse("$[?(@.profile.age >= %d && @.profile.age <= %d) && @.profile.active == %s]")

data = NestedDictContext({
    "profile": {
        "age": 30,
        "active": True
    }
})

spec.match(data, (25, 35, True))  # True
```

### Сложные выражения

```python
# Комбинация операторов
spec = parse("$[?(@.age >= %d && @.age <= %d) && @.status == %s]")
user = DictContext({"age": 30, "status": "active"})
spec.match(user, (25, 35, "active"))  # True

# Множественные именованные параметры
spec = parse("$[?@.age >= %(min_age)d && @.age <= %(max_age)d]")
user = DictContext({"age": 30})
spec.match(user, {"min_age": 25, "max_age": 35})  # True
```

## Тестирование

```bash
# Запустить тесты RFC 9535 парсера
python -m unittest ascetic_ddd.specification.domain.jsonpath.test_jsonpath_parser_rfc9535 -v

# Запустить примеры
python ascetic_ddd/specification/domain/jsonpath/example_usage_rfc9535.py

# Все тесты
python -m unittest discover -s ascetic_ddd/specification -p "test_*.py" -v
```

## Преимущества RFC 9535

1. **Соответствие стандарту** - полная совместимость с RFC 9535
2. **Официальная спецификация** - основан на официальном стандарте IETF
3. **Переносимость** - легко интегрируется с другими RFC 9535 системами
4. **Стабильность** - стандарт обеспечивает долгосрочную стабильность
5. **Понятный синтаксис** - `==` для равенства, `&&`/`||` для логики
6. **Активная поддержка** - библиотека jsonpath-rfc9535 активно поддерживается

## Зависимости

- `jsonpath-rfc9535` - парсинг JSONPath выражений (RFC 9535 compliant)
- Модули из `ascetic_ddd.specification.domain`:
  - `nodes` - AST узлы спецификации
  - `evaluate_visitor` - выполнение спецификаций

## Установка

```bash
pip install jsonpath-rfc9535
```

