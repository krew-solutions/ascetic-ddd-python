# JSONPath2 Specification Parser

Парсер JSONPath выражений для Specification Pattern с использованием библиотеки **jsonpath2**.

## Описание

Эта реализация использует библиотеку `jsonpath2` для парсинга JSONPath выражений и преобразует их в AST узлы Specification Pattern. Поддерживает параметризацию в стиле C-форматирования строк.

## Ключевые особенности

✅ **Использует jsonpath2** - проверенную библиотеку для парсинга JSONPath
✅ **Параметризация** - поддержка плейсхолдеров (%s, %d, %f, %(name)s)
✅ **Операторы сравнения** - `=`, `!=`, `>`, `<`, `>=`, `<=`
✅ **Коллекции с wildcard** - фильтрация элементов коллекций
✅ **Вложенные wildcards** ✨ - фильтрация по вложенным коллекциям (`$.categories[*][?@.items[*][?@.price > 100]]`)
✅ **Вложенные пути** - поддержка `@.profile.age`, `@.company.department.manager.level`
✅ **Группировка скобками** - автоматическое добавление скобок для фильтров
✅ **Переиспользование** - одна спецификация с разными параметрами
✅ **Те же возможности** - полная совместимость с другими версиями парсера

## Использование

```python
from ascetic_ddd.specification.domain.jsonpath.jsonpath2_parser import parse

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

## Сравнение с другими версиями

| Характеристика | JSONPath2 | RFC 9535 | Lambda Filter | Нативный |
|----------------|-----------|----------|---------------|----------|
| Внешние зависимости | jsonpath2 | jsonpath-rfc9535 | ❌ Нет | ❌ Нет |
| Размер кода | ~670 строк | ~550 строк | ~600 строк | ~500 строк |
| Поддерживаемые операторы | Все | Все | Все | Все |
| Параметризация | ✓ | ✓ | ✓ | ✓ |
| Wildcard коллекции | ✓ | ✓ | ❌ | ✓ |
| AND/OR операторы | ✓ (`&&`, `\|\|`) | ✓ (`&&`, `\|\|`) | ✓ (`and`, `or`) | ✓ (`&&`, `\|\|`) |
| Вложенные пути | ✓ | ✓ | ✓ | ✓ |
| Вложенные wildcards | ✓ | ✓ | ✓ | ✓ |
| Группировка скобками | ✓ (авто) | ✓ | ✓ | ✓ |
| Контроль над AST | Частичный | Частичный | Полный | Полный |
| Стабильность | Высокая | Высокая | Высокая | Средняя |

## Поддерживаемые возможности

### Операторы сравнения
- `=` - Равенство (jsonpath2 использует одиночный `=`, а не `==`)
- `!=` - Неравенство
- `>` - Больше
- `<` - Меньше
- `>=` - Больше или равно
- `<=` - Меньше или равно

### Параметризация
```python
# Позиционные
parse("$[?(@.age > %d)]")          # Целое число
parse("$[?(@.name = %s)]")          # Строка
parse("$[?(@.price > %f)]")         # Число с плавающей точкой

# Именованные
parse("$[?(@.age > %(min_age)d)]")
parse("$[?(@.name = %(name)s)]")
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

## Особенности jsonpath2

### Синтаксис

jsonpath2 библиотека **отклоняется от стандарта RFC 9535**:

- **Поддерживаются оба варианта**: `=` и `==` для равенства ✨
  - **RFC 9535 стандарт** определяет `==` для равенства
  - **jsonpath2 библиотека** отклоняется от стандарта и использует `=`
  - Наш парсер автоматически нормализует `==` → `=` для совместимости с библиотекой
  - Это обеспечивает лучший UX и совместимость с Native парсерами

- **Логические операторы полностью поддерживаются!** ✨
  - **RFC 9535 стандарт** использует: `&&` (AND), `||` (OR), `!` (NOT)
  - **jsonpath2 библиотека** использует: `and`, `or`, `not` (текстовые операторы)
  - Наш парсер автоматически нормализует: `&&` → `and`, `||` → `or`, `!` → `not`
  - **Полная поддержка RFC 9535 синтаксиса!**

- **Автоматическое добавление скобок в фильтрах** ✨
  - **jsonpath2 библиотека** требует скобки вокруг условий: `$[?(@.age > 25)]`
  - Наш парсер автоматически добавляет скобки, если они отсутствуют
  - Можно писать: `$[?@.age > 25]` → автоматически преобразуется в `$[?(@.age > 25)]`

- Строгая проверка синтаксиса с подробными сообщениями об ошибках

### Преимущества

1. **Простой синтаксис** - использует `=` вместо `==`
2. **Подробные ошибки** - детальные сообщения при синтаксических ошибках
3. **Производительность** - оптимизированный парсер на основе ANTLR
4. **Поддержка сообщества** - активная разработка и поддержка

### Ограничения (отклонения от RFC 9535)

1. **Синтаксис равенства** - использует `=` вместо стандартного `==`
   - Наше улучшение добавляет поддержку `==` через автоматическую нормализацию
2. **Требуются скобки** - фильтры требуют скобки вокруг условий
   - Наше улучшение автоматически добавляет скобки
3. **Строгая валидация** - более строгие требования к синтаксису

Благодаря нашим улучшениям (автоматическая нормализация синтаксиса), большинство ограничений скрыты от пользователя.

## Поддержка вложенных путей

JSONPath2 парсер поддерживает вложенные пути в фильтрах, позволяя обращаться к полям вложенных объектов:

### Синтаксис вложенных путей

```python
# Простой вложенный путь
spec = parse("$[?(@.profile.age > %d)]")

# Глубокая вложенность
spec = parse("$[?(@.company.department.manager.level >= %d)]")

# Вложенные пути в составных условиях
spec = parse("$[?(@.profile.age > %d && @.profile.status = %s)]")
```

### Примеры использования вложенных путей

```python
from ascetic_ddd.specification.domain.jsonpath.jsonpath2_parser import parse

# Класс контекста с поддержкой вложенных объектов
class NestedDictContext:
    def __init__(self, data):
        self._data = data

    def get(self, key):
        value = self._data[key]
        # Автоматически оборачиваем вложенные словари
        if isinstance(value, dict):
            return NestedDictContext(value)
        return value

# Простой вложенный путь
spec = parse("$[?(@.profile.age > %d)]")
user = NestedDictContext({
    "name": "Alice",
    "profile": {"age": 30, "city": "NYC"}
})
spec.match(user, (25,))  # True

# Глубокая вложенность (3+ уровня)
spec = parse("$[?(@.company.department.manager.level >= %d)]")
employee = NestedDictContext({
    "name": "Bob",
    "company": {
        "name": "TechCorp",
        "department": {
            "name": "Engineering",
            "manager": {"name": "Charlie", "level": 5}
        }
    }
})
spec.match(employee, (3,))  # True

# Вложенные пути в составных условиях
spec = parse("$[?(@.profile.age > %d && @.profile.status = %s)]")
user = NestedDictContext({
    "name": "Diana",
    "profile": {"age": 28, "status": "active"}
})
spec.match(user, (25, "active"))  # True

# Именованные параметры с вложенными путями
spec = parse("$[?(@.settings.notifications.email = %(enabled)s)]")
user = NestedDictContext({
    "name": "Eve",
    "settings": {
        "notifications": {"email": True, "sms": False}
    }
})
spec.match(user, {"enabled": True})  # True
```

### Важные замечания

1. **Автоматическая обработка цепочек**: Парсер автоматически распознает и обрабатывает вложенные пути любой глубины

2. **Требования к контексту**: Контекст должен возвращать вложенные объекты, которые также поддерживают протокол `get()`:
   ```python
   class NestedDictContext:
       def get(self, key):
           value = self._data[key]
           if isinstance(value, dict):
               return NestedDictContext(value)  # Важно!
           return value
   ```

3. **Совместимость**: Синтаксис полностью совместим с RFC 9535 и другими парсерами

## Примеры использования

### Базовое использование

```python
from ascetic_ddd.specification.domain.jsonpath.jsonpath2_parser import parse

# Простое сравнение
spec = parse("$[?(@.age > %d)]")
user = DictContext({"age": 30})
spec.match(user, (25,))  # True

# Строковое сравнение
spec = parse("$[?(@.status = %s)]")
task = DictContext({"status": "done"})
spec.match(task, ("done",))  # True

# Именованные параметры
spec = parse("$[?(@.score >= %(min_score)d)]")
student = DictContext({"score": 85})
spec.match(student, {"min_score": 80})  # True
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

### Вложенные Wildcards ✨

JSONPath2 парсер поддерживает вложенные wildcards для фильтрации по вложенным коллекциям:

```python
from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext

# Вложенные wildcards: фильтрация по вложенным коллекциям
spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

# Создаем структуру данных
item1 = DictContext({"name": "Laptop", "price": 999.0})
item2 = DictContext({"name": "Mouse", "price": 29.0})
items1 = CollectionContext([item1, item2])
category1 = DictContext({"name": "Electronics", "items": items1})

item3 = DictContext({"name": "Shirt", "price": 49.0})
items2 = CollectionContext([item3])
category2 = DictContext({"name": "Clothing", "items": items2})

categories = CollectionContext([category1, category2])
store = DictContext({"categories": categories})

# Есть ли хотя бы одна категория с товаром дороже 500?
spec.match(store, (500.0,))  # True (Laptop)
```

**Вложенные wildcards с логикой:**

```python
# Комбинация условий во вложенных фильтрах
spec = parse("$.categories[*][?@.items[*][?@.price > %f && @.price < %f]]")

item1 = DictContext({"name": "Monitor", "price": 599.0})
items = CollectionContext([item1])
category = DictContext({"name": "Displays", "items": items})
categories = CollectionContext([category])
store = DictContext({"categories": categories})

# Категория с товаром в диапазоне цен
spec.match(store, (500.0, 700.0))  # True
```

**С именованными параметрами:**

```python
spec = parse("$.categories[*][?@.items[*][?@.price > %(min_price)f]]")
spec.match(store, {"min_price": 500.0})  # True
```

## Тестирование

```bash
# Запустить тесты jsonpath2 парсера
python -m unittest ascetic_ddd.specification.domain.jsonpath.test_jsonpath_parser_jsonpath2 -v

# Все тесты
python -m unittest discover -s ascetic_ddd/specification -p "test_*.py" -v
```

## Зависимости

- `jsonpath2` - парсинг JSONPath выражений (RFC 9535)
- Модули из `ascetic_ddd.specification.domain`:
  - `nodes` - AST узлы спецификации
  - `evaluate_visitor` - выполнение спецификаций

