# Lambda Filter Parser для Specification Pattern

Парсер Python lambda функций для преобразования в AST узлы Specification Pattern.

## Описание

Этот модуль преобразует Python lambda функции в AST узлы Specification Pattern, вдохновлён подходом из **hypothesis.internal.filtering** и **hypothesis.internal.lambda_sources**.

## Ключевые возможности

✅ **Простые сравнения** - `==`, `!=`, `>`, `<`, `>=`, `<=`
✅ **Логические операторы** - `and`, `or`, `not`
✅ **Арифметические операторы** - `+`, `-`, `*`, `/`, `%`
✅ **Вложенные выражения** - сложные комбинации операторов
✅ **Wildcard коллекции** - `any([list comprehension])` и `any(generator)`
✅ **Вложенные wildcard** - `any([any([...]) for ...])` - Wildcard внутри Wildcard
✅ **Типы литералов** - строки, числа, boolean, float

## Использование

### Базовые примеры

```python
from ascetic_ddd.specification.domain.lambda_filter import parse
from ascetic_ddd.specification.domain.evaluate_visitor import EvaluateVisitor

# Простое сравнение
spec = parse(lambda user: user.age > 25)

class DictContext:
    def __init__(self, data):
        self._data = data

    def get(self, key):
        return self._data[key]

user = DictContext({"age": 30})
visitor = EvaluateVisitor(user)
spec.accept(visitor)
print(visitor.result())  # True
```

### Логические операторы

```python
# AND
spec = parse(lambda user: user.age > 25 and user.active == True)

# OR
spec = parse(lambda user: user.age < 18 or user.age > 65)

# NOT
spec = parse(lambda user: not user.deleted)

# Сложные выражения
spec = parse(lambda user: user.age >= 18 and user.age <= 65 and user.active == True)
```

### Wildcard коллекции (any)

```python
from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext

# Generator expression
spec = parse(lambda store: any(item.price > 500 for item in store.items))

item1 = DictContext({"name": "Laptop", "price": 999})
item2 = DictContext({"name": "Mouse", "price": 29})

items = CollectionContext([item1, item2])
store = DictContext({"items": items})

visitor = EvaluateVisitor(store)
spec.accept(visitor)
print(visitor.result())  # True (Laptop price > 500)
```

```python
# List comprehension
spec = parse(lambda store: any([item.price > 500 for item in store.items]))

# Сложный предикат
spec = parse(lambda store: any(
    item.price > 100 and item.available == True
    for item in store.items
))
```

### Вложенные Wildcard

```python
# Вложенный any - проверка товаров во всех категориях
spec = parse(lambda order: any([
    any([item.price > 100 for item in category.items])
    for category in order.categories
]))

# Создаём структуру данных
item1 = DictContext({"name": "Laptop", "price": 150})
item2 = DictContext({"name": "Mouse", "price": 50})
items = CollectionContext([item1, item2])
category = DictContext({"name": "Electronics", "items": items})

categories = CollectionContext([category])
order = DictContext({"id": 1, "categories": categories})

visitor = EvaluateVisitor(order)
spec.accept(visitor)
print(visitor.result())  # True (есть товар с ценой > 100)
```

## Поддерживаемые возможности

### Операторы сравнения
- `==` - Равенство
- `!=` - Неравенство
- `>` - Больше
- `<` - Меньше
- `>=` - Больше или равно
- `<=` - Меньше или равно

### Логические операторы
- `and` - Логическое И
- `or` - Логическое ИЛИ
- `not` - Логическое НЕ

### Арифметические операторы
- `+` - Сложение
- `-` - Вычитание
- `*` - Умножение
- `/` - Деление
- `%` - Остаток от деления (модуло)

### Коллекции
- `any(generator)` - Преобразуется в `Wildcard`
- `any([list comprehension])` - Преобразуется в `Wildcard`
- `all(generator)` - Преобразуется в `Wildcard`
- `all([list comprehension])` - Преобразуется в `Wildcard`
- **Вложенные wildcard** - `any([any([...]) for ...])` ✅ Поддерживается

### Типы литералов
```python
# Строки
parse(lambda user: user.name == "Alice")

# Числа
parse(lambda user: user.age > 25)
parse(lambda product: product.price > 99.99)

# Boolean
parse(lambda user: user.active == True)
parse(lambda user: user.deleted == False)
```

### Арифметические операции
```python
# Сложение
parse(lambda user: user.age + 5 > 30)

# Вычитание
parse(lambda user: user.age - 5 >= 18)

# Умножение
parse(lambda product: product.price * 2 > 100)

# Деление
parse(lambda user: user.score / 2 >= 40)

# Модуло (остаток от деления)
parse(lambda user: user.id % 2 == 0)  # Чётные ID

# Сложные выражения
parse(lambda user: (user.age + 5) * 2 > 60)
```

## Архитектура

### Процесс парсинга

```
Lambda Function
      ↓
[inspect.findsource] Извлечение исходного кода
      ↓
[ast.parse] Парсинг в Python AST
      ↓
[_find_all_lambdas] Поиск lambda узлов
      ↓
[_convert_node] Преобразование в Specification AST
      ↓
Specification Nodes (And, Or, Equal, Field, Value, Wildcard, etc.)
```

### Компоненты

1. **LambdaParser** - Основной класс парсера
   - `parse()` - Находит lambda в исходном коде
   - `_convert_node()` - Диспетчеризация по типам AST узлов
   - `_convert_compare()` - Операторы сравнения
   - `_convert_bool_op()` - Логические операторы
   - `_convert_call()` - Вызовы функций (any, all)
   - `_convert_generator_to_wildcard()` - Generator → Wildcard
   - `_convert_listcomp_to_wildcard()` - List comprehension → Wildcard

2. **Context Tracking**
   - `arg_name` - Имя аргумента lambda
   - `_in_item_context` - Флаг контекста внутри wildcard

3. **AST Nodes Mapping**
   ```
   ast.Compare + ast.Eq      → Equal
   ast.Compare + ast.Gt      → GreaterThan
   ast.Compare + ast.Lt      → LessThan
   ast.BoolOp + ast.And      → And
   ast.BoolOp + ast.Or       → Or
   ast.UnaryOp + ast.Not     → Not
   ast.BinOp + ast.Add       → Add
   ast.BinOp + ast.Sub       → Sub
   ast.BinOp + ast.Mult      → Mul
   ast.BinOp + ast.Div       → Div
   ast.BinOp + ast.Mod       → Mod
   ast.Attribute             → Field
   ast.Constant              → Value
   ast.GeneratorExp          → Wildcard
   ast.ListComp              → Wildcard
   ```

## Примеры AST преобразований

### Простое сравнение
```python
lambda user: user.age > 25

# Преобразуется в:
GreaterThan(
    Field(GlobalScope(), "age"),
    Value(25)
)
```

### Логическое И
```python
lambda user: user.age > 25 and user.active == True

# Преобразуется в:
And(
    GreaterThan(Field(GlobalScope(), "age"), Value(25)),
    Equal(Field(GlobalScope(), "active"), Value(True))
)
```

### Wildcard
```python
lambda store: any(item.price > 500 for item in store.items)

# Преобразуется в:
Wildcard(
    Object(GlobalScope(), "items"),
    GreaterThan(Field(Item(), "price"), Value(500))
)
```

### Вложенный Wildcard
```python
lambda order: any([
    any([item.price > 100 for item in category.items])
    for category in order.categories
])

# Преобразуется в:
Wildcard(
    Object(GlobalScope(), "categories"),
    Wildcard(
        Object(Item(), "items"),
        GreaterThan(Field(Item(), "price"), Value(100))
    )
)
```

### Арифметические операции
```python
lambda user: user.age + 5 > 30

# Преобразуется в:
GreaterThan(
    Add(Field(GlobalScope(), "age"), Value(5)),
    Value(30)
)
```

```python
lambda user: user.id % 2 == 0

# Преобразуется в:
Equal(
    Mod(Field(GlobalScope(), "id"), Value(2)),
    Value(0)
)
```

## Тестирование

```bash
# Запустить тесты lambda парсера
python -m unittest ascetic_ddd.specification.domain.lambda_filter.test_lambda_parser -v

# Все тесты (26 тестов: сравнения, логика, арифметика, wildcard, вложенные wildcard)
python -m unittest discover -s ascetic_ddd/specification/domain/lambda_filter -p "test_*.py" -v
```

## Сравнение с другими парсерами

| Характеристика | JSONPath | Lambda Filter |
|----------------|----------|---------------|
| Синтаксис | JSONPath строки | Python lambda |
| Типизация | Строки | Нативный Python |
| IDE поддержка | ❌ Нет | ✅ Автодополнение |
| Проверка типов | ❌ Runtime | ✅ Static (mypy) |
| Читаемость | Средняя | Высокая |
| Рефакторинг | Сложный | ✅ Простой |
| Внешние зависимости | jsonpath libs | ❌ Нет |
| Параметризация | C-style (%s) | Нативная |

## Когда использовать Lambda Filter

**Выбирайте Lambda Filter, если:**

- ✅ Нужна **IDE поддержка** и автодополнение
- ✅ Важна **статическая проверка типов** (mypy, pyright)
- ✅ Хотите **нативный Python синтаксис** без строк
- ✅ Требуется **рефакторинг** кода (rename fields, etc.)
- ✅ Минимум внешних зависимостей

**Выбирайте JSONPath, если:**

- ✅ Нужна **сериализация** спецификаций
- ✅ Спецификации **приходят извне** (API, config files)
- ✅ Требуется **RFC 9535 совместимость**
- ✅ Используете JSONPath в других частях системы

## Ограничения

Текущая версия **не поддерживает**:

- ❌ Вложенные lambda функции
- ❌ Lambda с несколькими аргументами
- ❌ Вызовы методов объектов (кроме any/all)
- ❌ Slice операции (e.g., `list[0:5]`)
- ❌ Тернарные операторы (`x if condition else y`)
- ❌ Битовые операции (кроме `<<`, `>>`)

## Вдохновение

Этот модуль вдохновлён подходами из:

- **hypothesis.internal.filtering** - AST анализ предикатов
- **hypothesis.internal.lambda_sources** - Извлечение исходного кода lambda
- **JSONPath парсеры** - Преобразование в Specification AST
