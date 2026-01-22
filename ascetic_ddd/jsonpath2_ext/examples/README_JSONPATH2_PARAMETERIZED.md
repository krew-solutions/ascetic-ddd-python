# JSONPath Parser Extension

Расширение для библиотеки `jsonpath2` с поддержкой параметризованных выражений.

## Описание

Модуль расширяет синтаксис парсера jsonpath2, добавляя поддержку плейсхолдеров. Выражение парсится один раз, значения передаются отдельно при выполнении (как prepared statements в SQL).

## Установка

```python
from ascetic_ddd.jsonpath2_ext.domain.jsonpath2_parameterized_parser import parse
```

## Использование

```python
# Парсим выражение с плейсхолдерами
path = parse("$[*][?(@.age > %(min_age)d)]")

# Выполняем с разными значениями
results1 = path.find(data, {"min_age": 27})
results2 = path.find(data, {"min_age": 30})
```

## API

### `parse(template: str) -> ParametrizedPath`

Парсит JSONPath выражение с плейсхолдерами.

- **template**: JSONPath выражение с плейсхолдерами (%s, %d, %f, %(name)s, %(age)d, %(price)f)
- **Возвращает**: `ParametrizedPath` объект

### `ParametrizedPath.match(data, params)`

Выполняет запрос с привязкой значений.

- **data**: данные для поиска
- **params**: значения (dict для именованных, tuple для позиционных)
- **Возвращает**: `Generator[MatchData]`

### `ParametrizedPath.find(data, params)`

Возвращает список найденных значений.

- **Возвращает**: `list`

### `ParametrizedPath.find_one(data, params)`

Возвращает первое найденное значение.

- **Возвращает**: `Any | None`

## Плейсхолдеры

### Именованные
```python
path = parse("$[*][?(@.age > %(age)d)]")
results = path.find(data, {"age": 27})
```

### Позиционные
```python
path = parse("$[*][?(@.age > %d)]")
results = path.find(data, (27,))
```

### Типы плейсхолдеров
- `%s` - строка (позиционный)
- `%d` - целое число (позиционный)
- `%f` - число с плавающей точкой (позиционный)
- `%(name)s` - строка (именованный)
- `%(age)d` - целое число (именованный)
- `%(price)f` - число с плавающей точкой (именованный)

## Примеры

```python
from ascetic_ddd.jsonpath2_ext.domain.jsonpath2_parameterized_parser import parse

users = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
]

# Базовый пример
path = parse("$[*]")
all_users = path.find(users, ())

# С позиционными плейсхолдерами
path2 = parse("$[*][?(@.age > %d)]")
adults = path2.find(users, (26,))

# С именованными плейсхолдерами
path3 = parse("$[*][?(@.name = %(name)s)]")
alice = path3.find(users, {"name": "Alice"})

# Повторное использование с разными значениями
for min_age in [25, 30, 35]:
    results = path2.find(users, (min_age,))
    print(f"Users older than {min_age}: {len(results)}")
```

## Безопасность

Значения передаются отдельно от выражения, что предотвращает injection-атаки:

```python
path = parse("$[*][?(@.name = %(name)s)]")
# Безопасно - значение передается как параметр, а не вставляется в строку
results = path.find(data, {"name": user_input})
```

Плейсхолдеры парсятся в AST и заменяются значениями при выполнении, как prepared statements в SQL.

## Файлы

- `jsonpath_parser.py` - основной модуль с `parse()` и `ParametrizedPath`
- `jsonpath2_filter_fix.py` - фикс бага в jsonpath2.FilterSubscript
- `test_jsonpath_parser.py` - тесты
- `example_usage.py` - примеры использования

## Тестирование

```bash
python -m unittest ascetic_ddd.jsonpath2_ext.domain.tests.test_jsonpath2_parameterized_parser -v
```
