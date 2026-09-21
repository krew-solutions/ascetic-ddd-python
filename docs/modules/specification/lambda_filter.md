# Lambda Filter Parser for Specification Pattern

```{index} Lambda Filter, Specification Pattern, AST
```

Parser for Python lambda functions that converts them into Specification Pattern AST nodes.

## Description

This module converts Python lambda functions into Specification Pattern AST nodes, inspired by the approach from **hypothesis.internal.filtering** and **hypothesis.internal.lambda_sources**.

This allows using predicate functions in the Specification Pattern while maintaining high performance.

## Key Features

- **Simple comparisons** - `==`, `!=`, `>`, `<`, `>=`, `<=`
- **Logical operators** - `and`, `or`, `not`
- **Arithmetic operators** - `+`, `-`, `*`, `/`, `%`
- **Nested expressions** - complex combinations of operators
- **Method-based operators** - `Eq()`, `Lt()`, `Gte()`, `IsNull()`, etc.
- **Wildcard collections** - `any([list comprehension])` and `any(generator)`
- **Nested wildcards** - `any([any([...]) for ...])` - Wildcard inside Wildcard
- **Literal types** - strings, numbers, boolean, float

## Usage

### Basic Examples

```python
from ascetic_ddd.specification.domain.lambda_filter import parse
from ascetic_ddd.specification.domain.evaluate_visitor import EvaluateVisitor

# Simple comparison
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

### Logical Operators

```python
# AND
spec = parse(lambda user: user.age > 25 and user.active == True)

# OR
spec = parse(lambda user: user.age < 18 or user.age > 65)

# NOT
spec = parse(lambda user: not user.deleted)

# Complex expressions
spec = parse(lambda user: user.age >= 18 and user.age <= 65 and user.active == True)
```

### Method-Based Operators

Method-based operators allow expressing comparisons as method calls on fields.
Both operator syntax and method syntax produce identical AST nodes.

```python
# Comparison methods
spec = parse(lambda user: user.age.Eq(30))       # Equal
spec = parse(lambda user: user.age.Ne(30))        # NotEqual
spec = parse(lambda user: user.age.Gt(25))        # GreaterThan
spec = parse(lambda user: user.age.Gte(25))       # GreaterThanEqual
spec = parse(lambda user: user.age.Lt(30))        # LessThan
spec = parse(lambda user: user.age.Lte(30))       # LessThanEqual

# Postfix methods
spec = parse(lambda user: user.email.IsNull())    # IsNull
spec = parse(lambda user: user.email.IsNotNull()) # IsNotNull

# Nested paths
spec = parse(lambda user: user.profile.age.Gte(18))

# Combined with logical operators
spec = parse(lambda user: user.age.Gte(18) and user.age.Lte(65))
spec = parse(lambda user: user.email.IsNull() or user.email.Eq(""))

# Inside wildcards
spec = parse(lambda store: any(item.price.Gt(500) for item in store.items))
```

#### Supported Method Aliases

| Node            | Method aliases                                               |
|-----------------|--------------------------------------------------------------|
| Equal           | `Equal()`, `Equals()`, `Eq()`                                |
| NotEqual        | `NotEqual()`, `NotEquals()`, `Ne()`, `Neq()`                 |
| LessThan        | `LessThan()`, `Lt()`                                         |
| LessThanEqual   | `LessThanOrEqual()`, `LessThanEqual()`, `Lte()`, `Le()`      |
| GreaterThan     | `GreaterThan()`, `Gt()`                                      |
| GreaterThanEqual| `GreaterThanOrEqual()`, `GreaterThanEqual()`, `Gte()`, `Ge()`|
| IsNull          | `IsNull()`                                                   |
| IsNotNull       | `IsNotNull()`                                                |

### Wildcard Collections (any)

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

# Complex predicate
spec = parse(lambda store: any(
    item.price > 100 and item.available == True
    for item in store.items
))
```

### Nested Wildcards

```python
# Nested any - check items across all categories
spec = parse(lambda order: any([
    any([item.price > 100 for item in category.items])
    for category in order.categories
]))

# Create data structure
item1 = DictContext({"name": "Laptop", "price": 150})
item2 = DictContext({"name": "Mouse", "price": 50})
items = CollectionContext([item1, item2])
category = DictContext({"name": "Electronics", "items": items})

categories = CollectionContext([category])
order = DictContext({"id": 1, "categories": categories})

visitor = EvaluateVisitor(order)
spec.accept(visitor)
print(visitor.result())  # True (there is an item with price > 100)
```

## Supported Features

### Comparison Operators
- `==` - Equal
- `!=` - Not equal
- `>` - Greater than
- `<` - Less than
- `>=` - Greater than or equal
- `<=` - Less than or equal

### Logical Operators
- `and` - Logical AND
- `or` - Logical OR
- `not` - Logical NOT

### Arithmetic Operators
- `+` - Addition
- `-` - Subtraction
- `*` - Multiplication
- `/` - Division
- `%` - Modulo

### Collections
- `any(generator)` - Converts to `Wildcard`
- `any([list comprehension])` - Converts to `Wildcard`
- `all(generator)` - Converts to `Not(Wildcard(..., Not(predicate)))`: no item fails the predicate
- `all([list comprehension])` - The same
- **The candidate inside a predicate** - `any(item.name == store.name for item in store.items)`
- **Nested wildcards** - `any([any([...]) for ...])` - Supported

### Unary Operators
- `not x` - `Not`
- `-x` - `Neg`; a negative literal, `-5`, is the value it looks like
- `+x` - `x`: the tree has no unary plus, which would change nothing in either reader

### None

`u.email is None` and `u.email == None` are `IsNull(email)`; `is not` and
`!=` are `IsNotNull`. So is a comparison with a variable from outside the
lambda that is `None`. In the tree, as in SQL, a comparison with null is null
and true of nothing; what the lambda means by it is the null test.

### Option

A member that is an `ascetic_ddd.option.Option` is what it holds, or a null,
to both readers of the tree. In a lambda:

- `a.discount.is_some_and(lambda d: d > 10)` -
  `And(IsNotNull(discount), GreaterThan(discount, Value(10)))`
- `a.discount.is_nothing_or(lambda d: d > 10)` -
  `Or(IsNull(discount), GreaterThan(discount, Value(10)))`
- `a.discount.is_nothing()` - `IsNull(discount)`; `is_some()` - `IsNotNull`
- `a.discount.unwrap()` - the member itself, `discount`
- `Some(x)` - `x`; `Nothing()` - the null, so `a.discount == Nothing()` is
  `IsNull(discount)`

The name the inner lambda gives stands for the Option: `d.percent` is
`discount.percent`. The null test beside the predicate makes the whole of two
values, as it is to the lambda, so the two agree under `not` too, and nothing
is unwrapped. An Option from outside the lambda is asked the same way,
`limit.is_some_and(lambda held: ...)`, and `limit.unwrap()` is the Option
itself, not what it holds when the lambda is parsed: the lambda unwraps it
behind its guard, and of a `Nothing` never does.

`Some` and `Nothing` are told by what the name stands for where the lambda is
written: `option.Some` and an alias are the maker, another function called
`Some` is not. `unwrap_or` and the other methods have no node and are refused.

The tree's logic is the storage's, of three values: `a.discount != Some(5)` is
true of `Nothing` to the lambda, and null - not selected - to the tree and to
SQL. Whoever means the absent too says so:
`a.discount.is_nothing_or(lambda d: d != 5)`.

### Values From Outside the Lambda

A name that is not the lambda's argument, nor the target of a comprehension,
is a variable the lambda sees from where it is written - of an enclosing
function, or of the module. Its value at the time of parsing becomes a
`Value`, so a specification can have parameters:

```python
def older_than(min_age: int):
    return parse(lambda user: user.age > min_age)
    # GreaterThan(Field(GlobalScope(), "age"), Value(min_age))

parse(lambda item: item.price <= limits.max_price)   # attributes are followed
```

A name that is nowhere is a `ValueError`, as before.

### Literal Types
```python
# Strings
parse(lambda user: user.name == "Alice")

# Numbers
parse(lambda user: user.age > 25)
parse(lambda product: product.price > 99.99)

# Boolean
parse(lambda user: user.active == True)
parse(lambda user: user.deleted == False)
```

### Arithmetic Operations
```python
# Addition
parse(lambda user: user.age + 5 > 30)

# Subtraction
parse(lambda user: user.age - 5 >= 18)

# Multiplication
parse(lambda product: product.price * 2 > 100)

# Division
parse(lambda user: user.score / 2 >= 40)

# Modulo
parse(lambda user: user.id % 2 == 0)  # Even IDs

# Complex expressions
parse(lambda user: (user.age + 5) * 2 > 60)
```

## Architecture

### Parsing Process

```
Lambda Function
      |
[inspect.findsource] Extract source code
      |
[ast.parse] Parse into Python AST
      |
[_find_all_lambdas] Find lambda nodes
      |
[_convert_node] Convert to Specification AST
      |
Specification Nodes (And, Or, Equal, Field, Value, Wildcard, etc.)
```

### Components

1. **LambdaParser** - Main parser class
   - `parse()` - Finds lambda in source code
   - `_convert_node()` - Dispatches by AST node type
   - `_convert_compare()` - Comparison operators
   - `_convert_bool_op()` - Logical operators
   - `_convert_call()` - Function and method calls (any, all, Eq, IsNull, etc.)
   - `_convert_method_comparison()` - Method-based comparisons (receiver.Method(arg))
   - `_convert_method_postfix()` - Postfix methods (receiver.Method())
   - `_convert_generator_to_wildcard()` - Generator -> Wildcard
   - `_convert_listcomp_to_wildcard()` - List comprehension -> Wildcard

2. **Context Tracking**
   - `arg_name` - Lambda argument name
   - `_in_item_context` - Flag for wildcard context

3. **AST Nodes Mapping**
   ```
   ast.Compare + ast.Eq      -> Equal
   ast.Compare + ast.Gt      -> GreaterThan
   ast.Compare + ast.Lt      -> LessThan
   ast.BoolOp + ast.And      -> And
   ast.BoolOp + ast.Or       -> Or
   ast.UnaryOp + ast.Not     -> Not
   ast.BinOp + ast.Add       -> Add
   ast.BinOp + ast.Sub       -> Sub
   ast.BinOp + ast.Mult      -> Mul
   ast.BinOp + ast.Div       -> Div
   ast.BinOp + ast.Mod       -> Mod
   ast.Call + .Eq()           -> Equal
   ast.Call + .IsNull()       -> IsNull
   ast.Attribute              -> Field
   ast.Constant               -> Value
   ast.GeneratorExp           -> Wildcard
   ast.ListComp               -> Wildcard
   ```

## AST Transformation Examples

### Simple Comparison
```python
lambda user: user.age > 25

# Transforms to:
GreaterThan(
    Field(GlobalScope(), "age"),
    Value(25)
)
```

### Logical AND
```python
lambda user: user.age > 25 and user.active == True

# Transforms to:
And(
    GreaterThan(Field(GlobalScope(), "age"), Value(25)),
    Equal(Field(GlobalScope(), "active"), Value(True))
)
```

### Method-Based Comparison
```python
lambda user: user.age.Gte(18)

# Transforms to:
GreaterThanEqual(
    Field(GlobalScope(), "age"),
    Value(18)
)
```

### Postfix Method
```python
lambda user: user.email.IsNull()

# Transforms to:
IsNull(
    Field(GlobalScope(), "email")
)
```

### Wildcard
```python
lambda store: any(item.price > 500 for item in store.items)

# Transforms to:
Wildcard(
    Object(GlobalScope(), "items"),
    GreaterThan(Field(Item(), "price"), Value(500))
)
```

### Nested Wildcard
```python
lambda order: any([
    any([item.price > 100 for item in category.items])
    for category in order.categories
])

# Transforms to:
Wildcard(
    Object(GlobalScope(), "categories"),
    Wildcard(
        Object(Item(), "items"),
        GreaterThan(Field(Item(), "price"), Value(100))
    )
)
```

### Arithmetic Operations
```python
lambda user: user.age + 5 > 30

# Transforms to:
GreaterThan(
    Add(Field(GlobalScope(), "age"), Value(5)),
    Value(30)
)
```

```python
lambda user: user.id % 2 == 0

# Transforms to:
Equal(
    Mod(Field(GlobalScope(), "id"), Value(2)),
    Value(0)
)
```

## Testing

```bash
# Run lambda parser tests
python -m unittest ascetic_ddd.specification.domain.tests.lambda_filter.test_lambda_parser -v
```

## When to Use Lambda Filter

**Choose Lambda Filter if:**

- You need **IDE support** and autocomplete
- **Static type checking** matters (mypy, pyright)
- You want **native Python syntax** without strings
- You need **refactoring** support (rename fields, etc.)
- Minimal external dependencies


## Limitations

The current version **does not support**:

- Nested lambda functions (`parse(lambda user: (lambda x: x > 25)(user.age))`)
- Lambdas with multiple arguments
- Slice operations (e.g., `list[0:5]`)
- Ternary operators (`x if condition else y`)
- Bitwise operations (except `<<`, `>>`)
- The item of an outer comprehension inside an inner one
  (`any(any(i.price > c.limit for i in c.items) for c in s.categories)`):
  the tree has one `Item()`, the nearest. It is a `ValueError`, whatever else
  bears the same name.

## Inspiration

This module is inspired by approaches from:

- **hypothesis.internal.filtering** - AST analysis of predicates
- **hypothesis.internal.lambda_sources** - Lambda source code extraction
- **JSONPath parsers** - Conversion to Specification AST
