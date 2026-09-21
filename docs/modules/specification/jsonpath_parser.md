```{index} Native JSONPath Parser, JSONPath, Specification Pattern
```

# Native JSONPath Parser (No External Dependencies)

## Description

A fully self-contained JSONPath expression parser that **requires no external libraries**. Directly converts RFC 9535 compatible JSONPath expressions into Specification AST.

## Key Advantages

- **No external dependencies** - runs on pure Python
- **RFC 9535 compatibility** - support for standard operators (`==`, `&&`, `||`, `!`)
- **Parentheses** - logical expression grouping (`$[?(@.age >= 18 && @.age <= 65) && @.active == true]`)
- **Full control** - transparent parsing logic
- **Lightweight** - minimal code, only the essentials
- **Easy to maintain** - all code in a single file
- **Full functionality** - all logical operators including NOT
- **Nested wildcards** - filtering by nested collections
- **Nested paths** - access to nested fields (`$[?@.a.b.c > 1]`) and to nested collections (`$.a.b.items[*][?@.x > 1]`)
- **The candidate inside a filter** - `$` is the candidate in any filter (`$.items[*][?@.price < $.limit]`)
- **A closed grammar** - what is not a template is refused where it stands, not read as something else

## Usage

```python
from ascetic_ddd.specification.domain.jsonpath.jsonpath_parser import parse

# Create specification
spec = parse("$[?(@.age > %d)]")


# Create context
class DictContext:
    def __init__(self, data):
        self._data = data

    def get(self, key):
        return self._data[key]


user = DictContext({"age": 30})

# Check match
result = spec.match(user, (25,))  # True
```

## Architecture

### Components

1. **Lexer** - Tokenization of JSONPath expressions
   - Recognizes operators, identifiers, literals
   - Handles placeholders

2. **Parser** - Token to AST conversion
   - Recursive expression parser
   - Direct creation of Specification nodes, deferred: a rule returns the
     function that builds its node once the parameters are there

3. **Placeholder Binding** - Parameter binding
   - Support for positional and named parameters
   - Typed placeholders (%s, %d, %f)
   - `bind(params)` calls the function the parser returned

### Parsing Process

```
JSONPath Template
      ↓
[Lexer] Tokenization
      ↓
Token Stream
      ↓
[Parser] Expression Parsing
      ↓
Function of the parameters            (kept; parsed once)
      ↓
[Binding] bind(params)
      ↓
Specification AST, with values
      ↓
[Evaluation] EvaluateVisitor          or transform(), or compile_to_sql()
      ↓
Boolean Result
```

A template is a translation that waits for its parameters. Kept as a tree, it
would need a node for "a value comes here later", and a specification has no
such word: a lambda and a tree built by hand have no placeholders, and every
reader of the tree would need a method to refuse it. Kept as a function, the
tree comes of it with values in it, or does not come at all:

```python
spec = parse("$.items[*][?@.price > %(price)f && @.owner == %(owner)s]")

spec.match(store, {"price": 9.5, "owner": "ann"})        # in memory

compile_to_sql(spec.bind({"price": 9.5, "owner": "ann"}))
# ('EXISTS (SELECT 1 FROM unnest("items") AS "item_1"
#   WHERE "item_1"."price" > $1 AND "item_1"."owner" = $2)', [9.5, 'ann'])

compile_to_sql(spec.bind({"price": 9.5, "owner": None}))
# ('EXISTS (... WHERE "item_1"."price" > $1 AND "item_1"."owner" IS NULL)', [9.5])
```

What a parameter is may decide what the tree is, as the last line shows, so a
query is compiled of a bound template, and not once for all parameters.

## RFC 9535 Compliance

Full support for the RFC 9535 standard:

### Comparison Operators
- `==` - Equal (RFC 9535: double sign)
- `!=` - Not equal
- `>` - Greater than
- `<` - Less than
- `>=` - Greater than or equal
- `<=` - Less than or equal

### Logical Operators
- `&&` - Logical AND (RFC 9535)
- `||` - Logical OR (RFC 9535)
- `!` - Logical NOT (RFC 9535)

### Parameterization
```python
# Positional
parse("$[?@.age > %d]")            # Integer
parse("$[?@.name == %s]")          # String (RFC 9535: ==)
parse("$[?@.price > %f]")          # Floating point number

# Named
parse("$[?@.age > %(min_age)d]")
parse("$[?@.name == %(name)s]")    # RFC 9535: ==

# Logical operators (RFC 9535)
parse("$[?@.age > %d && @.active == %s]")   # AND
parse("$[?@.age < %d || @.age > %d]")       # OR
parse("$[?!(@.active == %s)]")              # NOT
```

The letter of a placeholder says what it takes: `%d` an integer, `%f` a
number (`int`, `float`, `Decimal`), `%s` a value of any type - a Value Object
goes there; a `None` fits any. A parameter of another type is a
`JSONPathTypeError`, so parameters given in the wrong order are told at once.

`@.a == null` - spelled out, or a placeholder bound to `None` - is the null
test, `IsNull(@.a)`, and `!=` is `IsNotNull`: in the tree, as in SQL, a
comparison with null is null and true of nothing.

Placeholders are bound in the order they stand; one inside a string literal
(`'%s'`) is text. A template has placeholders of one style: `match()` takes a
tuple or a mapping, as Python's `%` does, and neither can bind a template
that mixes `%s` with `%(name)s`, so such a template is a `JSONPathSyntaxError`
when parsed. A parameter that is missing is an error when matched, not a
placeholder that silently equals nothing.

### Collections with Wildcard
```python
spec = parse("$.items[*][?(@.price > %f)]")

from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext

item1 = DictContext({"name": "Laptop", "price": 999.99})
item2 = DictContext({"name": "Mouse", "price": 29.99})

collection = CollectionContext([item1, item2])
store = DictContext({"items": collection})

# Check if there is at least one item with price > 500
spec.match(store, (500.0,))  # True
```

### Nested Wildcards
```python
# Nested collections: categories -> items
spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

# Create data structure
item1 = DictContext({"name": "Laptop", "price": 999.0})
items = CollectionContext([item1])
category = DictContext({"name": "Electronics", "items": items})

categories = CollectionContext([category])
store = DictContext({"categories": categories})

# Is there a category with an item costing more than 500?
spec.match(store, (500.0,))  # True
```

## Supported Features

The current implementation supports:
- Simple filters: `$[?@.field op value]`
- Logical expressions: `$[?@.a > 1 && @.b == 2]`, `$[?@.a < 1 || @.a > 10]`
- Negation: `$[?!(@.active == true)]`
- Wildcard collections: `$.collection[*][?@.field op value]`
- Nested wildcards: `$.categories[*][?@.items[*][?@.price > 100]]`
- Nested paths: `$[?@.a.b.c > 1]`, `$.a.b.items[*][?@.x > 1]`
- The candidate inside a filter: `$.items[*][?@.price < $.limit]`
- Either side of a comparison is an operand: `$[?%d < @.age]`, `$[?@.a < @.b]`
- A member as a test by itself: `$[?@.active]`, `$[?!@.active]`

### Grammar

```text
template   = "$" ( filter | ( "." name )+ "[*]" filter )
filter     = "[" "?" or "]"
or         = and ( "||" and )*
and        = primary ( "&&" primary )*
primary    = "!" primary | comparison
comparison = operand ( ( "==" | "!=" | "<" | "<=" | ">" | ">=" ) operand )?
operand    = "(" or ")" | value | query
query      = ( "@" | "$" ) ( "." name )+ ( "[*]" filter )?
value      = number | string | "true" | "false" | "null" | placeholder
number     = "-"? digit+ ( "." digit+ )? ( ( "e" | "E" ) ( "+" | "-" )? digit+ )?
string     = "'" ( character | escape )* "'" | '"' ( character | escape )* '"'
escape     = "\" ( "\" | "'" | '"' | "/" | "b" | "f" | "n" | "r" | "t" | "u" hex hex hex hex )
placeholder = "%s" | "%d" | "%f" | "%(" ( letter | digit | "_" )+ ")" ( "s" | "d" | "f" )
name       = ( letter | "_" ) ( letter | digit | "_" )*
```

Letters and digits are those of ASCII, in names, in placeholders and in numbers,
as they are in the Rust port and in Go.

Numbers and strings are RFC 9535's, and the same text is the same literal in
the Rust port. A number with a fraction or an exponent is a `float` - `1e3` is
`1000.0` - and any other an `int`. The evaluator computes an `int` as
PostgreSQL does a `bigint` and a `float` as a `double precision`, so a literal
that fits neither (`99999999999999999999`, `1e999`) is a `JSONPathSyntaxError`.

In a string a backslash takes the next character along, so a quote of the kind
the string is written in can stand inside it: `'it\'s'`, `"say \"hi\""`. A code
point beyond the basic plane is a pair of `\u` escapes, a high surrogate and a
low one; half a pair, and a backslash before anything the grammar does not
list, is a `JSONPathSyntaxError` at the backslash, as a string left open is one
at its quote. Earlier versions had no escapes: a string with quotes of both
kinds could not be written, and `\n` was a backslash and an `n`. They had no
exponent either: `1e3` was the number `1` and a name.

`$[?p]` is `p` of the candidate, and `@` in it is the candidate.
`$.a.items[*][?p]` is "some item of `a.items` satisfies `p`", and `@` in `p`
is the item. A filter applies to the items of a collection, so a path needs
its `[*]` before its filter.

Every token the grammar asks for is required, and nothing may follow the
template. Refused, where earlier versions read them as something else:

- a bracket or a parenthesis left open, or closed twice - `$[?@.age > 1`,
  `$[?(@.age > 1]`, `$[?@.age > 1]]`. A parenthesis used to be closed by the
  first primary that met it, so `$[?(@.a > 1 || @.b > 2) && @.c > 3]` was read
  as `a > 1 || (b > 2 && c > 3)`;
- a filter on a path without `[*]` - `$.items[?@.price > 1]` - whose path was
  dropped and the filter applied to the candidate;
- a name without `@` or `$` - `$[?age > 1]`;
- a comparison of a comparison - `$[?@.a == 1 == 2]`;
- positional and named placeholders in one template.

A template's tree has at most 128 levels, and a template nests at most 32
deep - groups, `!`, the filters of collections; beyond either it is refused,
"Expression is nested too deep". Every reader of a tree recurses, and a text
that is not trusted used to end in a `RecursionError`, which is not the error
of a template and is not caught as one. A chain of `&&` or `||` nests to the
left, so it has at most 128 operands. Within the bounds a template is parsed,
and its tree read by everything that reads one, in under 500 frames of
recursion, and in a time that grows as the length of the text.

In a query, an object on the way to a member - `@.owner.name` - is looked up
in the schema registry by the names that lead to it, as a collection is. Kept
in a table of its own,
`register_relational("items.owner", "owners", "id", "owner_id")`, it is read
through the key, by a subquery in the column's place. Not mentioned, it is a
composite kept in the item's row, `("item_1"."maker")."name"` - a Value
Object; one kept as columns with a prefix is for the transform context to say.
From the candidate an object not mentioned is a qualifier, `"s"."price"`, so a
composite column of the candidate's own row cannot be reached.

An object kept by a key and not said to be is taken for a composite, and
PostgreSQL reads a member called like a type it can cast to - `name`, `text` -
of a column that is no composite as that cast: the query selects nothing, in
silence. Any other member of such a column is an error. And an object that is
null has no members for the evaluator, which raises, where PostgreSQL has null
for a member of a null composite.

A constant with nothing but constants beside it has its type said in the
query, `$1::bigint + $2::bigint`, by the kind of its value; a null, which has
no kind, by what its operator is of. The server finds the type of a parameter
from what stands beside it, and there it has nothing to find it by; psycopg
sends a type with each value, an integer's by its size, so the server computed
`1 << 63` in sixteen bits and answered 0. Beside a column the type is not
said: the value adapts to the column.

Not supported (yet):
- JSONPath functions (len, min, max, etc.)
- Array indices: `$.items[0]`, `$.items[1:5]`

## Testing

```bash
# Run native parser tests
python -m unittest ascetic_ddd.specification.domain.jsonpath.test_jsonpath_parser -v

# All tests
python -m unittest discover -s ascetic_ddd/specification -p "test_*.py" -v
```

## Full Usage Example

Run the interactive example with 11 demonstrations:

```bash
python -m ascetic_ddd.specification.domain.jsonpath.example_usage
```

The example demonstrates:
- All comparison operators (`==`, `!=`, `>`, `<`, `>=`, `<=`)
- Positional and named placeholders
- RFC 9535 logical operators (`&&`, `||`, `!`)
- Wildcard collections
- Lexer operation (tokenization)
- Specification reuse
- Boolean values

See the file `ascetic_ddd/specification/domain/jsonpath/examples/jsonpath_example.py` for the full code.

## Examples

### Basic Usage

```python
from ascetic_ddd.specification.domain.jsonpath.jsonpath_parser import parse

# Simple comparison
spec = parse("$[?@.age > %d]")
user = DictContext({"age": 30})
spec.match(user, (25,))  # True

# String comparison (RFC 9535: ==)
spec = parse("$[?@.status == %s]")
task = DictContext({"status": "done"})
spec.match(task, ("done",))  # True

# Named parameters
spec = parse("$[?@.score >= %(min_score)d]")
student = DictContext({"score": 85})
spec.match(student, {"min_score": 80})  # True

# Logical operators (RFC 9535)
spec = parse("$[?@.age > %d && @.active == %s]")
user = DictContext({"age": 30, "active": True})
spec.match(user, (25, True))  # True

# NOT operator (RFC 9535)
spec = parse("$[?!(@.deleted == %s)]")
item = DictContext({"deleted": False})
spec.match(item, (True,))  # True
```

### Working with Collections

```python
from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext

spec = parse("$.users[*][?(@.age >= %d)]")

user1 = DictContext({"name": "Alice", "age": 30})
user2 = DictContext({"name": "Bob", "age": 25})

users = CollectionContext([user1, user2])
root = DictContext({"users": users})

# Is there at least one user with age >= 28?
spec.match(root, (28,))  # True (Alice)
```

### Nested Wildcards

```python
from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext

# Nested wildcards: filtering by nested collections
spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

# Create structure: categories -> items
item1 = DictContext({"name": "Laptop", "price": 999.0})
item2 = DictContext({"name": "Mouse", "price": 29.0})
items1 = CollectionContext([item1, item2])
category1 = DictContext({"name": "Electronics", "items": items1})

item3 = DictContext({"name": "Shirt", "price": 49.0})
items2 = CollectionContext([item3])
category2 = DictContext({"name": "Clothing", "items": items2})

categories = CollectionContext([category1, category2])
store = DictContext({"categories": categories})

# Is there a category with an item costing more than 500?
spec.match(store, (500.0,))  # True (category1 has Laptop)
```

**Nested wildcards with logic:**

```python
# Nested wildcard with AND operator
spec = parse("$.categories[*][?@.items[*][?@.price > %f && @.price < %f]]")

# Is there a category with an item in the 500-1000 range?
spec.match(store, (500.0, 1000.0))  # True (Laptop: 999)

# Is there a category with an item in the 1000-2000 range?
spec.match(store, (1000.0, 2000.0))  # False
```

**Multiple matches:**

```python
# Check for multiple categories with expensive items
spec = parse("$.categories[*][?@.items[*][?@.price > %f]]")

# Category 1 with expensive item
item1 = DictContext({"name": "Laptop", "price": 999.0})
items1 = CollectionContext([item1])
category1 = DictContext({"name": "Electronics", "items": items1})

# Category 2 with expensive item
item2 = DictContext({"name": "Designer Jeans", "price": 299.0})
items2 = CollectionContext([item2])
category2 = DictContext({"name": "Clothing", "items": items2})

categories = CollectionContext([category1, category2])
store = DictContext({"categories": categories})

# Both categories have items costing more than 200
spec.match(store, (200.0,))  # True
```

### Nested Paths

```python
# Create a special context for nested structures
class NestedDictContext:
    def __init__(self, data):
        self._data = data

    def get(self, key):
        value = self._data[key]
        # Automatically wrap nested dicts
        if isinstance(value, dict):
            return NestedDictContext(value)
        return value

# Simple nested path: $.store.products[*][?@.price > 500]
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

**Deeply nested paths:**

```python
# Deep nesting: $.company.department.team.members[*][?@.age > 28]
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

**Nested paths in filters:**

```python
# Filter on nested field: $[?@.user.profile.age > 25]
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

**Combining nested paths and logic:**

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

## Internals

### Tokens

The lexer recognizes the following token types:

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
EQ          # == (RFC 9535: double sign)
NE/GT/LT/GTE/LTE  # Comparison operators
NUMBER      # 123, 45.67, 1e3, -2.5E-2
STRING      # "text", 'text', 'it\'s'
PLACEHOLDER # %d, %s, %(name)d
IDENTIFIER  # age, name, status
```

### AST Nodes

The parser creates the following Specification nodes:

- `GlobalScope()` - root context
- `Item()` - current collection element (@)
- `Field(parent, name)` - field access
- `Value(val)` - literal value
- `Equal/NotEqual/GreaterThan/...` - comparison operators
- `And(left, right)` - logical AND (&&)
- `Or(left, right)` - logical OR (||)
- `Not(operand)` - logical NOT (!)
- `Wildcard(parent, predicate)` - collection filtering
