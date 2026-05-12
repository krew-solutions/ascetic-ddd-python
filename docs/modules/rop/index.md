# Railway-Oriented Programming (ROP)

```{index} ROP, Railway, Result, Validation, Error Accumulation
```

A Python port of Scott Wlaschin's
[Railway-Oriented Programming](https://fsharpforfunandprofit.com/posts/recipe-part2/)
toolkit. Models a computation as a two-track railway: one track for
Success values, one for Failures. The Failure track carries a
**non-empty list of errors**, so independent failures from parallel
validations can accumulate without loss.

A sister port exists in Go at `ascetic-ddd-go/asceticddd/rop/`. The two
ports share vocabulary and overall structure; differences are noted
below.

## Description

Two common needs come up in DDD codebases:

1. **Validate many independent fields** and report **all** problems
   to the caller at once — not just the first one that failed.
2. **Chain steps where each depends on the previous one** (parse →
   look up → save) and short-circuit on the first failure, because
   continuing makes no sense.

ROP gives both in one library: an **applicative** phase that accumulates
errors, and a **monadic** phase that short-circuits. They compose freely.

```python
from ascetic_ddd.rop import Succeed, Fail, map3

def validate_symbol(s):
    return Fail("symbol required") if s == "" else Succeed(s)

def validate_side(s):
    return Fail("side must be BUY or SELL") if s not in ("BUY", "SELL") else Succeed(s)

def validate_quantity(q):
    return Fail("quantity must be > 0") if q <= 0 else Succeed(q)

# All three problems are reported together:
form = map3(
    validate_symbol(""),
    validate_side("X"),
    validate_quantity(-1),
    lambda sym, side, qty: {"symbol": sym, "side": side, "quantity": qty},
)
# form.errors() == ["symbol required", "side must be BUY or SELL", "quantity must be > 0"]
```

With a single-error Result the user would have to fix-and-resubmit
three times.

## When to Use

- Validating forms / DTOs / command payloads with multiple
  independent rules.
- Composing pipelines that mix validation (accumulate errors) with
  effectful steps (short-circuit on first failure).
- Any place you currently use `try/except` to control branching rather
  than to react to an exceptional situation.

## Mapping to Wlaschin's canon

The 13 entries of Wlaschin's "Here they all are together" table:

| Wlaschin / OCaml         | Python                              | Notes                                                                |
|--------------------------|-------------------------------------|----------------------------------------------------------------------|
| `succeed` / `return`     | `Succeed(v)`                        | Success constructor                                                  |
| `fail`                   | `Fail(err)`                         | Single-error failure constructor                                     |
| —                        | `FailMany(errs)`                    | Multi-error constructor; raises `ValueError` on empty                |
| `either`                 | `r.either(success_fn, failure_fn)`  | Core eliminator                                                      |
| `map`                    | `r.map(f)`                          | Functor map on the Success track                                     |
| `bind` / `>>=`           | `r.and_then(f)` / `r.bind(f)`       | Monadic bind; short-circuits on first Failure                        |
| `apply` / `<*>`          | `apply(rf, rx)`                     | Applicative; concatenates error lists on dual Failure                |
| `both`                   | `r.both(rb)`                        | Pairs two Results, accumulates errors                                |
| `let+` / `and+`          | `map2`, `map3`, `map4`              | No syntactic sugar; fixed-arity helpers cover the common cases       |
| `switch`                 | `switch(f)`                         | Lifts a plain function into a switch                                 |
| `tee` / `tap`            | `tee(f, x)`                         | Side-effect pass-through                                             |
| `tryCatch`               | `try_catch(f, exc_handler)`         | **Catches `Exception`** — Python's failure model is exceptions       |
| `doubleMap` / `bimap`    | `r.double_map(success_fn, fail_fn)` | Maps Success and each Failure element                                |
| `plus` / `++` / `<+>`    | `plus(add_s, add_f, s1, s2)`        | Combine two switches; pluggable success/failure mergers              |
| `&&&`                    | `and_(v1, v2)`                      | Validation flavour of `plus`; renamed because `and` is a keyword     |
| `>=>`                    | `compose(f, g)`                     | Kleisli composition of two switch functions                          |
| `>>`                     | `pipe(f, g)`                        | Plain function composition (`g ∘ f`)                                 |
| `>>=` / `<*>` / `<!>`    | — (no operator overloading)         | Use the named functions / methods above                              |
| `of_result`              | `from_exception(v, exc)`            | Bridge to Python's `(value, exception)` idiom                        |
| —                        | `of_option(o, err)`                 | Bridge from `ascetic_ddd.option`                                     |

Total: 13/13 of the canonical table, plus four extensions (`FailMany`,
`apply`, `map2`/`map3`/`map4`, `from_exception`, `of_option`).

## Why a list of errors

The original Wlaschin / OCaml Rop uses a *single* error per Failure
because F#/Haskell-style applicative chains can pair Failures with a
semigroup instance for the error type. Python doesn't have that
infrastructure, so we follow the OCaml port's choice: the Failure
branch always holds a **non-empty list** of errors. The applicative
combinators (`apply`, `map2`, `map3`, `map4`, `plus`, `and_`) then
concatenate those lists when both arguments fail.

## Mixing accumulation with short-circuit

Applicative phase accumulates; monadic phase short-circuits. Mix freely
in one pipeline:

```python
from ascetic_ddd.rop import Succeed, Fail, map3

order = map3(
    validate_symbol(payload["symbol"]),
    validate_side(payload["side"]),
    validate_quantity(payload["qty"]),
    build_order,
)

# If any field failed, persist_to_db is never called; the original
# field errors propagate to the caller.
saved = order.and_then(persist_to_db)
```

## API

### Class `Result[T, E]`

Methods (instance-level, fluent):

`is_ok()`, `is_error()`, `errors()`, `unwrap()`, `unwrap_or(default)`,
`unwrap_or_else(f)`, `either(success_fn, failure_fn)`, `map(f)`,
`and_then(f)`, `bind(f)` *(alias of `and_then`)*, `both(rb)`,
`double_map(success_fn, failure_fn)`, `or_else(f)`, `__or__(alt)`
(`|` operator), `__eq__`, `__hash__`, `__repr__`, `__str__`.

### Top-level functions

**Constructors**: `Succeed(v)`, `Fail(err)`, `FailMany(errs)`.

**Bridges**:
- `from_exception(v, exc)` — `(value, Exception | None)` → Result.
- `of_option(o, err)` — bridges `ascetic_ddd.option.Option`.

**Adapters**: `switch(f)`, `tee(f, x)`, `try_catch(f, exc_handler)`.

**Multi-Result accumulators**: `apply(rf, rx)`, `map2(ra, rb, f)`,
`map3(ra, rb, rc, f)`, `map4(ra, rb, rc, rd, f)`.

**Switch combinators**: `plus(add_success, add_failure, s1, s2)`,
`and_(v1, v2)`.

**Composition**: `compose(f, g)` (Kleisli), `pipe(f, g)` (plain function
composition).

## Differences from the Go port

| Aspect                  | Python                                | Go                                                          |
|-------------------------|---------------------------------------|-------------------------------------------------------------|
| `map`/`and_then`/`both` | Methods on `Result`                   | Top-level functions (Go disallows new type-params on methods) |
| `try_catch`             | Catches `Exception`                   | Lifts `func(A) (B, error)`                                  |
| Bridge to runtime error | `from_exception(v, exc)`              | `FromError(v, err)`                                         |
| `|` operator            | Overloaded for `or_else`-flavoured or | Not available (no operator overloading in Go)               |

The reason `try_catch` differs: in Python the failure mechanism *is*
exceptions, matching OCaml. In Go it is the `error` return value, so
the Go port adapted accordingly. Both ports stay idiomatic to their
host language.

## References

- Scott Wlaschin, [Railway-Oriented Programming, Part 2 — The Recipe](https://fsharpforfunandprofit.com/posts/recipe-part2/)
- [Sibling Go port](https://github.com/krew-solutions/ascetic-ddd-go/tree/main/asceticddd/rop)
- [OCaml reference implementation](https://github.com/krew-solutions/trading-ml/tree/main/shared/lib/rop)
