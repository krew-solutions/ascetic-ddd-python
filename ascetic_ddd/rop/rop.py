import typing

from ascetic_ddd.option import Option

T = typing.TypeVar("T")
U = typing.TypeVar("U")
V = typing.TypeVar("V")
E = typing.TypeVar("E")
E2 = typing.TypeVar("E2")
X = typing.TypeVar("X")

__all__ = (
    "Result",
    "Succeed",
    "Fail",
    "FailMany",
    "from_exception",
    "of_option",
    "apply",
    "map2",
    "map3",
    "map4",
    "switch",
    "tee",
    "try_catch",
    "plus",
    "and_",
    "compose",
    "pipe",
)


class Result(typing.Generic[T, E]):
    """Two-track outcome: either a Success value of type T, or a Failure
    carrying a non-empty list of errors of type E.

    The error branch is a list (not a single error) so independent failures
    from parallel validations can be accumulated via apply / map2 / plus /
    and_ without loss.
    """

    __slots__ = ("_val", "_errs", "_ok")

    def __init__(self, val: T, errs: list[E], ok: bool):
        self._val = val
        self._errs = errs
        self._ok = ok

    def is_ok(self) -> bool:
        return self._ok

    def is_error(self) -> bool:
        return not self._ok

    def errors(self) -> list[E]:
        """Returns the failure list (empty if the Result is on the Success track)."""
        return self._errs

    def unwrap(self) -> T:
        """Returns the contained value.

        Raises:
            ValueError: If the Result is on the Failure track.
        """
        if not self._ok:
            raise ValueError("called unwrap on a Failure Result")
        return self._val

    def unwrap_or(self, default: T) -> T:
        """Returns the contained value or the provided default."""
        if self._ok:
            return self._val
        return default

    def unwrap_or_else(self, f: typing.Callable[[list[E]], T]) -> T:
        """Returns the contained value or computes one from the error list."""
        if self._ok:
            return self._val
        return f(self._errs)

    def either(
        self,
        success_fn: typing.Callable[[T], U],
        failure_fn: typing.Callable[[list[E]], U],
    ) -> U:
        """Applies success_fn on the Success track or failure_fn on the Failure track.

        Wlaschin's `either`.
        """
        if self._ok:
            return success_fn(self._val)
        return failure_fn(self._errs)

    def map(self, f: typing.Callable[[T], U]) -> "Result[U, E]":
        """Applies a pure function on the Success track. Wlaschin's `map`."""
        if self._ok:
            return Succeed(f(self._val))
        return Result(typing.cast(U, None), self._errs, False)

    def and_then(
        self, f: typing.Callable[[T], "Result[U, E]"]
    ) -> "Result[U, E]":
        """Monadic bind: short-circuits on the first Failure. Wlaschin's `bind`."""
        if self._ok:
            return f(self._val)
        return Result(typing.cast(U, None), self._errs, False)

    def bind(
        self, f: typing.Callable[[T], "Result[U, E]"]
    ) -> "Result[U, E]":
        """Alias for `and_then` using Wlaschin's canonical name."""
        return self.and_then(f)

    def both(self, rb: "Result[U, E]") -> "Result[tuple[T, U], E]":
        """Pairs two Results, accumulating errors if both fail."""
        if self._ok and rb._ok:
            return Succeed((self._val, rb._val))
        errs: list[E] = []
        if not self._ok:
            errs.extend(self._errs)
        if not rb._ok:
            errs.extend(rb._errs)
        return Result(typing.cast(tuple[T, U], None), errs, False)

    def double_map(
        self,
        success_fn: typing.Callable[[T], U],
        failure_fn: typing.Callable[[E], E2],
    ) -> "Result[U, E2]":
        """Bifunctor map: success_fn on the Success track and failure_fn
        element-wise across the failure list. Wlaschin's `doubleMap`/`bimap`.
        """
        if self._ok:
            return Succeed(success_fn(self._val))
        return Result(
            typing.cast(U, None),
            [failure_fn(e) for e in self._errs],
            False,
        )

    def __or__(self, alt: "Result[T, E]") -> "Result[T, E]":
        """Returns self if Ok, otherwise alt."""
        if self._ok:
            return self
        return alt

    def or_else(
        self, f: typing.Callable[[list[E]], "Result[T, E]"]
    ) -> "Result[T, E]":
        """Returns self if Ok, otherwise calls f with the error list."""
        if self._ok:
            return self
        return f(self._errs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Result):
            return NotImplemented
        if self._ok != other._ok:
            return False
        if self._ok:
            return bool(self._val == other._val)
        return self._errs == other._errs

    def __hash__(self) -> int:
        if self._ok:
            return hash((True, self._val))
        return hash((False, tuple(self._errs)))

    def __repr__(self) -> str:
        if self._ok:
            return "Ok(%r)" % (self._val,)
        return "Error(%r)" % (self._errs,)

    def __str__(self) -> str:
        if self._ok:
            return "Ok(%s)" % (self._val,)
        return "Error(%s)" % (self._errs,)


def Succeed(val: T) -> Result[T, typing.Any]:
    """Creates a Success Result. Wlaschin's `succeed`/`return`/`pure`."""
    return Result(val, [], True)


def Fail(err: E) -> Result[typing.Any, E]:
    """Creates a Failure Result with a single error. Wlaschin's `fail`."""
    return Result(typing.cast(typing.Any, None), [err], False)


def FailMany(errs: list[E]) -> Result[typing.Any, E]:
    """Creates a Failure Result from a non-empty error list.

    Raises:
        ValueError: If errs is empty.
    """
    if len(errs) == 0:
        raise ValueError("FailMany: errs must be non-empty")
    return Result(typing.cast(typing.Any, None), errs, False)


def from_exception(
    val: T, exc: BaseException | None
) -> Result[T, BaseException]:
    """Lifts a Python `(value, exception)` pair into a Result.

    If exc is not None the value is discarded and the exception becomes
    a Failure; otherwise the value becomes a Success.
    """
    if exc is not None:
        return Result(typing.cast(T, None), [exc], False)
    return Succeed(val)


def of_option(o: Option[T], err: E) -> Result[T, E]:
    """Lifts an Option into a Result: Some(v) → Succeed(v), Nothing → Fail(err)."""
    if o.is_some():
        return Succeed(o.unwrap())
    return Result(typing.cast(T, None), [err], False)


def apply(
    rf: Result[typing.Callable[[U], V], E], rx: Result[U, E]
) -> Result[V, E]:
    """Applies a wrapped function to a wrapped value. On dual Failure the
    error lists are concatenated — the core of accumulation.
    """
    if rf._ok and rx._ok:
        return Succeed(rf._val(rx._val))
    errs: list[E] = []
    if not rf._ok:
        errs.extend(rf._errs)
    if not rx._ok:
        errs.extend(rx._errs)
    return Result(typing.cast(V, None), errs, False)


def map2(
    ra: Result[T, E],
    rb: Result[U, E],
    f: typing.Callable[[T, U], V],
) -> Result[V, E]:
    """Applies f to two Results, accumulating errors if either branch fails."""
    if ra._ok and rb._ok:
        return Succeed(f(ra._val, rb._val))
    errs: list[E] = []
    if not ra._ok:
        errs.extend(ra._errs)
    if not rb._ok:
        errs.extend(rb._errs)
    return Result(typing.cast(V, None), errs, False)


W = typing.TypeVar("W")
Y = typing.TypeVar("Y")
Z = typing.TypeVar("Z")


def map3(
    ra: Result[T, E],
    rb: Result[U, E],
    rc: Result[V, E],
    f: typing.Callable[[T, U, V], W],
) -> Result[W, E]:
    """Applies f to three Results, accumulating errors from every failing branch."""
    if ra._ok and rb._ok and rc._ok:
        return Succeed(f(ra._val, rb._val, rc._val))
    errs: list[E] = []
    if not ra._ok:
        errs.extend(ra._errs)
    if not rb._ok:
        errs.extend(rb._errs)
    if not rc._ok:
        errs.extend(rc._errs)
    return Result(typing.cast(W, None), errs, False)


def map4(
    ra: Result[T, E],
    rb: Result[U, E],
    rc: Result[V, E],
    rd: Result[W, E],
    f: typing.Callable[[T, U, V, W], Y],
) -> Result[Y, E]:
    """Applies f to four Results, accumulating errors from every failing branch."""
    if ra._ok and rb._ok and rc._ok and rd._ok:
        return Succeed(f(ra._val, rb._val, rc._val, rd._val))
    errs: list[E] = []
    if not ra._ok:
        errs.extend(ra._errs)
    if not rb._ok:
        errs.extend(rb._errs)
    if not rc._ok:
        errs.extend(rc._errs)
    if not rd._ok:
        errs.extend(rd._errs)
    return Result(typing.cast(Y, None), errs, False)


def switch(
    f: typing.Callable[[T], U],
) -> typing.Callable[[T], Result[U, typing.Any]]:
    """Lifts a one-track function into a switch (always-success).

    Wlaschin's `switch`/`lift`.
    """

    def wrapped(x: T) -> Result[U, typing.Any]:
        return Succeed(f(x))

    return wrapped


def tee(f: typing.Callable[[T], typing.Any], x: T) -> T:
    """Runs f for its side effect and returns x unchanged.

    Wlaschin's `tee` / Unix `tee` / `tap`.
    """
    f(x)
    return x


def try_catch(
    f: typing.Callable[[T], U],
    exc_handler: typing.Callable[[BaseException], E],
) -> typing.Callable[[T], Result[U, E]]:
    """Lifts an exception-raising function into a switch. Any caught exception
    is routed through exc_handler onto the Failure track. Wlaschin's `tryCatch`.

    Python's failure mechanism IS exceptions, so this is the literal port from
    OCaml (unlike the Go port, which lifts `(T, error)` instead).
    """

    def wrapped(x: T) -> Result[U, E]:
        try:
            return Succeed(f(x))
        except Exception as e:
            return Fail(exc_handler(e))

    return wrapped


def plus(
    add_success: typing.Callable[[T, T], T],
    add_failure: typing.Callable[[list[E], list[E]], list[E]],
    s1: typing.Callable[[X], Result[T, E]],
    s2: typing.Callable[[X], Result[T, E]],
) -> typing.Callable[[X], Result[T, E]]:
    """Combines two switch functions over the same input. Successes are merged
    via add_success; failure lists via add_failure. Wlaschin's `plus`
    (a.k.a. `++`, `<+>`).
    """

    def wrapped(x: X) -> Result[T, E]:
        r1 = s1(x)
        r2 = s2(x)
        if r1._ok and r2._ok:
            return Succeed(add_success(r1._val, r2._val))
        if not r1._ok and not r2._ok:
            return Result(
                typing.cast(T, None), add_failure(r1._errs, r2._errs), False
            )
        if not r1._ok:
            return Result(typing.cast(T, None), r1._errs, False)
        return Result(typing.cast(T, None), r2._errs, False)

    return wrapped


def and_(
    v1: typing.Callable[[X], Result[T, E]],
    v2: typing.Callable[[X], Result[T, E]],
) -> typing.Callable[[X], Result[T, E]]:
    """Validation-flavoured plus: returns the first success value, concatenates
    failure lists. Wlaschin's `&&&`.

    Renamed `and_` because `and` is a reserved keyword in Python (cf.
    `operator.and_`).
    """
    return plus(
        lambda a, _b: a,
        lambda e1, e2: [*e1, *e2],
        v1,
        v2,
    )


def compose(
    f: typing.Callable[[T], Result[U, E]],
    g: typing.Callable[[U], Result[V, E]],
) -> typing.Callable[[T], Result[V, E]]:
    """Chains two switch functions into a new switch function (Kleisli
    composition). Wlaschin's `>=>`.
    """

    def wrapped(x: T) -> Result[V, E]:
        return f(x).and_then(g)

    return wrapped


def pipe(
    f: typing.Callable[[T], U],
    g: typing.Callable[[U], V],
) -> typing.Callable[[T], V]:
    """Composes two plain functions: pipe(f, g)(x) == g(f(x)). Wlaschin's `>>`."""

    def wrapped(x: T) -> V:
        return g(f(x))

    return wrapped
