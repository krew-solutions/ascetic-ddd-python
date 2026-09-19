"""The two readers of a specification against each other.

Whatever ``EvaluateVisitor`` says of a specification, PostgreSQL must say of
the query ``PostgresqlVisitor`` compiles it to: the same value of a constant
expression, the same kind of failure, the same rows selected. This is what
the claims of ``evaluate_visitor``, ``arithmetic``, ``comparison`` and
``postgresql_visitor`` rest on - the logic of nulls, integer division, shifts,
NaN, ``IS``, and parentheses that keep the shape of the tree - and no test of
one reader alone can hold them. The Rust port has the same test,
``crates/specification/tests/pg.rs``.
"""
import re
import typing
import unittest
from unittest import IsolatedAsyncioTestCase

from psycopg import errors

from ascetic_ddd.specification.domain.evaluate_visitor import (
    CollectionContext,
    EvaluateVisitor,
)
from ascetic_ddd.specification.domain.nodes import (
    Add, And, Div, Equal, Field, GlobalScope, GreaterThan, GreaterThanEqual,
    Is, IsNotNull, IsNull, Item, LeftShift, LessThan, LessThanEqual, Mod, Mul,
    Neg, Not, NotEqual, Object, Or, RightShift, Sub, Value, Visitable, Wildcard,
)
from ascetic_ddd.specification.infrastructure.postgresql_visitor import compile_to_sql
from ascetic_ddd.specification.infrastructure.schema import SchemaRegistry
from ascetic_ddd.utils.tests.db import make_pg_session_pool

BIGINT_MAX = 2 ** 63 - 1
BIGINT_MIN = -2 ** 63
NAN = float("nan")

# The failures of the evaluator and what PostgreSQL calls the same.
FAILURES: dict[type[Exception], type[Exception]] = {
    ZeroDivisionError: errors.DivisionByZero,
    OverflowError: errors.NumericValueOutOfRange,
    TypeError: errors.UndefinedFunction,
}


class DictContext:
    """Dictionary-based context for testing."""

    def __init__(self, data: dict[str, typing.Any]):
        self._data = data

    def get(self, key: str) -> typing.Any:
        """Get value by key."""
        return self._data[key]


def field(name: str) -> Field:
    return Field(GlobalScope(), name)


def item(name: str) -> Field:
    return Field(Item(), name)


def null() -> Value:
    return Value(None)


def type_of(value: typing.Any, null_type: str) -> str:
    """Return the type the server is told a parameter has.

    A constant expression gives it nothing to infer one from. They are the
    types the evaluator takes Python's to be; a null takes the type of the case.
    """
    if value is None:
        return null_type
    return {bool: "bool", int: "bigint", float: "float8", str: "text"}[type(value)]


def to_psycopg(sql: str, types: list[str] | None = None) -> str:
    """Return the query with psycopg's placeholders for PostgreSQL's.

    ``$1``, ``$2`` stand in the order of their numbers, so ``%s`` for each
    keeps the parameters in place; ``%`` of the modulo is escaped.
    """
    casts = iter(types or [])
    return re.sub(
        r"\$\d+",
        lambda match: "%s" + ("::" + next(casts) if types else ""),
        sql.replace("%", "%%"),
    )


class Constant(typing.NamedTuple):
    node: Visitable
    # The type of the nulls of the case
    null_type: str = "bigint"
    # The types of the parameters, where they are not what the values say:
    # PostgreSQL shifts a bigint by an integer.
    types: list[str] | None = None


def shift(node: Visitable) -> Constant:
    return Constant(node, types=["bigint", "integer"])


def boolean(node: Visitable) -> Constant:
    return Constant(node, null_type="bool")


def constants() -> list[Constant]:
    t, f = (lambda: Value(True)), (lambda: Value(False))
    return [
        # Arithmetic, and the parentheses that keep its shape
        Constant(Sub(Value(10), Sub(Value(4), Value(3)))),
        Constant(Sub(Sub(Value(10), Value(4)), Value(3))),
        Constant(Sub(Value(10), Add(Value(4), Value(3)))),
        Constant(Div(Value(100), Div(Value(10), Value(5)))),
        Constant(Div(Mul(Value(7), Value(3)), Value(2))),
        Constant(Mul(Add(Value(1), Value(2)), Value(3))),
        Constant(Add(Value(1), Mul(Value(2), Value(3)))),
        Constant(Div(Value(7), Value(2))),
        Constant(Div(Value(-7), Value(2))),
        Constant(Mod(Value(-7), Value(2))),
        Constant(Mod(Value(7), Value(-2))),
        Constant(Mod(Value(BIGINT_MIN), Value(-1))),
        Constant(Neg(Neg(Value(5)))),
        Constant(Sub(Value(5), Neg(Value(3)))),
        Constant(Neg(Add(Value(1), Value(2)))),
        Constant(Div(Value(7.0), Value(2))),
        Constant(Add(Value(1), Value(0.5))),
        Constant(Mul(Value(2.5), Value(4))),
        shift(LeftShift(Value(1), Value(3))),
        shift(LeftShift(Value(1), Value(64))),
        shift(LeftShift(Value(1), Value(-1))),
        shift(LeftShift(Value(1), Value(63))),
        shift(RightShift(Value(8), Value(65))),
        shift(RightShift(Value(-8), Value(1))),
        # Where it fails
        Constant(Div(Value(1), Value(0))),
        Constant(Mod(Value(1), Value(0))),
        Constant(Div(Value(1.0), Value(0.0))),
        Constant(Add(Value(BIGINT_MAX), Value(1))),
        Constant(Sub(Value(BIGINT_MIN), Value(1))),
        Constant(Mul(Value(BIGINT_MAX), Value(2))),
        Constant(Div(Value(BIGINT_MIN), Value(-1))),
        Constant(Neg(Value(BIGINT_MIN))),
        Constant(Mul(Value(1.7976931348623157e308), Value(2.0))),
        # What is not defined here is not defined there
        Constant(Add(Value("a"), Value("b"))),
        Constant(Mod(Value(5.5), Value(2))),
        Constant(Add(Value(True), Value(1))),
        Constant(Neg(Value("a"))),
        Constant(Equal(Value("a"), Value(1))),
        Constant(LessThan(Value(True), Value(2))),
        Constant(Is(Value("a"), Value(1))),
        # Comparisons
        Constant(Equal(Value(1), Value(1.0))),
        Constant(LessThan(Value(1), Value(1.5))),
        Constant(GreaterThanEqual(Value(2), Value(2))),
        Constant(LessThanEqual(Value(3), Value(2))),
        Constant(NotEqual(Value("a"), Value("b"))),
        Constant(LessThan(Value("a"), Value("b"))),
        Constant(GreaterThan(Value(True), Value(False))),
        Constant(Equal(Value(NAN), Value(NAN))),
        Constant(GreaterThan(Value(NAN), Value(1.7976931348623157e308))),
        Constant(LessThanEqual(Value(1.0), Value(NAN))),
        Constant(Equal(Value(-0.0), Value(0.0))),
        Constant(Equal(Equal(Value(1), Value(1)), Value(True))),
        Constant(Equal(Value(True), Equal(Value(1), Value(2)))),
        # Nulls
        Constant(Equal(null(), Value(1))),
        Constant(Equal(null(), null())),
        Constant(NotEqual(Value(1), null())),
        Constant(Add(Value(1), null())),
        Constant(Neg(null())),
        Constant(Div(null(), Value(0))),
        boolean(Not(null())),
        boolean(And(null(), f())),
        boolean(And(f(), null())),
        boolean(And(null(), t())),
        boolean(And(null(), null())),
        boolean(Or(null(), t())),
        boolean(Or(t(), null())),
        boolean(Or(null(), f())),
        boolean(And(Or(t(), f()), f())),
        boolean(Or(t(), And(f(), f()))),
        boolean(And(t(), And(t(), f()))),
        boolean(Not(And(t(), f()))),
        boolean(Not(Not(t()))),
        boolean(IsNull(Or(null(), f()))),
        boolean(IsNull(IsNull(null()))),
        boolean(Equal(IsNull(null()), t())),
        Constant(IsNull(Equal(Value(1), null()))),
        Constant(IsNotNull(Equal(Value(1), null()))),
        boolean(Not(IsNull(null()))),
        # IS
        boolean(Is(t(), t())),
        boolean(Is(t(), f())),
        boolean(Is(null(), null())),
        boolean(Is(null(), t())),
        Constant(Is(Value(1), null())),
        Constant(Is(Value(1), Value(1))),
        boolean(Equal(Is(t(), null()), f())),
        boolean(Is(Equal(Value(1), Value(1)), t())),
    ]


class Store(typing.NamedTuple):
    id: int
    a: int | None
    b: int | None
    flag: bool | None
    name: str | None
    items: list[tuple[int | None, bool | None]]

    def context(self) -> DictContext:
        return DictContext({
            "id": self.id, "a": self.a, "b": self.b, "flag": self.flag, "name": self.name,
            # Members named as PostgreSQL names other things, under columns
            # of those very names: `user` is the session's user if it is not
            # quoted, `order` does not parse, `createdAt` folds to `createdat`.
            "user": self.name, "order": self.a, "createdAt": self.b,
            "items": CollectionContext([
                DictContext({"price": price, "active": active}) for price, active in self.items
            ]),
        })


STORES = [
    Store(1, 1, 1, True, "one", [(900, True), (10, False)]),
    Store(2, 1, 2, False, "two", [(10, True)]),
    Store(3, None, 2, None, None, [(None, True), (10, None)]),
    Store(4, None, None, True, "four", []),
    Store(5, 7, None, False, "five", [(None, None)]),
    Store(6, -3, 0, None, "", [(900, None), (901, True)]),
]


def specifications() -> list[Visitable]:
    def dear() -> Visitable:
        return GreaterThan(item("price"), Value(500))

    def some(predicate: Visitable) -> Visitable:
        return Wildcard(Object(GlobalScope(), "items"), predicate)

    def every(predicate: Visitable) -> Visitable:
        return Not(some(Not(predicate)))

    return [
        Equal(field("a"), field("b")),
        Not(Equal(field("a"), field("b"))),
        NotEqual(field("a"), field("b")),
        Is(field("a"), field("b")),
        Not(Is(field("a"), field("b"))),
        IsNull(field("a")),
        And(IsNotNull(field("a")), IsNull(field("b"))),
        Or(Equal(field("a"), field("b")), field("flag")),
        And(Not(field("flag")), GreaterThan(field("b"), Value(1))),
        Not(Or(field("flag"), IsNull(field("name")))),
        GreaterThan(Sub(field("a"), Sub(field("b"), Value(1))), Value(0)),
        LessThan(Mul(Add(field("a"), Value(1)), Value(2)), Value(5)),
        Equal(IsNull(field("a")), field("flag")),
        Equal(field("name"), Value("")),
        LessThan(field("name"), Value("one")),
        Is(field("flag"), null()),
        some(dear()),
        Not(some(dear())),
        some(Or(dear(), item("active"))),
        some(And(dear(), item("active"))),
        some(Not(item("active"))),
        some(IsNull(item("price"))),
        some(GreaterThan(item("price"), field("a"))),
        every(item("active")),
        every(GreaterThan(item("price"), Value(5))),
        Not(every(IsNotNull(item("price")))),
        And(field("flag"), some(dear())),
        # A name is the column's, whatever else PostgreSQL knows by it.
        Equal(field("user"), Value("one")),
        GreaterThan(field("order"), Value(0)),
        Equal(field("createdAt"), Value(2)),
    ]


class PostgresqlAgreementIntegrationTestCase(IsolatedAsyncioTestCase):
    """The evaluator and PostgreSQL, on the same specifications."""

    async def asyncSetUp(self):
        self._session_pool = await make_pg_session_pool()

    async def asyncTearDown(self):
        await self._session_pool._pool.close()

    async def test_a_constant_expression_has_one_value_for_both_readers(self):
        nothing = DictContext({})
        async with self._session_pool.session() as session:
            for constant in constants():
                sql, params = compile_to_sql(constant.node)
                types = constant.types or [type_of(param, constant.null_type) for param in params]
                evaluated, failure = self._evaluate(constant.node, nothing)
                with self.subTest(sql=sql, params=params):
                    try:
                        # A transaction of its own: a failure is one of the
                        # answers, and must not take the connection with it.
                        async with session.connection.transaction():
                            cursor = await session.connection.execute(
                                "SELECT (%s)" % to_psycopg(sql, types), params,
                            )
                            answered = (await cursor.fetchone())[0]
                    except tuple(FAILURES.values()) as error:
                        # The same failure, not just a failure.
                        self.assertIsNotNone(failure, "PostgreSQL: %s" % error)
                        self.assertIsInstance(error, FAILURES[failure])
                        continue
                    self.assertIsNone(failure, "PostgreSQL has %r" % (answered,))
                    self._assert_same(evaluated, answered)

    def _evaluate(
        self, node: Visitable, context: DictContext
    ) -> tuple[typing.Any, type[Exception] | None]:
        try:
            return node.accept(EvaluateVisitor(context)), None
        except tuple(FAILURES) as error:
            return None, type(error)

    def _assert_same(self, evaluated: typing.Any, answered: typing.Any) -> None:
        self.assertIs(type(evaluated), type(answered))
        if isinstance(evaluated, float) and evaluated != evaluated:
            self.assertNotEqual(answered, answered)  # NaN is what both have
        else:
            self.assertEqual(evaluated, answered)

    async def test_a_specification_selects_the_rows_it_is_satisfied_by(self):
        relational = SchemaRegistry("spec_stores").register_relational(
            "items", "spec_items", "store_id", "id",
        )
        async with self._session_pool.session() as session:
            # Rolled back whatever happens: the tables are of this test alone.
            async with session.connection.transaction(force_rollback=True):
                await self._make_tables(session.connection)
                for specification in specifications():
                    satisfied = [
                        store.id for store in STORES
                        if specification.accept(EvaluateVisitor(store.context())) is True
                    ]
                    for storage, schema in (("embedded", None), ("relational", relational)):
                        sql, params = compile_to_sql(specification, schema)
                        with self.subTest(storage=storage, sql=sql):
                            cursor = await session.connection.execute(
                                "SELECT id FROM spec_stores WHERE %s ORDER BY id"
                                % to_psycopg(sql),
                                params,
                            )
                            selected = [row[0] for row in await cursor.fetchall()]
                            self.assertEqual(selected, satisfied)

    async def _make_tables(self, connection: typing.Any) -> None:
        await connection.execute(
            "CREATE TYPE pg_temp.spec_item AS (price int8, active bool)"
        )
        await connection.execute(
            "CREATE TEMP TABLE spec_stores ("
            " id int8 PRIMARY KEY, a int8, b int8, flag bool, name text,"
            " items pg_temp.spec_item[] NOT NULL,"
            ' "user" text, "order" int8, "createdAt" int8)'
        )
        await connection.execute(
            "CREATE TEMP TABLE spec_items (store_id int8 NOT NULL, price int8, active bool)"
        )
        for store in STORES:
            await connection.execute(
                "INSERT INTO spec_stores VALUES (%s, %s, %s, %s, %s, '{}', %s, %s, %s)",
                (
                    store.id, store.a, store.b, store.flag, store.name,
                    store.name, store.a, store.b,
                ),
            )
            for price, active in store.items:
                await connection.execute(
                    "UPDATE spec_stores SET items = items"
                    " || ROW(%s::int8, %s::bool)::pg_temp.spec_item WHERE id = %s",
                    (price, active, store.id),
                )
                await connection.execute(
                    "INSERT INTO spec_items VALUES (%s, %s, %s)", (store.id, price, active),
                )


if __name__ == "__main__":
    unittest.main()
