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
import datetime
import functools
import re
import sys
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
from ascetic_ddd.specification.infrastructure.postgresql_visitor import (
    compile_specification, compile_to_sql,
)
from ascetic_ddd.specification.infrastructure.transform_visitor import ITransformContext
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


def to_psycopg(sql: str) -> str:
    """Return the query with psycopg's placeholders for PostgreSQL's.

    ``$1``, ``$2`` stand in the order of their numbers, so ``%s`` for each
    keeps the parameters in place; ``%`` of the modulo is escaped. Nothing is
    said of their types: this test used to write one after every parameter
    itself, and so did not see what the server made of a constant with
    nothing but constants beside it.
    """
    return re.sub(r"\$\d+", "%s", sql.replace("%", "%%"))


def constants() -> list[Visitable]:
    t, f = (lambda: Value(True)), (lambda: Value(False))
    written = [
        # Arithmetic, and the parentheses that keep its shape
        Sub(Value(10), Sub(Value(4), Value(3))),
        Sub(Sub(Value(10), Value(4)), Value(3)),
        Sub(Value(10), Add(Value(4), Value(3))),
        Div(Value(100), Div(Value(10), Value(5))),
        Div(Mul(Value(7), Value(3)), Value(2)),
        Mul(Add(Value(1), Value(2)), Value(3)),
        Add(Value(1), Mul(Value(2), Value(3))),
        Div(Value(7), Value(2)),
        Div(Value(-7), Value(2)),
        Mod(Value(-7), Value(2)),
        Mod(Value(7), Value(-2)),
        Mod(Value(BIGINT_MIN), Value(-1)),
        Neg(Neg(Value(5))),
        Sub(Value(5), Neg(Value(3))),
        Neg(Add(Value(1), Value(2))),
        Div(Value(7.0), Value(2)),
        Add(Value(1), Value(0.5)),
        Mul(Value(2.5), Value(4)),
        LeftShift(Value(1), Value(3)),
        LeftShift(Value(1), Value(64)),
        LeftShift(Value(1), Value(-1)),
        LeftShift(Value(1), Value(63)),
        RightShift(Value(8), Value(65)),
        RightShift(Value(-8), Value(1)),
        # Where it fails
        Div(Value(1), Value(0)),
        Mod(Value(1), Value(0)),
        Div(Value(1.0), Value(0.0)),
        Add(Value(BIGINT_MAX), Value(1)),
        Sub(Value(BIGINT_MIN), Value(1)),
        Mul(Value(BIGINT_MAX), Value(2)),
        Div(Value(BIGINT_MIN), Value(-1)),
        Neg(Value(BIGINT_MIN)),
        Mul(Value(1.7976931348623157e308), Value(2.0)),
        # What is not defined here is not defined there
        Add(Value("a"), Value("b")),
        Mod(Value(5.5), Value(2)),
        Add(Value(True), Value(1)),
        Neg(Value("a")),
        Equal(Value("a"), Value(1)),
        LessThan(Value(True), Value(2)),
        Is(Value("a"), Value(1)),
        # Comparisons
        Equal(Value(1), Value(1.0)),
        LessThan(Value(1), Value(1.5)),
        GreaterThanEqual(Value(2), Value(2)),
        LessThanEqual(Value(3), Value(2)),
        NotEqual(Value("a"), Value("b")),
        LessThan(Value("a"), Value("b")),
        GreaterThan(Value(True), Value(False)),
        Equal(Value(NAN), Value(NAN)),
        GreaterThan(Value(NAN), Value(1.7976931348623157e308)),
        LessThanEqual(Value(1.0), Value(NAN)),
        Equal(Value(-0.0), Value(0.0)),
        Equal(Equal(Value(1), Value(1)), Value(True)),
        Equal(Value(True), Equal(Value(1), Value(2))),
        # Nulls
        Equal(null(), Value(1)),
        Equal(null(), null()),
        NotEqual(Value(1), null()),
        Add(Value(1), null()),
        Neg(null()),
        Div(null(), Value(0)),
        Not(null()),
        And(null(), f()),
        And(f(), null()),
        And(null(), t()),
        And(null(), null()),
        Or(null(), t()),
        Or(t(), null()),
        Or(null(), f()),
        And(Or(t(), f()), f()),
        Or(t(), And(f(), f())),
        And(t(), And(t(), f())),
        Not(And(t(), f())),
        Not(Not(t())),
        IsNull(Or(null(), f())),
        IsNull(IsNull(null())),
        Equal(IsNull(null()), t()),
        IsNull(Equal(Value(1), null())),
        IsNotNull(Equal(Value(1), null())),
        Not(IsNull(null())),
        # IS
        Is(t(), t()),
        Is(t(), f()),
        Is(null(), null()),
        Is(null(), t()),
        Is(Value(1), null()),
        Is(Value(1), Value(1)),
        Equal(Is(t(), null()), f()),
        Is(Equal(Value(1), Value(1)), t()),
    ]
    # Floats at their edges, every pair under every operator. What the server
    # makes of each - a value, "out of range" for a result too large or too
    # small to be one, "division by zero" - is the server's to say, and the
    # evaluator's to repeat: a zero from operands that are not zero is an
    # underflow, a NaN divided by zero is a NaN, one divided by infinity is a
    # zero and no underflow.
    edges = (
        0.0, 1.0, -1.0, 1e300, 1e-300, sys.float_info.max, sys.float_info.min,
        float("inf"), float("-inf"), NAN,
    )
    at_the_edges = [
        operator(Value(left), Value(right))
        for left in edges for right in edges for operator in (Add, Sub, Mul, Div)
    ]
    return written + at_the_edges


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
                DictContext({
                    "price": price, "active": active,
                    # A Value Object inside the item, which the storage keeps
                    # as a composite inside the item's row
                    "maker": DictContext({"name": maker_name(price)}),
                    # An object of its own, which the storage keeps in a table
                    # of its own and the item refers to by a key
                    "owner": DictContext({"name": owner_of(active)[1]}),
                }) for price, active in self.items
            ]),
            # The store's owner, kept as the owners of items are
            "owner": DictContext({"name": owner_of(self.flag)[1]}),
        })


def maker_name(price: int | None) -> str | None:
    """The name of the maker of an item of this price."""
    if price is None:
        return None
    return "dear" if price > 500 else "cheap"


def owner_of(known: bool | None) -> tuple[int, str | None]:
    """The key and the name of an owner. The third owner has no name."""
    return {True: (1, "ann"), False: (2, "bob"), None: (3, None)}[known]


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

    def maker_name_of_item() -> Visitable:
        return Field(Object(Item(), "maker"), "name")

    def owner_name_of_item() -> Visitable:
        return Field(Object(Item(), "owner"), "name")

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
        # A member of a Value Object inside the item: a composite inside the
        # item's row, in the array and in the table alike.
        some(Equal(maker_name_of_item(), Value("dear"))),
        some(And(IsNull(maker_name_of_item()), item("active"))),
        every(NotEqual(maker_name_of_item(), Value("cheap"))),
        # A member of an object referred to by a key: the schema says
        # `items.owner`, and `owner`, are kept in a table of their own.
        some(Equal(owner_name_of_item(), Value("ann"))),
        some(And(IsNull(owner_name_of_item()), dear())),
        every(NotEqual(owner_name_of_item(), Value("bob"))),
        some(Equal(owner_name_of_item(), maker_name_of_item())),
        # The same of the candidate itself, and both in one predicate.
        Equal(Field(Object(GlobalScope(), "owner"), "name"), Value("bob")),
        And(IsNull(Field(Object(GlobalScope(), "owner"), "name")), IsNotNull(field("a"))),
        some(Equal(owner_name_of_item(), Field(Object(GlobalScope(), "owner"), "name"))),
        # Constants with nothing but constants beside them: their types are
        # said in the text, for the server has nothing to find them by.
        GreaterThan(field("a"), Sub(Value(4), Value(3))),
        some(GreaterThan(item("price"), Mul(Value(100), Value(5)))),
        LessThan(field("a"), Neg(Value(-2))),
        Or(IsNull(null()), field("flag")),
        # A name is the column's, whatever else PostgreSQL knows by it.
        Equal(field("user"), Value("one")),
        GreaterThan(field("order"), Value(0)),
        Equal(field("createdAt"), Value(2)),
    ]


@functools.total_ordering
class Discount:
    """A Value Object, which a specification compares as a whole."""

    def __init__(self, percent: int):
        self.percent = percent

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other) and self.percent == typing.cast(Discount, other).percent

    def __lt__(self, other: "Discount") -> bool:
        return self.percent < other.percent

    def __hash__(self) -> int:
        return hash((type(self), self.percent))


class NoDiscount(Discount):
    """The special case, which answers for itself as Fowler's Special Case
    does: it is equal to itself and to no discount, and less than any.
    Nothing of it is null to the evaluator."""

    def __init__(self) -> None:
        super().__init__(0)


class DiscountsContext(ITransformContext):
    """What the storage has for them: a discount is its percent in a column,
    and the special case is that column's null."""

    def attr_node(self, path: list[str]) -> Visitable:
        raise ValueError("No such member: %s" % ".".join(path))

    def item_attr_node(self, path: list[str]) -> Visitable:
        if path == ["discount"]:
            return Field(Item(), "discount_percent")
        raise ValueError("No such member of an item: %s" % ".".join(path))

    def value_node(self, val: typing.Any) -> Visitable:
        return Value(None if isinstance(val, NoDiscount) else val.percent)


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
                sql, params = compile_to_sql(constant)
                evaluated, failure = self._evaluate(constant, nothing)
                with self.subTest(sql=sql, params=params):
                    try:
                        # A transaction of its own: a failure is one of the
                        # answers, and must not take the connection with it.
                        async with session.connection.transaction():
                            cursor = await session.connection.execute(
                                "SELECT (%s)" % to_psycopg(sql), params,
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
        # The owner of an item, and of the store, is in a table of its own in
        # either storage of the items.
        def with_owners(schema: SchemaRegistry) -> SchemaRegistry:
            return schema.register_relational(
                "items.owner", "spec_owners", "id", "owner_id",
            ).register_relational("owner", "spec_owners", "id", "owner_id")

        embedded = with_owners(SchemaRegistry("spec_stores"))
        relational = with_owners(SchemaRegistry("spec_stores").register_relational(
            "items", "spec_items", "store_id", "id",
        ))
        async with self._session_pool.session() as session:
            # Rolled back whatever happens: the tables are of this test alone.
            async with session.connection.transaction(force_rollback=True):
                await self._make_tables(session.connection)
                for specification in specifications():
                    satisfied = [
                        store.id for store in STORES
                        if specification.accept(EvaluateVisitor(store.context())) is True
                    ]
                    for storage, schema in (("embedded", embedded), ("relational", relational)):
                        sql, params = compile_to_sql(specification, schema)
                        with self.subTest(storage=storage, sql=sql):
                            cursor = await session.connection.execute(
                                "SELECT id FROM spec_stores WHERE %s ORDER BY id"
                                % to_psycopg(sql),
                                params,
                            )
                            selected = [row[0] for row in await cursor.fetchall()]
                            self.assertEqual(selected, satisfied)

    async def test_equality_with_a_special_case_kept_as_a_null_is_the_null_test(self):
        """A special case that answers for itself is equal to itself, and the
        storage has a null for it: ``discount = $1`` with a null is true of
        nothing, so the server found no shop where the evaluator found all
        three. Equality with a value the mapping made the storage's null is
        the null test.

        What stays the server's own: a null compared with a value is unknown
        to it, and so is the negation of that, where the special case answers
        false and true. A special case kept as a value, and not as a null,
        has none of this.
        """
        def shop(*discounts: Discount) -> DictContext:
            return DictContext({"items": CollectionContext([
                DictContext({"discount": discount}) for discount in discounts
            ])})

        shops = {
            1: shop(Discount(15), NoDiscount()),
            2: shop(NoDiscount(), Discount(15)),
            3: shop(NoDiscount()),
        }
        discount = Field(Item(), "discount")

        def some(predicate: Visitable) -> Visitable:
            return Wildcard(Object(GlobalScope(), "items"), predicate)

        def over(percent: int) -> Visitable:
            return GreaterThan(discount, Value(Discount(percent)))

        # The specification; the shops it is satisfied by; the shops the server selects.
        cases = (
            (some(Equal(discount, Value(NoDiscount()))), [1, 2, 3], [1, 2, 3]),
            (some(Equal(Value(NoDiscount()), discount)), [1, 2, 3], [1, 2, 3]),
            (some(NotEqual(discount, Value(NoDiscount()))), [1, 2], [1, 2]),
            (some(over(10)), [1, 2], [1, 2]),
            # The server's own logic of a null, which the null test does not reach.
            (some(Not(over(10))), [1, 2, 3], []),
            (some(NotEqual(discount, Value(Discount(15)))), [1, 2, 3], []),
        )
        async with self._session_pool.session() as session:
            async with session.connection.transaction(force_rollback=True):
                for statement in (
                    "CREATE TYPE pg_temp.spec_answering AS (price int8, discount_percent int8)",
                    "CREATE TEMP TABLE spec_answering_shops"
                    " (id int8, items pg_temp.spec_answering[])",
                    "INSERT INTO spec_answering_shops VALUES"
                    " (1, ARRAY[ROW(900, 15), ROW(100, NULL)]::pg_temp.spec_answering[]),"
                    " (2, ARRAY[ROW(100, NULL), ROW(900, 15)]::pg_temp.spec_answering[]),"
                    " (3, ARRAY[ROW(100, NULL)]::pg_temp.spec_answering[])",
                ):
                    await session.connection.execute(statement)
                for specification, in_memory, on_the_server in cases:
                    sql, params = compile_specification(DiscountsContext(), specification)
                    with self.subTest(sql=sql):
                        satisfied = [
                            id_ for id_, candidate in shops.items()
                            if specification.accept(EvaluateVisitor(candidate)) is True
                        ]
                        self.assertEqual(satisfied, in_memory)
                        cursor = await session.connection.execute(
                            "SELECT id FROM spec_answering_shops WHERE %s ORDER BY id"
                            % to_psycopg(sql),
                            params,
                        )
                        self.assertEqual([row[0] for row in await cursor.fetchall()], on_the_server)

    async def test_a_constant_beside_a_column_takes_the_columns_type(self):
        """Why a type is said only where nothing stands beside the constant.

        A time without zone is sent as a timestamp, as the column is. Said to
        be timestamptz beside that column, it would be compared in the
        session's time zone, and the row would not be found.
        """
        moment = datetime.datetime(2023, 11, 14, 22, 13, 20)
        zoned = moment.replace(tzinfo=datetime.timezone.utc)
        async with self._session_pool.session() as session:
            async with session.connection.transaction(force_rollback=True):
                for statement in (
                    "SET LOCAL TIME ZONE 'Asia/Tokyo'",
                    "CREATE TEMP TABLE spec_moments"
                    " (id int8, at timestamp, zoned timestamptz, small int2)",
                    "INSERT INTO spec_moments VALUES"
                    " (1, '2023-11-14 22:13:20', '2023-11-14 22:13:20+00', 7)",
                ):
                    await session.connection.execute(statement)
                for specification in (
                    Equal(field("at"), Value(moment)),
                    Equal(field("zoned"), Value(zoned)),
                    Equal(field("small"), Value(7)),
                    # And where nothing stands beside them, the constants say their own.
                    Equal(field("small"), Add(Value(3), Value(4))),
                ):
                    sql, params = compile_to_sql(specification)
                    with self.subTest(sql=sql):
                        cursor = await session.connection.execute(
                            "SELECT id FROM spec_moments WHERE %s" % to_psycopg(sql), params,
                        )
                        self.assertEqual([row[0] for row in await cursor.fetchall()], [1])

    async def _make_tables(self, connection: typing.Any) -> None:
        await connection.execute(
            "CREATE TYPE pg_temp.spec_maker AS (name text)"
        )
        await connection.execute(
            "CREATE TYPE pg_temp.spec_item AS"
            " (price int8, active bool, maker pg_temp.spec_maker, owner_id int8)"
        )
        await connection.execute(
            "CREATE TEMP TABLE spec_owners (id int8 PRIMARY KEY, name text)"
        )
        await connection.execute(
            "INSERT INTO spec_owners VALUES (1, 'ann'), (2, 'bob'), (3, NULL)"
        )
        await connection.execute(
            "CREATE TEMP TABLE spec_stores ("
            " id int8 PRIMARY KEY, a int8, b int8, flag bool, name text,"
            " items pg_temp.spec_item[] NOT NULL,"
            ' "user" text, "order" int8, "createdAt" int8, owner_id int8)'
        )
        await connection.execute(
            "CREATE TEMP TABLE spec_items ("
            " store_id int8 NOT NULL, price int8, active bool, maker pg_temp.spec_maker,"
            " owner_id int8 REFERENCES spec_owners)"
        )
        for store in STORES:
            await connection.execute(
                "INSERT INTO spec_stores VALUES (%s, %s, %s, %s, %s, '{}', %s, %s, %s, %s)",
                (
                    store.id, store.a, store.b, store.flag, store.name,
                    store.name, store.a, store.b, owner_of(store.flag)[0],
                ),
            )
            for price, active in store.items:
                await connection.execute(
                    "UPDATE spec_stores SET items = items"
                    " || ROW(%s::int8, %s::bool, ROW(%s::text), %s::int8)::pg_temp.spec_item"
                    " WHERE id = %s",
                    (price, active, maker_name(price), owner_of(active)[0], store.id),
                )
                await connection.execute(
                    "INSERT INTO spec_items VALUES"
                    " (%s, %s, %s, ROW(%s::text)::pg_temp.spec_maker, %s)",
                    (store.id, price, active, maker_name(price), owner_of(active)[0]),
                )


if __name__ == "__main__":
    unittest.main()
