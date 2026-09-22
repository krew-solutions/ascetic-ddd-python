"""Regression tests of the defects found while porting the package to Rust.

Each test names the defect it pins, and what the code did before the fix.
The expected texts are those of the Rust port's ``tests/pg_compile.rs``,
which a differential test holds against a live PostgreSQL.
"""
import unittest
from typing import Any, override

from ascetic_ddd.option import Nothing, Some
from ascetic_ddd.specification.domain.evaluate_visitor import EvaluateVisitor
from ascetic_ddd.specification.domain.nodes import (
    Add, And, Div, EmptiableObject, Equal, Field, GlobalScope, GreaterThan, Is,
    IsNull, Item, LeftShift, LessThan, Mul, Neg, Not, NotEqual, Object, Or, Sub, Value,
    Visitable,
    Wildcard,
)
from ascetic_ddd.specification.infrastructure.composite_expression_node import (
    CompositeExpression,
    CompositeExpressionIsEmptyError,
)
from ascetic_ddd.specification.domain.tests.describing import describe
from ascetic_ddd.specification.infrastructure.postgresql_visitor import (
    _quote,
    compile_specification,
    compile_to_sql,
)
from ascetic_ddd.specification.infrastructure.schema import (
    CollectionMapping,
    ForeignKeyPair,
    SchemaRegistry,
    StorageType,
)
from ascetic_ddd.specification.infrastructure.transform_visitor import (
    CompositeExpressionsDifferentLengthError,
    ITransformContext,
    TransformVisitor,
    transform,
)


def field(name: str) -> Field:
    return Field(GlobalScope(), name)


def item(name: str) -> Field:
    return Field(Item(), name)


def sql(node: Visitable, schema: SchemaRegistry | None = None) -> str:
    return compile_to_sql(node, schema)[0]


class DictContext:
    """Dictionary-based context for testing."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def get(self, key: str) -> Any:
        """Get value by key."""
        return self._data[key]


class TestTheItemOfAnEnclosingCollectionIsItsAlias(unittest.TestCase):
    """The item of an enclosing collection has an alias of its own, which the
    inner query names as SQL lets it: ``Item(1)`` is that alias.
    """

    def setUp(self):
        self.over_its_category = Wildcard(Object(GlobalScope(), "categories"), Wildcard(
            Object(Item(), "products"), GreaterThan(item("price"), Field(Item(1), "limit")),
        ))

    def test_embedded(self):
        self.assertEqual(
            sql(self.over_its_category),
            'EXISTS (SELECT 1 FROM unnest("categories") AS "category_1"'
            ' WHERE EXISTS (SELECT 1 FROM unnest("category_1"."products") AS "product_2"'
            ' WHERE "product_2"."price" > "category_1"."limit"))',
        )

    def test_relational(self):
        # The enclosing row is the one the keys point at, and its columns are
        # named the same way.
        schema = SchemaRegistry("shops").register_relational(
            "categories", "categories", "shop_id", "id",
        ).register_relational("categories.products", "products", "category_id", "id")
        self.assertEqual(
            sql(self.over_its_category, schema),
            'EXISTS (SELECT 1 FROM "categories" AS "category_1"'
            ' WHERE "category_1"."shop_id" = "shops"."id"'
            ' AND EXISTS (SELECT 1 FROM "products" AS "product_2"'
            ' WHERE "product_2"."category_id" = "category_1"."id"'
            ' AND "product_2"."price" > "category_1"."limit"))',
        )

    def test_two_collections_out_beside_the_candidates_row(self):
        three_deep = Wildcard(Object(GlobalScope(), "categories"), Wildcard(
            Object(Item(), "products"), Wildcard(Object(Item(), "tags"), And(
                GreaterThan(item("weight"), Field(Item(2), "limit")),
                LessThan(Field(Item(1), "price"), field("limit")),
            )),
        ))
        self.assertEqual(
            sql(three_deep, SchemaRegistry("shops")),
            'EXISTS (SELECT 1 FROM unnest("categories") AS "category_1"'
            ' WHERE EXISTS (SELECT 1 FROM unnest("category_1"."products") AS "product_2"'
            ' WHERE EXISTS (SELECT 1 FROM unnest("product_2"."tags") AS "tag_3"'
            ' WHERE "tag_3"."weight" > "category_1"."limit" AND "product_2"."price" < "shops"."limit")))',
        )

    def test_the_item_is_only_inside_a_collection(self):
        with self.assertRaises(ValueError):
            sql(Wildcard(Object(GlobalScope(), "categories"), GreaterThan(item("limit"), Field(Item(1), "limit"))))


class TestTheCandidatesColumnInsideAPredicateIsQualifiedWithItsRow(unittest.TestCase):
    """Unqualified, PostgreSQL read it from the innermost row that has a
    column of that name: a category with a ``limit`` of its own hid the
    shop's, and the query selected other rows than the evaluator was
    satisfied by. The row is what the schema calls it, so without a schema
    there is no query.
    """

    def setUp(self):
        self.over_the_shops_limit = Wildcard(
            Object(GlobalScope(), "categories"), GreaterThan(item("limit"), field("limit")),
        )

    def test_with_the_table(self):
        self.assertEqual(
            sql(self.over_the_shops_limit, SchemaRegistry("shops")),
            'EXISTS (SELECT 1 FROM unnest("categories") AS "category_1"'
            ' WHERE "category_1"."limit" > "shops"."limit")',
        )

    def test_with_the_alias(self):
        self.assertEqual(
            sql(self.over_the_shops_limit, SchemaRegistry("public.shops").with_parent_alias("s")),
            'EXISTS (SELECT 1 FROM unnest("categories") AS "category_1"'
            ' WHERE "category_1"."limit" > "s"."limit")',
        )

    def test_without_a_schema_there_is_no_query(self):
        with self.assertRaises(ValueError):
            sql(self.over_the_shops_limit)

    def test_what_stays(self):
        # A name of several parts the author qualified, and it stays as
        # written; outside a collection's predicate a name is unqualified.
        self.assertEqual(
            sql(Wildcard(Object(GlobalScope(), "categories"), GreaterThan(item("limit"), field("s.limit")))),
            'EXISTS (SELECT 1 FROM unnest("categories") AS "category_1"'
            ' WHERE "category_1"."limit" > "s"."limit")',
        )
        self.assertEqual(sql(GreaterThan(field("limit"), Value(1))), '"limit" > $1')


class TestAMappingKeepsTheItemWhereItWas(unittest.TestCase):
    """A mapping names the column of an item's member from ``Item()``, not
    knowing how far out the item is: the transformer puts the column where
    the member was.
    """

    def test_a_member_of_an_outer_item(self):
        class Mapping(ITransformContext):
            def attr_node(self, path: list[str]) -> Any:
                return field("_".join(path))

            def item_attr_node(self, path: list[str]) -> Any:
                if path == ["id"]:
                    return CompositeExpression(item("tenant_id"), item("member_id"))
                return Field(Object(Item(), "row"), "_".join(path))

            def value_node(self, val: Any) -> Any:
                return Value(val)

        transformed = transform(Mapping(), Wildcard(Object(GlobalScope(), "categories"), Wildcard(
            Object(Item(), "products"), And(
                GreaterThan(item("price"), Field(Item(1), "limit")),
                Equal(Field(Item(1), "id"), Field(Item(), "id")),
            ),
        )))
        self.assertEqual(
            describe(transformed),
            ("any", ("$", "categories"), ("any", ("@", "products"), ("AND",
                ("GT", ("field", ("@", "row"), "price"), ("field", ("@1", "row"), "limit")),
                ("AND",
                    ("EQ", ("field", "@1", "tenant_id"), ("field", "@", "tenant_id")),
                    ("EQ", ("field", "@1", "member_id"), ("field", "@", "member_id")),
                ),
            ))),
        )


class TestParenthesesAreWritten(unittest.TestCase):
    """No parenthesis was ever written. A precedence was looked up by
    ``"%s %s" % (operator, associativity)``, which for members of a
    ``str``-and-``Enum`` is ``"OPERATOR.AND ASSOCIATIVITY.LEFT_ASSOCIATIVE"``
    and is in no row of the table, so every operator had the precedence of
    "any other operator" and none bound looser than another.
    """

    def test_a_looser_operator_inside_a_tighter_one(self):
        a, b, c = field("a"), field("b"), field("c")
        cases = (
            (And(Or(a, b), c), '("a" OR "b") AND "c"'),
            (Or(And(a, b), c), '"a" AND "b" OR "c"'),
            (Not(And(a, b)), 'NOT ("a" AND "b")'),
            (Not(Equal(a, b)), 'NOT "a" = "b"'),
            (Mul(Add(a, b), c), '("a" + "b") * "c"'),
            (Add(Mul(a, b), c), '"a" * "b" + "c"'),
            (IsNull(Or(a, b)), '("a" OR "b") IS NULL'),
            (IsNull(Equal(a, b)), '"a" = "b" IS NULL'),
            (Neg(Add(a, b)), '-("a" + "b")'),
            (LeftShift(Add(a, b), c), '"a" + "b" << "c"'),
            (Add(a, LeftShift(b, c)), '"a" + ("b" << "c")'),
            (Equal(Is(a, b), c), '("a" IS NOT DISTINCT FROM "b") = "c"'),
        )
        for node, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(sql(node), expected)


class TestParenthesesFollowAssociativity(unittest.TestCase):
    """Precedences alone were compared, so an operand as tight as its
    operator was never parenthesised: ``a - (b - c)`` was written
    ``a - b - c``, which is another number, and ``(a = b) = c`` was written
    ``a = b = c``, which PostgreSQL does not parse.
    """

    def test_as_tight_by_the_side_the_operator_groups_to(self):
        a, b, c = field("a"), field("b"), field("c")
        cases = (
            (Sub(Sub(a, b), c), '"a" - "b" - "c"'),
            (Sub(a, Sub(b, c)), '"a" - ("b" - "c")'),
            (Sub(a, Add(b, c)), '"a" - ("b" + "c")'),
            (Div(a, Div(b, c)), '"a" / ("b" / "c")'),
            (Div(Mul(a, b), c), '"a" * "b" / "c"'),
        )
        for node, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(sql(node), expected)

    def test_a_comparison_groups_to_neither_side(self):
        a, b, c = field("a"), field("b"), field("c")
        cases = (
            (Equal(Equal(a, b), c), '("a" = "b") = "c"'),
            (Equal(a, Equal(b, c)), '"a" = ("b" = "c")'),
            (Equal(IsNull(a), c), '("a" IS NULL) = "c"'),
            (IsNull(IsNull(a)), '("a" IS NULL) IS NULL'),
        )
        for node, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(sql(node), expected)

    def test_the_connectives_regroup_freely(self):
        # AND and OR are associative, nulls included, so the parentheses of
        # a right-nested run would say nothing.
        a, b, c = field("a"), field("b"), field("c")
        self.assertEqual(sql(And(a, And(b, c))), '"a" AND "b" AND "c"')
        self.assertEqual(sql(Or(a, Or(b, c))), '"a" OR "b" OR "c"')
        self.assertEqual(sql(And(a, b, c)), '"a" AND "b" AND "c"')

    def test_two_minus_signs_are_a_comment(self):
        a, b = field("a"), field("b")
        self.assertEqual(sql(Neg(Neg(a))), '-(-"a")')
        self.assertEqual(sql(Sub(a, Neg(b))), '"a" - -"b"')
        self.assertEqual(sql(Not(Not(a))), 'NOT NOT "a"')
        self.assertEqual(compile_to_sql(Neg(Value(5))), ("-$1::bigint", [5]))


class Somebody:
    def __init__(self, id_: int):
        self.id = id_


class Nobody:
    """The special case: an owner that is nobody, equal to itself in the domain."""


class Pair:
    """Known by two numbers, of which the second may be nobody's."""

    def __init__(self, a: int, b: int | None):
        self.a, self.b = a, b


class OwnersContext(ITransformContext):
    def attr_node(self, path: list[str]) -> Any:
        if path == ["pair"]:
            return CompositeExpression(field("a"), field("b"))
        return field(".".join(path))

    def value_node(self, val: Any) -> Any:
        if isinstance(val, Somebody):
            return Value(val.id)
        if isinstance(val, Nobody):
            return Value(None)
        if isinstance(val, Pair):
            return CompositeExpression(Value(val.a), Value(val.b))
        return Value(val)


class TestEqualityWithWhatTheMappingMadeANullIsTheNullTest(unittest.TestCase):
    """A value of the domain that the storage keeps as a null - a special case
    that answers for itself in the domain - is equal to itself there, and
    ``owner = $1`` with a null is true of nothing: the server selected no row
    where the evaluator was satisfied. The transformer tests for it, where
    both operands are mapped and the node is built; the context maps operands
    and knows nothing of operators. Only a null the mapping made: a value that
    was null in the domain already stays compared. The rows are in
    ``test_postgresql_agreement``.
    """

    def test_where_it_is_tested_for_and_where_it_is_not(self):
        owner = field("owner")
        cases = (
            # Somebody is compared, as any value is.
            (Equal(owner, Value(Somebody(7))), '"owner" = $1'),
            (Equal(owner, Value(Nobody())), '"owner" IS NULL'),
            (Equal(Value(Nobody()), owner), '"owner" IS NULL'),
            (NotEqual(owner, Value(Nobody())), '"owner" IS NOT NULL'),
            (Equal(Value(Nobody()), Value(Nobody())), "$1::text IS NULL"),
            # Under any other operator it is the null it was made.
            (GreaterThan(owner, Value(Nobody())), '"owner" > $1'),
            # A null of the domain's own stays compared.
            (Equal(owner, Value(None)), '"owner" = $1'),
            # A part of a composite is tested for as a whole is.
            (Equal(field("pair"), Value(Pair(1, None))), '"a" = $1 AND "b" IS NULL'),
        )
        for node, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(compile_specification(OwnersContext(), node)[0], expected)


class TestAnOptionIsWhatItHoldsOrANull(unittest.TestCase):
    """A constant of a specification may be an ``Option`` of a value. The
    transformer handed the wrapper to the context, which knows the domain's
    values and not their wrappers. It is read first: the context is asked of
    what a ``Some`` holds, and a ``Nothing`` is the null it is in any storage -
    the domain's own null, so it stays compared, and is not taken for a value
    the mapping made a null of. The rows are in ``test_postgresql_agreement``.
    """

    def test_the_context_is_asked_of_the_value_and_not_of_the_wrapper(self):
        owner = field("owner")
        cases = (
            (Equal(owner, Value(Some(Somebody(7)))), '"owner" = $1', [7]),
            (Equal(owner, Value(Some(Some(Somebody(7))))), '"owner" = $1', [7]),
            # The domain's own null: compared, as a None is.
            (Equal(owner, Value(Nothing())), '"owner" = $1', [None]),
            (Is(owner, Value(Nothing())), '"owner" IS NOT DISTINCT FROM $1', [None]),
            # What it holds may be a special case, which the mapping makes a null of.
            (Equal(owner, Value(Some(Nobody()))), '"owner" IS NULL', []),
        )
        for node, expected, params in cases:
            with self.subTest(expected=expected):
                self.assertEqual(compile_specification(OwnersContext(), node), (expected, params))


class TestAConstantWithNothingBesideItHasItsTypeSaid(unittest.TestCase):
    """A constant is a parameter, and the server finds its type from what
    stands beside it. Where every operand of an operator is a constant there
    is nothing beside it - "operator is not unique: unknown + unknown" to a
    driver that asks the server, and to psycopg, which sends an integer's type
    by its size, arithmetic in sixteen bits: `1 << 63` was 0. So there the
    text says the type, by the kind of the value. Beside a column it does not:
    the value adapts to the column, which a type said would take away.
    """

    def test_where_it_is_said_and_where_it_is_not(self):
        price = field("price")
        cases = (
            # Beside a column, or beside what has a type already: as it was.
            (GreaterThan(price, Value(1)), '"price" > $1'),
            (GreaterThan(Add(price, Value(1)), Value(2)), '"price" + $1 > $2'),
            # Both operands constants.
            (GreaterThan(price, Add(Value(1), Value(2))), '"price" > $1::bigint + $2::bigint'),
            (LessThan(Value(1), Value(2.5)), "$1::bigint < $2::double precision"),
            (Equal(Value("a"), Value("b")), "$1::text = $2::text"),
            # What was typed so is a type for what stands beside it.
            (Mul(Add(Value(1), Value(2)), Value(3)), "($1::bigint + $2::bigint) * $3"),
            # PostgreSQL shifts a bigint by an integer.
            (LeftShift(Value(1), Value(4)), "$1::bigint << $2::integer"),
            # Alone under its operator.
            (Neg(Value(5)), "-$1::bigint"),
            (Not(Value(True)), "NOT $1::boolean"),
            (IsNull(Value(7)), "$1::bigint IS NULL"),
            # A null has no kind. Beside a constant it takes that one's type
            # from the server; alone, what its operator is of.
            (Add(Value(None), Value(1)), "$1 + $2::bigint"),
            (Add(Value(None), Value(None)), "$1::bigint + $2::bigint"),
            (Equal(Value(None), Value(None)), "$1 = $2"),
            (IsNull(Value(None)), "$1::text IS NULL"),
            (Neg(Value(None)), "-$1::bigint"),
            (Not(Value(None)), "NOT $1"),
        )
        for node, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(sql(node), expected)


class TestIsTakesAParameter(unittest.TestCase):
    """``x IS $1`` is a syntax error: PostgreSQL's ``IS`` takes a keyword —
    TRUE, NULL — and not a parameter. ``IS NOT DISTINCT FROM`` is the same
    equality, in which null is a value, and takes any expression.
    """

    def test_is(self):
        self.assertEqual(
            compile_to_sql(Is(field("active"), Value(True))),
            ('"active" IS NOT DISTINCT FROM $1', [True]),
        )

    def test_is_null_is_what_it_was(self):
        self.assertEqual(sql(IsNull(field("deleted_at"))), '"deleted_at" IS NULL')


class TestCompositeInequality(unittest.TestCase):
    """``!=`` of two composites was ``NOT (a1 != b1 AND a2 != b2)``: a
    composite was unequal to itself, and equal to one it shares no part with.
    It is the negation of their equality.
    """

    def setUp(self):
        self.columns = CompositeExpression(field("tenant_id"), field("member_id"))

    def unequal_to(self, tenant_id: int, member_id: int) -> Visitable:
        return self.columns != CompositeExpression(Value(tenant_id), Value(member_id))

    def test_the_text(self):
        self.assertEqual(
            compile_to_sql(self.unequal_to(10, 3)),
            ('NOT ("tenant_id" = $1 AND "member_id" = $2)', [10, 3]),
        )

    def test_nested(self):
        left = CompositeExpression(CompositeExpression(field("a"), field("b")), field("c"))
        right = CompositeExpression(CompositeExpression(Value(1), Value(2)), Value(3))
        self.assertEqual(sql(left != right), 'NOT ("a" = $1 AND "b" = $2 AND "c" = $3)')

    def test_the_meaning(self):
        row = DictContext({"tenant_id": 10, "member_id": 3})
        cases = (
            ((10, 3), False, "itself"),
            ((10, 4), True, "one part differs"),
            ((11, 4), True, "every part differs"),
        )
        for (tenant_id, member_id), expected, why in cases:
            with self.subTest(why=why):
                unequal = self.unequal_to(tenant_id, member_id).accept(EvaluateVisitor(row))
                self.assertIs(unequal, expected)


class TestThePredicateOfARelationalCollectionStaysInsideItsKeys(unittest.TestCase):
    """The predicate was written after the keys as it was:
    ``fk AND p OR q``, which selects through ``q`` the rows of other parents.
    """

    def test_a_disjunction_is_parenthesised(self):
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Items", "items", "store_id", "id")
        )
        dear_or_active = Wildcard(
            Object(GlobalScope(), "Items"),
            Or(item("Active"), GreaterThan(item("Price"), Value(500))),
        )
        self.assertEqual(
            sql(dear_or_active, schema),
            'EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."store_id" = "s"."id"'
            ' AND ("item_1"."Active" OR "item_1"."Price" > $1))',
        )

    def test_a_conjunction_is_not(self):
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Items", "items", "store_id", "id")
        )
        dear_and_active = Wildcard(
            Object(GlobalScope(), "Items"),
            And(item("Active"), GreaterThan(item("Price"), Value(500))),
        )
        self.assertEqual(
            sql(dear_and_active, schema),
            'EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."store_id" = "s"."id"'
            ' AND "item_1"."Active" AND "item_1"."Price" > $1)',
        )

    def test_an_embedded_collection_needs_none(self):
        dear_or_active = Wildcard(
            Object(GlobalScope(), "Items"),
            Or(item("Active"), GreaterThan(item("Price"), Value(500))),
        )
        self.assertEqual(
            sql(dear_or_active),
            'EXISTS (SELECT 1 FROM unnest("Items") AS "item_1"'
            ' WHERE "item_1"."Active" OR "item_1"."Price" > $1)',
        )


class TestACollectionIsNamedByItsWholePath(unittest.TestCase):
    """A schema named a collection by its last name alone, so the items of a
    store and the items of a category were one collection with one table.
    """

    def setUp(self):
        self.schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Items", "store_items", "store_id", "id")
            .register_relational("Categories", "categories", "store_id", "id")
            .register_relational("Categories.Items", "category_items", "category_id", "id")
        )

    def test_two_collections_of_one_name(self):
        of_the_store = Wildcard(Object(GlobalScope(), "Items"), item("Active"))
        of_a_category = Wildcard(
            Object(GlobalScope(), "Categories"),
            Wildcard(Object(Item(), "Items"), item("Active")),
        )
        self.assertEqual(
            sql(of_the_store, self.schema),
            'EXISTS (SELECT 1 FROM "store_items" AS "item_1"'
            ' WHERE "item_1"."store_id" = "s"."id" AND "item_1"."Active")',
        )
        self.assertEqual(
            sql(of_a_category, self.schema),
            'EXISTS (SELECT 1 FROM "categories" AS "category_1"'
            ' WHERE "category_1"."store_id" = "s"."id" AND EXISTS (SELECT 1 FROM "category_items" AS "item_2"'
            ' WHERE "item_2"."category_id" = "category_1"."id" AND "item_2"."Active"))',
        )

    def test_a_nested_collection_not_named_is_embedded(self):
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Items", "store_items", "store_id", "id")
            .register_relational("Categories", "categories", "store_id", "id")
        )
        of_a_category = Wildcard(
            Object(GlobalScope(), "Categories"),
            Wildcard(Object(Item(), "Items"), item("Active")),
        )
        self.assertEqual(
            sql(of_a_category, schema),
            'EXISTS (SELECT 1 FROM "categories" AS "category_1"'
            ' WHERE "category_1"."store_id" = "s"."id" AND EXISTS (SELECT 1 FROM unnest("category_1"."Items") AS "item_2"'
            ' WHERE "item_2"."Active"))',
        )

    def test_the_objects_on_the_way_are_a_part_of_the_name(self):
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Warehouse.Items", "warehouse_items", "store_id", "id")
        )
        in_the_warehouse = Wildcard(
            Object(Object(GlobalScope(), "Warehouse"), "Items"), item("Active"),
        )
        self.assertEqual(
            sql(in_the_warehouse, schema),
            'EXISTS (SELECT 1 FROM "warehouse_items" AS "item_1"'
            ' WHERE "item_1"."store_id" = "s"."id" AND "item_1"."Active")',
        )


class TestACollectionOfTheCandidateInsideAnotherJoinsToTheRoot(unittest.TestCase):
    """A relational collection inside the predicate of another was joined to
    the enclosing item whatever it was a collection of: the tags of the
    store, named from inside the predicate on its items, were looked for
    among the rows that point at an item.
    """

    def test_the_parent_is_what_the_path_starts_at(self):
        schema = (
            SchemaRegistry("stores")
            .with_parent_alias("s")
            .register_relational("Items", "items", "store_id", "id")
            .register_relational("Tags", "tags", "store_id", "id")
            .register_relational("Items.Tags", "item_tags", "item_id", "id")
        )
        on_sale = Equal(item("Name"), Value("sale"))
        of_the_store = Wildcard(
            Object(GlobalScope(), "Items"), Wildcard(Object(GlobalScope(), "Tags"), on_sale),
        )
        of_the_item = Wildcard(
            Object(GlobalScope(), "Items"), Wildcard(Object(Item(), "Tags"), on_sale),
        )
        self.assertEqual(
            sql(of_the_store, schema),
            'EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."store_id" = "s"."id"'
            ' AND EXISTS (SELECT 1 FROM "tags" AS "tag_2" WHERE "tag_2"."store_id" = "s"."id"'
            ' AND "tag_2"."Name" = $1))',
        )
        self.assertEqual(
            sql(of_the_item, schema),
            'EXISTS (SELECT 1 FROM "items" AS "item_1" WHERE "item_1"."store_id" = "s"."id"'
            ' AND EXISTS (SELECT 1 FROM "item_tags" AS "tag_2" WHERE "tag_2"."item_id" = "item_1"."id"'
            ' AND "tag_2"."Name" = $1))',
        )


class TestANameThatIsNotAnIdentifierIsRefused(unittest.TestCase):
    """A name was written into the query as it was, so a tree built of a text
    from outside could put SQL of its own there. A name is letters, digits
    and "_", not starting with a digit; anything else is refused.
    """

    def test_the_names_of_a_tree(self):
        cases = (
            field("age; DROP TABLE users"),
            field("1st"),
            field(""),
            field("a..b"),
            Field(Object(GlobalScope(), "users u"), "name"),
            Wildcard(Object(GlobalScope(), "items x"), item("active")),
            Wildcard(Object(GlobalScope(), "items"), item("active OR 1=1")),
        )
        for node in cases:
            with self.subTest(node=describe(node)):
                with self.assertRaises(ValueError):
                    sql(node)

    def test_the_names_of_a_schema(self):
        def schema(table="items", child="store_id", parent="id", alias="", parent_alias="s"):
            return SchemaRegistry("stores").with_parent_alias(parent_alias).register(
                "items",
                CollectionMapping(
                    storage=StorageType.RELATIONAL,
                    table=table,
                    foreign_keys=[ForeignKeyPair(child, parent)],
                    alias=alias,
                ),
            )

        cases = (
            schema(table="items; --"),
            schema(child="store_id = 1 OR 1"),
            schema(parent="id)"),
            schema(alias="i i"),
            schema(parent_alias="s, users"),
        )
        any_item = Wildcard(Object(GlobalScope(), "items"), item("active"))
        for number, registry in enumerate(cases):
            with self.subTest(case=number):
                with self.assertRaises(ValueError):
                    sql(any_item, registry)

    def test_what_is_an_identifier_is_written_between_quotes(self):
        self.assertEqual(sql(Field(Object(GlobalScope(), "users"), "_name1")), '"users"."_name1"')
        self.assertEqual(sql(field("users.name")), '"users"."name"')
        schema = SchemaRegistry("stores").register_relational(
            "items", "public.items", "store_id", "id",
        )
        self.assertEqual(
            sql(Wildcard(Object(GlobalScope(), "items"), item("active")), schema),
            'EXISTS (SELECT 1 FROM "public"."items" AS "item_1"'
            ' WHERE "item_1"."store_id" = "stores"."id" AND "item_1"."active")',
        )


class TestAMemberOfAnObjectInsideAnItemIsAMemberOfAComposite(unittest.TestCase):
    """The visitor asked whether the field's immediate parent was the item,
    not what its path started at: the parent of ``name`` in ``@.maker.name``
    is the object ``maker``, so the item's alias was dropped and
    ``"maker"."name"`` written - to PostgreSQL a table and a column, an error
    if there is no such table and the column of another table if the query
    has one of that name. A member of a Value Object inside an item is a
    member of a composite kept in the item's row. The rows are in
    ``test_postgresql_agreement``.
    """

    def maker(self, *names: str) -> Visitable:
        obj: EmptiableObject = Object(Item(), "maker")
        for name in names[:-1]:
            obj = Object(obj, name)
        return Equal(Field(obj, names[-1]), Value("x"))

    def test_in_an_array_and_in_a_table_of_its_own(self):
        items = Object(GlobalScope(), "items")
        self.assertEqual(
            sql(Wildcard(items, self.maker("name"))),
            'EXISTS (SELECT 1 FROM unnest("items") AS "item_1"'
            ' WHERE ("item_1"."maker")."name" = $1)',
        )
        self.assertEqual(
            sql(Wildcard(items, self.maker("country", "code"))),
            'EXISTS (SELECT 1 FROM unnest("items") AS "item_1"'
            ' WHERE (("item_1"."maker")."country")."code" = $1)',
        )
        schema = SchemaRegistry("stores").with_parent_alias("s").register_relational(
            "items", "store_items", "store_id", "id",
        )
        self.assertEqual(
            sql(Wildcard(items, self.maker("name")), schema),
            'EXISTS (SELECT 1 FROM "store_items" AS "item_1"'
            ' WHERE "item_1"."store_id" = "s"."id" AND ("item_1"."maker")."name" = $1)',
        )

    def test_the_item_of_an_inner_collection(self):
        inner = Wildcard(Object(Item(), "parts"), self.maker("name"))
        self.assertEqual(
            sql(Wildcard(Object(GlobalScope(), "items"), inner)),
            'EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE EXISTS'
            ' (SELECT 1 FROM unnest("item_1"."parts") AS "part_2"'
            ' WHERE ("part_2"."maker")."name" = $1))',
        )

    def test_from_the_candidate_the_dots_stay(self):
        # A qualified name: the column `maker` of `s`
        self.assertEqual(sql(Equal(field("s.maker"), Value("x"))), '"s"."maker" = $1')


class TestAMemberOfAnObjectKeptInATableOfItsOwnIsReadThroughTheKey(unittest.TestCase):
    """An object on the way to a member is looked up in the schema, as a
    collection is. Kept in a table of its own it is read through its key, by
    a subquery in the column's place: at most the one row the key names, and
    null if there is none. There was no way to say so: the dots were written
    as they stood, which PostgreSQL reads as a table and a column.
    """

    ITEMS = Object(GlobalScope(), "items")
    OWNER_NAME = Field(Object(Item(), "owner"), "name")

    def stores(self) -> SchemaRegistry:
        return SchemaRegistry("stores").with_parent_alias("s")

    def test_whether_the_items_are_an_array_or_a_table(self):
        named = Wildcard(self.ITEMS, Equal(self.OWNER_NAME, Value("ann")))
        embedded = self.stores().register_relational("items.owner", "owners", "id", "owner_id")
        self.assertEqual(
            sql(named, embedded),
            'EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE'
            ' (SELECT "owner_2"."name" FROM "owners" AS "owner_2"'
            ' WHERE "owner_2"."id" = "item_1"."owner_id") = $1)',
        )
        relational = self.stores().register_relational(
            "items", "store_items", "store_id", "id",
        ).register(
            "items.owner",
            CollectionMapping(
                storage=StorageType.RELATIONAL,
                table="owners",
                foreign_keys=[ForeignKeyPair("id", "owner_id")],
                alias="o",
            ),
        )
        self.assertEqual(
            sql(named, relational),
            'EXISTS (SELECT 1 FROM "store_items" AS "item_1"'
            ' WHERE "item_1"."store_id" = "s"."id" AND'
            ' (SELECT "o_2"."name" FROM "owners" AS "o_2"'
            ' WHERE "o_2"."id" = "item_1"."owner_id") = $1)',
        )

    def test_a_key_of_two_columns_and_a_composite_inside_the_row(self):
        schema = self.stores().register(
            "items.owner",
            CollectionMapping(
                storage=StorageType.RELATIONAL,
                table="public.owners",
                foreign_keys=[ForeignKeyPair("tenant_id", "tenant_id"), ForeignKeyPair("id", "owner_id")],
            ),
        )
        city = Field(Object(Object(Item(), "owner"), "address"), "city")
        self.assertEqual(
            sql(Wildcard(self.ITEMS, IsNull(city)), schema),
            'EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE'
            ' (SELECT ("owner_2"."address")."city" FROM "public"."owners" AS "owner_2"'
            ' WHERE "owner_2"."tenant_id" = "item_1"."tenant_id"'
            ' AND "owner_2"."id" = "item_1"."owner_id") IS NULL)',
        )

    def test_of_the_candidate_itself_and_each_with_an_alias_of_its_own(self):
        schema = self.stores().register_relational(
            "owner", "owners", "id", "owner_id",
        ).register_relational("items.owner", "owners", "id", "owner_id")
        of_the_store = Field(Object(GlobalScope(), "owner"), "name")
        self.assertEqual(
            sql(Wildcard(self.ITEMS, Equal(self.OWNER_NAME, of_the_store)), schema),
            'EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE'
            ' (SELECT "owner_2"."name" FROM "owners" AS "owner_2"'
            ' WHERE "owner_2"."id" = "item_1"."owner_id") ='
            ' (SELECT "owner_3"."name" FROM "owners" AS "owner_3"'
            ' WHERE "owner_3"."id" = "s"."owner_id"))',
        )
        # What the schema does not mention stays what the dots have meant.
        self.assertEqual(sql(Equal(field("s.name"), Value("x")), schema), '"s"."name" = $1')
        maker = Field(Object(Item(), "maker"), "name")
        self.assertEqual(
            sql(Wildcard(self.ITEMS, Equal(maker, Value("x"))), schema),
            'EXISTS (SELECT 1 FROM unnest("items") AS "item_1"'
            ' WHERE ("item_1"."maker")."name" = $1)',
        )


class TestANameIsTheColumnsAndNothingElse(unittest.TestCase):
    """A name was written into the query as it stood, and PostgreSQL reads a
    word it knows as what it knows: ``user = $1`` compares the user of the
    session and selects other rows than were asked for, ``order > $1`` does
    not parse. Which words these are depends on the version of the server, so
    every name is quoted, and between quotes it is the column's to the letter.
    The rows are in ``test_postgresql_agreement``.
    """

    def test_a_word_postgresql_knows_is_a_name(self):
        self.assertEqual(sql(Equal(field("user"), Value("ann"))), '"user" = $1')
        self.assertEqual(sql(GreaterThan(field("order"), Value(0))), '"order" > $1')

    def test_the_case_of_a_name_is_kept(self):
        self.assertEqual(sql(IsNull(field("createdAt"))), '"createdAt" IS NULL')

    def test_a_quote_inside_a_name_does_not_end_it(self):
        self.assertEqual(_quote('a" OR "b'), '"a"" OR ""b"')
        self.assertEqual(_quote('"'), '""""')
        # Nor does it get there: the alphabet of names has no quote.
        with self.assertRaises(ValueError):
            sql(field('a" OR "b'))


class Weight:
    """A value of the domain that is one column in the storage."""

    def __init__(self, grams: int):
        self.grams = grams


class PartsContext(ITransformContext):
    """A mapping of a domain whose parts have a weight, kept in grams."""

    def attr_node(self, path: list[str]) -> Visitable:
        if path == ["rank"]:
            return field("rank")
        raise ValueError("Unknown field: %s" % ".".join(path))

    def item_attr_node(self, path: list[str]) -> Visitable:
        if path == ["weight"]:
            return item("weight_grams")
        raise ValueError("Unknown field of an item: %s" % ".".join(path))

    def value_node(self, val: Any) -> Visitable:
        return Value(val.grams if isinstance(val, Weight) else val)


class StoreItemsContext(ITransformContext):
    """The members of a store by the names the storage has for them."""

    def attr_node(self, path: list[str]) -> Visitable:
        raise ValueError("No such member of a store: %s" % ".".join(path))

    def item_attr_node(self, path: list[str]) -> Visitable:
        if path == ["Price"]:
            return item("price_cents")
        raise ValueError("No such member of an item: %s" % ".".join(path))

    @override
    def collection_node(self, path: list[str]) -> EmptiableObject:
        if path == ["Items"]:
            return Object(GlobalScope(), "store_items")
        raise ValueError("No such collection of a store: %s" % ".".join(path))

    def value_node(self, val: Any) -> Visitable:
        return Value(val)


class TestAMappingAndASchemaAreGivenTogether(unittest.TestCase):
    """A mapping and a schema could not be given together:
    ``compile_specification`` took a context and no schema,
    ``compile_to_sql`` a schema and no context. Both are the repository's to
    know - a query cannot be written without knowing the table - and it
    gives both.
    """

    def test_a_mapped_collection_in_a_table_of_its_own(self):
        schema = SchemaRegistry("stores").with_parent_alias("s").register_relational(
            "store_items", "items", "store_id", "id",
        )
        dear = Wildcard(Object(GlobalScope(), "Items"), GreaterThan(item("Price"), Value(500)))
        self.assertEqual(
            compile_specification(StoreItemsContext(), dear, schema),
            (
                'EXISTS (SELECT 1 FROM "items" AS "store_item_1"'
                ' WHERE "store_item_1"."store_id" = "s"."id" AND "store_item_1"."price_cents" > $1)',
                [500],
            ),
        )


class TestThePredicateOfACollectionIsTransformed(unittest.TestCase):
    """The transformer returned a collection as it was, so the values of its
    predicate reached the query as the domain's objects, and its fields under
    the domain's names. And a field was mapped by its names alone: a mapping
    could not tell a member of the item from a member of the candidate.
    """

    def setUp(self):
        self.heavy = And(
            GreaterThan(field("rank"), Value(3)),
            Wildcard(
                Object(GlobalScope(), "parts"),
                GreaterThan(item("weight"), Value(Weight(100))),
            ),
        )

    def test_the_tree(self):
        self.assertEqual(
            describe(self.heavy.accept(TransformVisitor(PartsContext()))),
            (
                "AND",
                ("GT", ("field", "$", "rank"), ("value", 3)),
                ("any", ("$", "parts"), ("GT", ("field", "@", "weight_grams"), ("value", 100))),
            ),
        )

    def test_the_query(self):
        self.assertEqual(
            compile_specification(PartsContext(), self.heavy),
            (
                '"rank" > $1 AND EXISTS (SELECT 1 FROM unnest("parts") AS "part_1"'
                ' WHERE "part_1"."weight_grams" > $2)',
                [3, 100],
            ),
        )

    def test_a_member_of_the_candidate_inside_the_predicate(self):
        ranked = Wildcard(
            Object(GlobalScope(), "parts"), GreaterThan(item("weight"), field("rank")),
        )
        self.assertEqual(
            describe(ranked.accept(TransformVisitor(PartsContext()))),
            ("any", ("$", "parts"), ("GT", ("field", "@", "weight_grams"), ("field", "$", "rank"))),
        )

    def test_nested(self):
        nested = Wildcard(
            Object(GlobalScope(), "parts"),
            Wildcard(Object(Item(), "parts"), GreaterThan(item("weight"), Value(Weight(5)))),
        )
        self.assertEqual(
            describe(nested.accept(TransformVisitor(PartsContext()))),
            (
                "any",
                ("$", "parts"),
                ("any", ("@", "parts"), ("GT", ("field", "@", "weight_grams"), ("value", 5))),
            ),
        )


class StoredPartsContext(PartsContext):
    """A mapping that says as well where the collections are kept."""

    @override
    def collection_node(self, path: list[str]) -> EmptiableObject:
        if path == ["parts"]:
            return Object(GlobalScope(), "something_parts")
        raise ValueError("Unknown collection: %s" % ".".join(path))

    @override
    def item_collection_node(self, path: list[str]) -> EmptiableObject:
        if path == ["parts"]:
            return Object(Object(Item(), "detail"), "sub_parts")
        raise ValueError("Unknown collection of an item: %s" % ".".join(path))


class CandidateCollectionsContext(PartsContext):
    """A mapping that says where the collections of the candidate are kept,
    and nothing of those of an item."""

    @override
    def collection_node(self, path: list[str]) -> EmptiableObject:
        return Object(GlobalScope(), "something_" + path[-1])


class TestACollectionIsKeptWhereTheContextSays(unittest.TestCase):
    """The transformer mapped the fields and the values of a specification
    and left the collection under the domain's name: ``unnest(parts)`` of a
    column that is ``something_parts``. A context may say where a collection
    is kept; one that does not has the interface's answer, the same place.
    """

    def setUp(self):
        self.heavy = Wildcard(
            Object(GlobalScope(), "parts"),
            GreaterThan(item("weight"), Value(Weight(100))),
        )
        self.nested = Wildcard(
            Object(GlobalScope(), "parts"),
            Wildcard(Object(Item(), "parts"), GreaterThan(item("weight"), Value(Weight(5)))),
        )

    def test_a_collection_of_the_candidate(self):
        self.assertEqual(
            describe(self.heavy.accept(TransformVisitor(StoredPartsContext()))),
            ("any", ("$", "something_parts"), ("GT", ("field", "@", "weight_grams"), ("value", 100))),
        )
        self.assertEqual(
            compile_specification(StoredPartsContext(), self.heavy),
            (
                'EXISTS (SELECT 1 FROM unnest("something_parts") AS "something_part_1"'
                ' WHERE "something_part_1"."weight_grams" > $1)',
                [100],
            ),
        )

    def test_a_collection_of_an_item(self):
        self.assertEqual(
            describe(self.nested.accept(TransformVisitor(StoredPartsContext()))),
            (
                "any",
                ("$", "something_parts"),
                (
                    "any",
                    (("@", "detail"), "sub_parts"),
                    ("GT", ("field", "@", "weight_grams"), ("value", 5)),
                ),
            ),
        )

    def test_a_context_that_does_not_say_keeps_it_where_it_is(self):
        self.assertEqual(
            describe(self.nested.accept(TransformVisitor(PartsContext()))),
            (
                "any",
                ("$", "parts"),
                ("any", ("@", "parts"), ("GT", ("field", "@", "weight_grams"), ("value", 5))),
            ),
        )

    def test_a_context_may_say_of_one_root_and_not_of_the_other(self):
        self.assertEqual(
            describe(self.nested.accept(TransformVisitor(CandidateCollectionsContext()))),
            (
                "any",
                ("$", "something_parts"),
                ("any", ("@", "parts"), ("GT", ("field", "@", "weight_grams"), ("value", 5))),
            ),
        )

    def test_the_whole_path_is_asked_about(self):
        in_the_store = Wildcard(
            Object(Object(GlobalScope(), "warehouse"), "shelves"), item("weight"),
        )

        class Recording(PartsContext):
            def __init__(self) -> None:
                self.asked: list[list[str]] = []

            @override
            def collection_node(self, path: list[str]) -> EmptiableObject:
                self.asked.append(path)
                return Object(GlobalScope(), "warehouse_shelves")

        context = Recording()
        transformed = in_the_store.accept(TransformVisitor(context))
        self.assertEqual(context.asked, [["warehouse", "shelves"]])
        self.assertEqual(
            describe(transformed),
            ("any", ("$", "warehouse_shelves"), ("field", "@", "weight_grams")),
        )

    def test_a_refusal_of_the_context_is_not_hidden(self):
        unknown = Wildcard(Object(GlobalScope(), "wheels"), item("weight"))
        with self.assertRaises(ValueError):
            unknown.accept(TransformVisitor(StoredPartsContext()))

    def test_the_schema_names_the_collection_as_the_storage_does(self):
        # The query is compiled of the transformed tree: a schema is of the
        # storage, and knows the collection by the name it has there.
        schema = SchemaRegistry("things").with_parent_alias("t").register_relational(
            "something_parts", "parts", "thing_id", "id",
        )
        transformed = self.heavy.accept(TransformVisitor(StoredPartsContext()))
        self.assertEqual(
            sql(transformed, schema),
            'EXISTS (SELECT 1 FROM "parts" AS "something_part_1"'
            ' WHERE "something_part_1"."thing_id" = "t"."id" AND "something_part_1"."weight_grams" > $1)',
        )


class MemberId:
    """An identity of the domain that is two columns in the storage."""

    def __init__(self, tenant_id: int, member_id: int):
        self.tenant_id = tenant_id
        self.member_id = member_id


class MembersContext(ITransformContext):
    """A mapping of a domain whose `id` is composite, of a member and of an item."""

    @override
    def attr_node(self, path: list[str]) -> Any:
        if path == ["id"]:
            return CompositeExpression(field("tenant_id"), field("member_id"))
        return field(path[-1])

    @override
    def item_attr_node(self, path: list[str]) -> Any:
        if path == ["id"]:
            return CompositeExpression(item("tenant_id"), item("member_id"))
        return item(path[-1])

    @override
    def value_node(self, val: Any) -> Any:
        if isinstance(val, MemberId):
            return CompositeExpression(Value(val.tenant_id), Value(val.member_id))
        return Value(val)


class TestACompositeIsNotANode(unittest.TestCase):
    """A composite was a node of the tree whose ``accept`` raised
    NotImplementedError: the transformer had to catch every one before
    anything visited it, and caught those under ``=`` and ``!=`` only. One
    left elsewhere went into the tree and raised from inside the SQL
    compiler, saying nothing of where it came from. A composite is what a
    mapping may return instead of a node, and the transformer's result is a
    node: one that is left over is the transformer's error.
    """

    def test_a_composite_is_not_visitable(self):
        composite = CompositeExpression(field("tenant_id"), field("member_id"))
        self.assertNotIsInstance(composite, Visitable)
        self.assertFalse(hasattr(composite, "accept"))

    def test_an_equality_of_composites_is_a_node(self):
        by_id = Equal(field("id"), Value(MemberId(10, 3)))
        transformed = transform(MembersContext(), by_id)
        self.assertIsInstance(transformed, Visitable)
        self.assertEqual(
            compile_to_sql(transformed),
            ('"tenant_id" = $1 AND "member_id" = $2', [10, 3]),
        )
        self.assertEqual(
            compile_specification(MembersContext(), by_id),
            ('"tenant_id" = $1 AND "member_id" = $2', [10, 3]),
        )

    def test_inside_the_predicate_of_a_collection(self):
        of_member = Wildcard(
            Object(GlobalScope(), "members"), Equal(item("id"), Value(MemberId(10, 3))),
        )
        self.assertEqual(
            compile_specification(MembersContext(), of_member),
            (
                'EXISTS (SELECT 1 FROM unnest("members") AS "member_1"'
                ' WHERE "member_1"."tenant_id" = $1 AND "member_1"."member_id" = $2)',
                [10, 3],
            ),
        )

    def test_a_composite_where_a_node_is_needed_is_the_transformers_error(self):
        member = Value(MemberId(10, 3))
        cases = (
            (field("id"), "the whole specification"),
            (Not(member), "under a prefix operator"),
            (IsNull(field("id")), "under a postfix operator"),
            (Wildcard(Object(GlobalScope(), "members"), item("id")), "the predicate of a collection"),
        )
        for node, where in cases:
            with self.subTest(where=where):
                with self.assertRaises(ValueError) as raised:
                    transform(MembersContext(), node)
                self.assertIn("composite", str(raised.exception).lower())

    def test_a_composite_compares_with_a_composite(self):
        cases = (
            Equal(field("id"), Value(5)),
            # On the right alone it used to go into the tree as it was.
            Equal(Value(5), field("id")),
            And(field("active"), field("id")),
        )
        for node in cases:
            with self.subTest(operator=node.operator().name, left=type(node.left()).__name__):
                with self.assertRaises(CompositeExpressionsDifferentLengthError):
                    transform(MembersContext(), node)

    def test_a_composite_of_one_part_is_that_part(self):
        # It was an error of And, which takes two operands and more: "At
        # least one right operand is required", from inside the comparison.
        one = CompositeExpression(field("id")) == CompositeExpression(Value(1))
        self.assertEqual(compile_to_sql(one), ('"id" = $1', [1]))
        other = CompositeExpression(field("id")) != CompositeExpression(Value(1))
        self.assertEqual(compile_to_sql(other), ('NOT "id" = $1', [1]))

    def test_a_composite_of_no_parts_is_refused_by_name(self):
        # It was an IndexError.
        with self.assertRaises(CompositeExpressionIsEmptyError):
            CompositeExpression() == CompositeExpression()
        with self.assertRaises(CompositeExpressionIsEmptyError):
            CompositeExpression() != CompositeExpression()

    def test_parts_of_different_shapes_do_not_compare(self):
        flat = CompositeExpression(Value(1), Value(2))
        nested = CompositeExpression(CompositeExpression(Value(3), Value(4)), Value(5))
        # Of one length, and the first part of one is itself composite. With
        # the nested one on the right this built `1 = <composite>`.
        for left, right in ((nested, flat), (flat, nested)):
            with self.subTest(left=len(left._nodes)):
                with self.assertRaises(CompositeExpressionsDifferentLengthError):
                    left == right
                with self.assertRaises(CompositeExpressionsDifferentLengthError):
                    left != right

    def test_the_error_is_one_class_wherever_it_is_imported_from(self):
        from ascetic_ddd.specification.infrastructure import (
            composite_expression_node,
            transform_visitor,
        )
        self.assertIs(
            composite_expression_node.CompositeExpressionsDifferentLengthError,
            transform_visitor.CompositeExpressionsDifferentLengthError,
        )

    def test_a_composite_takes_only_equality(self):
        with self.assertRaises(ValueError):
            transform(MembersContext(), GreaterThan(field("id"), Value(MemberId(10, 3))))


class TestWhatAContextMustSayAndWhatItMay(unittest.TestCase):
    """The context was a Protocol: what a mapping lacked was found at the
    first specification that needed it, and what it may leave out could have
    no answer of its own. It is an interface to inherit: what a mapping must
    say is abstract, and what it may say has the interface's answer.
    """

    def test_a_context_that_does_not_say_what_it_must_cannot_be_made(self):
        class OfFieldsOnly(ITransformContext):
            @override
            def attr_node(self, path: list[str]) -> Visitable:
                return field(path[-1])

        with self.assertRaises(TypeError):
            OfFieldsOnly()  # type: ignore[abstract]

    def test_the_least_a_context_says(self):
        class Least(ITransformContext):
            @override
            def attr_node(self, path: list[str]) -> Visitable:
                return field("stored_" + path[-1])

            @override
            def value_node(self, val: Any) -> Visitable:
                return Value(val)

        ranked = GreaterThan(field("rank"), Value(3))
        self.assertEqual(
            describe(ranked.accept(TransformVisitor(Least()))),
            ("GT", ("field", "$", "stored_rank"), ("value", 3)),
        )
        # A collection stays where it is, at any depth and from either root.
        deep = Wildcard(
            Object(Object(GlobalScope(), "warehouse"), "shelves"),
            Wildcard(Object(Object(Item(), "box"), "parts"), GreaterThan(field("rank"), Value(1))),
        )
        self.assertEqual(
            describe(deep.accept(TransformVisitor(Least()))),
            (
                "any",
                (("$", "warehouse"), "shelves"),
                (
                    "any",
                    (("@", "box"), "parts"),
                    ("GT", ("field", "$", "stored_rank"), ("value", 1)),
                ),
            ),
        )

    def test_the_fields_of_an_item_have_no_answer_but_the_contexts(self):
        class Least(ITransformContext):
            @override
            def attr_node(self, path: list[str]) -> Visitable:
                return field(path[-1])

            @override
            def value_node(self, val: Any) -> Visitable:
                return Value(val)

        heavy = Wildcard(Object(GlobalScope(), "parts"), GreaterThan(item("weight"), Value(1)))
        # Left as it is, the field would reach the query under the domain's name.
        with self.assertRaises(NotImplementedError) as raised:
            heavy.accept(TransformVisitor(Least()))
        self.assertIn("weight", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
