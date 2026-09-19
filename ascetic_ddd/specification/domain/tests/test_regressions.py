"""Regression tests of the defects found while porting the package to Rust.

Each test names the defect it pins, and what the code did before the fix.
"""
import datetime
import decimal
import unittest
from typing import Any

from ascetic_ddd.specification.domain.constants import OPERATOR
from ascetic_ddd.specification.domain.evaluate_visitor import (
    CollectionContext,
    EvaluateVisitor,
)
from ascetic_ddd.specification.domain.nodes import (
    Add, And, Div, Equal, Field, GlobalScope, GreaterThan, GreaterThanEqual,
    Is, IsNotNull, IsNull, Item, LeftShift, LessThan, LessThanEqual, Mod, Mul,
    Neg, Not, NotEqual, Object, Or, RightShift, Sub, Value, Wildcard,
)


class DictContext:
    """Dictionary-based context for testing."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def get(self, key: str) -> Any:
        """Get value by key."""
        return self._data[key]


def evaluate(node: Any, data: dict[str, Any] | None = None) -> Any:
    return node.accept(EvaluateVisitor(DictContext(data or {})))


def division_by_zero() -> Any:
    """A boolean expression whose evaluation raises ZeroDivisionError."""
    return Equal(Div(Value(1), Value(0)), Value(1))


class TestUnaryOperatorsAreOperatorsOfTheirOwn(unittest.TestCase):
    """``POS = "+"`` and ``NEG = "-"`` repeated the values of ``ADD`` and
    ``SUB``, and an ``Enum`` makes a member with a repeated value an alias:
    ``OPERATOR.NEG is OPERATOR.SUB`` was true, so there was no unary minus.
    """

    def test_neg_is_not_sub(self):
        self.assertIsNot(OPERATOR.NEG, OPERATOR.SUB)

    def test_there_is_no_unary_plus(self):
        # It was the other alias, of ADD. Nothing made a node of it - a lambda's
        # `+x` is `x` - and every reader of the tree had to know it: an
        # operator has a node and a notation that writes it, or is not one.
        self.assertNotIn("POS", OPERATOR.__members__)

    def test_every_operator_has_a_value_of_its_own(self):
        self.assertEqual(len(OPERATOR.__members__), len(list(OPERATOR)))

    def test_neg_is_evaluated(self):
        cases = (
            (Neg(Value(5)), -5),
            (Neg(Neg(Value(5))), 5),
            (Sub(Value(5), Neg(Value(3))), 8),
            (Neg(Add(Value(1), Value(2))), -3),
            (Neg(Field(GlobalScope(), "balance")), -7),
        )
        for node, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(evaluate(node, {"balance": 7}), expected)


class TestConnectivesStopWhenDecided(unittest.TestCase):
    """``AND`` and ``OR`` evaluated both operands always, so the idiom
    ``a != 0 and 10 / a > 1`` raised for ``a == 0`` — in the tree, while the
    lambda the tree was parsed from returned False.
    """

    def test_a_false_left_decides_and(self):
        self.assertIs(evaluate(And(Value(False), division_by_zero())), False)

    def test_a_true_left_decides_or(self):
        self.assertIs(evaluate(Or(Value(True), division_by_zero())), True)

    def test_an_undecided_left_evaluates_the_right(self):
        with self.assertRaises(ZeroDivisionError):
            evaluate(And(Value(True), division_by_zero()))
        with self.assertRaises(ZeroDivisionError):
            evaluate(Or(Value(False), division_by_zero()))

    def test_the_left_is_always_evaluated(self):
        with self.assertRaises(ZeroDivisionError):
            evaluate(And(division_by_zero(), Value(False)))

    def test_the_guard_idiom(self):
        guarded = And(
            Not(Equal(Field(GlobalScope(), "a"), Value(0))),
            GreaterThan(Div(Value(10), Field(GlobalScope(), "a")), Value(1)),
        )
        self.assertIs(evaluate(guarded, {"a": 0}), False)
        self.assertIs(evaluate(guarded, {"a": 5}), True)

    def test_the_truth_tables_are_what_they_were(self):
        for left in (True, False):
            for right in (True, False):
                with self.subTest(left=left, right=right):
                    self.assertIs(evaluate(And(Value(left), Value(right))), left and right)
                    self.assertIs(evaluate(Or(Value(left), Value(right))), left or right)


class TestWildcardStopsAtItsFirstWitness(unittest.TestCase):
    """A wildcard evaluated its predicate for every item, where Python's
    ``any`` — which the lambda it may have been parsed from uses — stops at
    the first item that satisfies it.
    """

    def setUp(self):
        self.dear = Wildcard(
            Object(GlobalScope(), "items"),
            GreaterThan(Field(Item(), "price"), Value(500)),
        )

    def store(self, *prices: Any) -> dict[str, Any]:
        return {"items": CollectionContext([DictContext({"price": price}) for price in prices])}

    def test_items_after_the_witness_are_not_evaluated(self):
        # A text is not comparable with a number: evaluating it would raise.
        self.assertIs(evaluate(self.dear, self.store(999, "n/a")), True)

    def test_items_before_the_witness_are(self):
        with self.assertRaises(TypeError):
            evaluate(self.dear, self.store("n/a", 999))

    def test_no_witness(self):
        self.assertIs(evaluate(self.dear, self.store(1, 2)), False)
        self.assertIs(evaluate(self.dear, self.store()), False)


INT64_MAX = 2 ** 63 - 1
INT64_MIN = -2 ** 63


class TestNullsFollowSql(unittest.TestCase):
    """The evaluator had no logic of nulls: ``None == 1`` was False and
    ``not None`` was True, where the query the same tree compiles to has NULL
    for both, so an object could satisfy in memory a specification that its
    row does not satisfy in the database, and the reverse.
    """

    def test_a_null_operand_makes_a_null_result(self):
        cases = (
            Equal(Value(None), Value(1)),
            Equal(Value(None), Value(None)),
            NotEqual(Value(1), Value(None)),
            LessThan(Value(None), Value(1)),
            GreaterThanEqual(Value(1), Value(None)),
            Add(Value(1), Value(None)),
            Div(Value(None), Value(0)),
            LeftShift(Value(None), Value(1)),
            Neg(Value(None)),
            Not(Value(None)),
        )
        for node in cases:
            with self.subTest(operator=node.operator().name):
                self.assertIsNone(evaluate(node))

    def test_the_connectives_are_three_valued(self):
        cases = (
            (And(Value(True), Value(True)), True),
            (And(Value(True), Value(False)), False),
            (And(Value(None), Value(False)), False),
            (And(Value(False), Value(None)), False),
            (And(Value(None), Value(True)), None),
            (And(Value(True), Value(None)), None),
            (And(Value(None), Value(None)), None),
            (Or(Value(False), Value(False)), False),
            (Or(Value(False), Value(True)), True),
            (Or(Value(None), Value(True)), True),
            (Or(Value(True), Value(None)), True),
            (Or(Value(None), Value(False)), None),
            (Or(Value(False), Value(None)), None),
            (Not(Value(True)), False),
            (Not(Value(False)), True),
        )
        for node, expected in cases:
            with self.subTest(node=node.operator().name, expected=expected):
                self.assertIs(evaluate(node), expected)

    def test_a_null_left_does_not_decide(self):
        with self.assertRaises(ZeroDivisionError):
            evaluate(And(Value(None), division_by_zero()))
        with self.assertRaises(ZeroDivisionError):
            evaluate(Or(Value(None), division_by_zero()))

    def test_a_connective_takes_truth_values(self):
        for node in (And(Value(1), Value(True)), Or(Value(False), Value("x")), Not(Value(1))):
            with self.subTest(operator=node.operator().name):
                with self.assertRaises(TypeError):
                    evaluate(node)

    def test_is_and_is_null_are_never_null(self):
        cases = (
            (Is(Value(True), Value(True)), True),
            (Is(Value(True), Value(False)), False),
            (Is(Value(None), Value(None)), True),
            (Is(Value(None), Value(True)), False),
            (Is(Value(1), Value(None)), False),
            (IsNull(Value(None)), True),
            (IsNull(Value(42)), False),
            (IsNotNull(Value(42)), True),
            (IsNotNull(Value(None)), False),
            (IsNull(Equal(Value(1), Value(None))), True),
        )
        for node, expected in cases:
            with self.subTest(operator=node.operator().name, expected=expected):
                self.assertIs(evaluate(node), expected)

    def test_a_null_predicate_is_no_witness(self):
        store = {"items": CollectionContext([DictContext({"price": 999})])}
        unknown = Wildcard(
            Object(GlobalScope(), "items"),
            GreaterThan(Field(Item(), "price"), Value(None)),
        )
        # As EXISTS, a wildcard is true or false, never null.
        self.assertIs(evaluate(unknown, store), False)
        self.assertIs(evaluate(Not(unknown), store), True)

    def test_a_predicate_is_a_truth_value(self):
        store = {"items": CollectionContext([DictContext({"price": 999})])}
        with self.assertRaises(TypeError):
            evaluate(Wildcard(Object(GlobalScope(), "items"), Field(Item(), "price")), store)


class TestValuesOfDifferentKindsDoNotCompare(unittest.TestCase):
    """``"a" == 1`` was False and ``True == 1`` was True, where the query the
    same tree compiles to has "operator does not exist": a specification that
    compares a text with a number is wrong, and only one of its two readers
    said so.
    """

    def test_what_postgresql_has_no_operator_for(self):
        cases = (
            Equal(Value("a"), Value(1)),
            NotEqual(Value(1), Value("a")),
            Equal(Value(True), Value(1)),
            LessThan(Value(True), Value(2)),
            GreaterThan(Value("a"), Value(1.5)),
            Is(Value("a"), Value(1)),
        )
        for node in cases:
            with self.subTest(operator=node.operator().name):
                with self.assertRaises(TypeError):
                    evaluate(node)

    def test_what_it_has(self):
        cases = (
            (Equal(Value(1), Value(1.0)), True),
            (LessThan(Value(1), Value(1.5)), True),
            (Equal(Value("a"), Value("a")), True),
            (LessThan(Value("a"), Value("b")), True),
            # False comes before true.
            (GreaterThan(Value(True), Value(False)), True),
            (Is(Value(1), Value(1.0)), True),
            # NaN equals itself and is greater than any other number.
            (Equal(Value(float("nan")), Value(float("nan"))), True),
            (NotEqual(Value(float("nan")), Value(1.0)), True),
            (GreaterThan(Value(float("nan")), Value(1.7976931348623157e308)), True),
            (LessThan(Value(float("nan")), Value(1)), False),
            (LessThanEqual(Value(1), Value(float("nan"))), True),
        )
        for node, expected in cases:
            with self.subTest(operator=node.operator().name, expected=expected):
                self.assertIs(evaluate(node), expected)

    def test_other_types_compare_as_they_define(self):
        day = datetime.timedelta(days=1)
        self.assertIs(evaluate(LessThan(Value(day), Value(day * 2))), True)
        self.assertIs(
            evaluate(Equal(Value(decimal.Decimal("1.5")), Value(1.5))), True,
        )


class TestArithmeticIsPostgresqls(unittest.TestCase):
    """The evaluator computed as Python does: ``7 / 2`` was 3.5, ``-7 % 2``
    was 1, integers had no bounds and ``1 << 64`` was a number, where the
    query the same tree compiles to has 3, -1, "bigint out of range" and 1.
    """

    def test_integers(self):
        cases = (
            (Add(Value(5), Value(3)), 8),
            (Sub(Value(5), Value(3)), 2),
            (Mul(Value(5), Value(3)), 15),
            # Integer division truncates, towards zero.
            (Div(Value(7), Value(2)), 3),
            (Div(Value(-7), Value(2)), -3),
            (Div(Value(7), Value(-2)), -3),
            # The remainder has the sign of the dividend.
            (Mod(Value(7), Value(2)), 1),
            (Mod(Value(-7), Value(2)), -1),
            (Mod(Value(7), Value(-2)), 1),
            (Mod(Value(INT64_MIN), Value(-1)), 0),
            (Neg(Value(5)), -5),
            # The count of a shift is taken modulo 64.
            (LeftShift(Value(1), Value(3)), 8),
            (LeftShift(Value(1), Value(64)), 1),
            (LeftShift(Value(1), Value(-1)), INT64_MIN),
            (LeftShift(Value(1), Value(63)), INT64_MIN),
            (RightShift(Value(8), Value(2)), 2),
            (RightShift(Value(8), Value(65)), 4),
            (RightShift(Value(-8), Value(1)), -4),
        )
        for node, expected in cases:
            with self.subTest(operator=node.operator().name, expected=expected):
                result = evaluate(node)
                self.assertEqual(result, expected)
                self.assertIs(type(result), int)

    def test_floats(self):
        cases = (
            (Div(Value(7.0), Value(2)), 3.5),
            (Div(Value(7), Value(2.0)), 3.5),
            (Add(Value(1), Value(0.5)), 1.5),
            (Mul(Value(2.5), Value(4)), 10.0),
            (Neg(Value(2.5)), -2.5),
        )
        for node, expected in cases:
            with self.subTest(operator=node.operator().name, expected=expected):
                self.assertEqual(evaluate(node), expected)

    def test_what_does_not_fit_is_an_error(self):
        cases = (
            Add(Value(INT64_MAX), Value(1)),
            Sub(Value(INT64_MIN), Value(1)),
            Mul(Value(INT64_MAX), Value(2)),
            Div(Value(INT64_MIN), Value(-1)),
            Neg(Value(INT64_MIN)),
            Mul(Value(1.7976931348623157e308), Value(2.0)),
        )
        for node in cases:
            with self.subTest(operator=node.operator().name):
                with self.assertRaises(OverflowError):
                    evaluate(node)

    def test_a_division_by_zero_is_an_error(self):
        for node in (Div(Value(1), Value(0)), Mod(Value(1), Value(0)), Div(Value(1.0), Value(0.0))):
            with self.subTest(operator=node.operator().name):
                with self.assertRaises(ZeroDivisionError):
                    evaluate(node)

    def test_what_postgresql_has_no_operator_for(self):
        cases = (
            Mod(Value(5.5), Value(2)),
            LeftShift(Value(1.5), Value(1)),
            Add(Value(True), Value(1)),
            Neg(Value(True)),
            Add(Value("a"), Value("b")),
            Mul(Value("a"), Value(3)),
        )
        for node in cases:
            with self.subTest(operator=node.operator().name):
                with self.assertRaises(TypeError):
                    evaluate(node)

    def test_other_types_compute_as_they_define(self):
        day = datetime.timedelta(days=1)
        noon = datetime.datetime(2026, 1, 1, 12)
        cases = (
            (Sub(Value(noon + day), Value(noon)), day),
            (Add(Value(noon), Value(day)), noon + day),
            (Neg(Value(day)), -day),
            (Div(Value(decimal.Decimal("7")), Value(decimal.Decimal("2"))), decimal.Decimal("3.5")),
            (Add(Value(1), Value(decimal.Decimal("0.5"))), decimal.Decimal("1.5")),
        )
        for node, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(evaluate(node), expected)


if __name__ == "__main__":
    unittest.main()
