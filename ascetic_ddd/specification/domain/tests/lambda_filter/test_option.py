"""An ``Option`` inside a lambda: what its methods and its makers are in the tree.

An ``Option`` is what it holds, or a null, to both readers of a tree. So a
lambda that asks one whether it holds anything asks the null test, and one
that takes what it holds names the member itself.
"""
import unittest
from typing import Any

from ascetic_ddd import option
from ascetic_ddd.option import Nothing, Some
from ascetic_ddd.option import Some as Holding
from ascetic_ddd.specification.domain.evaluate_visitor import DictContext, EvaluateVisitor
from ascetic_ddd.specification.domain.lambda_filter.lambda_parser import parse
from ascetic_ddd.specification.domain.tests.describing import describe

DISCOUNT = ("field", "$", "discount")
OUTSIDE, EMPTY = Some(5), Nothing()


class Candidate:
    """What the lambda itself is called with: an object with attributes."""

    def __init__(self, discount: option.Option[int]):
        self.discount = discount


def satisfied(spec: Any, discount: option.Option[int]) -> Any:
    return spec.accept(EvaluateVisitor(DictContext({"discount": discount})))


class TestAnOptionIsAsked(unittest.TestCase):
    """``a.discount.is_nothing()`` raised "Unsupported function call"."""

    def test_whether_it_holds_anything_is_the_null_test(self):
        cases = (
            (parse(
                lambda a: a.discount.is_nothing()
            ), ("IS_NULL", DISCOUNT)),
            (parse(
                lambda a: a.discount.is_some()
            ), ("IS_NOT_NULL", DISCOUNT)),
            (parse(
                lambda a: not a.discount.is_nothing()
            ), ("NOT", ("IS_NULL", DISCOUNT))),
        )
        for spec, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(describe(spec), expected)

    def test_what_it_holds_is_the_member(self):
        spec = parse(
            lambda a: a.discount.is_some() and a.discount.unwrap() > 10
        )
        self.assertEqual(
            describe(spec),
            ("AND", ("IS_NOT_NULL", DISCOUNT), ("GT", DISCOUNT, ("value", 10))),
        )

    def test_the_member_of_an_item(self):
        spec = parse(
            lambda s: any(item.discount.is_nothing() for item in s.items)
        )
        self.assertEqual(
            describe(spec),
            ("any", ("$", "items"), ("IS_NULL", ("field", "@", "discount"))),
        )

    def test_a_method_takes_no_arguments(self):
        for predicate in (
            lambda a: a.discount.is_nothing(1),
            lambda a: a.discount.unwrap(1) > 10,
        ):
            with self.subTest():
                with self.assertRaises(ValueError):
                    parse(predicate)

    def test_a_default_has_no_node(self):
        with self.assertRaises(ValueError):
            parse(
                lambda a: a.discount.unwrap_or(0) > 10
            )


class TestAnOptionIsMade(unittest.TestCase):
    """``a.discount == Some(5)`` raised "Unsupported function call"."""

    def test_some_is_what_it_holds_and_nothing_is_the_null(self):
        cases = (
            (parse(
                lambda a: a.discount == Some(5)
            ), ("EQ", DISCOUNT, ("value", 5))),
            (parse(
                lambda a: a.discount == Nothing()
            ), ("IS_NULL", DISCOUNT)),
            (parse(
                lambda a: a.discount != Nothing()
            ), ("IS_NOT_NULL", DISCOUNT)),
            (parse(
                lambda a: Nothing() == a.discount
            ), ("IS_NULL", DISCOUNT)),
        )
        for spec, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(describe(spec), expected)

    def test_a_maker_is_told_by_what_it_is_not_by_its_spelling(self):
        cases = (
            (parse(
                lambda a: a.discount == Holding(5)
            ), ("EQ", DISCOUNT, ("value", 5))),
            (parse(
                lambda a: a.discount == option.Some(5)
            ), ("EQ", DISCOUNT, ("value", 5))),
            (parse(
                lambda a: a.discount == option.Nothing()
            ), ("IS_NULL", DISCOUNT)),
        )
        for spec, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(describe(spec), expected)

    def test_another_function_of_the_same_name_is_not_the_maker(self):
        def Some(value: int) -> int:  # noqa: N802
            return value

        with self.assertRaises(ValueError):
            parse(
                lambda a: a.discount == Some(5)
            )

    def test_some_of_a_member(self):
        spec = parse(
            lambda a: a.discount == Some(a.price)
        )
        self.assertEqual(describe(spec), ("EQ", DISCOUNT, ("field", "$", "price")))

    def test_a_maker_takes_what_it_takes(self):
        for predicate in (
            lambda a: a.discount == Some(),  # type: ignore[call-arg]
            lambda a: a.discount == Nothing(1),  # type: ignore[call-arg]
        ):
            with self.subTest():
                with self.assertRaises(ValueError):
                    parse(predicate)


class TestWhatAnOptionHoldsIsAskedUnderAName(unittest.TestCase):
    """``a.discount.is_some_and(lambda d: d > 10)`` raised "Unsupported
    function call". It is the null test and the predicate, of the same member:
    of two values, as it is to the lambda, so the two agree under ``not`` too.
    """

    def test_of_a_member(self):
        cases = (
            (parse(
                lambda a: a.discount.is_some_and(lambda d: d > 10)
            ), ("AND", ("IS_NOT_NULL", DISCOUNT), ("GT", DISCOUNT, ("value", 10)))),
            (parse(
                lambda a: a.discount.is_nothing_or(lambda d: d > 10)
            ), ("OR", ("IS_NULL", DISCOUNT), ("GT", DISCOUNT, ("value", 10)))),
        )
        for spec, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(describe(spec), expected)

    def test_a_member_of_what_is_held(self):
        spec = parse(
            lambda a: a.discount.is_some_and(lambda d: d.percent > 10)
        )
        percent = ("field", ("$", "discount"), "percent")
        self.assertEqual(
            describe(spec),
            ("AND", ("IS_NOT_NULL", DISCOUNT), ("GT", percent, ("value", 10))),
        )

    def test_of_the_member_of_an_item_beside_the_item(self):
        spec = parse(
            lambda s: any(item.discount.is_some_and(lambda d: d * 10 > item.price) for item in s.items)
        )
        discount = ("field", "@", "discount")
        self.assertEqual(
            describe(spec),
            ("any", ("$", "items"), ("AND", ("IS_NOT_NULL", discount), (
                "GT", ("MUL", discount, ("value", 10)), ("field", "@", "price"),
            ))),
        )

    def test_the_name_is_the_nearest_of_that_name(self):
        spec = parse(
            lambda a: a.discount.is_some_and(lambda a: a > 10)
        )
        self.assertEqual(
            describe(spec),
            ("AND", ("IS_NOT_NULL", DISCOUNT), ("GT", DISCOUNT, ("value", 10))),
        )

    def test_of_an_option_from_outside(self):
        limit = Some(10)
        spec = parse(
            lambda a: limit.is_some_and(lambda held: a.discount.is_some_and(lambda d: d > held))
        )
        self.assertEqual(
            describe(spec),
            ("AND", ("IS_NOT_NULL", ("value", limit)), (
                "AND", ("IS_NOT_NULL", DISCOUNT), ("GT", DISCOUNT, ("value", limit)),
            )),
        )

    def test_what_an_outer_item_holds_is_a_member_of_it(self):
        # One collection out, from the inner predicate. It used to be refused.
        spec = parse(
            lambda s: any(i.discount.is_some_and(lambda d: any(t.weight > d for t in i.tags)) for i in s.items)
        )
        discount = ("field", "@", "discount")
        self.assertEqual(
            describe(spec),
            ("any", ("$", "items"), ("AND", ("IS_NOT_NULL", discount), (
                "any", ("@", "tags"), ("GT", ("field", "@", "weight"), ("field", "@1", "discount")),
            ))),
        )

    def test_the_predicate_is_a_lambda_of_what_is_held(self):
        def check(held: int) -> bool:
            return held > 10

        for predicate in (
            lambda a: a.discount.is_some_and(check),
            lambda a: a.discount.is_some_and(lambda: True),
            lambda a: a.discount.is_some_and(lambda d, e: d > e),
            lambda a: a.discount.is_some_and(lambda d=1: d > 10),
            lambda a: a.discount.is_some_and(),
        ):
            with self.subTest():
                with self.assertRaises(ValueError):
                    parse(predicate)


class TestAnOptionFromOutsideTheLambda(unittest.TestCase):

    def test_what_it_holds_is_the_option_which_is_its_value_or_a_null(self):
        limit = Some(10)
        spec = parse(
            lambda a: a.discount.unwrap() > limit.unwrap()
        )
        self.assertEqual(describe(spec), ("GT", DISCOUNT, ("value", limit)))

    def test_a_guard_is_kept(self):
        """``limit.unwrap()`` used to be taken when the lambda was parsed, and
        raised of a Nothing the lambda never unwraps: its guard comes first.
        """
        limit = Nothing()
        predicate = (
            lambda a: limit.is_some() and a.discount.is_some() and a.discount.unwrap() > limit.unwrap()
        )
        spec = parse(predicate)
        for discount in (Nothing(), Some(11)):
            with self.subTest(discount=discount):
                self.assertIs(predicate(Candidate(discount)), False)
                self.assertIs(satisfied(spec, discount), False)

    def test_compared_as_it_is(self):
        held, empty = Some(5), Nothing()
        self.assertEqual(
            describe(parse(
                lambda a: a.discount == empty
            )),
            ("IS_NULL", DISCOUNT),
        )
        spec = parse(
            lambda a: a.discount == held
        )
        self.assertIs(satisfied(spec, Some(5)), True)
        self.assertIs(satisfied(spec, Some(6)), False)


class TestTheLambdaAndItsTreeAgree(unittest.TestCase):

    def test_on_what_holds_and_on_what_does_not(self):
        predicates = (
            lambda a: a.discount.is_nothing(),
            lambda a: a.discount.is_some(),
            lambda a: not a.discount.is_nothing(),
            lambda a: a.discount.is_some() and a.discount.unwrap() > 10,
            lambda a: a.discount.is_nothing() or a.discount.unwrap() <= 10,
            lambda a: not (a.discount.is_some() and a.discount.unwrap() > 10),
            lambda a: a.discount == Some(5),
            lambda a: a.discount == Nothing(),
            lambda a: a.discount != Nothing(),
            lambda a: a.discount.is_some_and(lambda d: d > 10),
            lambda a: not a.discount.is_some_and(lambda d: d > 10),
            lambda a: a.discount.is_nothing_or(lambda d: d > 10),
            lambda a: not a.discount.is_nothing_or(lambda d: d > 10),
            lambda a: OUTSIDE.is_some_and(lambda held: a.discount.is_some_and(lambda d: d > held)),
            lambda a: not EMPTY.is_some_and(lambda held: a.discount.is_some_and(lambda d: d > held)),
            lambda a: EMPTY.is_nothing_or(lambda held: a.discount.is_some_and(lambda d: d > held)),
        )
        for number, predicate in enumerate(predicates):
            spec = parse(predicate)
            for discount in (Nothing(), Some(5), Some(10), Some(11)):
                with self.subTest(predicate=number, discount=discount):
                    self.assertIs(
                        satisfied(spec, discount) is True,
                        predicate(Candidate(discount)),
                    )

    def test_a_comparison_with_what_is_absent_is_null_as_in_sql(self):
        """Where they do not: the tree's logic is the storage's, of three
        values. To the lambda nothing differs from five; to the tree, as to
        SQL, that is not known, and a candidate it is not known of is not
        selected. Whoever means the absent too says so: ``is_nothing() or``.
        """
        predicate = (
            lambda a: a.discount != Some(5)
        )
        spec = parse(predicate)
        self.assertIs(predicate(Candidate(Nothing())), True)
        self.assertIsNone(satisfied(spec, Nothing()))
        meaning_the_absent = parse(
            lambda a: a.discount.is_nothing() or a.discount != Some(5)
        )
        self.assertIs(satisfied(meaning_the_absent, Nothing()), True)


if __name__ == "__main__":
    unittest.main()
