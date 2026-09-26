"""Regression tests of the defects found while porting the package to Rust.

Each test names the defect it pins, and what the code did before the fix.
"""
import unittest
from typing import Any

from ascetic_ddd.specification.domain.evaluate_visitor import CollectionContext, DictContext, EvaluateVisitor
from ascetic_ddd.specification.domain.lambda_filter.lambda_parser import parse
from ascetic_ddd.specification.domain.tests.describing import describe

ADULT_AGE = 18


class Limits:
    """A value from outside the lambda that is reached by an attribute."""

    def __init__(self, max_price: int):
        self.max_price = max_price


def store(*items: dict[str, Any]) -> DictContext:
    return DictContext({"items": CollectionContext([DictContext(item) for item in items])})


def satisfied(spec: Any, candidate: DictContext) -> Any:
    return spec.accept(EvaluateVisitor(candidate))


def active(item: str = "@") -> Any:
    return ("field", item, "active")


class TestAllIsNotAny(unittest.TestCase):
    """``all(p for x in xs)`` was parsed into the same ``Wildcard`` as
    ``any(...)``: "every item is active" was read as "some item is active".
    It is "no item fails": ``NOT any(NOT p)``.
    """

    def test_all_over_a_generator(self):
        spec = parse(
            lambda s: all(item.active for item in s.items)
        )
        self.assertEqual(describe(spec), ("NOT", ("any", ("$", "items"), ("NOT", active()))))

    def test_all_over_a_list_comprehension(self):
        spec = parse(
            lambda s: all([item.active for item in s.items])
        )
        self.assertEqual(describe(spec), ("NOT", ("any", ("$", "items"), ("NOT", active()))))

    def test_any_is_what_it_was(self):
        spec = parse(
            lambda s: any(item.active for item in s.items)
        )
        self.assertEqual(describe(spec), ("any", ("$", "items"), active()))

    def test_all_agrees_with_the_lambda(self):
        predicate = (
            lambda s: all(item.active for item in s.items)
        )
        spec = parse(predicate)
        cases = (
            ((), True),
            (({"active": True}, {"active": True}), True),
            (({"active": True}, {"active": False}), False),
            (({"active": False}, {"active": False}), False),
        )
        for items, expected in cases:
            with self.subTest(items=items):
                self.assertIs(satisfied(spec, store(*items)), expected)

    def test_all_nested_in_any(self):
        spec = parse(
            lambda s: any(all(item.active for item in category.items) for category in s.categories)
        )
        self.assertEqual(
            describe(spec),
            ("any", ("$", "categories"), ("NOT", ("any", ("@", "items"), ("NOT", active())))),
        )


class TestUnaryMinus(unittest.TestCase):
    """``-x`` raised "Unsupported unary operator: USub": the tree had no
    negation to parse it into.
    """

    def test_a_negative_literal_is_a_value(self):
        spec = parse(
            lambda a: a.balance > -5
        )
        self.assertEqual(describe(spec), ("GT", ("field", "$", "balance"), ("value", -5)))

    def test_a_negated_member(self):
        spec = parse(
            lambda a: -a.balance < a.limit
        )
        self.assertEqual(
            describe(spec),
            ("LT", ("NEG", ("field", "$", "balance")), ("field", "$", "limit")),
        )
        self.assertIs(satisfied(spec, DictContext({"balance": 7, "limit": 0})), True)

    def test_unary_plus_is_its_operand(self):
        spec = parse(
            lambda a: +a.balance > 0
        )
        self.assertEqual(describe(spec), ("GT", ("field", "$", "balance"), ("value", 0)))


class TestValuesFromOutsideTheLambda(unittest.TestCase):
    """A name that is not the lambda's argument raised "Non-local variable",
    so a specification could not have a parameter: ``age > min_age`` had to be
    written with the number in it.
    """

    def test_a_variable_of_the_enclosing_function(self):
        min_age = 25
        spec = parse(
            lambda u: u.age > min_age
        )
        self.assertEqual(describe(spec), ("GT", ("field", "$", "age"), ("value", 25)))

    def test_a_variable_of_the_module(self):
        spec = parse(
            lambda u: u.age >= ADULT_AGE
        )
        self.assertEqual(describe(spec), ("GTE", ("field", "$", "age"), ("value", 18)))

    def test_an_attribute_of_a_variable(self):
        limits = Limits(max_price=500)
        spec = parse(
            lambda i: i.price <= limits.max_price
        )
        self.assertEqual(describe(spec), ("LTE", ("field", "$", "price"), ("value", 500)))

    def test_a_variable_inside_a_collection_predicate(self):
        min_price = 500
        spec = parse(
            lambda s: any(item.price > min_price for item in s.items)
        )
        self.assertEqual(
            describe(spec),
            ("any", ("$", "items"), ("GT", ("field", "@", "price"), ("value", 500))),
        )

    def test_the_value_is_taken_when_the_lambda_is_parsed(self):
        min_age = 25
        spec = parse(
            lambda u: u.age > min_age
        )
        min_age = 99
        self.assertEqual(describe(spec), ("GT", ("field", "$", "age"), ("value", 25)))
        self.assertEqual(min_age, 99)

    def test_a_name_that_is_nowhere_is_still_an_error(self):
        with self.assertRaises(ValueError):
            parse(
                lambda u: u.age > undefined_name  # noqa: F821
            )

    def test_a_module_variable_does_not_stand_in_for_an_outer_item(self):
        # ``ADULT_AGE`` is a variable of this module, and here it is also the
        # name of the outer item: the item, as Python reads it. It used to be
        # read as the module's value, and compiled to a wrong specification.
        spec = parse(
            lambda s: any(any(i.age > ADULT_AGE.limit for i in ADULT_AGE.items) for ADULT_AGE in s.groups)
        )
        self.assertEqual(
            describe(spec),
            ("any", ("$", "groups"), ("any", ("@", "items"), (
                "GT", ("field", "@", "age"), ("field", "@1", "limit"),
            ))),
        )

    def test_the_item_of_an_outer_collection_is_named_by_how_far_out_it_is(self):
        # The tree used to have one "@", the nearest, and an outer item could
        # not be named: ``Item(1)`` is the item one collection out.
        spec = parse(
            lambda s: any(any(i.price > c.limit and c.limit < s.limit for i in c.items) for c in s.categories)
        )
        self.assertEqual(
            describe(spec),
            ("any", ("$", "categories"), ("any", ("@", "items"), ("AND",
                ("GT", ("field", "@", "price"), ("field", "@1", "limit")),
                ("LT", ("field", "@1", "limit"), ("field", "$", "limit")),
            ))),
        )

    def test_what_an_outer_item_holds_is_a_member_of_it(self):
        spec = parse(
            lambda s: any(
                c.discount.is_some_and(lambda d: any(i.price > d for i in c.items))
                for c in s.categories
            )
        )
        self.assertEqual(
            describe(spec),
            ("any", ("$", "categories"), ("AND", ("IS_NOT_NULL", ("field", "@", "discount")), (
                "any", ("@", "items"), ("GT", ("field", "@", "price"), ("field", "@1", "discount")),
            ))),
        )

    def test_two_collections_out(self):
        spec = parse(
            lambda s: any(
                any(any(t.weight > c.limits.max for t in i.tags) for i in c.items)
                for c in s.categories
            )
        )
        self.assertEqual(
            describe(spec),
            ("any", ("$", "categories"), ("any", ("@", "items"), ("any", ("@", "tags"), (
                "GT", ("field", "@", "weight"), ("field", (("@2", "limits")), "max"),
            )))),
        )


class TestNoneIsTestedNotCompared(unittest.TestCase):
    """In the tree a comparison with null is null, as in SQL, and is true of
    nothing; ``u.email == None`` of a lambda is true of a None. What the
    lambda means is IS NULL. ``is None`` raised "Unsupported comparison".
    """

    def test_none_spelled_out(self):
        email = ("field", "$", "email")
        cases = (
            (parse(
                lambda u: u.email == None  # noqa: E711
            ), ("IS_NULL", email)),
            (parse(
                lambda u: u.email is None
            ), ("IS_NULL", email)),
            (parse(
                lambda u: u.email != None  # noqa: E711
            ), ("IS_NOT_NULL", email)),
            (parse(
                lambda u: u.email is not None
            ), ("IS_NOT_NULL", email)),
        )
        for spec, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(describe(spec), expected)

    def test_a_variable_that_is_none(self):
        email = None
        spec = parse(
            lambda u: u.email == email
        )
        self.assertEqual(describe(spec), ("IS_NULL", ("field", "$", "email")))

    def test_the_lambda_and_its_tree_agree(self):
        predicate = (
            lambda u: u.email is None
        )
        spec = parse(predicate)
        self.assertIs(satisfied(spec, DictContext({"email": None})), True)
        self.assertIs(satisfied(spec, DictContext({"email": "a@b"})), False)


class TestTheCandidateInsideACollectionPredicate(unittest.TestCase):
    """Inside ``any(... for item in s.items)`` the lambda's argument raised
    "Non-local variable": the parser kept one name, and the item's replaced
    it. The tree has the node for it — a field of the global scope.
    """

    def test_a_member_of_the_candidate(self):
        predicate = (
            lambda s: any(item.name == s.name for item in s.items)
        )
        spec = parse(predicate)
        self.assertEqual(
            describe(spec),
            ("any", ("$", "items"), ("EQ", ("field", "@", "name"), ("field", "$", "name"))),
        )
        named = DictContext({
            "name": "Pen",
            "items": CollectionContext([DictContext({"name": "Ink"}), DictContext({"name": "Pen"})]),
        })
        self.assertIs(satisfied(spec, named), True)


if __name__ == "__main__":
    unittest.main()
