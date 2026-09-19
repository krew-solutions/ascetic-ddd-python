"""Regression tests of the defects found while porting the package to Rust.

Each test names the defect it pins, and what the code did before the fix.

The template language had three parsers: the native one, and two that
converted the tree of a library, jsonpath-rfc9535 and jsonpath2. The two
were removed. They shared every defect of the language below and had more of
their own - the objects on the way to a collection dropped, a placeholder
after a nested collection given another's parameter, `||` and `&&` read left
to right, a negation at the start of a filter refused - and one that could not
be mended: a placeholder was a magic value put into the text, `-8765432109876`
or `"__JSONPATH_PLACEHOLDER_a1b2c3d4__"`, so a template with that literal in
it was refused by one and answered wrongly, in silence, by the other. The
tests written against all three stay, as tests of the language.
"""
import decimal
import unittest
from typing import Any

from ascetic_ddd.specification.domain import nodes
from ascetic_ddd.specification.domain.evaluate_visitor import (
    CollectionContext,
)
from ascetic_ddd.specification.domain.jsonpath import jsonpath_parser
from ascetic_ddd.specification.domain.jsonpath.jsonpath_parser import (
    JSONPathError,
    JSONPathSyntaxError,
    JSONPathTypeError,
)
from ascetic_ddd.specification.domain.tests.describing import describe
from ascetic_ddd.specification.infrastructure.postgresql_visitor import compile_to_sql

# What reads the template language; see the note above.
PARSERS = (
    ("native", jsonpath_parser),
)


class Context:
    """Nested dictionaries and lists as contexts."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def get(self, key: str) -> Any:
        """Get value by key."""
        value = self._data[key]
        if isinstance(value, dict):
            return Context(value)
        if isinstance(value, list):
            return CollectionContext([Context(item) for item in value])
        return value


def store() -> Context:
    return Context({
        "name": "Pen",
        "price": 700,
        "limit": 100,
        "a": 1,
        "b": 2,
        "items": [{"price": 5}],
        "store": {"items": [{"price": 999}]},
        "categories": [
            {"limit": 50, "shelf": {"items": [{"price": 60}]}},
        ],
    })


class TestAPathIsNotDropped(unittest.TestCase):
    """``$.store.items[?@.price > 600]`` was read as ``$[?@.price > 600]``: the
    path was dropped and the filter applied to the candidate, whose own
    ``price`` decided the match.
    """

    def test_a_filter_on_a_path_needs_its_wildcard(self):
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                with self.assertRaises(JSONPathError):
                    parser.parse("$.store.items[?@.price > 600]").match(store())

    def test_a_filter_on_a_nested_path_needs_its_wildcard(self):
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                with self.assertRaises(JSONPathError):
                    parser.parse(
                        "$.categories[*][?@.shelf.items[?@.price > 1]]"
                    ).match(store())

    def test_the_objects_on_the_way_to_a_collection_are_kept(self):
        # ``$.store.items`` was read as ``$.items`` by the two parsers that
        # converted a library's tree: only the last name was kept.
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                spec = parser.parse("$.store.items[*][?@.price > %d]")
                self.assertIs(spec.match(store(), (600,)), True)
                self.assertIs(spec.match(store(), (1000,)), False)

    def test_the_objects_on_the_way_to_a_nested_collection_are_kept(self):
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                spec = parser.parse("$.categories[*][?@.shelf.items[*][?@.price > %d]]")
                self.assertIs(spec.match(store(), (55,)), True)
                self.assertIs(spec.match(store(), (65,)), False)


class TestTheCandidateInsideAFilter(unittest.TestCase):
    """``$`` inside a filter was refused by every parser, though the tree
    has the node for it — a field of the global scope — and both the
    evaluator and the SQL compiler read it.
    """

    def test_a_member_of_the_candidate(self):
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                self.assertIs(parser.parse("$.items[*][?@.price < $.limit]").match(store()), True)
                self.assertIs(parser.parse("$.items[*][?@.price > $.limit]").match(store()), False)

    def test_inside_a_nested_collection(self):
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                spec = parser.parse("$.categories[*][?@.shelf.items[*][?@.price < $.limit]]")
                self.assertIs(spec.match(store()), True)

    def test_the_tree(self):
        spec = jsonpath_parser.parse("$.items[*][?@.price < $.limit]")
        self.assertEqual(
            describe(spec.bind()),
            ("any", ("$", "items"), ("LT", ("field", "@", "price"), ("field", "$", "limit"))),
        )


class TestPlaceholdersOfOneStyle(unittest.TestCase):
    """A template with ``%s`` and ``%(n)s`` has no parameters it could be
    matched with: a tuple has no names and a mapping no positions. The
    placeholders were listed named first and bound in the order they stand,
    so such a template took the wrong parameter, or none, and said nothing.
    """

    def test_a_mixed_template_is_refused_when_parsed(self):
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                with self.assertRaises(JSONPathSyntaxError):
                    parser.parse("$[?@.a == %s && @.b == %(n)s]")

    def test_the_native_error_points_at_the_second_style(self):
        with self.assertRaises(JSONPathSyntaxError) as raised:
            jsonpath_parser.parse("$[?@.a == %s && @.b == %(n)s]")
        self.assertEqual(raised.exception.position, 23)

    def test_a_placeholder_in_a_string_is_text(self):
        # The native parser found placeholders with a regular expression over
        # the whole template, string literals included, and counted this one.
        spec = jsonpath_parser.parse("$[?@.name != '%(x)s' && @.b == %d]")
        self.assertIs(spec.match(store(), (2,)), True)
        self.assertIs(spec.match(store(), (3,)), False)

    def test_placeholders_are_bound_in_the_order_they_stand(self):
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                spec = parser.parse("$[?@.a == %d && @.b == %d]")
                self.assertIs(spec.match(store(), (1, 2)), True)
                self.assertIs(spec.match(store(), (2, 1)), False)
                named = parser.parse("$[?@.a == %(a)d && @.b == %(b)d]")
                self.assertIs(named.match(store(), {"b": 2, "a": 1}), True)

    def test_a_placeholder_after_a_nested_collection(self):
        # The parser that converted jsonpath2's tree dropped the count of the
        # placeholders bound inside a nested collection, so the one after it
        # took the parameter of the one inside: (50, 55) matched.
        template = "$.categories[*][?@.shelf.items[*][?@.price > %d] && @.limit == %d]"
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                spec = parser.parse(template)
                self.assertIs(spec.match(store(), (55, 50)), True)
                self.assertIs(spec.match(store(), (55, 51)), False)
                self.assertIs(spec.match(store(), (50, 55)), False)

    def test_a_parameter_that_is_missing_is_an_error(self):
        # The native parser left the placeholder's marker in the tree, where
        # it compared unequal to everything: the match was a silent False.
        cases = (
            ("$[?@.a == %d && @.b == %d]", (1,)),
            ("$[?@.a == %(a)d]", {"b": 1}),
            ("$[?@.a == %(a)d]", (1,)),
            ("$[?@.a == %d]", {"a": 1}),
        )
        for name, parser in PARSERS:
            for template, params in cases:
                with self.subTest(parser=name, template=template):
                    with self.assertRaises((JSONPathError, ValueError)):
                        parser.parse(template).match(store(), params)


class TestNegationAtTheStartOfAFilter(unittest.TestCase):
    """The parser that converted jsonpath2's tree refused ``$[?!(...)]``: the
    library wants a filter in parentheses, and they were added only to a
    filter that starts with ``@``.
    """

    def test_it_is_read(self):
        cases = (
            ("$[?!(@.a == 1)]", False),
            ("$[?!(@.a == 2)]", True),
            ("$[?!@.flag]", True),
            ("$.items[*][?!(@.price > 10)]", True),
            # A member as a test by itself, which the same parser refused too.
            ("$[?@.flag]", False),
            ("$[?@.flag || @.a == 1]", True),
            ("$.items[*][?@.detail.listed]", True),
        )
        candidate = Context({
            "a": 1, "flag": False, "items": [{"price": 5, "detail": {"listed": True}}],
        })
        for name, parser in PARSERS:
            for template, expected in cases:
                with self.subTest(parser=name, template=template):
                    self.assertIs(parser.parse(template).match(candidate), expected)


class TestAPlaceholderSaysWhatItTakes(unittest.TestCase):
    """The letter of a placeholder was read and never used: ``%d`` took a
    string as readily as an integer, so parameters given in the wrong order
    were a comparison that is false, or a TypeError from somewhere inside.
    ``%d`` takes an integer, ``%f`` a number, ``%s`` a value of any type, as
    Python's ``%s`` does; a None fits any.
    """

    def test_what_fits(self):
        cases = (
            ("$[?@.a == %d]", (1,), True),
            ("$[?@.a == %f]", (1.0,), True),
            ("$[?@.a == %f]", (1,), True),
            ("$[?@.a == %f]", (decimal.Decimal(1),), True),
            ("$[?@.a == %s]", (1,), True),
            ("$[?@.name == %s]", ("x",), True),
            ("$[?@.a == %(a)d]", {"a": 1}, True),
            ("$[?@.a == %d]", (None,), False),
        )
        candidate = Context({"a": 1, "name": "x"})
        for name, parser in PARSERS:
            for template, params, expected in cases:
                with self.subTest(parser=name, template=template, params=params):
                    self.assertIs(parser.parse(template).match(candidate, params), expected)

    def test_what_does_not(self):
        cases = (
            ("$[?@.a == %d]", ("1",)),
            ("$[?@.a == %d]", (1.5,)),
            ("$[?@.a == %d]", (True,)),
            ("$[?@.a == %f]", ("1.5",)),
            ("$[?@.a == %f]", (True,)),
            ("$[?@.a == %(a)d]", {"a": "1"}),
            # The parameters of another order
            ("$[?@.name == %s && @.a == %d]", (1, "x")),
        )
        candidate = Context({"a": 1, "name": "x"})
        for name, parser in PARSERS:
            for template, params in cases:
                with self.subTest(parser=name, template=template, params=params):
                    with self.assertRaises(JSONPathTypeError):
                        parser.parse(template).match(candidate, params)


class TestATemplateIsAFunctionOfItsParameters(unittest.TestCase):
    """A placeholder was a Value whose value was a marker, the tuple
    ``("__PLACEHOLDER__", index)``: a value of a shape no value is supposed
    to have, which every reader of the tree took for a value. The tree of a
    template compiled to a query with the marker for a parameter, and
    evaluated to a comparison with a tuple: False, or a TypeError from inside.

    Then it was a node of its own, ``Placeholder``, which the readers refused.
    But a placeholder is a word of the template language and not of a
    specification: a lambda and a tree built by hand have none, and every
    reader of the tree, a user's own included, had to have a method whose one
    purpose was to refuse it. A template is a translation that waits for its
    parameters, and is kept as that: a function of them. The tree comes of
    binding, with values in it, so there is no tree with a placeholder for a
    reader to be handed by mistake.
    """

    def test_a_specification_has_no_word_for_a_placeholder(self):
        self.assertFalse(hasattr(nodes, "Placeholder"))
        self.assertFalse(hasattr(nodes.Visitor, "visit_placeholder"))

    def test_a_template_that_is_not_bound_is_read_by_no_one(self):
        # The one way to a tree asks for the parameters.
        spec = jsonpath_parser.parse("$[?@.age > %d]")
        with self.assertRaises(JSONPathSyntaxError) as raised:
            spec.bind()
        self.assertIn("Missing positional parameter", raised.exception.message)

    def test_a_bound_template_has_values(self):
        spec = jsonpath_parser.parse("$[?@.age > %d]")
        bound = spec.bind((25,))
        self.assertEqual(describe(bound), ("GT", ("field", "$", "age"), ("value", 25)))
        self.assertEqual(compile_to_sql(bound), ("age > $1", [25]))
        # The template is what it was: bound again, to something else.
        again = spec.bind((65,))
        self.assertEqual(describe(again), ("GT", ("field", "$", "age"), ("value", 65)))

    def test_binding_is_the_way_to_a_query(self):
        # The tree of a template could be had through a private attribute
        # alone, and was not bound.
        spec = jsonpath_parser.parse("$.items[*][?@.price > %(price)f && @.owner == %(owner)s]")
        self.assertEqual(
            compile_to_sql(spec.bind({"price": 9.5, "owner": "ann"})),
            (
                "EXISTS (SELECT 1 FROM unnest(items) AS item_1"
                " WHERE item_1.price > $1 AND item_1.owner = $2)",
                [9.5, "ann"],
            ),
        )
        # What a parameter is decides what the query is: see the null test.
        self.assertEqual(
            compile_to_sql(spec.bind({"price": 9.5, "owner": None})),
            (
                "EXISTS (SELECT 1 FROM unnest(items) AS item_1"
                " WHERE item_1.price > $1 AND item_1.owner IS NULL)",
                [9.5],
            ),
        )

    def test_a_placeholder_under_any_operator_is_bound(self):
        # `%s == null` is parsed into IS NULL of the placeholder: a postfix
        # operator, which the binder used to pass by.
        spec = jsonpath_parser.parse("$[?%s == null || !(@.a == %d)]")
        self.assertIs(spec.match(store(), (None, 1)), True)
        self.assertIs(spec.match(store(), (5, 1)), False)
        self.assertIs(spec.match(store(), (5, 2)), True)

    def test_a_tuple_is_a_value_like_any_other(self):
        spec = jsonpath_parser.parse("$[?@.pair == %s]")
        candidate = Context({"pair": ("__PLACEHOLDER__", 0)})
        self.assertIs(spec.match(candidate, (("__PLACEHOLDER__", 0),)), True)
        self.assertIs(spec.match(candidate, (("__PLACEHOLDER__", 1),)), False)


class TestALiteralIsNeverAPlaceholder(unittest.TestCase):
    """The removed parsers put a magic value into the text for a placeholder
    and looked for it in the library's tree, so a template with that value
    for a literal was refused by one and answered wrongly by the other.
    """

    def test_the_markers_of_the_removed_parsers_are_values(self):
        candidate = Context({
            "a": -8765432109876, "b": 7, "s": "__JSONPATH_PLACEHOLDER_a1b2c3d4__",
        })
        cases = (
            "$[?@.a == -8765432109876 && @.b == %d]",
            "$[?@.s == '__JSONPATH_PLACEHOLDER_a1b2c3d4__' && @.b == %d]",
        )
        for name, parser in PARSERS:
            for template in cases:
                with self.subTest(parser=name, template=template):
                    self.assertIs(parser.parse(template).match(candidate, (7,)), True)
                    self.assertIs(parser.parse(template).match(candidate, (8,)), False)


class TestNullIsTestedNotCompared(unittest.TestCase):
    """In the tree a comparison with null is null, as in SQL, and is true of
    nothing. In JSONPath null is a value, and ``@.a == null`` is how a null
    is found: what it means is IS NULL, and that is what it is parsed into,
    spelled out or bound to a placeholder.
    """

    def setUp(self):
        self.deleted = Context({"deleted_at": None, "name": "x"})
        self.alive = Context({"deleted_at": 5, "name": "x"})

    def test_null_spelled_out(self):
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                is_null = parser.parse("$[?@.deleted_at == null]")
                is_not_null = parser.parse("$[?@.deleted_at != null]")
                self.assertIs(is_null.match(self.deleted), True)
                self.assertIs(is_null.match(self.alive), False)
                self.assertIs(is_not_null.match(self.deleted), False)
                self.assertIs(is_not_null.match(self.alive), True)

    def test_null_bound_to_a_placeholder(self):
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                spec = parser.parse("$[?@.deleted_at == %s]")
                self.assertIs(spec.match(self.deleted, (None,)), True)
                self.assertIs(spec.match(self.alive, (None,)), False)
                self.assertIs(spec.match(self.alive, (5,)), True)
                self.assertIs(spec.match(self.deleted, (5,)), False)

    def test_the_tree(self):
        self.assertEqual(
            describe(jsonpath_parser.parse("$[?@.deleted_at == null]").bind()),
            ("IS_NULL", ("field", "$", "deleted_at")),
        )
        self.assertEqual(
            describe(jsonpath_parser.parse("$[?null != @.deleted_at]").bind()),
            ("IS_NOT_NULL", ("field", "$", "deleted_at")),
        )

    def test_a_null_member_compared_with_a_value_does_not_match(self):
        # `deleted_at > 1` is null for a null member, and so is its negation:
        # neither selects the row, in memory as in the database.
        for name, parser in PARSERS:
            with self.subTest(parser=name):
                self.assertIs(parser.parse("$[?@.deleted_at > 1]").match(self.deleted), False)
                self.assertIs(parser.parse("$[?(!(@.deleted_at > 1))]").match(self.deleted), False)
                self.assertIs(parser.parse("$[?@.deleted_at == 5]").match(self.deleted), False)
                self.assertIs(parser.parse("$[?@.deleted_at != 5]").match(self.deleted), False)


# A backslash, spelled so that no tool between the author and the file reads
# an escape of its own in the templates below.
BS = chr(92)


class TestLiteralsAreThoseOfRfc9535(unittest.TestCase):
    """A string was a quote, anything but that quote, and a quote: nothing
    could be escaped, so a string with quotes of both kinds could not be
    written at all, and ``%sn`` was a backslash and an ``n``. A number had no
    exponent: ``1e3`` was the number 1 and a name. The Rust port reads both as
    RFC 9535 has them, so a template of one port was a syntax error in the
    other.
    """ % BS

    def parsed(self, literal: str) -> Any:
        tree = jsonpath_parser.parse("$[?@.a == %s]" % literal).bind()
        return tree.right().value()

    def test_escapes(self):
        cases = (
            ("'it" + BS + "'s'", "it's"),
            ('"say ' + BS + '"hi' + BS + '""', 'say "hi"'),
            ("'say " + '"it' + BS + "'s" + '"' + "'", 'say "it' + "'" + 's"'),
            ("'a" + BS + BS + "b'", "a" + BS + "b"),
            ("'a" + BS + "/b'", "a/b"),
            ("'a" + BS + "nb" + BS + "tc" + BS + "rd'", "a" + chr(10) + "b" + chr(9) + "c" + chr(13) + "d"),
            ("'" + BS + "b" + BS + "f'", chr(8) + chr(12)),
            ("'" + BS + "u0041" + BS + "u00e9'", "A" + chr(0xE9)),
            # A code point beyond the basic plane is a pair of escapes.
            ("'" + BS + "uD83D" + BS + "uDE00'", chr(0x1F600)),
            ("'plain'", "plain"),
            ("''", ""),
        )
        for literal, expected in cases:
            with self.subTest(literal=literal):
                self.assertEqual(self.parsed(literal), expected)

    def test_what_is_not_an_escape_is_refused(self):
        cases = (
            ("'a" + BS + "qb'", 15, "an unknown escape"),
            ("'" + BS + "u00zz'", 14, "not four hexadecimal digits"),
            ("'" + BS + "uD83Dx'", 14, "a high surrogate alone"),
            ("'" + BS + "uDE00'", 14, "a low surrogate alone"),
        )
        for literal, position, why in cases:
            with self.subTest(why=why):
                with self.assertRaises(JSONPathSyntaxError) as raised:
                    jsonpath_parser.parse("$[?@.name == %s]" % literal)
                self.assertEqual(raised.exception.message, "Invalid escape")
                self.assertEqual(raised.exception.position, position)

    def test_a_string_left_open(self):
        with self.assertRaises(JSONPathSyntaxError) as raised:
            jsonpath_parser.parse("$[?@.name == 'open]")
        self.assertEqual(raised.exception.message, "Unterminated string")
        self.assertEqual(raised.exception.position, 13)

    def test_numbers(self):
        cases = (
            ("30", 30, int),
            ("-1", -1, int),
            ("1.5", 1.5, float),
            ("1e3", 1000.0, float),
            ("3E1", 30.0, float),
            ("300e-1", 30.0, float),
            ("-2.5e+2", -250.0, float),
        )
        for literal, expected, kind in cases:
            with self.subTest(literal=literal):
                value = self.parsed(literal)
                self.assertEqual(value, expected)
                self.assertIs(type(value), kind)

    def test_digits_are_those_of_ascii(self):
        # `\d` of a `str` pattern is any decimal digit of Unicode, and `int`
        # reads them: the Rust port and Go's `\d` take those of ASCII only.
        three = chr(0x0663)  # ARABIC-INDIC DIGIT THREE
        for literal in (three, "1." + three, "1e" + three):
            with self.subTest(literal=ascii(literal)):
                with self.assertRaises(JSONPathSyntaxError):
                    jsonpath_parser.parse("$[?@.a > %s]" % literal)

    def test_a_number_that_does_not_fit_is_refused(self):
        # A literal is a bigint or a double precision, as it is in the query.
        for literal in ("99999999999999999999", "-99999999999999999999", "1e999"):
            with self.subTest(literal=literal):
                with self.assertRaises(JSONPathSyntaxError) as raised:
                    jsonpath_parser.parse("$[?@.a > %s]" % literal)
                self.assertEqual(raised.exception.message, "Number out of range")
                self.assertEqual(raised.exception.position, 9)

    def test_a_number_matches(self):
        spec = jsonpath_parser.parse("$[?@.price > 1e3 && @.name == 'it" + BS + "'s']")
        self.assertIs(spec.match(Context({"price": 1500, "name": "it's"})), True)
        self.assertIs(spec.match(Context({"price": 999, "name": "it's"})), False)


class TestAPlaceholderIsNamedInAscii(unittest.TestCase):
    """``\\w`` of a ``str`` pattern is any letter or digit of Unicode; the Rust
    port and Go's ``\\w`` take those of ASCII only, so a template of one port
    was a syntax error in another.
    """

    def test_a_name_outside_ascii_is_refused(self):
        name = "n" + chr(0x00E9)  # LATIN SMALL LETTER E WITH ACUTE
        with self.assertRaises(JSONPathSyntaxError):
            jsonpath_parser.parse("$[?@.a == %(" + name + ")s]")

    def test_a_name_of_letters_digits_and_underscores(self):
        spec = jsonpath_parser.parse("$[?@.a == %(min_age_2)d]")
        self.assertIs(spec.match(Context({"a": 5}), {"min_age_2": 5}), True)


class TestTheGrammarIsClosed(unittest.TestCase):
    """The native parser stepped over a bracket or a parenthesis "if
    present", so a template with one missing, or one too many, was read as if
    it were well formed.
    """

    def test_what_is_not_a_template_is_refused(self):
        cases = (
            ("$[?@.age > 1", 12, "a bracket left open"),
            ("$[?(@.age > 1]", 13, "a parenthesis left open"),
            ("$[?@.age > 1)]", 12, "a parenthesis never opened"),
            ("$[?@.age > 1]]", 13, "a bracket too many"),
            ("$[?@.age > 1] extra", 14, "text after the filter"),
            ("$[@.age > 1]", 2, "a filter without its question mark"),
            ("@.age > 1", 0, "no root"),
            ("$[?age > 1]", 3, "a name without @ or $"),
            ("$[?@.a == 1 == 2]", 12, "a comparison of a comparison"),
            ("$.items[*]", 10, "a wildcard without a filter"),
        )
        for template, position, why in cases:
            with self.subTest(why=why, template=template):
                with self.assertRaises(JSONPathSyntaxError) as raised:
                    jsonpath_parser.parse(template)
                self.assertEqual(raised.exception.position, position)

    def test_what_was_a_template_still_is(self):
        cases = (
            ("$[?@.age > 1]", ("GT", ("field", "$", "age"), ("value", 1))),
            ("$[?(@.age > 1)]", ("GT", ("field", "$", "age"), ("value", 1))),
            ("$[?!(@.age > 1)]", ("NOT", ("GT", ("field", "$", "age"), ("value", 1)))),
            ("$[?!@.active]", ("NOT", ("field", "$", "active"))),
            ("$[?@.active]", ("field", "$", "active")),
            (
                "$[?(@.a > 1 || @.b > 2) && @.c > 3]",
                (
                    "AND",
                    (
                        "OR",
                        ("GT", ("field", "$", "a"), ("value", 1)),
                        ("GT", ("field", "$", "b"), ("value", 2)),
                    ),
                    ("GT", ("field", "$", "c"), ("value", 3)),
                ),
            ),
            (
                "$.categories[*][?@.items[*][?@.price > 100]]",
                (
                    "any",
                    ("$", "categories"),
                    ("any", ("@", "items"), ("GT", ("field", "@", "price"), ("value", 100))),
                ),
            ),
        )
        for template, expected in cases:
            with self.subTest(template=template):
                self.assertEqual(describe(jsonpath_parser.parse(template).bind()), expected)

    def test_either_side_of_a_comparison_is_an_operand(self):
        spec = jsonpath_parser.parse("$[?%d < @.a]")
        self.assertIs(spec.match(store(), (0,)), True)
        self.assertIs(spec.match(store(), (1,)), False)
        spec = jsonpath_parser.parse("$[?@.a < @.b]")
        self.assertIs(spec.match(store()), True)


if __name__ == "__main__":
    unittest.main()
