import unittest

from ascetic_ddd.option import Option, Some, Nothing


class TestSome(unittest.TestCase):

    def test_int(self):
        o = Some(42)
        self.assertTrue(o.is_some())
        self.assertFalse(o.is_nothing())
        self.assertEqual(42, o.unwrap())

    def test_string(self):
        o = Some("hello")
        self.assertTrue(o.is_some())
        self.assertEqual("hello", o.unwrap())

    def test_zero_value_is_valid(self):
        o = Some(0)
        self.assertTrue(o.is_some())
        self.assertEqual(0, o.unwrap())

    def test_empty_string_is_valid(self):
        o = Some("")
        self.assertTrue(o.is_some())
        self.assertEqual("", o.unwrap())

    def test_false_is_valid(self):
        o = Some(False)
        self.assertTrue(o.is_some())
        self.assertEqual(False, o.unwrap())


class TestNothing(unittest.TestCase):

    def test_is_none(self):
        o: Option[int] = Nothing()
        self.assertTrue(o.is_nothing())
        self.assertFalse(o.is_some())


class TestIsSomeAnd(unittest.TestCase):

    def test_some_is_asked(self):
        self.assertIs(Some(11).is_some_and(lambda held: held > 10), True)
        self.assertIs(Some(5).is_some_and(lambda held: held > 10), False)

    def test_nothing_is_not(self):
        def never(held):
            raise AssertionError("asked of nothing")

        self.assertIs(Nothing().is_some_and(never), False)


class TestIsNothingOr(unittest.TestCase):

    def test_some_is_asked(self):
        self.assertIs(Some(11).is_nothing_or(lambda held: held > 10), True)
        self.assertIs(Some(5).is_nothing_or(lambda held: held > 10), False)

    def test_nothing_is(self):
        def never(held):
            raise AssertionError("asked of nothing")

        self.assertIs(Nothing().is_nothing_or(never), True)


class TestUnwrap(unittest.TestCase):

    def test_some_returns_value(self):
        self.assertEqual(42, Some(42).unwrap())

    def test_nothing_raises(self):
        with self.assertRaises(ValueError) as ctx:
            Nothing().unwrap()
        self.assertEqual("called unwrap on a Nothing Option", str(ctx.exception))


class TestUnwrapOr(unittest.TestCase):

    def test_some_returns_value(self):
        self.assertEqual(42, Some(42).unwrap_or(0))

    def test_nothing_returns_default(self):
        o: Option[int] = Nothing()
        self.assertEqual(99, o.unwrap_or(99))


class TestUnwrapOrElse(unittest.TestCase):

    def test_some_returns_value_without_calling(self):
        called = []
        result = Some(42).unwrap_or_else(lambda: called.append(True) or 99)
        self.assertEqual(42, result)
        self.assertEqual([], called)

    def test_nothing_calls_closure(self):
        o: Option[int] = Nothing()
        self.assertEqual(99, o.unwrap_or_else(lambda: 99))


class TestMap(unittest.TestCase):

    def test_some_applies_function(self):
        result = Some(42).map(lambda v: "value: %d" % v)
        self.assertTrue(result.is_some())
        self.assertEqual("value: 42", result.unwrap())

    def test_nothing_returns_nothing(self):
        called = []
        result = Nothing().map(lambda v: called.append(True))
        self.assertTrue(result.is_nothing())
        self.assertEqual([], called)


class TestMapOr(unittest.TestCase):

    def test_some_applies_function(self):
        result = Some(3).map_or(0, lambda v: v * v)
        self.assertEqual(9, result)

    def test_nothing_returns_default(self):
        result = Nothing().map_or(42, lambda v: v * v)
        self.assertEqual(42, result)


class TestAndThen(unittest.TestCase):

    def test_some_chains(self):
        result = Some(2).and_then(lambda v: Some(v * v))
        self.assertTrue(result.is_some())
        self.assertEqual(4, result.unwrap())

    def test_some_to_nothing(self):
        result = Some(0).and_then(
            lambda v: Nothing() if v == 0 else Some(100 // v)
        )
        self.assertTrue(result.is_nothing())

    def test_nothing_short_circuits(self):
        called = []
        result = Nothing().and_then(lambda v: called.append(True) or Some(v))
        self.assertTrue(result.is_nothing())
        self.assertEqual([], called)


class TestOr(unittest.TestCase):

    def test_some_returns_self(self):
        result = Some(42) | Some(99)
        self.assertEqual(42, result.unwrap())

    def test_nothing_returns_alternative(self):
        o: Option[int] = Nothing()
        result = o| Some(99)
        self.assertEqual(99, result.unwrap())

    def test_both_nothing(self):
        o: Option[int] = Nothing()
        result = o| Nothing()
        self.assertTrue(result.is_nothing())


class TestOrElse(unittest.TestCase):

    def test_some_returns_self_without_calling(self):
        called = []
        result = Some(42).or_else(lambda: called.append(True) or Some(99))
        self.assertEqual(42, result.unwrap())
        self.assertEqual([], called)

    def test_nothing_calls_closure(self):
        o: Option[int] = Nothing()
        result = o.or_else(lambda: Some(99))
        self.assertEqual(99, result.unwrap())


class TestEquality(unittest.TestCase):

    def test_some_equal(self):
        self.assertEqual(Some(42), Some(42))

    def test_some_not_equal(self):
        self.assertNotEqual(Some(42), Some(99))

    def test_nothing_equal(self):
        self.assertEqual(Nothing(), Nothing())

    def test_some_not_equal_nothing(self):
        self.assertNotEqual(Some(42), Nothing())

    def test_nothing_not_equal_some(self):
        self.assertNotEqual(Nothing(), Some(42))

    def test_not_equal_to_other_types(self):
        self.assertNotEqual(Some(42), 42)


class TestHash(unittest.TestCase):

    def test_some_hashable(self):
        s = {Some(1), Some(2), Some(1)}
        self.assertEqual(2, len(s))

    def test_nothing_hashable(self):
        s = {Nothing(), Nothing()}
        self.assertEqual(1, len(s))

    def test_some_and_nothing_distinct(self):
        s = {Some(1), Nothing()}
        self.assertEqual(2, len(s))


class TestRepr(unittest.TestCase):

    def test_some(self):
        self.assertEqual("Some(42)", repr(Some(42)))
        self.assertEqual("Some('hello')", repr(Some("hello")))

    def test_nothing(self):
        self.assertEqual("Nothing", repr(Nothing()))


class TestStr(unittest.TestCase):

    def test_some(self):
        self.assertEqual("Some(42)", str(Some(42)))
        self.assertEqual("Some(hello)", str(Some("hello")))

    def test_nothing(self):
        self.assertEqual("Nothing", str(Nothing()))


class TestChaining(unittest.TestCase):

    def test_map_then_and_then(self):
        result = (
            Some(5)
            .map(lambda v: v * 2)
            .and_then(lambda v: Some("big") if v > 5 else Some("small"))
        )
        self.assertEqual("big", result.unwrap())

    def test_nothing_propagates_through_chain(self):
        result = (
            Nothing()
            .map(lambda v: v * 2)
            .and_then(lambda v: Some("should not reach"))
        )
        self.assertTrue(result.is_nothing())


class TestStructType(unittest.TestCase):

    def test_some_dict(self):
        user = {"name": "Alice", "age": 30}
        o = Some(user)
        self.assertTrue(o.is_some())
        self.assertEqual(user, o.unwrap())

    def test_map_dict_to_field(self):
        user = {"name": "Alice", "age": 30}
        name = Some(user).map(lambda u: u["name"])
        self.assertEqual("Alice", name.unwrap())


if __name__ == "__main__":
    unittest.main()
