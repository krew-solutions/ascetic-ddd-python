import unittest

from ascetic_ddd.option import Some, Nothing
from ascetic_ddd.rop import (
    Result,
    Succeed,
    Fail,
    FailMany,
    from_exception,
    of_option,
    apply,
    map2,
    map3,
    map4,
    switch,
    tee,
    try_catch,
    plus,
    and_,
    compose,
    pipe,
)


class TestSucceed(unittest.TestCase):

    def test_int(self):
        r = Succeed(42)
        self.assertTrue(r.is_ok())
        self.assertFalse(r.is_error())
        self.assertEqual(42, r.unwrap())
        self.assertEqual([], r.errors())


class TestFail(unittest.TestCase):

    def test_single_error(self):
        r: Result[int, str] = Fail("bad")
        self.assertFalse(r.is_ok())
        self.assertTrue(r.is_error())
        self.assertEqual(["bad"], r.errors())


class TestFailMany(unittest.TestCase):

    def test_non_empty(self):
        r: Result[int, str] = FailMany(["a", "b"])
        self.assertTrue(r.is_error())
        self.assertEqual(["a", "b"], r.errors())

    def test_empty_raises(self):
        with self.assertRaises(ValueError) as ctx:
            FailMany([])
        self.assertEqual("FailMany: errs must be non-empty", str(ctx.exception))


class TestUnwrap(unittest.TestCase):

    def test_ok_returns_value(self):
        self.assertEqual(42, Succeed(42).unwrap())

    def test_error_raises(self):
        with self.assertRaises(ValueError) as ctx:
            Fail("bad").unwrap()
        self.assertEqual("called unwrap on a Failure Result", str(ctx.exception))


class TestUnwrapOr(unittest.TestCase):

    def test_ok_returns_value(self):
        self.assertEqual(42, Succeed(42).unwrap_or(0))

    def test_error_returns_default(self):
        r: Result[int, str] = Fail("bad")
        self.assertEqual(99, r.unwrap_or(99))


class TestUnwrapOrElse(unittest.TestCase):

    def test_ok_skips_closure(self):
        called = []
        v = Succeed(42).unwrap_or_else(lambda errs: called.append(True) or 0)
        self.assertEqual(42, v)
        self.assertEqual([], called)

    def test_error_invokes_closure_with_list(self):
        r: Result[int, str] = FailMany(["a", "b"])
        self.assertEqual(2, r.unwrap_or_else(lambda errs: len(errs)))


class TestEither(unittest.TestCase):

    def test_ok(self):
        out = Succeed(42).either(
            lambda v: "got value",
            lambda errs: "got errors",
        )
        self.assertEqual("got value", out)

    def test_error(self):
        out: str = Fail("bad").either(
            lambda v: "got value",
            lambda errs: ",".join(errs),
        )
        self.assertEqual("bad", out)


class TestMap(unittest.TestCase):

    def test_ok_applies_function(self):
        r = Succeed(3).map(lambda v: v * v)
        self.assertEqual(9, r.unwrap())

    def test_error_short_circuits(self):
        called = []
        r: Result[int, str] = Fail("bad")
        out = r.map(lambda v: called.append(True) or v)
        self.assertTrue(out.is_error())
        self.assertEqual(["bad"], out.errors())
        self.assertEqual([], called)

    def test_changes_value_type(self):
        r = Succeed(42).map(lambda v: "n=%d" % v)
        self.assertEqual("n=42", r.unwrap())


class TestAndThen(unittest.TestCase):

    def test_ok_chains(self):
        r = Succeed(3).and_then(lambda v: Succeed(v * 2))
        self.assertEqual(6, r.unwrap())

    def test_ok_to_error(self):
        r: Result[int, str] = Succeed(0).and_then(
            lambda v: Fail("div by zero") if v == 0 else Succeed(100 // v)
        )
        self.assertTrue(r.is_error())
        self.assertEqual(["div by zero"], r.errors())

    def test_error_short_circuits(self):
        called = []
        r: Result[int, str] = Fail("upstream")
        out = r.and_then(lambda v: called.append(True) or Succeed(v))
        self.assertTrue(out.is_error())
        self.assertEqual(["upstream"], out.errors())
        self.assertEqual([], called)


class TestBind(unittest.TestCase):
    """Bind is an alias for and_then — smoke-test both paths."""

    def test_ok_chains(self):
        r = Succeed(3).bind(lambda v: Succeed(v * 2))
        self.assertEqual(6, r.unwrap())

    def test_error_short_circuits(self):
        called = []
        r: Result[int, str] = Fail("upstream")
        out = r.bind(lambda v: called.append(True) or Succeed(v))
        self.assertEqual(["upstream"], out.errors())
        self.assertEqual([], called)


class TestBoth(unittest.TestCase):

    def test_both_ok(self):
        r = Succeed(1).both(Succeed("a"))
        self.assertEqual((1, "a"), r.unwrap())

    def test_left_error(self):
        ra: Result[int, str] = Fail("a-err")
        rb: Result[str, str] = Succeed("b")
        r = ra.both(rb)
        self.assertEqual(["a-err"], r.errors())

    def test_both_error_accumulates(self):
        ra: Result[int, str] = Fail("a-err")
        rb: Result[str, str] = Fail("b-err")
        r = ra.both(rb)
        self.assertEqual(["a-err", "b-err"], r.errors())


class TestDoubleMap(unittest.TestCase):

    def test_success_transforms_value(self):
        r = Succeed(3).double_map(
            lambda v: v * v,
            lambda e: ValueError(e),
        )
        self.assertEqual(9, r.unwrap())

    def test_failure_transforms_every_error(self):
        r: Result[int, str] = FailMany(["a", "b"])
        out = r.double_map(lambda v: v, lambda e: "E:" + e)
        self.assertEqual(["E:a", "E:b"], out.errors())


class TestOr(unittest.TestCase):

    def test_ok_returns_self(self):
        r = Succeed(1) | Succeed(2)
        self.assertEqual(1, r.unwrap())

    def test_error_returns_alt(self):
        r: Result[int, str] = Fail("bad")
        out = r | Succeed(2)
        self.assertEqual(2, out.unwrap())


class TestOrElse(unittest.TestCase):

    def test_ok_skips_closure(self):
        called = []
        r = Succeed(1).or_else(
            lambda errs: called.append(True) or Succeed(2)
        )
        self.assertEqual(1, r.unwrap())
        self.assertEqual([], called)

    def test_error_invokes_closure(self):
        r: Result[int, str] = Fail("bad")
        out = r.or_else(lambda errs: Succeed(len(errs)))
        self.assertEqual(1, out.unwrap())


class TestEquality(unittest.TestCase):

    def test_succeed_equal(self):
        self.assertEqual(Succeed(42), Succeed(42))

    def test_succeed_not_equal(self):
        self.assertNotEqual(Succeed(42), Succeed(99))

    def test_fail_equal(self):
        self.assertEqual(Fail("a"), Fail("a"))

    def test_succeed_not_equal_fail(self):
        self.assertNotEqual(Succeed(42), Fail("bad"))

    def test_not_equal_to_other_types(self):
        self.assertNotEqual(Succeed(42), 42)


class TestHash(unittest.TestCase):

    def test_succeed_hashable(self):
        s = {Succeed(1), Succeed(2), Succeed(1)}
        self.assertEqual(2, len(s))

    def test_fail_hashable(self):
        s = {Fail("a"), Fail("b"), Fail("a")}
        self.assertEqual(2, len(s))

    def test_succeed_and_fail_distinct(self):
        s = {Succeed(1), Fail("bad")}
        self.assertEqual(2, len(s))


class TestRepr(unittest.TestCase):

    def test_succeed(self):
        self.assertEqual("Ok(42)", repr(Succeed(42)))

    def test_fail(self):
        self.assertEqual("Error(['a', 'b'])", repr(FailMany(["a", "b"])))


class TestStr(unittest.TestCase):

    def test_succeed(self):
        self.assertEqual("Ok(42)", str(Succeed(42)))

    def test_fail(self):
        self.assertEqual("Error(['a', 'b'])", str(FailMany(["a", "b"])))


class TestFromException(unittest.TestCase):

    def test_no_exception(self):
        r = from_exception(42, None)
        self.assertTrue(r.is_ok())
        self.assertEqual(42, r.unwrap())

    def test_exception_failure_value_discarded(self):
        boom = ValueError("boom")
        r = from_exception(42, boom)
        self.assertTrue(r.is_error())
        self.assertEqual([boom], r.errors())


class TestOfOption(unittest.TestCase):

    def test_some_succeeds(self):
        r = of_option(Some(7), "missing")
        self.assertTrue(r.is_ok())
        self.assertEqual(7, r.unwrap())

    def test_nothing_fails_with_provided_error(self):
        r = of_option(Nothing(), "missing")
        self.assertTrue(r.is_error())
        self.assertEqual(["missing"], r.errors())


class TestApply(unittest.TestCase):

    def test_ok_ok(self):
        add2 = lambda x: x + 2
        r = apply(Succeed(add2), Succeed(3))
        self.assertEqual(5, r.unwrap())

    def test_err_ok(self):
        rf: Result = Fail("f bad")
        r = apply(rf, Succeed(3))
        self.assertEqual(["f bad"], r.errors())

    def test_ok_err(self):
        add2 = lambda x: x + 2
        rx: Result[int, str] = Fail("x bad")
        r = apply(Succeed(add2), rx)
        self.assertEqual(["x bad"], r.errors())

    def test_err_err_accumulates(self):
        rf: Result = Fail("f bad")
        rx: Result[int, str] = Fail("x bad")
        r = apply(rf, rx)
        self.assertEqual(["f bad", "x bad"], r.errors())


class TestMap2(unittest.TestCase):

    def test_ok_ok(self):
        r = map2(Succeed(2), Succeed(3), lambda a, b: a + b)
        self.assertEqual(5, r.unwrap())

    def test_err_ok(self):
        ra: Result[int, str] = Fail("a")
        r = map2(ra, Succeed(3), lambda a, b: a + b)
        self.assertEqual(["a"], r.errors())

    def test_err_err_accumulates(self):
        ra: Result[int, str] = Fail("a")
        rb: Result[int, str] = Fail("b")
        r = map2(ra, rb, lambda a, b: a + b)
        self.assertEqual(["a", "b"], r.errors())


class TestMap3(unittest.TestCase):

    def test_all_ok(self):
        r = map3(
            Succeed(1), Succeed(2), Succeed(3), lambda a, b, c: a + b + c
        )
        self.assertEqual(6, r.unwrap())

    def test_all_error_accumulates_in_order(self):
        ra: Result[int, str] = Fail("a")
        rb: Result[int, str] = Fail("b")
        rc: Result[int, str] = Fail("c")
        r = map3(ra, rb, rc, lambda a, b, c: a + b + c)
        self.assertEqual(["a", "b", "c"], r.errors())

    def test_middle_error(self):
        rb: Result[int, str] = Fail("b")
        r = map3(Succeed(1), rb, Succeed(3), lambda a, b, c: a + b + c)
        self.assertEqual(["b"], r.errors())


class TestMap4(unittest.TestCase):

    def test_all_ok(self):
        r = map4(
            Succeed("BTCUSD"),
            Succeed("BUY"),
            Succeed(10),
            Succeed(50000),
            lambda s, side, q, p: (s, side, q, p),
        )
        self.assertEqual(("BTCUSD", "BUY", 10, 50000), r.unwrap())

    def test_three_errors_accumulate(self):
        r1: Result[str, str] = Fail("bad symbol")
        r2: Result[str, str] = Fail("bad side")
        r3: Result[int, str] = Fail("negative quantity")
        r = map4(
            r1,
            r2,
            r3,
            Succeed(50000),
            lambda s, side, q, p: (s, side, q, p),
        )
        self.assertEqual(
            ["bad symbol", "bad side", "negative quantity"], r.errors()
        )


class TestSwitch(unittest.TestCase):

    def test_lifts_one_track_function(self):
        double = switch(lambda v: v * 2)
        r = double(21)
        self.assertEqual(42, r.unwrap())


class TestTee(unittest.TestCase):

    def test_side_effect_and_passthrough(self):
        seen = []
        out = tee(lambda v: seen.append(v), 7)
        self.assertEqual([7], seen)
        self.assertEqual(7, out)


class TestTryCatch(unittest.TestCase):

    def test_no_exception(self):
        safe = try_catch(
            lambda v: 100 // v,
            lambda exc: "caught: " + str(exc),
        )
        self.assertEqual(50, safe(2).unwrap())

    def test_exception_caught(self):
        safe = try_catch(
            lambda v: 100 // v,
            lambda exc: "caught: " + type(exc).__name__,
        )
        r = safe(0)
        self.assertEqual(["caught: ZeroDivisionError"], r.errors())


class TestDoubleMapDirect(unittest.TestCase):

    def test_through_method(self):
        r: Result[int, str] = FailMany(["a", "b"])
        out = r.double_map(lambda v: v, lambda e: "E:" + e)
        self.assertEqual(["E:a", "E:b"], out.errors())


class TestPlus(unittest.TestCase):

    def test_both_ok_merges_values(self):
        p = plus(
            lambda a, b: a + b,
            lambda e1, e2: [*e1, *e2],
            lambda x: Succeed(x),
            lambda x: Succeed(x * 10),
        )
        self.assertEqual(33, p(3).unwrap())

    def test_both_error_merges_lists(self):
        f1: Result[int, str] = Fail("a")
        f2: Result[int, str] = Fail("b")
        p = plus(
            lambda a, b: a + b,
            lambda e1, e2: [*e1, *e2],
            lambda x: f1,
            lambda x: f2,
        )
        self.assertEqual(["a", "b"], p(0).errors())


class TestAnd(unittest.TestCase):

    def test_both_pass(self):
        not_empty = lambda s: (
            Fail("must not be empty") if s == "" else Succeed(s)
        )
        not_too_long = lambda s: (
            Fail("must be <= 5 chars") if len(s) > 5 else Succeed(s)
        )
        validate = and_(not_empty, not_too_long)
        self.assertEqual("ok", validate("ok").unwrap())

    def test_first_fails(self):
        not_empty = lambda s: (
            Fail("must not be empty") if s == "" else Succeed(s)
        )
        not_too_long = lambda s: (
            Fail("must be <= 5 chars") if len(s) > 5 else Succeed(s)
        )
        validate = and_(not_empty, not_too_long)
        self.assertEqual(
            ["must not be empty"], validate("").errors()
        )

    def test_both_fail_accumulate(self):
        neg = lambda n: Fail("negative") if n < 0 else Succeed(n)
        even = lambda n: Fail("odd") if n % 2 else Succeed(n)
        v = and_(neg, even)
        self.assertEqual(["negative", "odd"], v(-3).errors())


class TestCompose(unittest.TestCase):

    def test_success_path(self):
        parse = lambda s: Fail("parse failed") if s == "bad" else Succeed(len(s))
        check = lambda n: Fail("zero") if n == 0 else Succeed(n * 2)
        pipeline = compose(parse, check)
        self.assertEqual(6, pipeline("foo").unwrap())

    def test_first_stage_fails_short_circuits(self):
        parse = lambda s: Fail("parse failed") if s == "bad" else Succeed(len(s))
        check = lambda n: Fail("zero") if n == 0 else Succeed(n * 2)
        pipeline = compose(parse, check)
        self.assertEqual(["parse failed"], pipeline("bad").errors())

    def test_second_stage_fails(self):
        parse = lambda s: Fail("parse failed") if s == "bad" else Succeed(len(s))
        check = lambda n: Fail("zero") if n == 0 else Succeed(n * 2)
        pipeline = compose(parse, check)
        self.assertEqual(["zero"], pipeline("").errors())


class TestPipe(unittest.TestCase):

    def test_composes_plain_functions(self):
        add_one = lambda n: n + 1
        to_str = lambda n: "x" * n
        p = pipe(add_one, to_str)
        self.assertEqual("xxx", p(2))


class TestPipelineMixingAccumulationAndShortCircuit(unittest.TestCase):

    @staticmethod
    def _valid_symbol(s):
        return Fail("symbol required") if s == "" else Succeed(s)

    @staticmethod
    def _valid_side(s):
        return Fail("side must be BUY or SELL") if s not in ("BUY", "SELL") else Succeed(s)

    @staticmethod
    def _valid_qty(q):
        return Fail("quantity must be > 0") if q <= 0 else Succeed(q)

    def _build(self, sym, side, qty):
        return {"symbol": sym, "side": side, "quantity": qty}

    def test_all_three_fail_all_errors_reported(self):
        form = map3(
            self._valid_symbol(""),
            self._valid_side("X"),
            self._valid_qty(-1),
            self._build,
        )
        self.assertEqual(
            [
                "symbol required",
                "side must be BUY or SELL",
                "quantity must be > 0",
            ],
            form.errors(),
        )

    def test_validate_then_chain_to_monadic_step(self):
        form = map3(
            self._valid_symbol("BTCUSD"),
            self._valid_side("BUY"),
            self._valid_qty(10),
            self._build,
        )
        final = form.and_then(
            lambda f: (
                Fail("over limit")
                if f["quantity"] > 100
                else Succeed(f["symbol"] + ":" + f["side"])
            )
        )
        self.assertEqual("BTCUSD:BUY", final.unwrap())

    def test_validation_fails_monadic_step_skipped(self):
        called = []
        form = map3(
            self._valid_symbol(""),
            self._valid_side("BUY"),
            self._valid_qty(10),
            self._build,
        )
        final = form.and_then(
            lambda f: called.append(True) or Succeed("never")
        )
        self.assertEqual(["symbol required"], final.errors())
        self.assertEqual([], called)


if __name__ == "__main__":
    unittest.main()
