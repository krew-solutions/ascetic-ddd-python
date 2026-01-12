"""Unit tests for Deferred pattern implementation."""
import unittest

from ..deferred import Deferred, noop


class TestNoop(unittest.TestCase):
    """Test noop function."""

    def test_noop_returns_none(self):
        """Test that noop returns None for any input."""
        self.assertIsNone(noop(42))
        self.assertIsNone(noop("test"))
        self.assertIsNone(noop(None))


class TestDeferredBasics(unittest.TestCase):
    """Test basic Deferred functionality."""

    def test_resolve_triggers_success_handler(self):
        """Test that resolving triggers the success handler."""
        deferred = Deferred[int]()
        result = []

        def on_success(value: int) -> None:
            result.append(value)
            return None

        deferred.then(on_success, noop)
        deferred.resolve(42)

        self.assertEqual([42], result)

    def test_reject_triggers_error_handler(self):
        """Test that rejecting triggers the error handler."""
        deferred = Deferred[int]()
        result = []

        def on_error(err: Exception) -> None:
            result.append(err)
            return None

        test_error = ValueError("test error")
        deferred.then(noop, on_error)
        deferred.reject(test_error)

        self.assertEqual([test_error], result)

    def test_resolve_before_then(self):
        """Test resolving before registering handlers."""
        deferred = Deferred[int]()
        result = []

        deferred.resolve(42)

        def on_success(value: int) -> None:
            result.append(value)
            return None

        deferred.then(on_success, noop)

        self.assertEqual([42], result)

    def test_reject_before_then(self):
        """Test rejecting before registering handlers."""
        deferred = Deferred[int]()
        result = []

        test_error = ValueError("test error")
        deferred.reject(test_error)

        def on_error(err: Exception) -> None:
            result.append(err)
            return None

        deferred.then(noop, on_error)

        self.assertEqual([test_error], result)


class TestDeferredChaining(unittest.TestCase):
    """Test Deferred chaining with then()."""

    def test_chain_success_handlers(self):
        """Test chaining multiple success handlers."""
        deferred = Deferred[int]()
        results = []

        def handler1(value: int) -> None:
            results.append(f"handler1: {value}")
            return None

        def handler2(_: bool) -> None:
            results.append("handler2")
            return None

        deferred.then(handler1, noop).then(handler2, noop)
        deferred.resolve(42)

        self.assertEqual(["handler1: 42", "handler2"], results)

    def test_chain_with_error_propagation(self):
        """Test error propagation through chain."""
        deferred = Deferred[int]()
        results = []

        test_error = ValueError("test error")

        def handler1(value: int) -> Exception:
            results.append(f"handler1: {value}")
            return test_error

        def handler2(_: bool) -> None:
            results.append("handler2: should not be called")
            return None

        def error_handler(err: Exception) -> None:
            results.append(f"error: {err}")
            return None

        deferred.then(handler1, noop).then(handler2, error_handler)
        deferred.resolve(42)

        self.assertEqual(["handler1: 42", f"error: {test_error}"], results)

    def test_multiple_handlers_on_same_deferred(self):
        """Test registering multiple handlers on the same deferred."""
        deferred = Deferred[int]()
        results = []

        def handler1(value: int) -> None:
            results.append(f"handler1: {value}")
            return None

        def handler2(value: int) -> None:
            results.append(f"handler2: {value}")
            return None

        deferred.then(handler1, noop)
        deferred.then(handler2, noop)
        deferred.resolve(42)

        # Both handlers should be called
        self.assertIn("handler1: 42", results)
        self.assertIn("handler2: 42", results)
        self.assertEqual(2, len(results))


class TestErrorCollection(unittest.TestCase):
    """Test error collection with occurred_err()."""

    def test_occurred_err_empty_when_no_errors(self):
        """Test that occurred_err returns empty list when no errors."""
        deferred = Deferred[int]()
        deferred.then(noop, noop)
        deferred.resolve(42)

        errors = deferred.occurred_err()
        self.assertEqual([], errors)

    def test_occurred_err_collects_handler_errors(self):
        """Test that occurred_err collects errors from handlers."""
        deferred = Deferred[int]()
        error1 = ValueError("error 1")

        def failing_handler(_: int) -> Exception:
            return error1

        deferred.then(failing_handler, noop)
        deferred.resolve(42)

        errors = deferred.occurred_err()
        self.assertEqual([error1], errors)

    def test_occurred_err_collects_nested_errors(self):
        """Test that occurred_err collects errors from entire chain."""
        deferred = Deferred[int]()
        error1 = ValueError("error 1")
        error2 = RuntimeError("error 2")

        def failing_handler1(_: int) -> Exception:
            return error1

        def failing_error_handler(err: Exception) -> Exception:
            return error2

        deferred.then(failing_handler1, noop).then(noop, failing_error_handler)
        deferred.resolve(42)

        errors = deferred.occurred_err()
        self.assertEqual([error1, error2], errors)

    def test_occurred_err_with_multiple_branches(self):
        """Test error collection from multiple handler branches."""
        deferred = Deferred[int]()
        error1 = ValueError("error 1")
        error2 = RuntimeError("error 2")

        def failing_handler1(_: int) -> Exception:
            return error1

        def failing_handler2(_: int) -> Exception:
            return error2

        deferred.then(failing_handler1, noop)
        deferred.then(failing_handler2, noop)
        deferred.resolve(42)

        errors = deferred.occurred_err()
        # Both errors should be collected
        self.assertIn(error1, errors)
        self.assertIn(error2, errors)
        self.assertEqual(2, len(errors))


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and special scenarios."""

    def test_resolve_with_none(self):
        """Test resolving with None value."""
        deferred = Deferred[None]()
        result = []

        def on_success(value: None) -> None:
            result.append(value)
            return None

        deferred.then(on_success, noop)
        deferred.resolve(None)

        self.assertEqual([None], result)

    def test_error_handler_returning_none(self):
        """Test error handler that returns None (no error)."""
        deferred = Deferred[int]()
        results = []

        test_error = ValueError("test error")

        def on_error(err: Exception) -> None:
            results.append("error handled")
            return None  # No error

        def next_handler(_: bool) -> None:
            results.append("next handler should not be called")
            return None

        deferred.then(noop, on_error).then(next_handler, noop)
        deferred.reject(test_error)

        # Error handler should be called, but next handler should not
        # because error handler returned None (no propagation)
        self.assertEqual(["error handled"], results)

    def test_multiple_resolves_triggers_handlers_multiple_times(self):
        """Test that multiple resolves trigger handlers each time."""
        deferred = Deferred[int]()
        results = []

        def on_success(value: int) -> None:
            results.append(value)
            return None

        deferred.then(on_success, noop)
        deferred.resolve(42)
        deferred.resolve(100)  # Triggers handlers again

        # Each resolve triggers all handlers
        self.assertEqual([42, 100], results)


class TestComplexScenarios(unittest.TestCase):
    """Test complex real-world scenarios."""

    def test_cleanup_chain(self):
        """Test a cleanup chain with multiple operations."""
        deferred = Deferred[str]()
        cleanup_log = []

        def cleanup1(resource: str) -> None:
            cleanup_log.append(f"cleanup1: {resource}")
            return None

        def cleanup2(_: bool) -> None:
            cleanup_log.append("cleanup2")
            return None

        def cleanup3(_: bool) -> None:
            cleanup_log.append("cleanup3")
            return None

        deferred.then(cleanup1, noop).then(cleanup2, noop).then(cleanup3, noop)
        deferred.resolve("database_connection")

        self.assertEqual(
            ["cleanup1: database_connection", "cleanup2", "cleanup3"], cleanup_log
        )

    def test_partial_failure_chain(self):
        """Test chain where some operations fail and some succeed."""
        deferred = Deferred[int]()
        results = []

        error = ValueError("step 2 failed")

        def step1(value: int) -> None:
            results.append(f"step1: {value}")
            return None

        def step2(_: bool) -> Exception:
            results.append("step2: failing")
            return error

        def step3(_: bool) -> None:
            results.append("step3: should not execute")
            return None

        def handle_error(err: Exception) -> None:
            results.append(f"error handler: {err}")
            return None

        (
            deferred.then(step1, noop)
            .then(step2, noop)
            .then(step3, handle_error)
        )

        deferred.resolve(42)

        self.assertEqual(
            ["step1: 42", "step2: failing", f"error handler: {error}"], results
        )


if __name__ == "__main__":
    unittest.main()
