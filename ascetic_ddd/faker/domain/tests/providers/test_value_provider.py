import typing
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.faker.domain.providers.value_provider import ValueProvider
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.values.empty import empty


class MockDistributor(IM2ODistributor):
    """Mock distributor for testing."""

    def __init__(self, values: list = None, raise_cursor_at: int | None = None):
        self._values = values or []
        self._index = 0
        self._raise_cursor_at = raise_cursor_at
        self._appended = []
        self._provider_name = None

    async def next(self, session: ISession, specification=None):
        if self._raise_cursor_at is not None and self._index >= self._raise_cursor_at:
            raise Cursor(position=self._index, callback=self._append)
        if self._index < len(self._values):
            value = self._values[self._index]
            self._index += 1
            return value
        raise Cursor(position=self._index, callback=self._append)

    async def _append(self, session: ISession, value, position: int | None):
        self._appended.append((value, position))
        self._values.append(value)

    async def append(self, session: ISession, value):
        await self._append(session, value, None)

    @property
    def provider_name(self):
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value):
        self._provider_name = value

    async def setup(self, session: ISession):
        pass

    async def cleanup(self, session: ISession):
        pass

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    def attach(self, aspect, observer, id_=None):
        pass

    def detach(self, aspect, observer, id_=None):
        pass

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass

    def bind_external_source(self, external_source: typing.Any) -> None:
        pass


class MockSession:
    """Mock session for testing."""
    pass


class ValueProviderBasicTestCase(IsolatedAsyncioTestCase):
    """Basic tests for ValueProvider."""

    async def test_populate_from_distributor_existing_value(self):
        """When distributor has a value, populate() should use it."""
        distributor = MockDistributor(values=['existing_value'])
        generator = AsyncMock(return_value='new_value')
        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertTrue(provider.is_complete())
        self.assertEqual(provider._output, 'existing_value')
        generator.assert_not_called()

    async def test_populate_creates_new_value_when_cursor_raised(self):
        """When distributor raises Cursor, populate() should create new value."""
        distributor = MockDistributor(values=[], raise_cursor_at=0)
        call_count = 0

        async def generator(session, position=None):
            nonlocal call_count
            call_count += 1
            return 'generated_value'

        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertTrue(provider.is_complete())
        self.assertEqual(call_count, 1)
        self.assertEqual(len(distributor._appended), 1)

    async def test_populate_skips_if_already_complete(self):
        """populate() should do nothing if already complete."""
        distributor = MockDistributor(values=['value1', 'value2'])
        generator = AsyncMock(return_value='new_value')
        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)
        first_result = provider._output

        await provider.populate(session)
        second_result = provider._output

        self.assertEqual(first_result, second_result)
        self.assertEqual(distributor._index, 1)

    async def test_create_returns_output(self):
        """create() should return the output result."""
        distributor = MockDistributor(values=['test_output'])
        generator = AsyncMock()
        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)
        result = await provider.create(session)

        self.assertEqual(result, 'test_output')

    async def test_is_complete_false_before_populate(self):
        """is_complete() should return False before populate()."""
        distributor = MockDistributor(values=['value'])
        generator = AsyncMock()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )

        self.assertFalse(provider.is_complete())

    async def test_is_complete_true_after_populate(self):
        """is_complete() should return True after populate()."""
        distributor = MockDistributor(values=['value'])
        generator = AsyncMock()
        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertTrue(provider.is_complete())

    async def test_is_complete_true_after_populate_with_cursor(self):
        """is_complete() should return True after populate() when ICursor is raised."""
        # Use raise_cursor_at=0 to force ICursor branch
        distributor = MockDistributor(raise_cursor_at=0)
        session = MockSession()

        async def generator(session, position=None):
            return 'generated_value'

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        # Key assertion: is_complete() must be True even when ICursor was raised
        self.assertTrue(provider.is_complete())
        self.assertEqual(provider._output, 'generated_value')

    async def test_is_complete_true_after_populate_with_cursor_no_generator(self):
        """is_complete() should return True after populate() with ICursor and no input_generator."""
        # This tests auto-increment PK scenario
        distributor = MockDistributor(raise_cursor_at=0)
        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=None,  # No generator - simulates auto-increment PK
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertTrue(provider.is_complete())
        self.assertIsNone(provider._output)

    async def test_reset_clears_state(self):
        """reset() should clear the provider state."""
        distributor = MockDistributor(values=['value'])
        generator = AsyncMock()
        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)
        self.assertTrue(provider.is_complete())

        provider.reset()

        self.assertFalse(provider.is_complete())
        self.assertEqual(provider._input, empty)
        self.assertEqual(provider._output, empty)


class ValueProviderWithFactoriesTestCase(IsolatedAsyncioTestCase):
    """Tests for ValueProvider with output_factory and result_exporter."""

    async def test_output_factory_transforms_generated_value(self):
        """output_factory should transform the generated value to output."""
        distributor = MockDistributor(values=[], raise_cursor_at=0)

        async def generator(session, position=None):
            return 42

        output_factory = lambda x: f"transformed_{x}"
        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
            output_factory=output_factory,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertEqual(provider._output, 'transformed_42')

    async def test_result_exporter_extracts_input_from_output(self):
        """result_exporter should extract input value from distributor output."""
        distributor = MockDistributor(values=[{'id': 1, 'name': 'test'}])
        generator = AsyncMock()
        result_exporter = lambda x: x['name']
        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
            result_exporter=result_exporter,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertEqual(provider.get(), 'test')
        self.assertEqual(provider._output, {'id': 1, 'name': 'test'})


class ValueProviderGeneratorTypesTestCase(IsolatedAsyncioTestCase):
    """Tests for different value generator types."""

    async def test_callable_generator(self):
        """ValueProvider should work with plain callable."""
        distributor = MockDistributor(values=[], raise_cursor_at=0)
        call_count = 0

        def generator():
            nonlocal call_count
            call_count += 1
            return f"value_{call_count}"

        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertEqual(call_count, 1)

    async def test_iterable_generator(self):
        """ValueProvider should work with iterable."""
        distributor = MockDistributor(values=[], raise_cursor_at=0)
        values = iter(['first', 'second', 'third'])
        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=values,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertTrue(provider.is_complete())

    async def test_async_callable_generator(self):
        """ValueProvider should work with async callable."""
        distributor = MockDistributor(values=[], raise_cursor_at=0)

        async def async_generator(session, position=None):
            return f"async_value_{position}"

        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=async_generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertTrue(provider.is_complete())


class ValueProviderCursorPositionTestCase(IsolatedAsyncioTestCase):
    """Tests for cursor position handling."""

    async def test_cursor_position_passed_to_generator(self):
        """Cursor position should be passed to the value generator."""
        distributor = MockDistributor(values=[], raise_cursor_at=0)
        received_positions = []

        async def generator(session, position=None):
            received_positions.append(position)
            return f"value_at_{position}"

        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertEqual(len(received_positions), 1)
        self.assertEqual(received_positions[0], 0)


class ValueProviderSetGetTestCase(IsolatedAsyncioTestCase):
    """Tests for set() and get() methods."""

    async def test_set_updates_input(self):
        """set() should update the input value."""
        distributor = MockDistributor(values=['output'])
        generator = AsyncMock()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )

        provider.set('manual_value')

        self.assertEqual(provider.get(), 'manual_value')

    async def test_get_returns_input(self):
        """get() should return the input value set during populate()."""
        distributor = MockDistributor(values=['output_value'])
        generator = AsyncMock()
        session = MockSession()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )
        provider.provider_name = 'test_provider'

        await provider.populate(session)

        self.assertEqual(provider.get(), 'output_value')


class ValueProviderProviderNameTestCase(IsolatedAsyncioTestCase):
    """Tests for provider_name property."""

    async def test_provider_name_propagates_to_distributor(self):
        """provider_name should be set on the distributor."""
        distributor = MockDistributor()
        generator = AsyncMock()

        provider = ValueProvider(
            distributor=distributor,
            input_generator=generator,
        )

        provider.provider_name = 'my_provider'

        self.assertEqual(provider.provider_name, 'my_provider')
        self.assertEqual(distributor.provider_name, 'my_provider')


if __name__ == '__main__':
    import unittest
    unittest.main()
