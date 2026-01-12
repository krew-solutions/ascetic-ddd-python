from unittest import IsolatedAsyncioTestCase, mock

from ..disposable import Disposable


# noinspection PyMethodMayBeStatic
# noinspection PyShadowingBuiltins
class DisposableTestCase(IsolatedAsyncioTestCase):
    async def test_dispose(self):
        callable = mock.AsyncMock()
        disposable = Disposable(callable)
        await disposable.dispose()
        callable.assert_called_once_with()

    async def test_add(self):
        callable1 = mock.AsyncMock()
        disposable1 = Disposable(callable1)
        callable2 = mock.AsyncMock()
        disposable2 = Disposable(callable2)
        disposable = disposable1 + disposable2
        await disposable.dispose()
        callable1.assert_called_once_with()
        callable2.assert_called_once_with()


# noinspection PyMethodMayBeStatic
# noinspection PyShadowingBuiltins
class CompositeDisposableTestCase(IsolatedAsyncioTestCase):
    async def test_add(self):
        callable1 = mock.AsyncMock()
        disposable1 = Disposable(callable1)
        callable2 = mock.AsyncMock()
        disposable2 = Disposable(callable2)
        disposable1and2 = disposable1 + disposable2
        callable3 = mock.AsyncMock()
        disposable3 = Disposable(callable3)
        disposable = disposable1and2 + disposable3
        await disposable.dispose()
        callable1.assert_called_once_with()
        callable2.assert_called_once_with()
        callable3.assert_called_once_with()
