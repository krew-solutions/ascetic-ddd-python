import gc
from unittest import TestCase

from ...session import IdentityMap


class Model:
    def __init__(self, pk: int):
        self.id = pk


class IdentityMapTestCase(TestCase):
    def test_get(self):
        identity_map = IdentityMap()
        pk = 3
        obj = Model(pk)
        identity_map.add(pk, obj)
        result = identity_map.get(pk)
        self.assertIs(obj, result)
        with self.assertRaises(KeyError):
            identity_map.get(10)

    # noinspection SpellCheckingInspection
    def test_get_weakref_cache(self):
        identity_map = IdentityMap(10)
        pk = 3
        obj = Model(pk)
        obj_id = id(obj)
        identity_map.add(pk, obj)
        del obj
        gc.collect()
        result = identity_map.get(pk)
        self.assertEqual(obj_id, id(result))

    # noinspection SpellCheckingInspection
    def test_get_weakref_cache_crowded(self):
        identity_map = IdentityMap(1)
        pk = 3
        obj = Model(pk)
        identity_map.add(pk, obj)
        del obj
        identity_map.add(10, Model(10))
        gc.collect()
        with self.assertRaises(KeyError):
            identity_map.get(pk)

    def test_has(self):
        identity_map = IdentityMap()
        pk = 3
        obj = Model(pk)
        identity_map.add(pk, obj)
        self.assertTrue(identity_map.has(pk))
        self.assertFalse(identity_map.has(10))

    def test_remove(self):
        identity_map = IdentityMap()
        pk = 3
        obj = Model(pk)
        identity_map.add(pk, obj)
        identity_map.remove(pk)
        with self.assertRaises(KeyError):
            identity_map.get(pk)

    def test_clear(self):
        identity_map = IdentityMap()
        pk = 3
        obj = Model(pk)
        identity_map.add(pk, obj)
        identity_map.clear()
        with self.assertRaises(KeyError):
            identity_map.get(pk)
