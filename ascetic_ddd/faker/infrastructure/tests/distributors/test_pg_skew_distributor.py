import logging

from ascetic_ddd.faker.infrastructure.distributors import pg_skew_distributor_factory
from ascetic_ddd.faker.domain.tests.distributors import test_skew_distributor as tsd

# logging.basicConfig(level="DEBUG")


class PgDefaultKeySkewDistributorTestCase(tsd.DefaultKeySkewDistributorTestCase):
    distributor_factory = staticmethod(pg_skew_distributor_factory)


class PgSpecificKeySkewDistributorTestCase(tsd.SpecificKeySkewDistributorTestCase):
    distributor_factory = staticmethod(pg_skew_distributor_factory)


class PgCollectionSkewDistributorTestCase(tsd.CollectionSkewDistributorTestCase):
    distributor_factory = staticmethod(pg_skew_distributor_factory)
