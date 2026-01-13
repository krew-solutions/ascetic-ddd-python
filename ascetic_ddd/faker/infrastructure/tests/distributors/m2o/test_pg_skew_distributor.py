from ascetic_ddd.faker.infrastructure.distributors.m2o.factory import pg_distributor_factory
from ascetic_ddd.faker.domain.tests.distributors.m2o import test_skew_distributor as tsd


# logging.basicConfig(level="DEBUG")


class PgDefaultKeySkewDistributorTestCase(tsd.DefaultKeySkewDistributorTestCase):
    distributor_factory = staticmethod(pg_distributor_factory)


class PgSpecificKeySkewDistributorTestCase(tsd.SpecificKeySkewDistributorTestCase):
    distributor_factory = staticmethod(pg_distributor_factory)


class PgCollectionSkewDistributorTestCase(tsd.CollectionSkewDistributorTestCase):
    distributor_factory = staticmethod(pg_distributor_factory)
