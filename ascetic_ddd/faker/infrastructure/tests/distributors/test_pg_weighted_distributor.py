from ascetic_ddd.faker.infrastructure.distributors import pg_distributor_factory
from ascetic_ddd.faker.domain.tests.distributors.m2o import test_weighted_distributor as td


# logging.basicConfig(level="DEBUG")


class PgDefaultKeyDistributorTestCase(td.DefaultKeyDistributorTestCase):
    distributor_factory = staticmethod(pg_distributor_factory)


class PgSpecificKeyDistributorTestCase(td.SpecificKeyDistributorTestCase):
    distributor_factory = staticmethod(pg_distributor_factory)


class PgCollectionDistributorTestCase(td.CollectionDistributorTestCase):
    distributor_factory = staticmethod(pg_distributor_factory)
