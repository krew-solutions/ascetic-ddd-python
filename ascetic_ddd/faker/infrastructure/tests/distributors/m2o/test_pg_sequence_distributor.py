from ascetic_ddd.faker.domain.tests.distributors.m2o.test_sequence_distributor import SequenceDistributorTestCase
from ascetic_ddd.faker.infrastructure.distributors.m2o.factory import pg_distributor_factory


# logging.basicConfig(level="INFO")


class PgSequenceDistributorTestCase(SequenceDistributorTestCase):
    distributor_factory = staticmethod(pg_distributor_factory)
