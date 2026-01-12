import logging

from ascetic_ddd.faker.domain.tests.distributors.test_sequence_distributor import SequenceDistributorTestCase
from ascetic_ddd.faker.infrastructure.distributors.factory import pg_distributor_factory


# logging.basicConfig(level="INFO")


class PgSequenceDistributorTestCase(SequenceDistributorTestCase):
    distributor_factory = staticmethod(pg_distributor_factory)
