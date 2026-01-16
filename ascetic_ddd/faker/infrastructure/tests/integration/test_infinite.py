import re
import json
import uuid
import typing
import pprint
import logging
import dataclasses
import datetime
import pstats

import requests

from collections import Counter
import cProfile as profile
from unittest import IsolatedAsyncioTestCase

from http.server import BaseHTTPRequestHandler

from ascetic_ddd.faker.domain.distributors.m2o.factory import distributor_factory
from ascetic_ddd.faker.domain.distributors.m2o.dummy_distributor import DummyDistributor
from ascetic_ddd.faker.domain.providers.aggregate_provider import AggregateProvider, IAggregateRepository
from ascetic_ddd.faker.domain.providers.composite_value_provider import CompositeValueProvider
from ascetic_ddd.faker.domain.providers.interfaces import IValueProvider, IReferenceProvider, IEntityProvider
from ascetic_ddd.faker.domain.providers.reference_provider import ReferenceProvider
from ascetic_ddd.faker.domain.providers.value_provider import ValueProvider
from ascetic_ddd.faker.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.values.empty import empty
from ascetic_ddd.faker.infrastructure.distributors.m2o import pg_distributor_factory
from ascetic_ddd.faker.infrastructure.session import CompositeSessionPool
from ascetic_ddd.faker.infrastructure.session.rest_session import RestSessionPool
from ascetic_ddd.faker.infrastructure.tests.db import make_internal_pg_session_pool

from ascetic_ddd.faker.infrastructure.utils.json import JSONEncoder
from ascetic_ddd.faker.infrastructure.repositories import (
    InternalPgRepository, InMemoryRepository, RestRepository,
    CompositeAutoPkRepository as CompositeRepository
)
from ascetic_ddd.seedwork.infrastructure.tests.mock_server import get_free_port, start_mock_server

# logging.basicConfig(level="INFO")


# ################## Stub Session Pool for InMemory testing ##############################


class StubSession:
    """Simple stub session for in-memory testing."""

    def __init__(self, parent=None):
        self._parent = parent

    def atomic(self):
        return StubTransactionContext(self)

    @property
    def response_time(self):
        return 0.0

    @property
    def stats(self):
        from ascetic_ddd.faker.domain.utils.stats import Collector
        return Collector()


class StubTransactionContext:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return StubSession(self._session)

    async def __aexit__(self, exc_type, exc, tb):
        pass


class StubSessionPool:
    """Simple stub session pool for in-memory testing."""

    def session(self):
        return StubSessionContext()

    @property
    def response_time(self):
        return 0.0

    @property
    def stats(self):
        from ascetic_ddd.faker.domain.utils.stats import Collector
        return Collector()

    def attach(self, aspect, observer, id_=None):
        pass

    def detach(self, aspect, observer, id_=None):
        pass

    def notify(self, aspect, *args, **kwargs):
        pass

    async def anotify(self, aspect, *args, **kwargs):
        pass


class StubSessionContext:
    async def __aenter__(self):
        return StubSession()

    async def __aexit__(self, exc_type, exc, tb):
        pass


# ################## Models ##############################


FirstModelPk: typing.TypeAlias = uuid.UUID


@dataclasses.dataclass(kw_only=True)
class FirstModel:
    id: FirstModelPk
    attr2: str

    def __hash__(self):
        return hash(self.id)


SecondModelLocalPk: typing.TypeAlias = uuid.UUID


@dataclasses.dataclass(kw_only=True)
class SecondModelPk:
    id: SecondModelLocalPk
    first_model_id: FirstModelPk

    def __hash__(self):
        assert self.id is not empty
        assert self.first_model_id is not empty
        return hash((self.id, self.first_model_id))


@dataclasses.dataclass(kw_only=True)
class SecondModel:
    id: SecondModelPk
    attr2: str

    def __hash__(self):
        return hash(self.id)


ThirdModelLocalPk: typing.TypeAlias = uuid.UUID


@dataclasses.dataclass(kw_only=True)
class ThirdModelPk:
    id: ThirdModelLocalPk
    first_model_id: FirstModelPk

    def __hash__(self):
        assert self.id is not empty
        assert self.first_model_id is not empty
        return hash((self.id, self.first_model_id))


@dataclasses.dataclass(kw_only=True)
class ThirdModel:
    id: ThirdModelPk
    second_model_id: SecondModelPk
    attr2: str

    def __hash__(self):
        return hash(self.id)


# ################## Repositories #############################


class FirstModelRepository(RestRepository[FirstModel]):
    _id_attr = 'id'
    _path = "first-model"


class SecondModelRepository(RestRepository[SecondModel]):
    _id_attr = 'id.id'
    _path = "second-model"


class ThirdModelRepository(RestRepository[ThirdModel]):
    _id_attr = 'id.id'
    _path = "third-model"


# ################## Value Generators ##################################


class Attr2ValueGenerator:
    def __init__(self):
        self._count = 0

    async def __call__(self, session: ISession, position: int | None = None):
        val = "attr2_%s" % self._count
        self._count += 1
        return val


async def uuid_generator(session: ISession, position: int | None = None):
    return uuid.uuid4()


# ################## Fakers ##################################


class FirstModelFaker(AggregateProvider[dict, FirstModel]):
    id: ValueProvider[FirstModelPk, FirstModelPk]
    attr2: ValueProvider[str, str]
    _id_attr = 'id'

    def __init__(self, repository: IAggregateRepository[FirstModel], make_distributor):
        # ID is auto-generated by server - use DummyDistributor without value_generator
        self.id = ValueProvider(distributor=DummyDistributor())
        self.attr2 = ValueProvider(
            distributor=make_distributor(
                weights=[0.9, 0.5, 0.1, 0.01],
                mean=10,
            ),
            value_generator=Attr2ValueGenerator(),
        )
        super().__init__(
            repository=repository,
            result_factory=FirstModel,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(agg: FirstModel) -> dict:
        return {'id': agg.id, 'attr2': agg.attr2}


class SecondModelPkFaker(CompositeValueProvider[dict, SecondModelPk]):
    id: ValueProvider[SecondModelLocalPk, SecondModelLocalPk]
    first_model_id: ReferenceProvider

    def __init__(
            self,
            first_model_faker: IEntityProvider[dict, FirstModel],
            make_distributor
    ) -> None:
        self.first_model_id = ReferenceProvider(
            distributor=make_distributor(
                weights=[0.9, 0.5, 0.1, 0.01],
                mean=10,
            ),
            aggregate_provider=first_model_faker,
        )
        # ID is auto-generated by DB - use DummyDistributor without value_generator
        self.id = ValueProvider(distributor=DummyDistributor())
        super().__init__(
            distributor=make_distributor(),
            result_factory=SecondModelPk,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(pk: SecondModelPk) -> dict:
        return {'id': pk.id, 'first_model_id': pk.first_model_id}


class SecondModelFaker(AggregateProvider[dict, SecondModel]):
    id: SecondModelPkFaker
    attr2: ValueProvider[str, str]
    _id_attr = 'id'

    def __init__(
            self,
            repository: IAggregateRepository[SecondModel],
            first_model_faker: FirstModelFaker,
            make_distributor
    ):
        self.id = SecondModelPkFaker(first_model_faker, make_distributor)
        self.attr2 = ValueProvider(
            distributor=make_distributor(
                weights=[0.9, 0.5, 0.1, 0.01],
                mean=10,
            ),
            value_generator=Attr2ValueGenerator(),
        )
        super().__init__(
            repository=repository,
            result_factory=SecondModel,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(agg: SecondModel) -> dict:
        if agg is None:
            return None
        return {
            'id': SecondModelPkFaker._export(agg.id),
            'attr2': agg.attr2,
        }


class ThirdModelPkFaker(CompositeValueProvider[dict, ThirdModelPk]):
    id: ValueProvider[ThirdModelLocalPk, ThirdModelLocalPk]
    first_model_id: ReferenceProvider

    def __init__(
            self,
            first_model_faker: IEntityProvider[dict, FirstModel],
            make_distributor
    ) -> None:
        self.first_model_id = ReferenceProvider(
            distributor=make_distributor(
                weights=[0.9, 0.5, 0.1, 0.01],
                mean=10,
            ),
            aggregate_provider=first_model_faker,
        )
        # ID is auto-generated by DB - use DummyDistributor without value_generator
        self.id = ValueProvider(distributor=DummyDistributor())
        super().__init__(
            distributor=make_distributor(),
            result_factory=ThirdModelPk,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(pk: ThirdModelPk) -> dict:
        return {'id': pk.id, 'first_model_id': pk.first_model_id}


class SecondModelFkFaker(CompositeValueProvider[dict, SecondModelPk]):
    """FK provider for SecondModel reference with nullable support."""
    id: ValueProvider[SecondModelLocalPk, SecondModelLocalPk]
    first_model_id: ReferenceProvider

    def __init__(
            self,
            first_model_faker: IEntityProvider[dict, FirstModel],
            make_distributor,
            weights: list[float],
            null_weight: float,
            mean: float,
    ) -> None:
        self.first_model_id = ReferenceProvider(
            distributor=make_distributor(
                weights=[0.9, 0.5, 0.1, 0.01],
                mean=10,
            ),
            aggregate_provider=first_model_faker,
        )
        # ID is auto-generated by DB - use DummyDistributor without value_generator
        self.id = ValueProvider(distributor=DummyDistributor())
        super().__init__(
            distributor=make_distributor(
                weights=weights,
                mean=mean,
                null_weight=null_weight,
            ),
            result_factory=SecondModelPk,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(pk: SecondModelPk) -> dict:
        return {'id': pk.id, 'first_model_id': pk.first_model_id}


class ThirdModelFaker(AggregateProvider[dict, ThirdModel]):
    id: ThirdModelPkFaker
    attr2: ValueProvider[str, str]
    second_model_id: ReferenceProvider
    _id_attr = 'id'

    def __init__(
            self,
            repository: IAggregateRepository[ThirdModel],
            first_model_faker: FirstModelFaker,
            second_model_faker: SecondModelFaker,
            make_distributor
    ):
        self.id = ThirdModelPkFaker(first_model_faker, make_distributor)
        self.second_model_id = ReferenceProvider(
            distributor=make_distributor(
                weights=[0.9, 0.5, 0.1, 0.01],
                null_weight=0.5,
                mean=10,
            ),
            aggregate_provider=second_model_faker,
        )
        self.attr2 = ValueProvider(
            distributor=make_distributor(
                weights=[0.9, 0.5, 0.1, 0.01],
                mean=10,
            ),
            value_generator=Attr2ValueGenerator(),
        )
        super().__init__(
            repository=repository,
            result_factory=ThirdModel,
            result_exporter=self._export,
        )

    @staticmethod
    def _export(agg: ThirdModel) -> dict:
        return {
            'id': ThirdModelPkFaker._export(agg.id),
            'second_model_id': SecondModelPkFaker._export(agg.second_model_id) if agg.second_model_id and agg.second_model_id is not empty else None,
            'attr2': agg.attr2,
        }

    async def populate(self, session: ISession) -> None:
        if self.is_complete():
            return
        await self.do_populate(session)
        await self.id.populate(session)
        id_ = await self.id.create(session)
        self.second_model_id.set({
            'id': {'first_model_id': id_.first_model_id},
        })
        for attr, provider in self._providers.items():
            if attr == 'id':
                continue
            await provider.populate(session)


# ################## Mock Server ################################################


class MockServerRequestHandler(BaseHTTPRequestHandler):
    URL_PATTERN = re.compile(r'/first-model')
    COMPOSITE_PK_URL_PATTERN = re.compile(r'/(second-model|third-model)')

    def do_POST(self):
        content_len = int(self.headers.get('Content-Length'))
        logging.debug(self.rfile.read(content_len))
        if re.search(self.URL_PATTERN, self.path):
            # Add response status code.
            self.send_response(requests.codes.ok)

            # Add response headers.
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()

            # Add response content.
            response_content = json.dumps({'id': uuid.uuid4()}, cls=JSONEncoder)
            self.wfile.write(response_content.encode('utf-8'))
            return
        elif re.search(self.COMPOSITE_PK_URL_PATTERN, self.path):
            # Add response status code.
            self.send_response(requests.codes.ok)

            # Add response headers.
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()

            # Add response content.
            response_content = json.dumps({'id': {'id': uuid.uuid4()}}, cls=JSONEncoder)
            self.wfile.write(response_content.encode('utf-8'))
            return


# ################## TestCases ###################################################


class RestPgIntegrationTestCase(IsolatedAsyncioTestCase):
    make_distributor = staticmethod(pg_distributor_factory)

    async def asyncSetUp(self):
        self.mock_server_port = get_free_port()
        self.mock_server = start_mock_server(self.mock_server_port, MockServerRequestHandler)
        self.session_pool = await self._make_session_pool()

    async def test_first_model_faker(self):
        faker = self._make_first_model_faker()
        async with self.session_pool.session() as session:
            async with session.atomic() as ts_session:
                await faker.setup(ts_session)
                await faker.populate(ts_session)
                agg = await faker.create(ts_session)
                self.assertIsInstance(agg.id, uuid.UUID)
                await faker.cleanup(ts_session)

    async def test_second_model_faker(self):
        faker = self._make_second_model_faker()
        async with self.session_pool.session() as session, session.atomic() as ts_session:
            await faker.setup(ts_session)
            await faker.populate(ts_session)
            agg = await faker.create(ts_session)
            self.assertIsInstance(agg.id.id, uuid.UUID)
            self.assertIsInstance(agg.id.first_model_id, uuid.UUID)

            id_ = await faker.id_provider.create(ts_session)
            self.assertEqual(id_, agg.id)
            await faker.cleanup(ts_session)

    async def test_third_model_faker(self):
        faker = self._make_third_model_faker()
        async with self.session_pool.session() as session, session.atomic() as ts_session:
            await faker.setup(ts_session)
            await faker.populate(ts_session)
            agg = await faker.create(ts_session)
            self.assertIsInstance(agg.id.id, uuid.UUID)
            self.assertIsInstance(agg.id.first_model_id, uuid.UUID)

            id_ = await faker.id_provider.create(ts_session)
            self.assertEqual(id_, agg.id)

            if agg.second_model_id is not None:
                self.assertIsInstance(agg.second_model_id.id, uuid.UUID)
                self.assertIsInstance(agg.second_model_id.first_model_id, uuid.UUID)

            second_model_id = await faker.second_model_id.create(ts_session)
            self.assertEqual(second_model_id, agg.second_model_id)
            await faker.cleanup(ts_session)

    async def test_batch_third_model_faker(self):
        debug = False
        start_date = datetime.datetime.now()

        if debug:
            profiler = profile.Profile()
            profiler.enable()

        faker = self._make_third_model_faker()
        second_model_aggs = dict()
        third_model_aggs = []
        async with self.session_pool.session() as session, session.atomic() as ts_session:
            await faker.setup(ts_session)
            for _ in range(1000):
                await faker.populate(ts_session)
                agg = await faker.create(ts_session)
                third_model_aggs.append(agg)
                if agg.second_model_id is not None:
                    second_model_agg = await faker.second_model_id.aggregate_provider.create(ts_session)
                    second_model_aggs[second_model_agg.id] = second_model_agg
                self.assertIsInstance(agg.id.id, uuid.UUID)
                self.assertIsInstance(agg.id.first_model_id, uuid.UUID)

                id_ = await faker.id_provider.create(ts_session)
                self.assertEqual(id_, agg.id)

                if agg.second_model_id is not None:
                    self.assertIsInstance(agg.second_model_id.id, uuid.UUID)
                    self.assertIsInstance(agg.second_model_id.first_model_id, uuid.UUID)

                second_model_id = await faker.second_model_id.create(ts_session)
                self.assertEqual(second_model_id, agg.second_model_id)

                faker.reset()

            await faker.cleanup(ts_session)

        if debug:
            profiler.disable()
            profiler.print_stats(sort='cumulative')

            # Below code is to add the stats to the file in human-readable format
            profiler.dump_stats('output.prof')
            stream = open('output.txt', 'w')
            stats = pstats.Stats('output.prof', stream=stream)
            stats.sort_stats('cumtime')
            stats.print_stats()

        run_time = datetime.datetime.now() - start_date
        logging.info("Run time: %s" % run_time)

        if debug:
            return

        logging.debug("SecondModel.id.first_model_id:")
        counter = Counter([agg.id.first_model_id for agg in second_model_aggs.values()])
        counter_repr = [(k, v) for k, v in sorted(counter.items(), key=lambda item: item[1], reverse=True)]
        logging.debug(pprint.pformat(counter_repr))

        logging.debug("ThirdModel.id.first_model_id:")
        counter = Counter([agg.id.first_model_id for agg in third_model_aggs])
        counter_repr = [(k, v) for k, v in sorted(counter.items(), key=lambda item: item[1], reverse=True)]
        logging.debug(pprint.pformat(counter_repr))

        logging.debug("ThirdModel.second_model_id:")
        counter = Counter([agg.second_model_id for agg in third_model_aggs])
        counter_repr = [(k, v) for k, v in sorted(counter.items(), key=lambda item: item[1], reverse=True)]
        logging.debug(pprint.pformat(counter_repr))

    async def _make_session_pool(self):
        rest_session_pool = RestSessionPool()
        pg_session_pool = await make_internal_pg_session_pool()
        session_pool = CompositeSessionPool(rest_session_pool, pg_session_pool)
        return session_pool

    def _make_first_model_faker(self):
        repository = self._make_first_model_repository()
        faker = FirstModelFaker(repository, self.make_distributor)
        faker.provider_name = 'FirstModelFaker'
        return faker

    def _make_second_model_faker(self, first_model_faker=None):
        if first_model_faker is None:
            first_model_faker = self._make_first_model_faker()
        repository = self._make_second_model_repository()
        faker = SecondModelFaker(
            repository,
            first_model_faker,
            self.make_distributor
        )
        faker.provider_name = 'SecondModelFaker'
        return faker

    def _make_third_model_faker(self):
        first_model_faker = self._make_first_model_faker()
        repository = self._make_third_model_repository()
        faker = ThirdModelFaker(
            repository,
            first_model_faker,
            self._make_second_model_faker(first_model_faker),
            self.make_distributor
        )
        faker.provider_name = 'ThirdModelFaker'
        return faker

    def _make_first_model_repository(self):
        external_repository = FirstModelRepository("http://localhost:%s" % self.mock_server_port)
        internal_repository = InternalPgRepository("first_model", FirstModelFaker._export)
        return CompositeRepository[FirstModel](external_repository, internal_repository)

    def _make_second_model_repository(self):
        external_repository = SecondModelRepository("http://localhost:%s" % self.mock_server_port)
        internal_repository = InternalPgRepository("second_model", SecondModelFaker._export)
        return CompositeRepository[SecondModel](external_repository, internal_repository)

    def _make_third_model_repository(self):
        external_repository = ThirdModelRepository("http://localhost:%s" % self.mock_server_port)
        internal_repository = InternalPgRepository("third_model", ThirdModelFaker._export)
        return CompositeRepository[ThirdModel](external_repository, internal_repository)

    async def asyncTearDown(self):
        await self.session_pool[1]._pool.close()


class RestMemoryIntegrationTestCase(RestPgIntegrationTestCase):
    make_distributor = staticmethod(distributor_factory)

    async def asyncTearDown(self):
        pass  # StubSessionPool doesn't need cleanup

    async def _make_session_pool(self):
        rest_session_pool = RestSessionPool()
        stub_session_pool = StubSessionPool()
        return CompositeSessionPool(rest_session_pool, stub_session_pool)

    def _make_first_model_repository(self):
        external_repository = FirstModelRepository("http://localhost:%s" % self.mock_server_port)
        internal_repository = InMemoryRepository(FirstModelFaker._export)
        return CompositeRepository[FirstModel](external_repository, internal_repository)

    def _make_second_model_repository(self):
        external_repository = SecondModelRepository("http://localhost:%s" % self.mock_server_port)
        internal_repository = InMemoryRepository(SecondModelFaker._export)
        return CompositeRepository[SecondModel](external_repository, internal_repository)

    def _make_third_model_repository(self):
        external_repository = ThirdModelRepository("http://localhost:%s" % self.mock_server_port)
        internal_repository = InMemoryRepository(ThirdModelFaker._export)
        return CompositeRepository[ThirdModel](external_repository, internal_repository)
