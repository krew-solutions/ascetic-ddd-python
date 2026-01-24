import hashlib
import typing

from ascetic_ddd.faker.domain.distributors.m2o.cursor import Cursor
from ascetic_ddd.observable.observable import Observable
from ascetic_ddd.seedwork.infrastructure.utils.pg import escape
from ascetic_ddd.faker.infrastructure.session.pg_session import extract_internal_connection

from ascetic_ddd.faker.domain.distributors.m2o.interfaces import IM2ODistributor
from ascetic_ddd.seedwork.domain.session.interfaces import ISession
from ascetic_ddd.faker.domain.specification.empty_specification import EmptySpecification
from ascetic_ddd.faker.domain.specification.interfaces import ISpecification


__all__ = ('PgSequenceDistributor',)


T = typing.TypeVar("T", covariant=True)


class PgSequenceDistributor(Observable, IM2ODistributor[T], typing.Generic[T]):
    _extract_connection = staticmethod(extract_internal_connection)
    _initialized: bool = False
    _provider_name: str | None = None
    _table: str | None = None
    _default_spec: ISpecification

    def __init__(
            self,
            initialized: bool = False
    ):
        self._initialized = initialized
        self._default_spec = EmptySpecification()
        super().__init__()

    async def next(
            self,
            session: ISession,
            specification: ISpecification[T] | None = None,
    ) -> T:
        if specification is None:
            specification = self._default_spec

        if not self._initialized:
            await self.setup(session)

        key = str(specification)

        while True:
            async with self._extract_connection(session).cursor() as acursor:
                sql = """
                    WITH seq AS (SELECT sequence_name FROM %s WHERE scope = %%s)
                    SELECT nextval(sequence_name::regclass) FROM seq
                """ % (
                    self._table,
                )
                await acursor.execute(sql, (key,))
                row = await acursor.fetchone()
                if row is not None:
                    position = row[0]
                    break

                sql = """
                    INSERT INTO %s (scope, sequence_name) VALUES (%%s, %%s)
                    ON CONFLICT DO NOTHING;
                """ % (
                    self._table,
                )
                await acursor.execute(sql, (key, self._make_sequence_name(key)))

        raise Cursor(
            position=position,
            callback=self._append,
        )

    async def _append(self, session: ISession, value: T, position: int | None):
        await self.anotify('value', session, value)

    async def append(self, session: ISession, value: T):
        await self._append(session, value, None)

    @property
    def provider_name(self):
        return self._provider_name

    @provider_name.setter
    def provider_name(self, value):
        if self._provider_name is None:
            self._provider_name = value
            self._set_table(value)

    def _set_table(self, name):
        self._table = escape(self._make_pg_identity('sequences_for_', name))

    @staticmethod
    def _make_pg_identity(prefix, name):
        prefix_len = len(prefix)
        max_pg_name_len = 63
        max_name_len = max_pg_name_len - prefix_len
        if len(name) > max_name_len:
            name = name[-max_name_len:]
        return prefix + name

    def _make_sequence_name(self, key: typing.Hashable):
        name = hashlib.md5(("%s_%s" % (self.provider_name, key,)).encode('utf-8')).hexdigest()
        return self._make_pg_identity('sequence_', name)

    async def setup(self, session: ISession):
        if not self._initialized:  # Fixes diamond problem
            if not (await self._is_initialized(session)):
                await self._setup(session)
            self._initialized = True

    async def _setup(self, session: ISession):
        seq_factory_name = escape(self._make_pg_identity("make_seq_", self.provider_name))
        async with self._extract_connection(session).cursor() as acursor:
            sql = """
                CREATE TABLE IF NOT EXISTS %s (
                    scope VARCHAR(255) NOT NULL PRIMARY KEY,
                    sequence_name VARCHAR(63) NOT NULL UNIQUE
                )
            """ % (
                self._table,
            )
            await acursor.execute(sql)

            sql = """
                CREATE FUNCTION %s() RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                begin
                    execute format('CREATE SEQUENCE IF NOT EXISTS %%s MINVALUE 0', NEW.sequence_name);
                    return NEW;
                end
                $$;
            """ % (
                seq_factory_name
            )
            await acursor.execute(sql)

            sql = """
                CREATE TRIGGER %s AFTER INSERT ON %s FOR EACH ROW EXECUTE PROCEDURE %s();
            """ % (
                seq_factory_name,
                self._table,
                seq_factory_name,
            )
            await acursor.execute(sql)

    async def cleanup(self, session: ISession):
        self._initialized = False
        if not (await self._is_initialized(session)):
            return
        seq_factory_name = escape(self._make_pg_identity("make_seq_", self.provider_name))
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute("SELECT sequence_name FROM %s" % self._table)
            seq_names = [i[0] for i in await acursor.fetchall()]
            for seq_name in seq_names:
                await acursor.execute("DROP SEQUENCE IF EXISTS %s CASCADE" % escape(seq_name))

            await acursor.execute("DROP FUNCTION IF EXISTS %s CASCADE" % seq_factory_name)
            await acursor.execute("DROP TABLE IF EXISTS %s CASCADE" % self._table)

    def __copy__(self):
        return self

    def __deepcopy__(self, memodict={}):
        return self

    def bind_external_source(self, external_source: typing.Any) -> None:
        """PgSequenceDistributor не использует external_source."""
        pass

    async def _is_initialized(self, session: ISession) -> bool:
        sql = """SELECT to_regclass(%s)"""
        async with self._extract_connection(session).cursor() as acursor:
            await acursor.execute(sql, (self._table, ))
            regclass = (await acursor.fetchone())[0]
        return regclass is not None
