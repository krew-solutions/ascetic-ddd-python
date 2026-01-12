import os
from psycopg_pool import AsyncConnectionPool

from ascetic_ddd.faker.infrastructure.session import PgSessionPool


async def make_internal_pg_session_pool():
    postgresql_url = os.environ.get(
        'TEST_INTERNAL_POSTGRESQL_URL',
        ''
    )
    pool = AsyncConnectionPool(postgresql_url, open=False)
    await pool.open()
    return PgSessionPool(pool)
