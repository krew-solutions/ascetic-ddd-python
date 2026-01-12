import os
from psycopg_pool import AsyncConnectionPool

from ascetic_ddd.utils.amemo import amemo
from ..session import PgSessionPool


@amemo
async def make_pg_session_pool():
    postgresql_url = os.environ.get(
        'TEST_POSTGRESQL_URL',
        ''
    )
    pool = AsyncConnectionPool(postgresql_url, open=False)
    await pool.open()
    return PgSessionPool(pool)
