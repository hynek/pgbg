from typing import assert_type

from psycopg_pool import ConnectionPool

from pgbg import (
    IntervalOnlyWakeup,
    SupervisedElectedService,
    as_work_factory,
    init_db,
)
from pgbg.typing import ConnectionProvider


pool = ConnectionPool("postgresql://pgbg@127.0.0.1/pgbg", open=False)

# A pool's connection() context manager is a ConnectionProvider as-is.
provider: ConnectionProvider = pool.connection

with pool.connection() as conn:
    init_db(conn)
    init_db(conn, "app.leases")


def process_orders() -> bool:
    return False


assert_type(
    SupervisedElectedService.start(
        as_work_factory(process_orders),
        pool.connection,
        name="orders",
        leases="public.service_leases",
        worker_id="worker-01",
        wakeup=IntervalOnlyWakeup(),
    ),
    SupervisedElectedService,
)
