from typing import Any, assert_type

import psycopg

from sqlalchemy import create_engine

from pgbg import (
    IntervalOnlyWakeup,
    SupervisedDispatcher,
    SupervisedElectedService,
    as_work_factory,
)
from pgbg.sqlalchemy import (
    connection_factory_from_engine,
    pooled_connection_factory_from_engine,
    start_dispatcher,
    start_elected_service,
)
from pgbg.typing import ConnectionProvider, DoWork, WorkFactory


engine = create_engine("postgresql+psycopg://pgbg@127.0.0.1/pgbg")

factory = connection_factory_from_engine(engine)

assert_type(factory(), psycopg.Connection[Any])
engine_provider: ConnectionProvider = factory

pooled: ConnectionProvider = pooled_connection_factory_from_engine(engine)

with pooled() as lent:
    assert_type(lent, psycopg.Connection[Any])


def process_orders() -> bool:
    return False


work: DoWork = process_orders
work_factory: WorkFactory = as_work_factory(work)

assert_type(
    start_dispatcher(
        engine, interval=0.5, name="dispatch", initial_backoff=0.1
    ),
    SupervisedDispatcher,
)

assert_type(
    start_elected_service(
        work_factory,
        engine,
        name="orders",
        leases="public.service_leases",
        worker_id="worker-01",
        wakeup=IntervalOnlyWakeup(),
    ),
    SupervisedElectedService,
)
