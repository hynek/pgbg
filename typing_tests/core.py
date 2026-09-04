import threading

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, assert_type

import psycopg

from bgt import IntervalOnlyWakeup, Supervisor, as_work_factory
from bgt.typing import DoWork, Loop, Wakeup, WorkFactory
from psycopg import sql

from pgbg import (
    ElectedService,
    NotifyDispatcher,
    Subscription,
    SupervisedDispatcher,
    SupervisedElectedService,
    init_db,
    make_create_leases_table_sql,
)
from pgbg.typing import ConnectionProvider, Subscribable


def connect() -> psycopg.Connection[Any]:
    return psycopg.connect("postgresql://pgbg@127.0.0.1/pgbg")


# A bare connect factory satisfies ConnectionProvider, because a Psycopg
# connection is its own context manager.
bare_provider: ConnectionProvider = connect


@contextmanager
def lend() -> Iterator[psycopg.Connection[Any]]:
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


ctx_provider: ConnectionProvider = lend


def process_orders() -> bool:
    return False


work: DoWork = process_orders
work_factory: WorkFactory = as_work_factory(work)


@contextmanager
def make_work() -> Iterator[DoWork]:
    yield process_orders


ctx_work_factory: WorkFactory = make_work

dispatcher = NotifyDispatcher(interval=0.5)
subscribable: Subscribable = dispatcher

subscription = dispatcher.subscribe("orders")
notification_wakeup: Wakeup = subscription

assert_type(subscription, Subscription)
assert_type(subscription.wait(5.0), bool)
assert_type(subscription.channel, str)
assert_type(subscription.initial_listen_established, threading.Event)
subscription.close()


class CustomWakeup:
    def wait(self, timeout: float) -> bool:
        return False

    def wake(self) -> None:
        pass

    def close(self) -> None:
        pass


custom_wakeup: Wakeup = CustomWakeup()

supervised = SupervisedDispatcher.start(
    connect, interval=0.5, name="dispatch", initial_backoff=0.1
)

assert_type(supervised.subscribe("orders"), Subscription)
assert_type(supervised.is_running, bool)
assert_type(supervised.stop(1.0), bool)
supervised_subscribable: Subscribable = supervised

with SupervisedDispatcher.start(connect) as handle:
    assert_type(handle, SupervisedDispatcher)

service = ElectedService.build(
    work_factory,
    connect,
    name="orders",
    leases="public.service_leases",
    worker_id="worker-01",
    wakeup=subscription,
    interval=1.0,
    lease_ttl=30.0,
)
elected_loop: Loop = service

# An elected service runs under bgt's supervisor like any other loop.
with Supervisor.start(service, name="orders") as held:
    assert_type(held, Supervisor)

any_wakeup_service = ElectedService.build(
    ctx_work_factory,
    lend,
    name="orders",
    worker_id="worker-01",
    wakeup=custom_wakeup,
)
assert_type(any_wakeup_service.is_leader, bool)

elected = SupervisedElectedService.start(
    work_factory,
    lend,
    name="orders",
    leases="public.service_leases",
    worker_id="worker-01",
    wakeup=IntervalOnlyWakeup(),
    interval=1.0,
    lease_ttl=30.0,
    initial_backoff=0.1,
)

assert_type(elected.is_running, bool)
assert_type(elected.is_leader, bool)
assert_type(elected.stop(5.0), bool)

with SupervisedElectedService.start(
    ctx_work_factory,
    lend,
    name="orders",
    leases="service_leases",
    worker_id="worker-01",
    wakeup=IntervalOnlyWakeup(),
) as elected_handle:
    assert_type(elected_handle, SupervisedElectedService)

assert_type(make_create_leases_table_sql(), sql.Composed)
assert_type(make_create_leases_table_sql("app.leases"), sql.Composed)

with lend() as ddl_conn:
    init_db(ddl_conn)
    init_db(ddl_conn, "app.leases")
