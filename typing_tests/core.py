import threading

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, assert_type

import psycopg

from psycopg import sql

from pgbg import (
    ElectedService,
    IntervalOnlyWakeup,
    NotifyDispatcher,
    Service,
    Subscription,
    SupervisedDispatcher,
    SupervisedElectedService,
    SupervisedService,
    Supervisor,
    as_work_factory,
    init_db,
    make_create_leases_table_sql,
)
from pgbg.typing import (
    ConnectionProvider,
    DoWork,
    Loop,
    Subscribable,
    Wakeup,
    WorkFactory,
)


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

interval_only_wakeup = IntervalOnlyWakeup()
poll_wakeup: Wakeup = interval_only_wakeup

assert_type(interval_only_wakeup.wait(1.0), bool)
interval_only_wakeup.wake()
interval_only_wakeup.close()

dispatcher = NotifyDispatcher(interval=0.5)
subscribable: Subscribable = dispatcher

subscription = dispatcher.subscribe("orders")
notification_wakeup: Wakeup = subscription

assert_type(subscription, Subscription)
assert_type(subscription.wait(5.0), bool)
assert_type(subscription.channel, str)
assert_type(subscription.initial_listen_established, threading.Event)
subscription.close()


class NullLoop:
    has_completed_cycle: bool = False

    def run(self, stop: threading.Event) -> None:
        pass

    def wake(self) -> None:
        pass

    def close(self) -> None:
        pass


custom_loop: Loop = NullLoop()

supervisor = Supervisor.start(
    custom_loop, name="dispatch", initial_backoff=0.1
)

assert_type(supervisor.is_running, bool)
assert_type(supervisor.stop(1.0), bool)

with Supervisor.start(custom_loop, name="dispatch") as held:
    assert_type(held, Supervisor)


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

plain = Service.build(
    work_factory,
    name="stats",
    wakeup=custom_wakeup,
    interval=5.0,
)
plain_loop: Loop = plain

running_plain = SupervisedService.start(
    work_factory,
    name="stats",
    wakeup=IntervalOnlyWakeup(),
    interval=5.0,
    initial_backoff=0.1,
)

assert_type(running_plain.is_running, bool)
assert_type(running_plain.stop(5.0), bool)

with SupervisedService.start(
    make_work, name="stats", wakeup=IntervalOnlyWakeup()
) as plain_handle:
    assert_type(plain_handle, SupervisedService)

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
assert_type(
    make_create_leases_table_sql(name="leases", schema="app"), sql.Composed
)

with lend() as ddl_conn:
    init_db(ddl_conn)
    init_db(ddl_conn, name="leases", schema="app")
