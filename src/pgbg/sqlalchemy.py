"""
Optional SQLAlchemy integration.
"""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import psycopg

from sqlalchemy import Engine

from ._dispatcher import SupervisedDispatcher
from ._services import SupervisedElectedService
from .typing import ConnectionProvider, Wakeup, WorkFactory


__all__ = [
    "connection_factory_from_engine",
    "pooled_connection_factory_from_engine",
    "start_dispatcher",
    "start_elected_service",
]


def start_dispatcher(
    engine: Engine,
    *,
    interval: float = 1.0,
    name: str = "dispatch",
    initial_backoff: float = 0.1,
) -> SupervisedDispatcher:
    """
    Build a dispatcher, connect via *engine*'s URL, and run it supervised.

    Since the `LISTEN` connection is long-lived, the *engine*'s pool is only
    used to derive configuration parameters that are used to connect using Psycopg
    directly, so we don't clobber the pool. See
    [`connection_factory_from_engine()`][pgbg.sqlalchemy.connection_factory_from_engine] for details.

    See [`SupervisedDispatcher.start()`][pgbg.SupervisedDispatcher.start] for
    the arguments.

    Returns:
        Handle to the supervised dispatcher.
    """
    return SupervisedDispatcher.start(
        connection_factory_from_engine(engine),
        interval=interval,
        name=name,
        initial_backoff=initial_backoff,
    )


def start_elected_service(
    work_factory: WorkFactory,
    engine: Engine,
    *,
    name: str,
    worker_id: str,
    wakeup: Wakeup,
    leases: str = "pgbg_leases",
    interval: float = 1.0,
    lease_ttl: float | None = None,
    initial_backoff: float = 0.1,
) -> SupervisedElectedService:
    """
    Build an elected service for *work_factory* on *engine*'s pool and run it
    supervised.

    [Elections](leader-election.md#elections) borrow connections from *engine*'s pool.

    See
    [`SupervisedElectedService.start()`][pgbg.SupervisedElectedService.start]
    for the arguments.

    Returns:
        Handle to the supervised elected service.
    """
    return SupervisedElectedService.start(
        work_factory,
        pooled_connection_factory_from_engine(engine),
        name=name,
        leases=leases,
        worker_id=worker_id,
        wakeup=wakeup,
        interval=interval,
        lease_ttl=lease_ttl,
        initial_backoff=initial_backoff,
    )


def connection_factory_from_engine(
    engine: Engine,
) -> Callable[[], psycopg.Connection[Any]]:
    """
    Derive a Psycopg connection factory from *engine*'s URL.

    The factory opens dedicated Psycopg connections **outside** the engine's
    pool and sets connection parameters to ensure that network problems are
    detected early. User-supplied limits take precedence.

    Extras passed to `create_engine(connect_args=...)` do **not** travel
    through the URL. Write your own factory if you need them.

    Args:
        engine: The SQLAlchemy engine whose URL to use for connections.

    Returns:
        A callable that returns a new Psycopg connection when called, based on
            *engine*'s URL.
    """
    url = engine.url.set(drivername="postgresql")
    # libpq's defaults wait forever on a connect and for the kernel's
    # retransmission eternity on a dead path. These turn both into a
    # prompt crash the supervisor recovers from.
    #
    # The keepalives also detect a dead peer during pure reads and keep
    # NAT/firewall state alive. tcp_user_timeout bounds unACKed writes and is
    # Linux-only (without effect elsewhere).
    for param, value in (
        ("connect_timeout", "10"),
        ("tcp_user_timeout", "30000"),
        ("keepalives_idle", "30"),
        ("keepalives_interval", "10"),
        ("keepalives_count", "3"),
    ):
        if param not in url.query:
            url = url.update_query_pairs([(param, value)])

    conninfo = url.render_as_string(hide_password=False)

    def connect() -> psycopg.Connection[Any]:
        """
        Open one dedicated Psycopg connection.
        """
        return psycopg.connect(conninfo)

    return connect


def pooled_connection_factory_from_engine(
    engine: Engine,
) -> ConnectionProvider:
    """
    Derive a [`ConnectionProvider`][pgbg.typing.ConnectionProvider] that
    borrows from *engine*'s pool.

    Each `with provider() as conn:` checks a connection out of the pool, lends
    out the underlying Psycopg connection, and returns it to the pool on exit.

    This is the right provider for an [`ElectedService`][pgbg.ElectedService]'s
    frequent, short-lived election queries.

    The pool's return-reset only rolls back. Borrowers must not mutate session
    state (autocommit, `SET`, `LISTEN`), since a later pool user would silently
    inherit it.

    The pool connects with the engine's own settings, so put
    a `connect_timeout` and the TCP keepalive/`tcp_user_timeout` parameters
    into the engine's URL or `connect_args`. A fresh connect against an
    unreachable server, and lease statements on a dead network path, are
    otherwise unbounded.

    Args:
        engine: The SQLAlchemy engine whose pool to use for connections.

    Returns:
        A callable that returns a new Psycopg connection when called, based on
            a connection from *engine*'s pool.
    """

    if engine.dialect.driver != "psycopg":
        msg = f"Expected 'psycopg' dialect, got '{engine.dialect.driver}'"
        raise ValueError(msg)

    @contextmanager
    def provide() -> Iterator[psycopg.Connection[Any]]:
        """
        Lend out one pooled Psycopg connection.
        """
        proxied = engine.raw_connection()
        try:
            assert proxied.driver_connection is not None
            yield proxied.driver_connection
        finally:
            proxied.close()

    return provide
