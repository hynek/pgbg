from importlib.util import find_spec
from unittest.mock import patch

import psycopg
import pytest
import structlog

from pgbg import (
    NotifyDispatcher,
    SupervisedDispatcher,
    Supervisor,
    init_db,
)
from pgbg._dispatcher import DispatchLoop


collect_ignore = []

if not find_spec("sqlalchemy"):
    collect_ignore.append("tests/test_sqlalchemy.py")

_POSTGRES_DSN = "postgresql://postgres@127.0.0.1/postgres"
_PGBG_DSN = "postgresql://pgbg@127.0.0.1/pgbg"


@pytest.fixture(autouse=True)
def configure_structlog():
    """
    Configures cleanly structlog for each test case.
    """
    structlog.stdlib.recreate_defaults(log_level=None)


@pytest.fixture(name="pgbg_dsn", scope="session")
def _pgbg_dsn(pgbg_database):
    """
    A libpq DSN for the pgbg database.
    """
    return _PGBG_DSN


@pytest.fixture(name="pg_connect")
def _pg_connect(pgbg_dsn):
    """
    Return a connect factory for the pgbg database.

    Also satisfies `ConnectionProvider`, because a Psycopg connection
    is its own context manager.
    """

    def connect():
        return psycopg.connect(pgbg_dsn)

    return connect


@pytest.fixture(name="running_dispatcher")
def _running_dispatcher(pg_connect):
    """
    A dispatcher supervised against the real database, stopped at teardown.
    """
    with SupervisedDispatcher.start(
        pg_connect, interval=0.05, initial_backoff=0.01
    ) as dispatcher:
        yield dispatcher


@pytest.fixture(name="pgbg_database", scope="session", autouse=True)
def _database() -> None:
    """
    At the beginning of each test run, re-create the whole database.

    The lease table comes from pgbg's own `init_db`, so the suite runs
    against the DDL that we ship.
    """
    with psycopg.connect(_POSTGRES_DSN, autocommit=True) as conn:
        conn.execute("DROP DATABASE IF EXISTS pgbg WITH (FORCE)")
        conn.execute("DROP ROLE IF EXISTS pgbg")
        conn.execute("CREATE ROLE pgbg LOGIN")
        conn.execute(
            "CREATE DATABASE pgbg "
            "OWNER pgbg TEMPLATE template0 LOCALE 'en_US.UTF-8'"
        )

    with psycopg.connect(_PGBG_DSN, autocommit=True) as conn:
        init_db(conn, name="pgbg_test_service_leases")


@pytest.fixture(name="pgbg_maintenance_conn", scope="session")
def _maintenance_conn(pgbg_database):
    """
    A session-long autocommit connection for test housekeeping.
    """
    with psycopg.connect(_PGBG_DSN, autocommit=True) as conn:
        yield conn


@pytest.fixture(autouse=True)
def _clean_leases(pgbg_maintenance_conn):
    """
    Truncate the lease table before each test.
    """
    pgbg_maintenance_conn.execute("TRUNCATE pgbg_test_service_leases")


@pytest.fixture(name="run_supervised")
def _run_supervised(pg_connect):
    """
    Return a factory that runs dispatchers under supervision against
    the real database, and stops the supervisors at teardown.
    """
    started = []

    def run(
        *, dispatcher=None, connect=None, interval=0.05, initial_backoff=0.01
    ):
        """
        Start a supervised dispatcher with test-friendly defaults.

        Pass a pre-built *dispatcher* to subscribe before supervision
        starts, which makes the first run's LISTEN wake deterministic
        for those subscriptions.
        """
        if dispatcher is None:
            dispatcher = NotifyDispatcher(interval=interval)

        if connect is None:
            connect = pg_connect

        supervisor = Supervisor.start(
            DispatchLoop(connect=connect, dispatcher=dispatcher),
            name="dispatch",
            initial_backoff=initial_backoff,
        )

        started.append(supervisor)

        return dispatcher, supervisor

    yield run

    for supervisor in started:
        supervisor.stop()


@pytest.fixture(name="no_jitter")
def _no_jitter():
    with patch("pgbg._backoff.random.uniform", return_value=1):
        yield
