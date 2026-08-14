import threading

import pytest

from sqlalchemy import create_engine, text

from pgbg import IntervalOnlyWakeup, as_work_factory
from pgbg.sqlalchemy import (
    connection_factory_from_engine,
    pooled_connection_factory_from_engine,
    start_dispatcher,
    start_elected_service,
)


@pytest.fixture(name="pgbg_engine")
def _pgbg_engine(pgbg_dsn):
    engine = create_engine(
        pgbg_dsn.replace("postgresql://", "postgresql+psycopg://"),
        isolation_level="READ COMMITTED",
    )

    yield engine

    engine.dispose()


def test_connection_factory_from_engine_opens_working_connections(
    pgbg_engine,
):
    """
    The factory derived from a SQLAlchemy engine drops the `+psycopg` driver
    suffix (Psycopg rejects it in a conninfo URI outright) and opens working
    Psycopg connections to the same database.
    """
    factory = connection_factory_from_engine(pgbg_engine)

    with factory() as conn:
        assert [(1,)] == conn.execute("SELECT 1").fetchall()


def test_connection_factory_bounds_connect_time(pgbg_engine):
    """
    The derived factory adds a libpq connect timeout when the engine's URL sets
    none, so a reconnect against a dead server cannot hang forever.
    """
    factory = connection_factory_from_engine(pgbg_engine)

    with factory() as conn:
        assert "10" == conn.info.get_parameters()["connect_timeout"]


@pytest.fixture(name="timeout_engine")
def _timeout_engine(pgbg_dsn):
    """
    An engine whose URL carries its own transport bounds.
    """
    engine = create_engine(
        pgbg_dsn.replace("postgresql://", "postgresql+psycopg://")
        + "?connect_timeout=3&keepalives_idle=7"
    )

    yield engine

    engine.dispose()


def test_connection_factory_keeps_explicit_transport_params(timeout_engine):
    """
    Transport bounds in the engine's URL win over the defaults.
    """
    factory = connection_factory_from_engine(timeout_engine)

    with factory() as conn:
        params = conn.info.get_parameters()

    assert "3" == params["connect_timeout"]
    assert "7" == params["keepalives_idle"]


def test_connection_factory_guards_against_dead_paths(pgbg_engine):
    """
    The derived factory adds TCP keepalives and an unACKed-data timeout when
    the engine's URL sets none, so a dead network path crashes the run promptly
    instead of wedging it.
    """
    factory = connection_factory_from_engine(pgbg_engine)

    with factory() as conn:
        params = conn.info.get_parameters()

    assert "30000" == params["tcp_user_timeout"]
    assert "30" == params["keepalives_idle"]
    assert "10" == params["keepalives_interval"]
    assert "3" == params["keepalives_count"]


def test_start_dispatcher_is_one_handle_for_everything(pgbg_engine):
    """
    The handle start_dispatcher() returns subscribes, delivers a NOTIFY,
    reports health, and stops itself as a context manager.
    """
    with start_dispatcher(
        pgbg_engine, interval=0.05, initial_backoff=0.01
    ) as dispatcher:
        sub = dispatcher.subscribe("orders")

        assert sub.initial_listen_established.wait(1)
        # Drain the wake the LISTEN going live delivered.
        assert True is sub.wait(1)

        with pgbg_engine.connect() as conn:
            conn.execute(text("SELECT pg_notify('orders', 'hi')"))
            conn.commit()

        assert True is sub.wait(1)
        assert dispatcher.is_running

    assert not dispatcher.is_running


def test_start_elected_service_is_one_handle_for_everything(pgbg_engine):
    """
    The handle start_elected_service() returns takes leadership, runs a work
    unit, reports health, and stops itself as a context manager.
    """
    worked = threading.Event()

    def do_work():
        worked.set()
        return False

    with start_elected_service(
        as_work_factory(do_work),
        pgbg_engine,
        name="sqla-elected",
        leases="pgbg_test_service_leases",
        worker_id="worker-sqla",
        wakeup=IntervalOnlyWakeup(),
        interval=0.05,
        initial_backoff=0.01,
    ) as service:
        assert worked.wait(2.0)
        assert service.is_running
        assert service.is_leader

    assert not service.is_running


@pytest.fixture(name="single_conn_engine")
def _single_conn_engine(pgbg_engine):
    """
    An engine whose pool holds exactly one connection, so identity assertions
    cannot depend on pool order.
    """
    engine = create_engine(pgbg_engine.url, pool_size=1, max_overflow=0)

    yield engine

    engine.dispose()


def test_pooled_factory_borrows_and_returns_pool_connections(
    single_conn_engine,
):
    """
    The pooled provider lends the pool's raw Psycopg connection and returns it
    on exit instead of closing it.
    """
    provider = pooled_connection_factory_from_engine(single_conn_engine)

    with provider() as conn:
        assert [(1,)] == conn.execute("SELECT 1").fetchall()

    assert not conn.closed

    with provider() as again:
        assert again is conn


def test_pooled_factory_raises_on_driver_mismatch(pgbg_engine):
    """
    The factory raises a ValueError when the engine uses a non-Psycopg dialect.
    """

    url = str(pgbg_engine.url).replace(
        "postgresql+psycopg://", "postgresql+pg8000://"
    )

    engine = create_engine(url)

    # Engine is fully functional, but...
    with engine.connect() as conn:
        assert 1 == conn.execute(text("select 1")).scalar_one()

    # ...we reject it, if it's not our driver.
    with pytest.raises(ValueError) as exc_info:
        pooled_connection_factory_from_engine(engine)
    assert "Expected 'psycopg'" in str(exc_info.value)

    engine.dispose()
