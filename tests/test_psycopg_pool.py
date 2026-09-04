import threading

import pytest

from bgt import IntervalOnlyWakeup, as_work_factory
from psycopg.pq import TransactionStatus
from psycopg_pool import ConnectionPool

from pgbg import SupervisedElectedService, init_db


@pytest.fixture(name="pgbg_pool")
def _pgbg_pool(pgbg_dsn):
    """
    A pool that holds exactly one connection, so every checkout lends out the
    same connection, and a checkout that is never returned fails fast instead
    of hanging the test.
    """
    with ConnectionPool(
        pgbg_dsn, min_size=1, max_size=1, timeout=1, open=False
    ) as pool:
        yield pool


def test_pool_connection_is_a_connection_provider(pgbg_pool):
    """
    `ConnectionPool.connection` is an elected service's connection provider
    as-is: the service gets elected, works, and resigns through the pool, and
    hands every borrowed connection back healthy and free of session state.
    """
    worked = threading.Event()

    def do_work():
        worked.set()
        return False

    with SupervisedElectedService.start(
        as_work_factory(do_work),
        pgbg_pool.connection,
        name="pool-elected",
        leases="pgbg_test_service_leases",
        worker_id="worker-pool",
        wakeup=IntervalOnlyWakeup(),
        interval=0.05,
        initial_backoff=0.01,
    ) as service:
        assert worked.wait(2.0)
        assert service.is_leader

    stats = pgbg_pool.get_stats()

    # At least the election and the final resign went through the pool...
    assert 2 <= stats["requests_num"]
    # ...and no connection came back broken. The stats are a Counter, so
    # a key that was never incremented is absent.
    assert 0 == stats.get("returns_bad", 0)

    with pgbg_pool.connection() as conn:
        assert TransactionStatus.IDLE == conn.pgconn.transaction_status
        # The statement timeout of the lease SQL is transaction-local and
        # does not stick to the pooled connection.
        assert ("0",) == conn.execute("SHOW statement_timeout").fetchone()
        # Stopping resigned the lease through the pool, too.
        assert None is (
            conn.execute(
                "SELECT worker_id FROM pgbg_test_service_leases "
                "WHERE name = %s",
                ("pool-elected",),
            ).fetchone()
        )


@pytest.fixture(name="fresh_pool_leases")
def _fresh_pool_leases(pgbg_pool):
    """
    The name of a lease table that is absent before and after the test.
    """

    def drop():
        """
        Drop the table if it exists.
        """
        with pgbg_pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS pgbg_pool_leases")

    drop()

    yield "pgbg_pool_leases"

    drop()


def test_init_db_on_a_pooled_connection_commits_the_table(
    pgbg_pool, fresh_pool_leases, pg_connect
):
    """
    init_db() on a connection borrowed from the pool is committed when the
    checkout ends, so connections outside the pool see the table.
    """
    with pgbg_pool.connection() as conn:
        init_db(conn, fresh_pool_leases)

    with pg_connect() as conn:
        assert (fresh_pool_leases,) == conn.execute(
            "SELECT to_regclass(%s)", (fresh_pool_leases,)
        ).fetchone()
