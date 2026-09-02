import datetime as dt
import threading
import time

from contextlib import contextmanager, suppress
from unittest.mock import Mock, patch

import psycopg.errors
import psycopg.sql
import pytest
import structlog

from prometheus_client import REGISTRY

from pgbg import (
    NotifyDispatcher,
    Service,
    SupervisedElectedService,
    SupervisedService,
    as_work_factory,
)
from pgbg._services import (
    SERVICE_LAST_WORK_UNIT,
    SERVICE_LEADERSHIP_CONFIRMED,
    SERVICE_LEASE_FAILURES,
    SERVICE_LEASE_OVERRUNS,
    ElectedService,
    IntervalOnlyWakeup,
    LeaderTerm,
    _attempt_election,
    _attempt_renewal,
    _resign_as_leader,
)
from pgbg.exceptions import SuppressedCrashError


_CHANNEL = "pgbg_test_channel"
_LEASES_NAME = "pgbg_test_service_leases"
_LEASES = psycopg.sql.Identifier(_LEASES_NAME)


@pytest.fixture(name="provider")
def _provider(pg_connect):
    """
    Fresh short-lived connections for elections.
    """
    return pg_connect


def delete_lease(dsn, name=None):
    """
    Delete lease rows directly, the way an outside actor would.
    """
    with psycopg.connect(dsn) as conn:
        if name is None:
            conn.execute(f"DELETE FROM {_LEASES_NAME}")
        else:
            conn.execute(
                f"DELETE FROM {_LEASES_NAME} WHERE name = %s", (name,)
            )


def make_fake_clock(start=0):
    """
    Return a manually-advanced fake *time_source* and its advance function.

    Drives `LeaderTerm`'s notion of "now" deterministically, so renewal
    deadlines and expiry are exact instead of racing the wall clock.
    """
    now = start

    def clock():
        """
        Return the current fake time, like `time.monotonic`.
        """
        return now

    def advance(seconds):
        """
        Move the fake clock forward by *seconds*.
        """
        nonlocal now
        now += seconds

    return clock, advance


def signalling_work(ran):
    """
    Return a `do_work` that records its run in *ran* and stops.
    """

    def do_work():
        """
        Signal that a work unit ran and decline further work.
        """
        ran.set()

        return False

    return do_work


def noop_work():
    """
    Do nothing and report no further work.
    """
    return False


def wait_for_thread(name, timeout=2.0):  # pragma: no cover
    """
    Wait until a thread called *name* exists, or fail.

    Excluded from coverage: how many loop turns run (if any) depends on
    thread scheduling.
    """
    deadline = time.monotonic() + timeout
    while name not in {thread.name for thread in threading.enumerate()}:
        if time.monotonic() > deadline:
            pytest.fail(f"thread {name!r} never appeared")
        time.sleep(0.01)


def forbidden_provider():
    """
    Fail the test if anything asks for a connection.
    """
    pytest.fail("the database must not be touched here")


@pytest.fixture(name="release")
def _release():
    """
    An Event that is guaranteed set at teardown.

    Units parked on it always resume, even when the test fails before
    its own release.set() line.
    """
    event = threading.Event()

    yield event

    event.set()


@pytest.fixture(name="build_service")
def _build_service(provider):
    """
    Return a factory for `ElectedService`s built without a supervisor.

    Handy for driving `_run_once` / `_wait_for_wakeup` directly: no thread, no
    supervisor, fully deterministic. Pass an explicit *term* to inject a fake
    clock.
    """

    def build(
        *,
        work_factory=None,
        term=None,
        name="test-service",
        wakeup=None,
        interval=30,
        worker_id="test-worker",
    ):
        """
        Build an `ElectedService` without supervising it.
        """
        if term is None:
            term = LeaderTerm(
                name,
                worker_id,
                60,
                get_connection=provider,
                leases=_LEASES,
            )

        return ElectedService(
            interval=interval,
            term=term,
            wakeup=wakeup if wakeup is not None else IntervalOnlyWakeup(),
            work_factory=(
                work_factory
                if work_factory is not None
                else as_work_factory(noop_work)
            ),
        )

    return build


@pytest.fixture(name="run_service")
def _run_service(provider):
    """
    Return a factory that starts supervised elected services, stopping
    them at teardown.

    Defaults are tuned for tests: a tight interval, a near-zero initial backoff
    so restarts don't stall, and an auto-generated worker id.

    Returns the running `SupervisedElectedService` handle.
    """
    handles = []

    def run(
        *,
        work_factory=None,
        worker_id=None,
        wakeup=None,
        interval=0.05,
        initial_backoff=0.01,
        lease_ttl=None,
        name="test-service",
    ):
        """
        Start a supervised service.
        """
        handle = SupervisedElectedService.start(
            work_factory
            if work_factory is not None
            else as_work_factory(noop_work),
            provider,
            name=name,
            leases=_LEASES_NAME,
            worker_id=worker_id or f"test-worker-{len(handles)}",
            wakeup=wakeup if wakeup is not None else IntervalOnlyWakeup(),
            interval=interval,
            lease_ttl=lease_ttl,
            initial_backoff=initial_backoff,
        )
        handles.append(handle)

        return handle

    yield run

    for handle in handles:
        handle.stop()


class TestLeadershipElections:
    def test_second_stays_follower_until_resignation(self, provider):
        """
        A second term stays a follower while the first's lease is live, and
        takes over once the first resigns.
        """
        term_a = LeaderTerm(
            "test-election",
            "worker-a",
            60,
            get_connection=provider,
            leases=_LEASES,
        )
        term_b = LeaderTerm(
            "test-election",
            "worker-b",
            60,
            get_connection=provider,
            leases=_LEASES,
        )

        term_a.ensure()

        assert term_a.is_leader

        term_b.ensure()

        assert not term_b.is_leader

        term_a.resign_best_effort()
        term_b.ensure()

        assert not term_a.is_leader
        assert term_b.is_leader

    def test_independent_per_name(self, pgbg_dsn, provider):
        """
        Two differently-named services lease independently, both leading at
        once.
        """
        term_a = LeaderTerm(
            "test-independent-a",
            "worker-a",
            60,
            get_connection=provider,
            leases=_LEASES,
        )
        term_b = LeaderTerm(
            "test-independent-b",
            "worker-b",
            60,
            get_connection=provider,
            leases=_LEASES,
        )

        term_a.ensure()
        term_b.ensure()

        assert term_a.is_leader
        assert term_b.is_leader

        with psycopg.connect(pgbg_dsn) as conn:
            names = {
                name
                for (name,) in conn.execute(f"SELECT name FROM {_LEASES_NAME}")
            }

        assert {"test-independent-a", "test-independent-b"} == names

    def test_ensure_before_renewal_deadline_touches_no_database(
        self, provider
    ):
        """
        Between deadlines, `ensure` answers from memory and never asks for a
        connection.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-noop",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )

        term.ensure()

        assert term.is_leader

        advance(29)  # renew_at is 30s (half the 60s ttl); stay under it

        term.get_connection = forbidden_provider
        term.ensure()

        assert term.is_leader

    def test_ensure_past_renewal_deadline_renews_and_extends_validity(
        self, provider
    ):
        """
        Past the renewal deadline, `ensure` renews and pushes the local
        validity window further out.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-renew",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )

        term.ensure()
        valid_until_before = term._valid_until

        advance(30)  # exactly at the renewal deadline
        term.ensure()

        assert term.is_leader
        assert term._valid_until > valid_until_before

    def test_is_leader_expires_locally_without_renewal(
        self, pgbg_dsn, provider
    ):
        """
        Without a renewal, `is_leader` goes False once local validity lapses,
        even though the lease row is still sitting untouched in the table.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-expiry",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )

        term.ensure()

        assert term.is_leader

        advance(60)  # past _valid_until, with no renewal in between

        assert not term.is_leader

        with psycopg.connect(pgbg_dsn) as conn:
            row = conn.execute(
                f"SELECT * FROM {_LEASES_NAME} WHERE name = %s",
                ("test-expiry",),
            ).fetchone()

        assert row is not None

    def test_renewal_lost_when_lease_row_deleted(self, pgbg_dsn, provider):
        """
        If the lease row vanishes before the next renewal, `ensure` notices the
        renewal failed at the deadline and drops leadership.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-lost",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )

        term.ensure()

        assert term.is_leader

        delete_lease(pgbg_dsn, "test-lost")

        advance(30)  # reach the renewal deadline (half the 60s ttl)
        term.ensure()

        assert not term.is_leader

    def test_resign_best_effort_swallows_connect_failure(self, provider):
        """
        `resign_best_effort` logs and swallows a connect failure and keeps
        leadership, since the record is only cleared on a successful resign.
        """
        term = LeaderTerm(
            "test-resign",
            "worker",
            60,
            get_connection=provider,
            leases=_LEASES,
        )

        term.ensure()

        assert term.is_leader

        def failing_provider():
            """
            Fail like an unreachable database.
            """
            raise RuntimeError("simulated resign-connect failure")

        term.get_connection = failing_provider
        term.resign_best_effort()

        assert term.is_leader

    def test_resign_without_a_held_term_touches_no_database(self):
        """
        Resigning a term that was never elected is a no-op.
        """
        term = LeaderTerm(
            "test-resign-noop",
            "worker",
            60,
            get_connection=forbidden_provider,
            leases=_LEASES,
        )

        term.resign_best_effort()

        assert not term.is_leader


_ELECTED_AT = dt.datetime(2026, 1, 1).astimezone()


@pytest.fixture(name="lock_leaders")
def _lock_leaders(pg_connect):
    """
    A callable that locks the lease table for the rest of the test.

    Every lease statement blocks on the lock until the transaction ends
    at teardown.
    """
    conn = pg_connect()

    def lock():
        """
        Take the lock in the connection's open transaction.
        """
        conn.execute(f"LOCK TABLE {_LEASES_NAME} IN ACCESS EXCLUSIVE MODE")

    yield lock

    conn.rollback()
    conn.close()


@pytest.fixture(name="locked_leaders")
def _locked_leaders(lock_leaders):
    """
    A lease table that is already locked when the test starts.
    """
    lock_leaders()


class TestLeaseStatementTimeouts:
    @pytest.mark.parametrize(
        "attempt",
        [
            pytest.param(
                lambda conn: _attempt_election(
                    conn,
                    _LEASES,
                    "timeout-svc",
                    "worker",
                    60,
                    timeout_seconds=0.05,
                ),
                id="election",
            ),
            pytest.param(
                lambda conn: _attempt_renewal(
                    conn,
                    _LEASES,
                    "timeout-svc",
                    "worker",
                    _ELECTED_AT,
                    60,
                    timeout_seconds=0.05,
                ),
                id="renewal",
            ),
            pytest.param(
                lambda conn: _resign_as_leader(
                    conn,
                    _LEASES,
                    "timeout-svc",
                    "worker",
                    _ELECTED_AT,
                    timeout_seconds=0.05,
                ),
                id="resign",
            ),
        ],
    )
    @pytest.mark.usefixtures("locked_leaders")
    def test_blocked_lease_statement_is_canceled(self, provider, attempt):
        """
        A lease statement that blocks on a lock is canceled by its server-side
        timeout instead of waiting forever.
        """
        with (
            provider() as conn,
            pytest.raises(psycopg.errors.QueryCanceled),
        ):
            attempt(conn)

    def test_timeout_ends_with_the_lease_transaction(self, provider):
        """
        The statement timeout a lease call sets reverts when its transaction
        ends, so a pooled connection returns unchanged.
        """
        with provider() as conn:
            _attempt_election(
                conn,
                _LEASES,
                "revert-svc",
                "worker",
                60,
                timeout_seconds=5.0,
            )

            assert "0" == conn.execute("SHOW statement_timeout").fetchone()[0]

    @pytest.mark.usefixtures("locked_leaders")
    def test_ensure_swallows_an_election_timeout(self, provider):
        """
        An election canceled by its timeout leaves the term a follower and
        warns instead of raising.
        """
        term = LeaderTerm(
            "timeout-elect",
            "worker",
            60,
            get_connection=provider,
            leases=_LEASES,
        )
        failures_before = SERVICE_LEASE_FAILURES.labels(
            name="timeout-elect"
        )._value.get()

        with (
            patch.object(LeaderTerm, "_statement_timeout", 0.05),
            structlog.testing.capture_logs() as logs,
        ):
            term.ensure()

        assert not term.is_leader
        assert ["service_leader.lease_failed"] == [
            entry["event"] for entry in logs
        ]
        assert (
            failures_before + 1
            == SERVICE_LEASE_FAILURES.labels(name="timeout-elect")._value.get()
        )

    @pytest.mark.usefixtures("locked_leaders")
    def test_ensure_swallows_an_inherited_lock_timeout(self, pgbg_dsn):
        """
        An election canceled by the connection's own lock timeout leaves
        the term a follower and warns instead of raising.
        """

        def provider():
            """
            Connect with a deployment-style lock timeout in place.
            """
            return psycopg.connect(pgbg_dsn, options="-c lock_timeout=50")

        term = LeaderTerm(
            "timeout-lock",
            "worker",
            60,
            get_connection=provider,
            leases=_LEASES,
        )

        with structlog.testing.capture_logs() as logs:
            term.ensure()

        assert not term.is_leader
        assert ["service_leader.lease_failed"] == [
            entry["event"] for entry in logs
        ]

    def test_ensure_swallows_a_renewal_timeout(self, provider, lock_leaders):
        """
        A renewal canceled by its timeout keeps the held lease and warns
        instead of raising, leaving the validity window to decay.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "timeout-renew",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()

        advance(31)
        lock_leaders()

        with (
            patch.object(LeaderTerm, "_statement_timeout", 0.05),
            structlog.testing.capture_logs() as logs,
        ):
            term.ensure()

        assert term.is_leader
        assert ["service_leader.lease_failed"] == [
            entry["event"] for entry in logs
        ]

        # At exactly 60, the lease has decayed and we're follower.
        advance(29)
        assert not term.is_leader

    @pytest.mark.parametrize(
        ("lease_ttl", "expected"),
        [
            pytest.param(0.2, 0.5, id="floor"),
            pytest.param(11.0, 2.75, id="keeper-cadence"),
            pytest.param(100.0, 5.0, id="cap"),
        ],
    )
    def test_statement_timeout_is_the_clamped_keeper_cadence(
        self, lease_ttl, expected
    ):
        """
        A term limits its lease statements by the keeper cadence, a quarter of
        the TTL, clamped between half a second and five seconds.
        """
        term = LeaderTerm(
            "timeout-clamp",
            "worker",
            lease_ttl,
            get_connection=forbidden_provider,
            leases=_LEASES,
        )

        assert expected == term._statement_timeout


class TestRunOnce:
    def test_skips_the_work_unit_for_a_follower(self, build_service, provider):
        """
        A follower's cycle returns without ever invoking `do_work`.
        """
        leader = LeaderTerm(
            "test-gated",
            "leader",
            60,
            get_connection=provider,
            leases=_LEASES,
        )
        leader.ensure()

        do_work = Mock(return_value=False)
        service = build_service(
            term=LeaderTerm(
                "test-gated",
                "follower",
                60,
                get_connection=provider,
                leases=_LEASES,
            ),
        )

        service._run_once(do_work, threading.Event())

        do_work.assert_not_called()

    def test_repeats_the_work_unit_while_it_reports_work(self, build_service):
        """
        A `do_work` returning True is re-invoked right away within one wakeup until
        it reports no more work.
        """
        calls = []

        def do_work():
            """
            Report more work three times.
            """
            calls.append(True)

            return len(calls) < 3

        service = build_service()

        service._run_once(do_work, threading.Event())

        assert 3 == len(calls)
        assert all(calls)

    def test_stops_between_units_when_stop_event_set(self, build_service):
        """
        Work stops between work units the moment `stop_event` is set, even
        while `do_work` still reports more work.
        """
        calls = []

        def do_work():
            """
            Always report more work.
            """
            calls.append(True)

            return True

        service = build_service()
        stop = threading.Event()
        stop.set()

        service._run_once(do_work, stop)

        assert 1 == len(calls)

    def test_renews_the_lease_across_a_drain_longer_than_the_ttl(
        self, build_service, provider
    ):
        """
        A drain far longer than the TTL keeps leadership, because `ensure`
        renews the lease at its deadline between work units instead of once per
        cycle.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-drain",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        calls = []

        def draining_work():
            """
            Advance well past a renewal deadline each work unit, until drained.
            """
            calls.append(True)
            advance(20.0)

            return len(calls) < 6

        service = build_service(term=term)

        service._run_once(draining_work, threading.Event())

        assert 6 == len(calls)
        assert all(calls)
        # 120 fake-seconds elapsed, twice the 60s ttl, yet still leader. Only
        # possible because renewal pushed _valid_until out past the original.
        assert clock() > term.lease_ttl
        assert term.is_leader
        assert term._valid_until > 60

    def test_reports_work_that_ends_after_the_lease(
        self, build_service, provider
    ):
        """
        A work unit that ends after its local lease deadline emits a warning
        and increments the service's overrun counter.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-overrun",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        counter = SERVICE_LEASE_OVERRUNS.labels(name="test-overrun")
        counter_before = counter._value.get()

        def slow_work():
            """
            Run past the local lease deadline.
            """
            advance(61)

            return False

        service = build_service(term=term)

        with structlog.testing.capture_logs() as logs:
            service._run_once(slow_work, threading.Event())

        overruns = [
            entry
            for entry in logs
            if entry["event"] == "service.lease_overrun"
        ]

        assert counter_before + 1 == counter._value.get()
        assert 1 == len(overruns)
        assert "test-overrun" == overruns[0]["name"]
        assert "worker" == overruns[0]["worker_id"]
        assert 1 == overruns[0]["overrun_seconds"]
        assert "lapsed" == overruns[0]["reason"]
        assert "warning" == overruns[0]["log_level"]


@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"interval": 0},
        {"interval": -1.0},
        {"worker_id": ""},
        {"name": ""},
        {"leases": ""},
        {"leases": "public..leases"},
        {"lease_ttl": 0},
        {"lease_ttl": -1.0},
    ],
)
def test_service_thread_build_validates_arguments(provider, bad_kwargs):
    """
    `build` rejects invalid arguments before constructing the service.
    """
    base_kwargs = {
        "name": "test-validate",
        "leases": _LEASES_NAME,
        "worker_id": "test-validate-worker",
        "wakeup": IntervalOnlyWakeup(),
    }

    with pytest.raises(ValueError):
        ElectedService.build(
            as_work_factory(noop_work),
            provider,
            **{**base_kwargs, **bad_kwargs},
        )


class TestServiceLoopLifecycle:
    def test_run_returns_without_waiting_when_stopped_mid_work_unit(
        self, build_service
    ):
        """
        Stop set while a work unit runs makes run exit without waiting for
        a wakeup.
        """
        stop = threading.Event()

        def do_work():
            """
            Request the stop, then report no more work.
            """
            stop.set()

            return False

        service = build_service(
            work_factory=as_work_factory(do_work)
        )  # interval 30: a wait would hang

        service.run(stop)

    def test_recovers_from_a_work_unit_error(self, run_service):
        """
        A transient error from `do_work` is retried.
        """
        fail = True
        worked = threading.Event()

        def do_work():
            """
            Fail once, then signal and stop.
            """
            nonlocal fail
            if fail:
                fail = False
                raise RuntimeError("simulated transient do_work failure")

            worked.set()

            return False

        handle = run_service(work_factory=as_work_factory(do_work))

        assert worked.wait(timeout=2.0)
        assert handle.is_running

    def test_wait_for_wakeup_returns_on_a_subscription_wake(
        self, build_service
    ):
        """
        A wake delivered to the subscription ends the wait promptly and logs
        it, instead of waiting out the interval.
        """
        dispatcher = NotifyDispatcher()
        sub = dispatcher.subscribe(_CHANNEL)
        service = build_service(wakeup=sub)
        sub.wake()

        with structlog.testing.capture_logs() as logs:
            service._wait_for_wakeup()

        assert "service.notified" in [entry["event"] for entry in logs]

    def test_wait_for_wakeup_times_out_without_a_wake(self, build_service):
        """
        With no wake within the interval, the wait ends without logging one.
        """
        dispatcher = NotifyDispatcher()
        sub = dispatcher.subscribe(_CHANNEL)
        service = build_service(wakeup=sub, interval=0.01)

        with structlog.testing.capture_logs() as logs:
            service._wait_for_wakeup()

        assert "service.notified" not in [entry["event"] for entry in logs]

    def test_interval_only_wakeup_ends_the_wait_when_woken(
        self, build_service
    ):
        """
        Without a dispatcher the service polls on its interval, and
        service.wake() ends the wait promptly.
        """
        service = build_service()  # interval-only wakeup

        service.wake()
        service._wait_for_wakeup()

    def test_interval_only_wakeup_consumes_the_wake(self):
        """
        One wait consumes the wake, so the next wait blocks again instead of
        staying woken forever.
        """
        wakeup = IntervalOnlyWakeup()

        wakeup.wake()

        assert True is wakeup.wait(0.01)
        assert False is wakeup.wait(0.01)

    def test_interval_only_wakeup_is_born_woken(self):
        """
        A fresh wakeup starts woken, so a wait-first service runs its first
        work unit at startup.
        """
        assert True is IntervalOnlyWakeup().wait(1000)

    def test_runs_and_stops_under_a_real_dispatcher(
        self, run_service, running_dispatcher
    ):
        """
        A service wired to a running dispatcher establishes its subscription,
        runs a work unit, and stops promptly when `stop` wakes its subscription wait.
        """
        worked = threading.Event()
        sub = running_dispatcher.subscribe(_CHANNEL)
        handle = run_service(
            work_factory=as_work_factory(signalling_work(worked)), wakeup=sub
        )

        assert handle._service._wakeup.initial_listen_established.wait(2.0)
        assert worked.wait(2.0)

        assert handle.stop(2.0)
        assert not handle.is_running

    def test_startup_runs_exactly_one_work_unit(
        self, run_service, running_dispatcher
    ):
        """
        The LISTEN going live triggers the startup work unit, and only that
        one.
        """
        work_units = []
        worked = threading.Event()

        def do_work():
            """
            Record the work unit.
            """
            work_units.append(True)
            worked.set()

            return False

        sub = running_dispatcher.subscribe(_CHANNEL)
        run_service(
            work_factory=as_work_factory(do_work), wakeup=sub, interval=30
        )

        assert worked.wait(2000)
        # Give a second activation ample time to land before counting.
        time.sleep(0.02)

        assert 1 == len(work_units)

    def test_runs_and_stops_without_a_dispatcher(self, run_service):
        """
        With no dispatcher the interval-only service takes leadership and, on
        stop, wakes out of its interval wait instead of sitting it out.
        """
        # interval=30: stopping within 2s is only possible if stop wakes the
        # interval wait rather than waiting it out.
        handle = run_service(interval=30)

        deadline = time.monotonic() + 2
        while not handle.is_leader:
            if time.monotonic() > deadline:
                pytest.fail("service did not take leadership in time")
            time.sleep(0.01)

        assert handle.stop(2.0)
        assert not handle.is_running

    def test_start_elected_service_is_one_handle(self, provider):
        """
        The one-call entry point returns a handle that reports leadership
        and health, and stops itself as a context manager.
        """
        worked = threading.Event()

        with SupervisedElectedService.start(
            as_work_factory(signalling_work(worked)),
            provider,
            name="test-one-handle",
            leases=_LEASES_NAME,
            worker_id="one-handle-worker",
            wakeup=IntervalOnlyWakeup(),
            interval=0.05,
            lease_ttl=60,
            initial_backoff=0.01,
        ) as handle:
            assert worked.wait(2.0)
            # The work unit can only have run as the leader.
            assert handle.is_leader
            assert handle.is_running

        assert not handle.is_running


class TestElectedAtGauge:
    def test_election_stamps_the_gauge(self, provider):
        """
        Winning an election stamps the gauge with the current unix time.
        """
        before = time.time()
        term = LeaderTerm(
            "gauge-elect",
            "worker",
            60,
            get_connection=provider,
            leases=_LEASES,
        )

        term.ensure()

        assert term.is_leader
        assert (
            before
            <= SERVICE_LEADERSHIP_CONFIRMED.labels(
                name="gauge-elect"
            )._value.get()
        )

    def test_renewal_refreshes_the_gauge(self, provider):
        """
        A successful renewal restamps the gauge, so a live leader never
        goes stale.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "gauge-renew",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()

        advance(31)
        before = time.time()
        term.ensure()

        assert term.is_leader
        assert (
            before
            <= SERVICE_LEADERSHIP_CONFIRMED.labels(
                name="gauge-renew"
            )._value.get()
        )

    def test_expiry_zeroes_the_gauge(self, provider):
        """
        A `renew_if_due` that clears an expired lease zeroes the gauge.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "gauge-expire",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()

        advance(61)
        term.get_connection = forbidden_provider
        term.renew_if_due()

        assert (
            0.0
            == SERVICE_LEADERSHIP_CONFIRMED.labels(
                name="gauge-expire"
            )._value.get()
        )

    def test_deposition_zeroes_the_gauge(self, provider, pgbg_dsn):
        """
        A renewal that finds its lease row gone zeroes the gauge.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "gauge-depose",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()

        delete_lease(pgbg_dsn, "gauge-depose")
        advance(31)
        term.renew_if_due()

        assert not term.is_leader
        assert (
            0.0
            == SERVICE_LEADERSHIP_CONFIRMED.labels(
                name="gauge-depose"
            )._value.get()
        )

    def test_resigning_zeroes_the_gauge(self, provider):
        """
        A successful resignation zeroes the gauge.
        """
        term = LeaderTerm(
            "gauge-resign",
            "worker",
            60,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()

        term.resign_best_effort()

        assert not term.is_leader
        assert (
            0.0
            == SERVICE_LEADERSHIP_CONFIRMED.labels(
                name="gauge-resign"
            )._value.get()
        )

    def test_run_initializes_the_gauge_for_a_follower(self, build_service):
        """
        A run that starts without a lease creates the series at 0, so absence
        cannot hide the staleness signal.
        """
        service = build_service(name="gauge-init")
        stop = threading.Event()
        stop.set()

        service.run(stop)

        assert (
            0.0
            == SERVICE_LEADERSHIP_CONFIRMED.labels(
                name="gauge-init"
            )._value.get()
        )

    def test_run_keeps_the_gauge_of_a_held_lease(
        self, provider, build_service
    ):
        """
        A restarted run that still holds its lease keeps the last confirmation
        instead of zeroing it.
        """
        term = LeaderTerm(
            "gauge-keep",
            "worker",
            60,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()
        stamped = SERVICE_LEADERSHIP_CONFIRMED.labels(
            name="gauge-keep"
        )._value.get()

        service = build_service(term=term)
        stop = threading.Event()
        stop.set()
        service.run(stop)

        assert (
            stamped
            == SERVICE_LEADERSHIP_CONFIRMED.labels(
                name="gauge-keep"
            )._value.get()
        )


class TestRenewIfDue:
    def test_renews_a_due_lease(self, provider):
        """
        A `renew_if_due` past the renewal deadline extends the local validity
        window.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-hb-renew",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()
        deadline_before = term.lease_deadline

        advance(30)
        term.renew_if_due()

        assert term.is_leader
        assert term.lease_deadline > deadline_before

    def test_before_the_renewal_deadline_touches_no_database(self, provider):
        """
        An undue `renew_if_due` asks for no connection.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-hb-undue",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()

        advance(29)
        term.get_connection = forbidden_provider
        term.renew_if_due()

        assert term.is_leader

    def test_without_a_lease_touches_no_database(self):
        """
        `renew_if_due` without a held lease asks for no connection.
        """
        term = LeaderTerm(
            "test-hb-none",
            "worker",
            60,
            get_connection=forbidden_provider,
            leases=_LEASES,
        )

        term.renew_if_due()

        assert not term.is_leader

    def test_clears_an_expired_lease_without_a_database_visit(self, provider):
        """
        `renew_if_due` on an expired lease clears it locally and logs the loss
        without asking for a connection.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-hb-expired",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()

        advance(61)
        term.get_connection = forbidden_provider
        with structlog.testing.capture_logs() as logs:
            term.renew_if_due()

        assert not term.is_leader
        assert [("service_leader.lost", "expired")] == [
            (entry["event"], entry["reason"]) for entry in logs
        ]

    def test_notices_deposition(self, provider, pgbg_dsn):
        """
        A due renewal whose lease row vanished drops leadership and logs the
        loss as deposed.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-hb-deposed",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()

        delete_lease(pgbg_dsn, "test-hb-deposed")

        advance(30)
        with structlog.testing.capture_logs() as logs:
            term.renew_if_due()

        assert not term.is_leader
        assert [("service_leader.lost", "deposed")] == [
            (entry["event"], entry["reason"]) for entry in logs
        ]

    def test_renew_without_a_record_touches_no_database(self):
        """
        A renewal that lost its record to a concurrent expiry is a no-op.
        """
        term = LeaderTerm(
            "test-hb-raceless",
            "worker",
            60,
            get_connection=forbidden_provider,
            leases=_LEASES,
        )

        term._renew()

        assert not term.is_leader

    def test_failure_is_logged_not_raised(self, provider, build_service):
        """
        A failing renewal is downgraded to a warning instead of killing the
        keeper thread.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-hb-fail",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()
        advance(30)

        def failing_provider():
            """
            Fail like an unreachable database.
            """
            raise RuntimeError("simulated renewal-connect failure")

        term.get_connection = failing_provider
        failures_before = SERVICE_LEASE_FAILURES.labels(
            name="test-hb-fail"
        )._value.get()

        with structlog.testing.capture_logs() as logs:
            term._renew_safely()

        assert "service_leader.lease_failed" in [
            entry["event"] for entry in logs
        ]
        assert term.is_leader  # still locally valid; retried next do_work
        assert (
            failures_before + 1
            == SERVICE_LEASE_FAILURES.labels(name="test-hb-fail")._value.get()
        )


class TestLeaseKeeper:
    def test_keeper_death_is_logged_not_lost(self):
        """
        A BaseException from a renewal ends the keeper with an error
        log instead of dying silently on stderr.
        """
        term = LeaderTerm(
            "keeper-death",
            "worker",
            0.04,
            get_connection=forbidden_provider,
            leases=_LEASES,
        )

        with (
            patch.object(
                LeaderTerm,
                "_renew_safely",
                side_effect=SystemExit("boom"),
            ),
            structlog.testing.capture_logs() as logs,
        ):
            term._keep_alive(threading.Event())

        (entry,) = logs
        assert "service_leader.keeper_died" == entry["event"]
        assert "error" == entry["log_level"]

    def test_keeps_the_lease_through_a_slow_unit(self, run_service):
        """
        A work unit far slower than the TTL neither loses the lease nor counts as an
        overrun, because the keeper renews under it.
        """
        counter = SERVICE_LEASE_OVERRUNS.labels(name="test-service")
        counter_before = counter._value.get()
        worked = threading.Event()
        worked_again = threading.Event()

        def slow_work():
            """
            Outlive several TTLs once, then signal every further work unit.
            """
            if not worked.is_set():
                time.sleep(0.4)
                worked.set()
            else:
                worked_again.set()

            return False

        handle = run_service(
            work_factory=as_work_factory(slow_work), lease_ttl=0.2
        )

        assert worked.wait(2.0)
        assert worked_again.wait(2.0)
        assert handle.is_leader
        assert counter_before == counter._value.get()

    def test_keeper_dies_with_the_run(self, run_service):
        """
        The keeper thread lives while the service runs and is joined when it
        stops.
        """
        handle = run_service(name="test-keeper-life")

        wait_for_thread("lease-keeper-test-keeper-life")

        assert handle.stop(2.0)

        assert "lease-keeper-test-keeper-life" not in {
            thread.name for thread in threading.enumerate()
        }

    def test_deposition_mid_unit_is_an_overrun(
        self, run_service, pgbg_dsn, release
    ):
        """
        Losing the lease under a running work unit drops leadership promptly
        and counts the work unit as an overrun when it ends.
        """
        counter = SERVICE_LEASE_OVERRUNS.labels(name="test-service")
        counter_before = counter._value.get()
        in_unit = threading.Event()

        def parked_work():
            """
            Park inside the work unit until the test releases it.
            """
            in_unit.set()
            release.wait(5)

            return False

        handle = run_service(
            work_factory=as_work_factory(parked_work), lease_ttl=0.2
        )

        assert in_unit.wait(2.0)

        delete_lease(pgbg_dsn)

        # The keeper's next due renewal finds the row gone and deposes us.
        deadline = time.monotonic() + 2
        while handle.is_leader:
            if time.monotonic() > deadline:
                pytest.fail("keeper did not notice the deposition in time")
            time.sleep(0.01)

        release.set()

        deadline = time.monotonic() + 2
        while counter._value.get() == counter_before:
            if time.monotonic() > deadline:
                pytest.fail(
                    "the deposed work unit was not counted as an overrun"
                )
            time.sleep(0.01)

    def test_stale_failed_renewal_does_not_depose_a_fresh_term(
        self, provider, pgbg_dsn
    ):
        """
        A renewal that resolves after a fresh election replaced its term is
        discarded instead of deposing the new term.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-hb-stale",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()
        first_elected_at = term._record.elected_at
        advance(30)  # renewal due

        def usurping_provider():
            """
            Replace the term mid-visit, as a racing election would.
            """
            delete_lease(pgbg_dsn, "test-hb-stale")

            term.get_connection = provider
            term._elect()

            return provider()

        term.get_connection = usurping_provider
        with structlog.testing.capture_logs() as logs:
            term.renew_if_due()

        assert term.is_leader
        assert first_elected_at != term._record.elected_at
        assert [] == [
            entry for entry in logs if entry["event"] == "service_leader.lost"
        ]

    def test_a_slower_renewal_cannot_shrink_a_newer_deadline(self, provider):
        """
        A renewal that resolves after a faster renewal of the same term is
        discarded instead of regressing the validity window.
        """
        clock, advance = make_fake_clock()
        term = LeaderTerm(
            "test-hb-monotonic",
            "worker",
            60,
            time_source=clock,
            get_connection=provider,
            leases=_LEASES,
        )
        term.ensure()
        advance(30)  # renewal due; the slow renewal's t0 is now

        def delaying_provider():
            """
            Let a faster renewal of the same term finish mid-visit.
            """
            term.get_connection = provider
            advance(20)
            term._renew()

            return provider()

        term.get_connection = delaying_provider
        term.renew_if_due()

        assert term.is_leader
        assert 110 == term._valid_until  # the faster renewal's window


class TestService:
    def test_run_once_repeats_while_work_reports_more(self):
        """
        Work units repeat while `do_work` returns True and stop when it reports
        no further work.
        """
        calls = []

        def do_work():
            """
            Record the work unit and ask to run twice more.
            """
            calls.append(True)

            return len(calls) < 3

        service = Service.build(
            as_work_factory(do_work),
            name="plain",
            wakeup=IntervalOnlyWakeup(),
            interval=30,
        )

        service._run_once(do_work, threading.Event())

        assert 3 == len(calls)

    def test_run_once_stamps_the_last_work_unit_gauge(self):
        """
        Every unit stamps the last-work-unit gauge for this service.
        """
        before = SERVICE_LAST_WORK_UNIT.labels(
            name="plain-service"
        )._value.get()
        service = Service.build(
            as_work_factory(noop_work),
            name="plain-service",
            wakeup=IntervalOnlyWakeup(),
            interval=30,
        )

        service._run_once(noop_work, threading.Event())

        assert (
            before
            < SERVICE_LAST_WORK_UNIT.labels(name="plain-service")._value.get()
        )

    def test_run_returns_without_waiting_when_stopped_mid_work_unit(self):
        """
        A stop request during a work unit ends the run without a wait.
        """
        stop = threading.Event()

        def do_work():
            """
            Request the stop and claim more work.
            """
            stop.set()

            return True

        service = Service.build(
            as_work_factory(do_work),
            name="plain-stop",
            wakeup=IntervalOnlyWakeup(),
            interval=30,
        )

        service.run(stop)

        assert service.has_completed_cycle

    @pytest.mark.parametrize(
        "bad_kwargs",
        [{"interval": 0}, {"name": ""}],
    )
    def test_build_validates_arguments(self, bad_kwargs):
        """
        Bad intervals and empty names are rejected up front.
        """
        kwargs = {
            "name": "plain",
            "wakeup": IntervalOnlyWakeup(),
            "interval": 30,
        } | bad_kwargs

        with pytest.raises(ValueError):
            Service.build(as_work_factory(noop_work), **kwargs)

    def test_crash_mid_drain_keeps_completed_progress(self):
        """
        A work unit that succeeded counts as progress even when a later work
        unit of the same drain crashes, so the supervisor resets its backoff.
        """
        calls = []

        def do_work():
            """
            Succeed once, then crash.
            """
            calls.append(True)
            if len(calls) > 1:
                raise RuntimeError("second work unit crashed")

            return True

        service = Service.build(
            as_work_factory(do_work),
            name="plain-drain-crash",
            wakeup=IntervalOnlyWakeup(),
            interval=30,
        )

        with pytest.raises(RuntimeError, match="second work unit crashed"):
            service.run(threading.Event())

        assert service.has_completed_cycle

    def test_crashing_from_birth_still_creates_the_last_work_unit_series(self):
        """
        The last-work-unit series exists from run start, so a staleness alert never
        sits in no-data for a service that cannot complete a work unit.
        """

        def do_work():
            """
            Crash before any work unit completes.
            """
            raise RuntimeError("broken from birth")

        service = Service.build(
            as_work_factory(do_work),
            name="plain-birth-crash",
            wakeup=IntervalOnlyWakeup(),
            interval=30,
        )

        with pytest.raises(RuntimeError, match="broken from birth"):
            service.run(threading.Event())

        assert 0.0 == REGISTRY.get_sample_value(
            "pgbg_service_last_work_unit_timestamp_seconds",
            {"name": "plain-birth-crash"},
        )

    def test_wait_for_wakeup_times_out_without_a_wake(self):
        """
        Without a wake, the wait falls back to the interval timeout.
        """
        wakeup = IntervalOnlyWakeup()
        # Consume the initial wake; this test is about the timeout path.
        assert True is wakeup.wait(0)
        service = Service.build(
            as_work_factory(noop_work),
            name="plain-timeout",
            wakeup=wakeup,
            interval=0.01,
        )

        with structlog.testing.capture_logs() as logs:
            service._wait_for_wakeup()

        assert [] == logs

    def test_wait_for_wakeup_returns_on_a_wake(self):
        """
        A wake ends the wait promptly and is logged.
        """
        wakeup = IntervalOnlyWakeup()
        wakeup.wake()
        service = Service.build(
            as_work_factory(noop_work),
            name="plain-woken",
            wakeup=wakeup,
            interval=30,
        )

        with structlog.testing.capture_logs() as logs:
            service._wait_for_wakeup()

        assert ["service.notified"] == [entry["event"] for entry in logs]

    def test_service_wakes_on_a_real_notification(
        self, running_dispatcher, pgbg_dsn
    ):
        """
        A NOTIFY travels through a real dispatcher and wakes the service for
        its first work unit.
        """
        first_unit = threading.Event()

        def do_work():
            """
            Signal the work unit.
            """
            first_unit.set()

            return False

        sub = running_dispatcher.subscribe(_CHANNEL)
        assert sub.initial_listen_established.wait(5)
        # Drain the wake the LISTEN going live delivered, so only a real
        # NOTIFY can wake the service into its first work unit.
        assert True is sub.wait(1)

        with SupervisedService.start(
            as_work_factory(do_work),
            name="plain-notified",
            wakeup=sub,
            interval=30000,
            initial_backoff=0.01,
        ):
            with psycopg.connect(pgbg_dsn) as conn:
                conn.execute(
                    "SELECT pg_notify(%(channel)s, '')",
                    {"channel": _CHANNEL},
                )
                conn.commit()

            assert first_unit.wait(5)

    def test_supervised_service_is_one_handle(self):
        """
        start() runs the service under supervision, and the handle stops it as
        a context manager.
        """
        ran = threading.Event()

        def do_work():
            """
            Signal that a work unit ran.
            """
            ran.set()

            return False

        with SupervisedService.start(
            as_work_factory(do_work),
            name="plain-supervised",
            wakeup=IntervalOnlyWakeup(),
            interval=0.05,
            initial_backoff=0.01,
        ) as handle:
            assert ran.wait(2)
            assert handle.is_running

        assert not handle.is_running


class TestWorkFactory:
    def test_as_work_factory_yields_the_plain_callable(self):
        """
        The wrapped factory yields the callable itself and hands out a fresh
        context manager per call.
        """
        factory = as_work_factory(noop_work)

        with factory() as first, factory() as second:
            assert noop_work is first
            assert noop_work is second

    def test_sets_up_and_cleans_up_around_a_run(self):
        """
        A run enters the factory before the first work unit and exits it when
        the run ends.
        """
        events = []
        stop = threading.Event()

        def do_work():
            """
            Record the work unit and request the stop.
            """
            events.append("work")
            stop.set()

            return False

        @contextmanager
        def work_factory():
            """
            Record setup and cleanup around the run.
            """
            events.append("setup")
            try:
                yield do_work
            finally:
                events.append("cleanup")

        service = Service.build(
            work_factory,
            name="factory-lifecycle",
            wakeup=IntervalOnlyWakeup(),
            interval=30,
        )

        service.run(stop)

        assert ["setup", "work", "cleanup"] == events

    def test_cleanup_runs_on_a_crash(self):
        """
        A crashing work unit still unwinds through the factory's cleanup, and
        the crash propagates.
        """
        events = []

        def crashing_work():
            """
            Crash the work unit.
            """
            raise RuntimeError("simulated work crash")

        @contextmanager
        def work_factory():
            """
            Record cleanup on the way out.
            """
            try:
                yield crashing_work
            finally:
                events.append("cleanup")

        service = Service.build(
            work_factory,
            name="factory-crash",
            wakeup=IntervalOnlyWakeup(),
            interval=30,
        )

        with pytest.raises(RuntimeError, match="simulated work crash"):
            service.run(threading.Event())

        assert ["cleanup"] == events

    def test_reenters_the_factory_per_run(self):
        """
        A crash restart calls the factory again.
        """
        setups = []
        worked = threading.Event()

        def do_work():
            """
            Crash the first run's work unit, then signal and stop.
            """
            if len(setups) < 2:
                raise RuntimeError("simulated transient failure")
            worked.set()

            return False

        @contextmanager
        def work_factory():
            """
            Count the setups.
            """
            setups.append(True)
            yield do_work

        with SupervisedService.start(
            work_factory,
            name="factory-reenter",
            wakeup=IntervalOnlyWakeup(),
            interval=0.05,
            initial_backoff=0.01,
        ):
            assert worked.wait(1)

        assert 2 == len(setups)

    def test_suppressed_crash_is_raised(self):
        """
        A factory that swallows the run's crash raises SuppressedCrashError
        instead of ending the run silently.
        """

        def crashing_work():
            """
            Crash the work unit.
            """
            raise RuntimeError("simulated work crash")

        @contextmanager
        def swallowing_factory():
            """
            Swallow the crash, as a well-meaning user might.
            """
            with suppress(RuntimeError):
                yield crashing_work

        service = Service.build(
            swallowing_factory,
            name="factory-swallow",
            wakeup=IntervalOnlyWakeup(),
            interval=30,
        )

        with pytest.raises(SuppressedCrashError):
            service.run(threading.Event())

    def test_suppressed_crash_is_raised_for_an_elected_service(
        self, build_service
    ):
        """
        The elected service raises SuppressedCrashError just the same.
        """

        def crashing_work():
            """
            Crash the work unit.
            """
            raise RuntimeError("simulated work crash")

        @contextmanager
        def swallowing_factory():
            """
            Swallow the crash.
            """
            with suppress(RuntimeError):
                yield crashing_work

        service = build_service(
            work_factory=swallowing_factory,
            name="factory-swallow-elected",
        )

        with pytest.raises(SuppressedCrashError):
            service.run(threading.Event())
