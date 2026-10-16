import socket
import threading

from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from selectors import EVENT_READ, DefaultSelector
from unittest.mock import Mock, call, patch

import attrs
import psycopg
import pytest

from bgt import SupervisedService, as_work_factory
from prometheus_client import REGISTRY

from pgbg import NotifyDispatcher
from pgbg._dispatcher import (
    DISPATCHER_LAST_CYCLE,
    DispatchLoop,
    _SocketWakeup,
)


def send_notify(dsn, channel, payload=""):
    """
    Send a `pg_notify()` on *channel* with *payload* from a fresh, committed
    transaction, the way an unrelated process would.
    """
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "SELECT pg_notify(%(channel)s, %(payload)s)",
            {"channel": channel, "payload": payload},
        )
        conn.commit()


@pytest.fixture(name="pg_conn")
def _pg_conn(pg_connect):
    """
    A dedicated autocommit Psycopg connection, like the one `run` drives.
    """
    conn = pg_connect()
    conn.autocommit = True

    yield conn

    conn.close()


def listening_channels(conn):
    """
    The channels *conn*'s session is currently listening on.
    """
    rows = conn.execute("SELECT pg_listening_channels()")
    return {channel for (channel,) in rows}


@pytest.fixture(name="fake_conn")
def _fake_conn():
    """
    A fake connection whose file descriptor is real but never readable, so
    `run` can register it with its selector and only a wake ends the wait.
    """
    reader, writer = socket.socketpair()
    conn = Mock()
    conn.fileno.return_value = reader.fileno()

    yield conn

    reader.close()
    writer.close()


@pytest.fixture(name="wakeup")
def _wakeup():
    """
    A socket wakeup that is closed at teardown.
    """
    wakeup = _SocketWakeup()

    yield wakeup

    wakeup.close()


@attrs.define
class RecordingDispatcher(NotifyDispatcher):
    """
    A dispatcher that reports every reconcile's LISTEN set and every routed
    channel to a queue, so a test can observe loop passes from the outside.
    """

    reconciled: Queue = attrs.field(factory=Queue, init=False)
    routed: Queue = attrs.field(factory=Queue, init=False)

    def _reconcile(self, pgconn, listening):
        """
        Reconcile, then report the resulting LISTEN set.
        """
        super()._reconcile(pgconn, listening)
        self.reconciled.put(set(listening))

    def _route(self, channel):
        """
        Route, then report the channel.
        """
        super()._route(channel)
        self.routed.put(channel)


class TestReconcile:
    def test_two_subscriptions_on_one_channel_share_its_listen(self, pg_conn):
        """
        Two subscriptions on one channel leave the session listening on it, and
        both are established.
        """
        dispatcher = NotifyDispatcher()
        sub_a = dispatcher.subscribe("orders")
        sub_b = dispatcher.subscribe("orders")
        listening = set()

        dispatcher._reconcile(pg_conn, listening)

        assert {"orders"} == listening_channels(pg_conn)
        assert {"orders"} == listening
        assert sub_a.initial_listen_established.is_set()
        assert sub_b.initial_listen_established.is_set()

    def test_stays_listening_while_one_subscription_remains(self, pg_conn):
        """
        Closing one of two subscriptions on a channel leaves the session
        listening on it while the other subscription is live.
        """
        dispatcher = NotifyDispatcher()
        sub_a = dispatcher.subscribe("orders")
        dispatcher.subscribe("orders")
        listening = set()
        dispatcher._reconcile(pg_conn, listening)

        sub_a.close()
        sub_a.close()
        dispatcher._reconcile(pg_conn, listening)

        assert {"orders"} == listening_channels(pg_conn)
        assert {"orders"} == listening

    def test_stops_listening_when_the_last_subscription_closes(self, pg_conn):
        """
        Closing the last subscription on a channel leaves the session no longer
        listening on it.
        """
        dispatcher = NotifyDispatcher()
        sub_a = dispatcher.subscribe("orders")
        sub_b = dispatcher.subscribe("orders")
        listening = set()
        dispatcher._reconcile(pg_conn, listening)

        sub_a.close()
        sub_b.close()
        dispatcher._reconcile(pg_conn, listening)

        assert set() == listening_channels(pg_conn)
        assert set() == listening

    def test_relistens_every_desired_channel_from_a_fresh_set(self, pg_conn):
        """
        Reconciling with an empty `listening` set re-LISTENs every desired
        channel, the way a run recovers after a reconnect.
        """
        dispatcher = NotifyDispatcher()
        dispatcher.subscribe("orders")
        dispatcher.subscribe("payments")

        dispatcher._reconcile(pg_conn, set())

        assert {"orders", "payments"} == listening_channels(pg_conn)

    def test_a_late_subscription_is_established_on_reconcile(self, pg_conn):
        """
        A subscription that joins an already-listened channel is established on
        the next reconcile.
        """
        dispatcher = NotifyDispatcher()
        dispatcher.subscribe("orders")
        listening = set()
        dispatcher._reconcile(pg_conn, listening)

        late_sub = dispatcher.subscribe("orders")
        dispatcher._reconcile(pg_conn, listening)

        assert late_sub.initial_listen_established.is_set()
        assert {"orders"} == listening_channels(pg_conn)


class TestUnsubscribe:
    def test_close_is_idempotent(self, pg_conn):
        """
        Closing a subscription twice unsubscribes once and leaves the session
        no longer listening on its channel.
        """
        dispatcher = NotifyDispatcher()
        sub = dispatcher.subscribe("orders")
        listening = set()
        dispatcher._reconcile(pg_conn, listening)

        sub.close()
        sub.close()
        dispatcher._reconcile(pg_conn, listening)

        assert set() == listening_channels(pg_conn)


class TestRoute:
    def test_wakes_only_matching_channels_subscriptions(self):
        """
        Routing a notification wakes only that channel's subscriptions,
        leaving other channels' subscriptions untouched.
        """
        dispatcher = NotifyDispatcher()
        orders_sub = dispatcher.subscribe("orders")
        payments_sub = dispatcher.subscribe("payments")

        dispatcher._route("orders")

        assert True is orders_sub.wait(0)
        assert False is payments_sub.wait(0)

    def test_unknown_channel_is_a_noop(self):
        """
        Routing a notification for a channel with no subscribers is a no-op.
        """
        dispatcher = NotifyDispatcher()
        sub = dispatcher.subscribe("orders")

        dispatcher._route("nonexistent")

        assert False is sub.wait(0)


class TestWait:
    def test_wakes_between_two_waits_coalesce_into_one(self):
        """
        Any number of wakes between two `wait()` calls collapse into exactly
        one: the first wait consumes it and the next one blocks again.
        """
        dispatcher = NotifyDispatcher()
        sub = dispatcher.subscribe("orders")

        dispatcher._route("orders")
        dispatcher._route("orders")
        sub.wake()

        assert True is sub.wait(0)
        assert False is sub.wait(0)

    def test_a_wake_after_a_consumed_one_is_kept(self):
        """
        A wake that arrives after the previous one was consumed re-arms the
        subscription instead of being absorbed.
        """
        dispatcher = NotifyDispatcher()
        sub = dispatcher.subscribe("orders")

        sub.wake()
        assert True is sub.wait(0)

        sub.wake()

        assert True is sub.wait(0)
        assert False is sub.wait(0)

    def test_returns_false_on_timeout(self):
        """
        Waiting for a wake that never comes times out and returns False.
        """
        dispatcher = NotifyDispatcher()
        sub = dispatcher.subscribe("orders")

        assert False is sub.wait(0.0001)

    def test_wake_ends_the_next_wait(self):
        """
        `wake()` wakes the subscriber locally, without touching the database.
        """
        dispatcher = NotifyDispatcher()
        sub = dispatcher.subscribe("orders")

        sub.wake()

        assert True is sub.wait(0)


class TestListenWake:
    def test_reconcile_wakes_newly_listened_channels_once(self, pg_conn):
        """
        A LISTEN going live delivers one wake to its channel's subscriptions,
        and an already-listening channel gets no further wake.
        """
        dispatcher = NotifyDispatcher()
        orders_sub = dispatcher.subscribe("orders")
        payments_sub = dispatcher.subscribe("payments")
        listening = set()

        dispatcher._reconcile(pg_conn, listening)

        assert True is orders_sub.wait(0)
        assert True is payments_sub.wait(0)

        dispatcher._reconcile(pg_conn, listening)

        assert False is orders_sub.wait(0)
        assert False is payments_sub.wait(0)

    def test_subscription_joining_a_listen_in_flight_gets_gap_wake(
        self, pg_conn, pgbg_dsn
    ):
        """
        A subscription added after reconcile takes its snapshot but before its
        LISTEN goes live is woken to cover notifications lost in that gap.
        """
        dispatcher = NotifyDispatcher()
        dispatcher.subscribe("orders")
        listening = set()
        late_sub = None

        def subscribe_and_notify_before_listen(statement):
            nonlocal late_sub

            late_sub = dispatcher.subscribe("orders")
            send_notify(pgbg_dsn, "orders", "lost before LISTEN")

            return pg_conn.execute(statement)

        conn = Mock()
        conn.execute.side_effect = subscribe_and_notify_before_listen

        dispatcher._reconcile(conn, listening)

        assert late_sub is not None
        assert late_sub.initial_listen_established.is_set()
        assert True is late_sub.wait(0)
        assert False is late_sub.wait(0)


class TestRun:
    def test_relistens_and_wakes_everyone_before_the_first_cycle(
        self, pg_conn
    ):
        """
        A fresh run re-LISTENs every live subscription and wakes every
        subscriber before entering the wait loop.
        """
        dispatcher = NotifyDispatcher()
        sub = dispatcher.subscribe("orders")
        stop = threading.Event()
        stop.set()  # zero cycles: only the pre-loop work runs

        dispatcher.run(pg_conn, stop)

        assert {"orders"} == listening_channels(pg_conn)
        assert sub.initial_listen_established.is_set()
        assert True is sub.wait(0)

    def test_a_stop_with_a_wake_ends_the_wait_without_a_probe(self, fake_conn):
        """
        A stop request followed by a wake, the way `Supervisor.stop()` does
        it, ends the run at once: the connection sees the session setup and
        one zero-timeout notification poll, no probe runs, and the last-cycle
        gauge is untouched.
        """
        dispatcher = NotifyDispatcher(interval=30.0)
        stop = threading.Event()

        def stop_and_wake(timeout):
            """
            Request the stop from inside the poll and deliver nothing.
            """
            stop.set()
            dispatcher.wake()
            yield from ()

        fake_conn.notifies.side_effect = stop_and_wake
        last_cycle_before = DISPATCHER_LAST_CYCLE.labels(
            name="dispatch"
        )._value.get()

        dispatcher.run(fake_conn, stop)

        assert [call(timeout=0)] == fake_conn.notifies.call_args_list
        # No channel is subscribed, so reconcile issues nothing: only the
        # session's connection-wide ('false') statement-timeout setup runs.
        assert [
            call("SELECT set_config('statement_timeout', '5000', false)"),
        ] == fake_conn.execute.call_args_list
        assert not dispatcher._has_completed_cycle
        assert (
            last_cycle_before
            == DISPATCHER_LAST_CYCLE.labels(name="dispatch")._value.get()
        )

    def test_a_probe_completes_the_cycle(self, fake_conn):
        """
        Once the interval has passed, the loop probes the connection with
        `SELECT 1`, which completes the cycle and advances the last-cycle
        gauge.
        """
        dispatcher = NotifyDispatcher(interval=0.01)
        stop = threading.Event()

        def stop_after_the_probe(statement, *args):
            """
            Let the session setup through and request the stop from the probe.
            """
            if statement == "SELECT 1":
                stop.set()
                dispatcher.wake()

            return Mock()

        def empty_poll(timeout):
            """
            Deliver nothing, so the loop waits out its interval.
            """
            yield from ()

        fake_conn.execute.side_effect = stop_after_the_probe
        fake_conn.notifies.side_effect = empty_poll
        last_cycle_before = DISPATCHER_LAST_CYCLE.labels(
            name="dispatch"
        )._value.get()

        dispatcher.run(fake_conn, stop)

        assert [
            call("SELECT set_config('statement_timeout', '5000', false)"),
            call("SELECT 1"),
        ] == fake_conn.execute.call_args_list
        assert dispatcher._has_completed_cycle
        assert (
            last_cycle_before
            < DISPATCHER_LAST_CYCLE.labels(name="dispatch")._value.get()
        )

    def test_run_creates_the_last_cycle_series_at_zero(self, pg_conn):
        """
        A loop run creates the last-cycle series at 0 under the dispatcher's
        name, so a staleness alert never sits in no-data, and it never
        clobbers an earlier stamp.
        """
        dispatcher = NotifyDispatcher(name="fresh-gauge")
        stop = threading.Event()
        stop.set()  # zero cycles: only the pre-loop work runs

        dispatcher.run(pg_conn, stop)

        assert 0.0 == REGISTRY.get_sample_value(
            "pgbg_dispatcher_last_cycle_timestamp_seconds",
            {"name": "fresh-gauge"},
        )

        DISPATCHER_LAST_CYCLE.labels(name="fresh-gauge").set(123.0)
        dispatcher.run(pg_conn, stop)

        assert 123.0 == REGISTRY.get_sample_value(
            "pgbg_dispatcher_last_cycle_timestamp_seconds",
            {"name": "fresh-gauge"},
        )

    def test_a_dead_connection_ends_the_run(self, pg_conn):
        """
        A failure ends the run and propagates to the caller: no retry, no
        reconnect. Closing the connection makes its first statement fail for
        real.
        """
        dispatcher = NotifyDispatcher()
        dispatcher.subscribe("orders")
        pg_conn.close()
        stop = threading.Event()

        with pytest.raises(psycopg.OperationalError):
            dispatcher.run(pg_conn, stop)

    def test_a_failed_liveness_probe_is_crash_only(self, fake_conn):
        """
        The `SELECT 1` liveness probe surfaces a silently-dead connection and
        crashes run.
        """
        dispatcher = NotifyDispatcher(interval=0.01)
        dispatcher.subscribe("orders")
        stop = threading.Event()

        # A fake connection is the only way to fail the probe selectively
        # while letting LISTEN succeed.
        def execute(statement, *args):
            """
            Let LISTEN through but fail the probe round-trip.
            """
            if statement == "SELECT 1":
                raise RuntimeError("simulated dead connection")

            return Mock()

        def empty_poll(timeout):
            """
            Deliver nothing, so the loop waits out its interval and probes.
            """
            yield from ()

        fake_conn.execute.side_effect = execute
        fake_conn.notifies.side_effect = empty_poll

        with pytest.raises(RuntimeError, match="simulated dead connection"):
            dispatcher.run(fake_conn, stop)

    def test_run_sets_statement_timeout_for_the_session(self, pg_conn):
        """
        A run puts a session-wide statement timeout on its dedicated
        connection, so a statement that blocks on a live server crashes the
        loop instead of wedging it.
        """
        stop = threading.Event()
        stop.set()  # zero cycles: only the pre-loop work runs

        NotifyDispatcher().run(pg_conn, stop)

        assert "5s" == pg_conn.execute("SHOW statement_timeout").fetchone()[0]


@pytest.mark.parametrize("bad_interval", [0, -1.0])
def test_dispatcher_rejects_a_non_positive_interval(bad_interval):
    """
    Construction rejects non-positive intervals.
    """
    with pytest.raises(ValueError):
        NotifyDispatcher(interval=bad_interval)


class TestDispatchLifecycle:
    def test_subscribe_then_notify_wakes(self, run_supervised, pgbg_dsn):
        """
        Once a subscription's LISTEN is established, a `pg_notify()` from
        another connection wakes it after that transaction commits, with or
        without a payload.
        """
        dispatcher = NotifyDispatcher(interval=0.05)
        sub = dispatcher.subscribe("orders")
        _, supervisor = run_supervised(dispatcher=dispatcher)

        assert sub.initial_listen_established.wait(1)

        # Drain the wake the first loop run's LISTEN delivers (guaranteed
        # for a pre-run subscription) so the asserts below can only be
        # satisfied by real NOTIFY delivery.
        assert True is sub.wait(1)

        send_notify(pgbg_dsn, "orders", "this payload is ignored")

        assert True is sub.wait(1)

        send_notify(pgbg_dsn, "orders")

        assert True is sub.wait(1)
        assert supervisor.stop()
        assert not supervisor.is_running

    def test_two_subscriptions_on_different_channels_wake_independently(
        self, run_supervised, pgbg_dsn
    ):
        """
        Notifying one channel wakes only that channel's subscription, not one
        on a different channel.
        """
        dispatcher = NotifyDispatcher(interval=0.05)
        sentinel = dispatcher.subscribe("sentinel")
        run_supervised(dispatcher=dispatcher)

        assert sentinel.initial_listen_established.wait(1)
        assert True is sentinel.wait(1)

        orders_sub = dispatcher.subscribe("orders")
        payments_sub = dispatcher.subscribe("payments")

        assert orders_sub.initial_listen_established.wait(1)
        assert payments_sub.initial_listen_established.wait(1)
        # Drain the one wake each LISTEN going live delivered, so the
        # asserts below can only be satisfied by real NOTIFY delivery.
        assert True is orders_sub.wait(1)
        assert True is payments_sub.wait(1)

        send_notify(pgbg_dsn, "orders", "for orders only")

        assert True is orders_sub.wait(1)
        assert False is payments_sub.wait(0)

    def test_late_subscribe_while_running_still_establishes(
        self, run_supervised, pgbg_dsn
    ):
        """
        A subscription made while the loop is already cycling is established
        and receives notifications.
        """
        dispatcher = NotifyDispatcher(interval=0.05)
        sentinel = dispatcher.subscribe("sentinel")
        run_supervised(dispatcher=dispatcher)

        assert sentinel.initial_listen_established.wait(1)
        # The sentinel's observed wake proves the first loop run's reconcile
        # finished, so the subscription below is a genuinely late one.
        assert True is sentinel.wait(1)

        sub = dispatcher.subscribe("orders")

        assert sub.initial_listen_established.wait(1)
        # Drain the one wake its own LISTEN going live delivered.
        assert True is sub.wait(1)

        send_notify(pgbg_dsn, "orders", "late but delivered")

        assert True is sub.wait(1)

    def test_unlisten_runs_against_postgres_and_stops_delivery(
        self, run_supervised, pgbg_dsn
    ):
        """
        Closing a channel's last subscription UNLISTENs it without crashing the
        run, and the loop keeps serving other channels.
        """
        dispatcher = NotifyDispatcher(interval=0.05)
        sentinel = dispatcher.subscribe("sentinel")
        run_supervised(dispatcher=dispatcher)

        assert sentinel.initial_listen_established.wait(1)
        assert True is sentinel.wait(1)

        orders_sub = dispatcher.subscribe("orders")
        payments_sub = dispatcher.subscribe("payments")

        assert orders_sub.initial_listen_established.wait(1)
        assert payments_sub.initial_listen_established.wait(1)
        # Drain the one wake each LISTEN going live delivered.
        assert True is orders_sub.wait(1)
        assert True is payments_sub.wait(1)

        restarts_before = REGISTRY.get_sample_value(
            "bgt_supervisor_restarts_total", {"name": "dispatch"}
        )
        orders_sub.close()
        probe = dispatcher.subscribe("probe")

        # The probe establishing proves a reconcile ran after the close;
        # that same reconcile issued the UNLISTEN for orders.
        assert probe.initial_listen_established.wait(1)
        assert restarts_before == REGISTRY.get_sample_value(
            "bgt_supervisor_restarts_total", {"name": "dispatch"}
        )

        send_notify(pgbg_dsn, "orders")
        # Sequencing control: NOTIFYs arrive in commit order, so once the
        # payments wake lands, an orders notification would already have
        # been routed.
        send_notify(pgbg_dsn, "payments", "sequencing control")

        assert True is payments_sub.wait(1)
        assert False is orders_sub.wait(0)


class TestSocketWakeup:
    def test_wakes_make_the_selector_ready_until_drained(self, wakeup):
        """
        Any number of wakes make a selector ready, a drain makes it block
        again, and a later wake makes it ready again.
        """
        with DefaultSelector() as selector:
            selector.register(wakeup, EVENT_READ)
            for _ in range(10_000):
                wakeup.wake()

            assert 1 == len(selector.select(timeout=0))

            wakeup.drain()

            assert [] == selector.select(timeout=0)

            wakeup.wake()

            assert 1 == len(selector.select(timeout=0))

    def test_close_is_safe_during_and_after_concurrent_wakes(self, wakeup):
        """
        Closing while other threads wake never raises, and wake and close
        stay harmless on a closed wakeup.
        """
        started = threading.Barrier(3)

        def wake_repeatedly():
            """
            Hammer the wakeup once every thread is ready.
            """
            started.wait(timeout=2)
            for _ in range(10_000):
                wakeup.wake()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(wake_repeatedly) for _ in range(2)]
            started.wait(timeout=2)
            wakeup.close()
            for future in futures:
                future.result(timeout=2)

        wakeup.wake()
        wakeup.close()

        assert -1 == wakeup.fileno()


class TestInterruptibleWait:
    def test_a_stop_ends_an_idle_wait_at_once(self, run_supervised):
        """
        A stop request ends an idle loop's wait right away instead of after
        the probe interval.
        """
        dispatcher = NotifyDispatcher(interval=5)
        sub = dispatcher.subscribe("orders")
        _, supervisor = run_supervised(dispatcher=dispatcher)

        assert sub.initial_listen_established.wait(1)
        assert supervisor.stop(timeout=0.5)

    def test_a_new_subscription_is_listened_at_once(self, run_supervised):
        """
        Subscribing wakes the idle loop, so the LISTEN goes live right away
        instead of at the next probe.
        """
        dispatcher = NotifyDispatcher(interval=5)
        sentinel = dispatcher.subscribe("sentinel")
        run_supervised(dispatcher=dispatcher)

        # The sentinel proves that the loop is past its first reconcile.
        assert sentinel.initial_listen_established.wait(1)

        sub = dispatcher.subscribe("orders")

        assert sub.initial_listen_established.wait(0.5)

    def test_closing_the_last_subscription_unlistens_at_once(
        self, run_supervised
    ):
        """
        Closing a channel's last subscription wakes the idle loop, so the
        UNLISTEN runs right away instead of at the next probe.
        """
        dispatcher = RecordingDispatcher(interval=5)
        sub = dispatcher.subscribe("orders")
        run_supervised(dispatcher=dispatcher)

        assert {"orders"} == dispatcher.reconciled.get(timeout=1)

        sub.close()

        assert set() == dispatcher.reconciled.get(timeout=0.5)

    def test_notifications_buffered_by_a_statement_are_routed_at_once(
        self, run_supervised, pg_connect, pgbg_dsn
    ):
        """
        A notification that a statement pulled into Psycopg's backlog is
        routed before the loop waits for socket input, so it is not stuck
        until the next probe.
        """

        def connect_with_a_backlog():
            """
            Open a connection whose Psycopg backlog holds a notification.
            """
            conn = pg_connect()
            conn.autocommit = True
            conn.execute("LISTEN orders")
            send_notify(pgbg_dsn, "orders")

            with DefaultSelector() as selector:
                selector.register(conn, EVENT_READ)
                # Once the notification sits in the socket, the next statement
                # is what reads it into the backlog.
                assert selector.select(timeout=1)

            conn.execute("SELECT 1")

            return conn

        dispatcher = RecordingDispatcher(interval=5)
        dispatcher.subscribe("orders")
        run_supervised(dispatcher=dispatcher, connect=connect_with_a_backlog)

        assert "orders" == dispatcher.routed.get(timeout=0.5)

    def test_a_connection_closed_by_the_server_is_replaced_at_once(
        self, run_supervised, pg_connect, pgbg_maintenance_conn
    ):
        """
        When PostgreSQL closes the LISTEN connection, the loop crashes on the
        socket's end of file instead of at the next probe, and the restarted
        run re-establishes the existing subscriptions.
        """
        pids = Queue()

        def connect():
            """
            Open a connection and report its backend PID.
            """
            conn = pg_connect()
            pids.put(conn.info.backend_pid)

            return conn

        dispatcher = NotifyDispatcher(interval=5)
        sub = dispatcher.subscribe("orders")
        run_supervised(dispatcher=dispatcher, connect=connect)
        old_pid = pids.get(timeout=1)

        assert sub.initial_listen_established.wait(1)
        # Drain the wake the first run's LISTEN delivered.
        assert True is sub.wait(1)

        pgbg_maintenance_conn.execute(
            "SELECT pg_terminate_backend(%s)", (old_pid,)
        )

        assert old_pid != pids.get(timeout=2)
        # The fresh run's re-LISTEN wakes the subscription.
        assert True is sub.wait(2)


class TestDispatchLoop:
    def test_resets_progress_before_a_failing_connect(self):
        """
        A connect that fails propagates and leaves the dispatcher's progress
        flag reset, so a connect storm can't reset the backoff ladder off
        a stale True from the last healthy run.
        """
        dispatcher = NotifyDispatcher()
        dispatcher._has_completed_cycle = True

        def unreachable():
            """
            Fail like an unreachable database.
            """
            raise RuntimeError("no route to database")

        runner = DispatchLoop(connect=unreachable, dispatcher=dispatcher)

        with pytest.raises(RuntimeError, match="no route to database"):
            runner.run(threading.Event())

        assert dispatcher._has_completed_cycle is False

    def test_close_failure_does_not_mask_a_crash(self):
        """
        A connection that dies mid-run is closed best-effort, and a close that
        also fails never masks the run's own crash.
        """
        dispatcher = NotifyDispatcher()
        dispatcher.subscribe("orders")  # forces a LISTEN on reconcile

        broken = Mock()
        broken.execute.side_effect = RuntimeError("run died")
        broken.close.side_effect = RuntimeError("close died too")

        runner = DispatchLoop(connect=lambda: broken, dispatcher=dispatcher)

        with pytest.raises(RuntimeError, match="run died"):
            runner.run(threading.Event())

        assert 1 == broken.close.call_count

    def test_closes_the_connection_on_a_clean_stop(self, pg_connect):
        """
        A run that returns because stop is already set still closes its
        connection.
        """
        conn = pg_connect()
        stop = threading.Event()
        stop.set()

        runner = DispatchLoop(
            connect=lambda: conn, dispatcher=NotifyDispatcher()
        )
        runner.run(stop)

        assert conn.closed


def test_crash_restart_wakes_established_subscribers(run_supervised):
    """
    A mid-flight crash is restarted on a fresh connection whose re-LISTENs
    wake every established subscriber with no NOTIFY ever sent: a crash gap
    cannot lose wakeups.
    """
    dispatcher = NotifyDispatcher(interval=0.05)
    sentinel = dispatcher.subscribe("sentinel")
    _, supervisor = run_supervised(dispatcher=dispatcher)

    assert sentinel.initial_listen_established.wait(1)
    assert True is sentinel.wait(1)

    sub = dispatcher.subscribe("orders")

    assert sub.initial_listen_established.wait(1)
    # Drain the one wake its own LISTEN going live delivered.
    assert True is sub.wait(1)
    assert False is sub.wait(0)

    counter = 0
    real_reconcile = NotifyDispatcher._reconcile

    def dying_reconcile(self, pgconn, listening):
        """
        Fail the next reconcile, then reconcile for real.
        """
        nonlocal counter

        counter += 1
        if counter == 1:
            raise RuntimeError("simulated mid-flight connection death")

        return real_reconcile(self, pgconn, listening)

    with patch.object(NotifyDispatcher, "_reconcile", dying_reconcile):
        assert True is sub.wait(2)

    assert counter > 1
    assert supervisor.is_running


def test_subscription_wakes_a_service_on_a_real_notification(
    running_dispatcher, pgbg_dsn
):
    """
    A NOTIFY travels through a real dispatcher and wakes a *bgt* service
    that waits on the subscription, for its first work unit.
    """
    first_unit = threading.Event()

    def do_work():
        """
        Signal the work unit.
        """
        first_unit.set()

        return False

    sub = running_dispatcher.subscribe("wakeups")
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
                {"channel": "wakeups"},
            )
            conn.commit()

        assert first_unit.wait(5)
