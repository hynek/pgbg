import random
import threading

from unittest.mock import Mock, patch

import pytest
import structlog

from prometheus_client import REGISTRY

from pgbg import NotifyDispatcher, Supervisor
from pgbg._dispatcher import DispatchLoop
from pgbg._supervisor import _MAX_BACKOFF_SECONDS, SUPERVISOR_RESTARTS


class ScriptedLoop:
    """
    A `Loop` whose runs follow a fixed script, one entry per run.

    "crash" raises before completing a cycle, "cycle" completes one and then
    raises, "stop" requests the stop and returns cleanly, "stop_then_crash"
    requests the stop and then raises, "exit" raises SystemExit, and "wait"
    parks until someone else requests the stop.
    """

    def __init__(self, script):
        self._script = list(script)
        self.has_completed_cycle = False
        self.closed = False
        # Set as soon as the first run begins.
        self.running = threading.Event()
        # Every run and wake in order, for asserting their interleaving.
        self.calls = []

    def run(self, stop):
        """
        Perform the next scripted run.
        """
        self.calls.append("run")
        self.running.set()
        self.has_completed_cycle = False
        if not self._script:
            pytest.fail("unexpected extra run")

        match action := self._script.pop(0):
            case "cycle":
                self.has_completed_cycle = True
                raise RuntimeError("crashed after a full cycle")
            case "crash":
                raise RuntimeError("crashed before a cycle")
            case "stop_then_crash":
                stop.set()
                raise RuntimeError("crash racing the shutdown")
            case "exit":
                raise SystemExit("scripted exit")
            case "base_exception":
                raise GeneratorExit("base exception")
            case "stop":
                stop.set()
            case "wait":
                stop.wait(5)
            case _:
                pytest.fail(f"unknown scripted action {action!r}")

    def wake(self):
        """
        Record the wake: run reacts to the stop event directly.
        """
        self.calls.append("wake")

    def close(self):
        """
        Record that teardown happened.
        """
        self.closed = True


def test_restarts_the_loop_after_a_crash():
    """
    A crashed run is counted and restarted, and the loop is closed once the
    supervisor finally stops.
    """
    loop = ScriptedLoop(["crash", "stop"])
    supervisor = Supervisor(
        name="test",
        loop=loop,
        initial_backoff=0,
    )
    restarts_before = SUPERVISOR_RESTARTS.labels(name="test")._value.get()

    with structlog.testing.capture_logs() as logs:
        supervisor._supervise()

    assert (
        restarts_before + 1
        == SUPERVISOR_RESTARTS.labels(name="test")._value.get()
    )
    assert loop.closed
    assert [
        "supervisor.started",
        "supervisor.loop_crashed",
        "supervisor.restarting",
        "supervisor.stopped",
    ] == [ll["event"] for ll in logs]


def test_wakes_the_loop_before_a_crash_restart():
    """
    A crash restart pre-arms the loop's wakeup, so the next run's first wait
    returns immediately instead of sitting out its interval.
    """
    loop = ScriptedLoop(["crash", "stop"])
    supervisor = Supervisor(
        name="wake-restart",
        loop=loop,
        initial_backoff=0,
    )

    supervisor._supervise()

    assert ["run", "wake", "run"] == loop.calls


@pytest.mark.usefixtures("no_jitter")
def test_backoff_grows_and_resets_after_a_completed_cycle():
    """
    The restart backoff grows across crashes that never complete a cycle, then
    resets once a run completes at least one.
    """
    supervisor = Supervisor(
        name="test",
        loop=ScriptedLoop(["crash", "crash", "crash", "cycle", "stop"]),
        initial_backoff=0.01,
    )

    waits = []

    def recording_wait(timeout=None):
        """
        Record each backoff instead of sleeping.
        """
        waits.append(timeout)
        return False

    supervisor._stop_event.wait = recording_wait

    with structlog.testing.capture_logs():
        supervisor._supervise()

    assert [0.01, 0.02, 0.04, 0.01] == waits


@pytest.mark.usefixtures("no_jitter")
def test_never_gives_up():
    """
    The supervisor restarts past any finite limit.
    """
    num_crashed = random.randint(5, 20)
    crashes = ["crash"] * num_crashed
    loop = ScriptedLoop([*crashes, "stop"])
    supervisor = Supervisor(
        name="test",
        loop=loop,
        initial_backoff=0,
    )
    supervisor._stop_event.wait = lambda timeout=None: False  # don't sleep
    restarts_before = SUPERVISOR_RESTARTS.labels(name="test")._value.get()

    with structlog.testing.capture_logs():
        supervisor._supervise()

    assert (
        restarts_before + num_crashed
        == SUPERVISOR_RESTARTS.labels(name="test")._value.get()
    )
    assert loop.closed


def test_an_exit_exception_ends_supervision_gracefully():
    """
    A BaseException that indicates a clean exit from a run ends supervision
    gracefully.
    """
    loop = ScriptedLoop(["exit"])
    supervisor = Supervisor(
        name="test",
        loop=loop,
        initial_backoff=0,
    )
    restarts_before = SUPERVISOR_RESTARTS.labels(name="test")._value.get()

    with structlog.testing.capture_logs() as logs:
        supervisor._supervise()

    assert (
        restarts_before == SUPERVISOR_RESTARTS.labels(name="test")._value.get()
    )
    assert loop.closed
    assert [
        "supervisor.started",
        "supervisor.stopped",
    ] == [entry["event"] for entry in logs]
    assert "killed" == logs[-1]["reason"]


def test_a_base_exception_ends_supervision_loudly():
    """
    A BaseException from a run is logged as a crash and ends supervision with
    reason "crashed" instead of looking like a clean stop.
    """
    loop = ScriptedLoop(["base_exception"])
    supervisor = Supervisor(name="test", loop=loop, initial_backoff=0)
    restarts_before = SUPERVISOR_RESTARTS.labels(name="test")._value.get()

    with structlog.testing.capture_logs() as logs:
        supervisor._supervise()

    assert (
        restarts_before == SUPERVISOR_RESTARTS.labels(name="test")._value.get()
    )
    assert loop.closed
    assert [
        "supervisor.started",
        "supervisor.crashed",
        "supervisor.stopped",
    ] == [entry["event"] for entry in logs]
    assert "crashed" == logs[-1]["reason"]


def test_a_clean_stop_reports_reason_stopped():
    """
    A stop-requested run ends supervision with reason "stopped".
    """
    loop = ScriptedLoop(["stop"])
    supervisor = Supervisor(name="test", loop=loop, initial_backoff=0)

    with structlog.testing.capture_logs() as logs:
        supervisor._supervise()

    assert "supervisor.stopped" == logs[-1]["event"]
    assert "stopped" == logs[-1]["reason"]


class BrokenCloseLoop(ScriptedLoop):
    """
    A `ScriptedLoop` whose close always fails.
    """

    def close(self):
        """
        Fail the teardown.
        """
        raise RuntimeError("close died")


def test_a_failing_close_is_logged_not_lost():
    """
    A close that raises is logged as an error and the stop is still reported,
    instead of the thread dying silently.
    """
    loop = BrokenCloseLoop(["stop"])
    supervisor = Supervisor(name="test", loop=loop, initial_backoff=0)

    with structlog.testing.capture_logs() as logs:
        supervisor._supervise()

    assert [
        "supervisor.started",
        "supervisor.close_failed",
        "supervisor.stopped",
    ] == [entry["event"] for entry in logs]


class ExitingCloseLoop(ScriptedLoop):
    """
    A `ScriptedLoop` whose close raises SystemExit.
    """

    def close(self):
        """
        Fail the teardown with a BaseException.
        """
        raise SystemExit("close exited")


def test_a_close_raising_a_base_exception_is_logged_not_lost():
    """
    A close that raises a BaseException is logged and the stop is still
    reported, instead of the thread dying with no record at all.
    """
    loop = ExitingCloseLoop(["stop"])
    supervisor = Supervisor(name="test", loop=loop, initial_backoff=0)

    with structlog.testing.capture_logs() as logs:
        supervisor._supervise()

    assert [
        "supervisor.started",
        "supervisor.close_failed",
        "supervisor.stopped",
    ] == [entry["event"] for entry in logs]


def test_context_manager_stops_on_exit():
    """
    Leaving the with block stops the supervisor and closes the loop.
    """
    loop = ScriptedLoop(["wait"])

    with Supervisor.start(loop, name="test", initial_backoff=0) as supervisor:
        # Wait for the run to begin, so the stop interrupts a parked loop
        # instead of beating the thread to its first action.
        assert loop.running.wait(1)
        assert supervisor.is_running

    assert not supervisor.is_running
    assert loop.closed


def test_a_crash_racing_the_shutdown_is_not_a_restart():
    """
    A run that dies while stop is already requested is not counted or reported
    as a restart and is logged as a shutdown crash instead.
    """
    loop = ScriptedLoop(["stop_then_crash"])
    supervisor = Supervisor(name="test", loop=loop, initial_backoff=0)
    restarts_before = SUPERVISOR_RESTARTS.labels(name="test")._value.get()

    with structlog.testing.capture_logs() as logs:
        supervisor._supervise()

    assert (
        restarts_before == SUPERVISOR_RESTARTS.labels(name="test")._value.get()
    )
    assert loop.closed
    assert "supervisor.loop_crashed_during_shutdown" in [
        entry["event"] for entry in logs
    ]


def test_stop_during_backoff_is_not_a_restart():
    """
    Stopping during the backoff prevents another run, so it is neither counted
    nor logged as a restart.
    """
    loop = ScriptedLoop(["crash"])
    supervisor = Supervisor(name="test", loop=loop, initial_backoff=30)
    restarts_before = SUPERVISOR_RESTARTS.labels(name="test")._value.get()

    def stop_during_wait(timeout=None):
        supervisor._stop_event.set()
        return True

    supervisor._stop_event.wait = stop_during_wait

    with structlog.testing.capture_logs() as logs:
        supervisor._supervise()

    assert (
        restarts_before == SUPERVISOR_RESTARTS.labels(name="test")._value.get()
    )
    assert loop.closed
    assert "supervisor.restarting" not in [entry["event"] for entry in logs]


def test_stop_before_the_first_cycle_runs_nothing():
    """
    With the stop event already set, the loop body never runs and the loop
    is still closed.
    """
    loop = Mock()
    supervisor = Supervisor(
        name="preset-stop", loop=loop, initial_backoff=0.01
    )
    supervisor._stop_event.set()

    supervisor._supervise()

    loop.run.assert_not_called()
    loop.close.assert_called_once_with()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"initial_backoff": -1.0},  # negative backoff
        {"initial_backoff": _MAX_BACKOFF_SECONDS + 1},  # above the cap
    ],
)
def test_start_rejects_invalid_backoff_settings(kwargs):
    """
    start() validates the backoff range up front, instead of only crashing on
    the first restart.
    """
    with pytest.raises(ValueError):
        Supervisor.start(ScriptedLoop([]), name="test", **kwargs)


def test_start_creates_the_restart_series_at_zero():
    """
    Starting a supervisor creates its restart-counter series at 0, so the
    very first restart is already visible to rate().
    """
    supervisor = Supervisor.start(
        ScriptedLoop(["stop"]), name="fresh-restarts"
    )

    assert 0.0 == REGISTRY.get_sample_value(
        "pgbg_supervisor_restarts_total", {"name": "fresh-restarts"}
    )
    assert supervisor.stop(1)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"initial_backoff": 0},  # the floor
        {"initial_backoff": _MAX_BACKOFF_SECONDS},  # equal to the cap
    ],
)
def test_start_accepts_valid_backoff_edges(kwargs):
    """
    Boundary-valid settings are accepted and start a running supervisor.
    """
    supervisor = Supervisor.start(
        ScriptedLoop(["stop"]), name="test", **kwargs
    )

    assert supervisor.stop(1)


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


@pytest.mark.usefixtures("no_jitter")
def test_stop_interrupts_the_backoff_wait(run_supervised):
    """
    `stop` during a backoff wait returns promptly because the wait is on the
    same event that stop sets.
    """
    attempted = threading.Event()

    def always_failing_connect():
        attempted.set()

        raise RuntimeError("simulated permanent connect failure")

    _, supervisor = run_supervised(
        connect=always_failing_connect, initial_backoff=30.0
    )

    assert attempted.wait(2)
    assert supervisor.stop(1)

    assert not supervisor.is_running


def test_stop_is_idempotent(run_supervised):
    """
    Calling `stop` more than once is safe.
    """
    _, supervisor = run_supervised()

    assert supervisor.stop()
    assert not supervisor.is_running
    assert supervisor.stop()
    assert not supervisor.is_running
