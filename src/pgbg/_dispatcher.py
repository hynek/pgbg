"""
A single, process-wide LISTEN connection that fans out wakeups to in-process
subscribers.
"""

import threading

from collections.abc import Callable
from contextlib import closing, suppress
from types import TracebackType
from typing import Any, Self

import attrs
import psycopg
import psycopg.sql
import structlog

from prometheus_client import Gauge

from ._supervisor import Supervisor


logger = structlog.stdlib.get_logger("pgbg")

DISPATCHER_LAST_CYCLE = Gauge(
    "pgbg_dispatcher_last_cycle_timestamp_seconds",
    "Timestamp of the dispatcher's last healthy cycle",
    ["name"],
)


@attrs.define(eq=False)
class Subscription:
    """
    A coalescing in-process subscription to one notification channel.

    A subscription is a handle: equality is identity, so two subscriptions to
    the same channel stay distinct.

    Every notification on the channel delivers one wake, and any number of
    wakes between two [`wait()`][pgbg.Subscription.wait] calls collapse into
    exactly one.

    The notification's payload is ignored: `LISTEN` / `NOTIFY` is best-effort
    transport, so treat a wake as a doorbell and read the data you care
    about from your tables on each wake.

    Attributes:
        initial_listen_established (threading.Event):
            Set once the initial `LISTEN` for this subscription's channel has
            been issued on a live connection.

            Notifications sent before the `LISTEN` are lost server-side, but
            every `LISTEN` going live also wakes its channel's subscriptions,
            so a poll on each wake covers the gap.
    """

    channel: str
    # Removes this subscription from its dispatcher's registry. Injected by
    # `subscribe()`.
    _unsubscribe: Callable[[], None] = attrs.field(
        alias="unsubscribe", repr=False
    )
    _woken: threading.Event = attrs.field(
        init=False, factory=threading.Event, repr=False
    )
    initial_listen_established: threading.Event = attrs.field(
        init=False, factory=threading.Event
    )

    def wake(self) -> None:
        """
        Wake the subscriber locally, without touching the database.

        Idempotent while a wake is pending: that is what coalesces any number
        of notifications into one wake.

        Use it to interrupt your own [`wait()`][pgbg.Subscription.wait]. For
        example, from a stop path.
        """
        self._woken.set()

    def wait(self, timeout: float) -> bool:
        """
        Wait up to *timeout* seconds for a wake.

        Args:
            timeout: The maximum time to wait in seconds.

        Returns:
            `True` when woken and `False` on a timeout.
        """
        if self._woken.wait(timeout):
            # Consume wake, so the next `wait` blocks again.
            self._woken.clear()
            return True

        return False

    def close(self) -> None:
        """
        Cancel the subscription.

        Idempotent.
        """
        self._unsubscribe()


@attrs.define
class NotifyDispatcher:
    """
    Registry of subscriptions plus the blocking dispatch loop.

    You usually want [`SupervisedDispatcher`][pgbg.SupervisedDispatcher].
    Use this directly to run the dispatch loop on a thread you own: `run` is
    a plain blocking call, so you can put it on your main thread and let
    your process supervisor restart the process.

    The registry outlives any single loop run, so subscriptions stay valid
    across crashes and restarts.
    """

    # Set to False when a loop run starts and to True once the loop made a
    # complete loop cycle. Read by the internal supervision adapter to
    # decide on backoff behavior.
    _has_completed_cycle: bool = attrs.field(
        init=False, default=False, repr=False
    )

    # Seconds `conn.notifies()` waits for notifications before a loop cycle
    # re-checks subscriptions for changes. Also the bound on how long `run`
    # takes to notice its stop event.
    interval: float = attrs.field(
        default=1.0, validator=attrs.validators.gt(0.0)
    )
    # Names this dispatcher in its metric's label. `SupervisedDispatcher`
    # passes its supervisor name down, so the two metrics correlate.
    name: str = "dispatch"
    _subs_lock: threading.Lock = attrs.field(
        init=False, factory=threading.Lock, repr=False
    )
    _subscriptions: dict[str, list[Subscription]] = attrs.field(
        init=False, factory=dict, repr=False
    )

    def subscribe(self, channel: str) -> Subscription:
        """
        Subscribe to *channel*.

        Multiple subscriptions to the same channel share one `LISTEN`. This
        only mutates in-process state, so it is legal at any time. The `LISTEN`
        itself happens at the running loop's next reconcile, within roughly one
        *interval*, and subscriptions made while no loop is running (including
        during a crash gap) are picked up by the next loop run's first
        reconcile.

        The channel's subscriptions are woken whenever their `LISTEN` goes
        live, so a subscriber that polls its backlog on every wake misses
        nothing across the subscribe-to-`LISTEN` window. A *producer* that
        needs its `NOTIFY`s delivered must still wait on
        `initial_listen_established` before sending.

        Channel names are quoted as SQL identifiers but are not validated. The
        caller must use a valid PostgreSQL identifier that PostgreSQL does not
        truncate.

        Args:
            channel: The notification channel to subscribe to.

        Returns:
            A subscription handle for the channel.
        """

        def unsubscribe() -> None:
            self._drop_subscription(subscription)

        subscription = Subscription(channel=channel, unsubscribe=unsubscribe)
        with self._subs_lock:
            self._subscriptions.setdefault(channel, []).append(subscription)

        return subscription

    def _drop_subscription(self, subscription: Subscription) -> None:
        """
        Remove *subscription* from its channel's subscriber list, pruning the
        channel entry itself once its last subscription is gone (which is what
        makes the next reconcile `UNLISTEN` it).
        """
        with self._subs_lock:
            subs = self._subscriptions.get(subscription.channel)
            if subs is None:
                return  # Idempotent

            if subscription in subs:
                subs.remove(subscription)

            if not subs:
                del self._subscriptions[subscription.channel]

    def run(
        self,
        conn: psycopg.Connection[Any],
        stop: threading.Event,
    ) -> None:
        """
        Dispatch on *conn* until *stop* is set or something breaks.

        Any failure propagates to the caller and ends the loop run.
        """
        self._has_completed_cycle = False
        # Create the series at 0 without clobbering an earlier stamp.
        DISPATCHER_LAST_CYCLE.labels(name=self.name)
        conn.autocommit = True
        # A session-wide 5s bound for every statement (LISTEN/UNLISTEN
        # and the liveness probe): a statement that blocks or crawls on
        # a live server crashes the loop run instead of wedging it. The
        # server enforces it, so a dead network path is NOT covered.
        conn.execute("SELECT set_config('statement_timeout', '5000', false)")

        # A fresh loop run's empty *listening* makes the first reconcile
        # LISTEN (and thereby wake) every subscribed channel. Notifications
        # may have been missed while no loop run was listening.
        listening: set[str] = set()
        self._reconcile(conn, listening)

        while not stop.is_set():
            with closing(conn.notifies(timeout=self.interval)) as notifies:
                for notify in notifies:
                    self._route(notify.channel)

            self._reconcile(conn, listening)
            # Ensure the connection is really alive.
            conn.execute("SELECT 1")
            DISPATCHER_LAST_CYCLE.labels(name=self.name).set_to_current_time()
            self._has_completed_cycle = True

    def _reconcile(
        self,
        pgconn: psycopg.Connection[Any],
        listening: set[str],
    ) -> None:
        """
        Bring the connection's LISTEN set in line with current subscriptions,
        then mark every currently-listened channel's subscriptions established.

        Self-healing: a fresh loop run's *listening* starts empty, so
        everything re-LISTENs here with no special-case code.
        """
        with self._subs_lock:
            desired = set(self._subscriptions)

        newly_listened = desired - listening

        for channel in newly_listened:
            pgconn.execute(
                psycopg.sql.SQL("LISTEN {}").format(
                    psycopg.sql.Identifier(channel)
                )
            )
            listening.add(channel)
            logger.debug("dispatcher.listen", channel=channel)
            # The LISTEN is live only *now*. A NOTIFY sent between the subscribe
            # and this statement was lost server-side. One wake makes the
            # subscribers poll and cover that window. This is also what covers
            # notifications missed while no loop run was listening at all
            # (crash gaps, first start).

        for channel in listening - desired:
            pgconn.execute(
                psycopg.sql.SQL("UNLISTEN {}").format(
                    psycopg.sql.Identifier(channel)
                )
            )
            listening.discard(channel)
            logger.debug("dispatcher.unlisten", channel=channel)

        # Take this snapshot after the statements: a subscription added while
        # LISTEN was in flight belongs to its loss window and needs the wake.
        # One added after this snapshot joins an already-live LISTEN and has no
        # gap to cover. The fresh snapshot also establishes subscriptions that
        # joined a channel which was already being listened to.
        with self._subs_lock:
            listened_subs = {
                channel: list(self._subscriptions.get(channel, []))
                for channel in listening
            }

        for channel, subscriptions in listened_subs.items():
            if channel in newly_listened:
                for subscription in subscriptions:
                    subscription.wake()

            for subscription in subscriptions:
                subscription.initial_listen_established.set()

    def _route(self, channel: str) -> None:
        """
        Wake every subscription on *channel*. An unknown channel is a no-op.
        """
        logger.debug("dispatcher.notified", channel=channel)
        with self._subs_lock:
            for subscription in self._subscriptions.get(channel, []):
                subscription.wake()


@attrs.define
class DispatchLoop:
    """
    A `NotifyDispatcher` made supervisable by binding it to a connection
    factory.

    Internal: `SupervisedDispatcher` builds one and hands it to
    `Supervisor.start`.

    Each `run` opens a fresh connection via *connect*, dispatches on it
    until the stop event fires or the loop run crashes, and closes it. That
    reconnect on every loop run is what lets a `Supervisor` turn the
    dispatch loop into a resilient one.
    """

    _connect: Callable[[], psycopg.Connection[Any]] = attrs.field(
        alias="connect"
    )
    _dispatcher: NotifyDispatcher = attrs.field(alias="dispatcher")

    @property
    def has_completed_cycle(self) -> bool:
        """
        Whether the dispatcher completed a loop cycle in the current loop
        run.
        """
        return self._dispatcher._has_completed_cycle

    def run(self, stop: threading.Event) -> None:
        """
        Open a connection, dispatch on it until stopped, then close it.
        """
        # Reset here, not only inside the dispatcher's run: a connect
        # that fails must count as no progress, or a storm of connect
        # failures would keep resetting the supervisor's backoff ladder
        # off a stale True left by the last healthy loop run.
        self._dispatcher._has_completed_cycle = False
        conn = self._connect()
        try:
            self._dispatcher.run(conn, stop)
        finally:
            # Best-effort: never mask the loop run's own failure with a
            # close error on an already-broken connection.
            with suppress(Exception):
                conn.close()

    def wake(self) -> None:
        """
        Do nothing. The dispatch loop's `conn.notifies` wait isn't
        interruptible from another thread, so stop is noticed at the next
        loop cycle, bounded by the dispatcher's *interval*.
        """

    def close(self) -> None:
        """
        Do nothing. Each loop run already closed its own connection.
        """


@attrs.frozen
class SupervisedDispatcher:
    """
    Handle for a dispatcher that runs under supervision.

    Construct via [`start()`][pgbg.SupervisedDispatcher.start] (or
    [`pgbg.sqlalchemy.start_dispatcher`][pgbg.sqlalchemy.start_dispatcher]
    from an [`Engine`][sqlalchemy.engine.Engine]).

    !!! info "See also"
        [`NOTIFY` Dispatch](dispatch.md)
    """

    _dispatcher: NotifyDispatcher = attrs.field(alias="dispatcher")
    _supervisor: Supervisor = attrs.field(alias="supervisor")

    @classmethod
    def start(
        cls,
        connect: Callable[[], psycopg.Connection[Any]],
        *,
        interval: float = 1.0,
        name: str = "dispatch",
        initial_backoff: float = 0.1,
    ) -> Self:
        """
        Build a dispatcher and start running it under a
        [`Supervisor`][pgbg.Supervisor].

        Args:
            connect:
                Opens the dedicated `LISTEN` connection. Called once per
                (re-)started loop run.

            interval:
                The dispatcher's cycle time: the bound on how long a new
                subscription waits for its `LISTEN` and on how long `stop`
                takes to be noticed.

            name:
                Identifies the supervisor in its thread name, logs, and
                the restart and last-cycle metrics' labels.

            initial_backoff:
                How long the supervisor waits after a crash before restarting.
        """
        dispatcher = NotifyDispatcher(interval=interval, name=name)

        return cls(
            dispatcher=dispatcher,
            supervisor=Supervisor.start(
                DispatchLoop(connect=connect, dispatcher=dispatcher),
                name=name,
                initial_backoff=initial_backoff,
            ),
        )

    def subscribe(self, channel: str) -> Subscription:
        """
        Subscribe to *channel*.

        See [`NotifyDispatcher.subscribe()`][pgbg.NotifyDispatcher.subscribe].

        Args:
            channel: The notification channel to subscribe to.

        Returns:
            A subscription handle for the channel.
        """
        return self._dispatcher.subscribe(channel)

    @property
    def is_running(self) -> bool:
        """
        Return whether the supervising thread is still alive.
        """
        return self._supervisor.is_running

    def stop(self, timeout: float | None = None) -> bool:
        """
        Stop the supervision and the dispatch loop.

        See [`Supervisor.stop()`][pgbg.Supervisor.stop].

        Args:
            timeout:
                Optional timeout in seconds to wait for the supervisor to stop.
                If omitted, waits indefinitely.

        Returns:
            `True` if the supervisor stopped within the timeout, `False` otherwise.
        """
        return self._supervisor.stop(timeout)

    def __enter__(self) -> Self:
        """
        Return the running dispatcher itself.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """
        Stop on exit, whether or not the body raised.
        """
        self.stop()
