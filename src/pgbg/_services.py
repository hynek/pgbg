"""
Background service threads, with and without leader election.
"""

import math
import threading
import time

from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from datetime import datetime
from types import TracebackType
from typing import Any, Self

import attrs
import psycopg
import psycopg.errors
import psycopg.sql
import structlog

from prometheus_client import Counter, Gauge

from ._supervisor import Supervisor
from .exceptions import SuppressedCrashError
from .typing import ConnectionProvider, DoWork, Wakeup, WorkFactory


logger = structlog.stdlib.get_logger("pgbg")

SERVICE_LAST_WORK_UNIT = Gauge(
    "pgbg_service_last_work_unit_timestamp_seconds",
    "Timestamp of a service's last completed work unit",
    ["service_name"],
)
SERVICE_LEASE_OVERRUNS = Counter(
    "pgbg_service_lease_overruns_total",
    "Number of work units that ended after their lease lapsed or was lost",
    ["service_name"],
)
SERVICE_LEASE_FAILURES = Counter(
    "pgbg_service_lease_failures_total",
    "Number of lease operations (elections and renewals) that failed and"
    " were downgraded to a warning",
    ["service_name"],
)
SERVICE_LEADERSHIP_CONFIRMED = Gauge(
    "pgbg_service_leadership_confirmed_timestamp_seconds",
    "Timestamp of this process's last confirmed leadership for a service;"
    " 0 while it is not the leader",
    ["service_name"],
)

_DEFAULT_LEASE_TTL_GRACE = 10.0
# How far into a term's TTL we let it run before renewing, as a fraction
# of lease_ttl. Renewing at half the TTL rather than close to the
# deadline leaves a full half-TTL of margin for the renewal itself to
# run and succeed before the term would otherwise lapse.
_RENEWAL_FRACTION = 0.5


@attrs.frozen
class _LeaseRecord:
    """
    A lease row identifying a leadership term.
    """

    name: str
    worker_id: str
    elected_at: datetime
    expires_at: datetime


SQL_DELETE_LAPSED_LEASES = psycopg.sql.SQL(
    """
DELETE FROM {leases}
WHERE
    name = %(name)s
    AND expires_at < statement_timestamp()
"""
)

SQL_INSERT_LEASE = psycopg.sql.SQL(
    """
INSERT INTO
    {leases}
    (name, worker_id, elected_at, expires_at)
VALUES (
    %(name)s,
    %(worker_id)s,
    statement_timestamp(),
    statement_timestamp() + make_interval(secs => %(ttl)s)
)
ON CONFLICT (name) DO NOTHING
RETURNING name, worker_id, elected_at, expires_at
"""
)

SQL_RENEW = psycopg.sql.SQL(
    """
UPDATE {leases}
SET
    expires_at = statement_timestamp() + make_interval(secs => %(ttl)s)
WHERE
    name = %(name)s
    AND worker_id = %(worker_id)s
    AND elected_at = %(elected_at)s
    AND expires_at >= statement_timestamp()
RETURNING name, worker_id, elected_at, expires_at
    """
)

SQL_RESIGN = psycopg.sql.SQL(
    """
DELETE FROM {leases}
WHERE
    name = %(name)s
    AND worker_id = %(worker_id)s
    AND elected_at = %(elected_at)s
"""
)


def _set_statement_timeout_for_transaction(
    conn: psycopg.Connection[Any], timeout_seconds: float
) -> None:
    """
    Limit each following statement of the current transaction.
    """
    conn.execute(
        "SELECT set_config('statement_timeout', %(ms)s, true)",
        {"ms": str(int(timeout_seconds * 1000))},
    )


def _attempt_election(
    conn: psycopg.Connection[Any],
    leases: psycopg.sql.Identifier,
    service_name: str,
    worker_id: str,
    ttl_seconds: float,
    *,
    timeout_seconds: float,
) -> _LeaseRecord | None:
    """
    Try to become the leader for *service_name*.
    """
    with conn.transaction():
        _set_statement_timeout_for_transaction(conn, timeout_seconds)
        conn.execute(
            SQL_DELETE_LAPSED_LEASES.format(leases=leases),
            {"name": service_name},
        )
        row = conn.execute(
            SQL_INSERT_LEASE.format(leases=leases),
            {
                "name": service_name,
                "worker_id": worker_id,
                "ttl": ttl_seconds,
            },
        ).fetchone()

    if row is None:
        return None

    return _LeaseRecord(*row)


def _attempt_renewal(
    conn: psycopg.Connection[Any],
    leases: psycopg.sql.Identifier,
    service_name: str,
    worker_id: str,
    elected_at: datetime,
    ttl_seconds: float,
    *,
    timeout_seconds: float,
) -> _LeaseRecord | None:
    """
    Extend a leadership term if it is still ours.
    """
    with conn.transaction():
        _set_statement_timeout_for_transaction(conn, timeout_seconds)
        row = conn.execute(
            SQL_RENEW.format(leases=leases),
            {
                "name": service_name,
                "worker_id": worker_id,
                "elected_at": elected_at,
                "ttl": ttl_seconds,
            },
        ).fetchone()

    if row is None:
        return None

    return _LeaseRecord(*row)


def _resign_as_leader(
    conn: psycopg.Connection[Any],
    leases: psycopg.sql.Identifier,
    service_name: str,
    worker_id: str,
    elected_at: datetime,
    *,
    timeout_seconds: float,
) -> None:
    """
    Delete the leadership term if it is still ours.
    """
    with conn.transaction():
        _set_statement_timeout_for_transaction(conn, timeout_seconds)
        conn.execute(
            SQL_RESIGN.format(leases=leases),
            {
                "name": service_name,
                "worker_id": worker_id,
                "elected_at": elected_at,
            },
        )


@attrs.define
class LeaderTerm:
    """
    This process's lease for a named service.

    Leadership is tracked as a locally-timed validity window: `is_leader`
    answers from memory. `ensure` (election at work-unit boundaries) and
    `renew_if_due` (renewal under running work units) keep that memory honest.
    """

    service_name: str
    worker_id: str
    lease_ttl: float
    # Provides the short-lived connections for elections, renewals, and the
    # final resign.
    get_connection: ConnectionProvider = attrs.field(kw_only=True)
    # The lease table's quoted identifier.
    leases: psycopg.sql.Identifier = attrs.field(kw_only=True)
    # Injectable clock, so tests can control leadership timing
    # deterministically instead of racing real time.
    time_source: Callable[[], float] = time.monotonic
    _record: _LeaseRecord | None = attrs.field(init=False, default=None)
    # The instant (per *time_source*) after which a held record is no longer
    # trusted locally.
    _valid_until: float = attrs.field(init=False, default=0.0)
    # The instant (per *time_source*) at or after which `ensure` attempts
    # a renewal.
    _renew_at: float = attrs.field(init=False, default=0.0)
    # Guards the term state: the work loop (election at work-unit boundaries)
    # and the keeper thread (renewal under running work units) both touch it.
    _lock: threading.Lock = attrs.field(
        init=False, factory=threading.Lock, repr=False
    )

    @property
    def is_leader(self) -> bool:
        """
        Return whether this process currently holds the leadership lease.
        """
        with self._lock:
            return (
                self._record is not None
                and self.time_source() < self._valid_until
            )

    @property
    def _statement_timeout(self) -> float:
        """
        The server-side bound for one election or renewal statement.

        The keeper cadence (a quarter of the TTL), clamped into [0.5, 5.0].
        Any longer and a slow-but-successful renewal could finish after the
        local validity window already ended.
        """
        return max(
            0.5,
            min(
                5.0,
                self.lease_ttl * _RENEWAL_FRACTION / 2,
            ),
        )

    @property
    def lease_deadline(self) -> float | None:
        """
        The local validity deadline of the held lease, or None without one.
        """
        with self._lock:
            return None if self._record is None else self._valid_until

    def snapshot(self) -> tuple[bool, float | None]:
        """
        One consistent (is_leader, lease deadline) pair.

        Unlike two property reads, a deposition by the keeper cannot fall
        between them: a True never comes with a None deadline.
        """
        with self._lock:
            leader = (
                self._record is not None
                and self.time_source() < self._valid_until
            )
            return leader, (self._valid_until if leader else None)

    def ensure(self) -> None:
        """
        Bring leadership state up to date, touching the database only when
        necessary.

        A lease statement canceled by a server-side timeout is downgraded to
        a warning and leaves the state as it was. A follower stays a follower
        and retries at the next work-unit boundary, and a held lease decays
        through its validity window unless a later renewal succeeds.
        """
        with self._lock:
            now = self.time_source()
            leader = self._record is not None and now < self._valid_until
            renew_due = leader and now >= self._renew_at

        try:
            if not leader:
                # Not a leader, so try to become one.
                self._elect()
            elif renew_due:
                # We're leader, but we've passed renewal threshold, renew.
                self._renew()
        except (
            psycopg.errors.QueryCanceled,
            psycopg.errors.LockNotAvailable,
        ):
            SERVICE_LEASE_FAILURES.labels(service_name=self.service_name).inc()
            logger.warning(
                "service_leader.lease_failed",
                service_name=self.service_name,
                worker_id=self.worker_id,
                exc_info=True,
            )

    def renew_if_due(self) -> None:
        """
        Renew the held lease if its renewal deadline has passed.

        One keeper cadence step: free when there is nothing to do, one renewal
        round trip when the deadline passed. An expired record is cleared and
        logged here.

        Safe to call from any thread.
        """
        with self._lock:
            if self._record is None:
                return

            now = self.time_source()
            expired = now >= self._valid_until
            if expired:
                self._record = None
            due = not expired and now >= self._renew_at

        if expired:
            SERVICE_LEADERSHIP_CONFIRMED.labels(
                service_name=self.service_name
            ).set(0)
            logger.info(
                "service_leader.lost",
                service_name=self.service_name,
                worker_id=self.worker_id,
                reason="expired",
            )
            return

        if due:
            self._renew()

    def _record_elected(self, t0: float, record: _LeaseRecord) -> None:
        """
        Store a freshly (re)elected record and derive its local deadlines.

        *t0*, captured by the caller with `time_source()` right before the
        statement that produced *record* ran, seeds both deadlines instead of
        the database's own `expires_at` which uses wall time while we use
        a deterministic time source.

        Callers hold `_lock`.
        """
        self._record = record
        self._valid_until = t0 + self.lease_ttl
        self._renew_at = t0 + self.lease_ttl * _RENEWAL_FRACTION
        # Wall time, not *t0*: Prometheus needs unix time, like the other
        # gauges.
        SERVICE_LEADERSHIP_CONFIRMED.labels(
            service_name=self.service_name
        ).set_to_current_time()

    def _elect(self) -> None:
        """
        Attempt to become leader.
        """
        t0 = self.time_source()
        with self.get_connection() as conn:
            record = _attempt_election(
                conn,
                self.leases,
                self.service_name,
                self.worker_id,
                self.lease_ttl,
                timeout_seconds=self._statement_timeout,
            )
        if record is None:
            return

        with self._lock:
            self._record_elected(t0, record)

        logger.info(
            "service_leader.elected",
            service_name=self.service_name,
            worker_id=self.worker_id,
        )

    def _renew(self) -> None:
        """
        Attempt to extend the current term.

        A record that vanished between the caller's check and here (the keeper
        can expire it) makes this a no-op.

        The result applies only to the term it renewed, and only when it
        extends the current window.
        """
        with self._lock:
            if self._record is None:
                return
            elected_at = self._record.elected_at

        t0 = self.time_source()
        with self.get_connection() as conn:
            record = _attempt_renewal(
                conn,
                self.leases,
                self.service_name,
                self.worker_id,
                elected_at,
                self.lease_ttl,
                timeout_seconds=self._statement_timeout,
            )

        with self._lock:
            stale = (
                self._record is None or self._record.elected_at != elected_at
            )
            if stale:
                return
            if record is None:
                self._record = None
            elif t0 + self.lease_ttl > self._valid_until:
                self._record_elected(t0, record)

        if record is None:
            SERVICE_LEADERSHIP_CONFIRMED.labels(
                service_name=self.service_name
            ).set(0)
            logger.info(
                "service_leader.lost",
                service_name=self.service_name,
                worker_id=self.worker_id,
                reason="deposed",
            )

    @contextmanager
    def keep_alive(self) -> Iterator[None]:
        """
        Renew the lease from a keeper thread while the block runs.

        The thread renews on a fixed cadence: half the renewal deadline,
        a quarter of the TTL.

        A due renewal runs comfortably before the lease can lapse, and it is
        joined on exit, crash or not.
        """
        keeper_stop = threading.Event()
        keeper = threading.Thread(
            target=self._keep_alive,
            args=(keeper_stop,),
            name=f"lease-keeper-{self.service_name}",
            daemon=True,
        )
        keeper.start()

        try:
            yield
        finally:
            keeper_stop.set()
            keeper.join()

    def _keep_alive(self, keeper_stop: threading.Event) -> None:
        """
        Thread body: renew the lease on a fixed cadence until stopped.
        """
        cadence = self.lease_ttl * _RENEWAL_FRACTION / 2

        try:
            while not keeper_stop.wait(cadence):
                self._renew_safely()
        except BaseException:
            # Even a SystemExit must not erase the record that the keeper died.
            logger.exception(
                "service_leader.keeper_died",
                service_name=self.service_name,
                worker_id=self.worker_id,
            )

    def _renew_safely(self) -> None:
        """
        Run one `renew_if_due`, downgrading any failure to a warning.

        A failed renewal is retried at the next cadence step. Meanwhile the
        local validity window keeps shrinking conservatively, so a dead
        database deposes this process locally instead of crashing the keeper
        thread.
        """
        try:
            self.renew_if_due()
        except Exception:
            SERVICE_LEASE_FAILURES.labels(service_name=self.service_name).inc()
            logger.warning(
                "service_leader.lease_failed",
                service_name=self.service_name,
                worker_id=self.worker_id,
                exc_info=True,
            )

    def resign_best_effort(self) -> None:
        """
        Try to give up leadership promptly.
        """
        with self._lock:
            record = self._record
        if record is None:
            return

        try:
            with self.get_connection() as conn:
                _resign_as_leader(
                    conn,
                    self.leases,
                    self.service_name,
                    self.worker_id,
                    record.elected_at,
                    timeout_seconds=1,
                )

            logger.info(
                "service_leader.resigned",
                service_name=self.service_name,
                worker_id=self.worker_id,
            )
            with self._lock:
                self._record = None
            SERVICE_LEADERSHIP_CONFIRMED.labels(
                service_name=self.service_name
            ).set(0)
        except Exception:
            logger.warning(
                "service_leader.resign_failed",
                service_name=self.service_name,
                exc_info=True,
            )


def _make_set_event() -> threading.Event:
    """
    Return an already-set event: an `IntervalOnlyWakeup` starts "woken".
    """
    event = threading.Event()
    event.set()

    return event


@attrs.define
class IntervalOnlyWakeup:
    """
    The wakeup a service uses with no dispatcher.

    It starts woken, so a service's first work unit runs at startup, just like
    [`Subscription`][pgbg.Subscription]s get woken after their `LISTEN` goes
    live.

    After that, it has no external notification source, so
    [`wait()`][pgbg.IntervalOnlyWakeup.wait] only ever times out and the
    [`Service`][pgbg.Service] polls on its own `interval`.

    [`wake()`][pgbg.IntervalOnlyWakeup.wake] ends the wait instantly.
    """

    _woken: threading.Event = attrs.field(init=False, factory=_make_set_event)

    def wait(self, timeout: float) -> bool:
        """
        Block up to *timeout* seconds.

        Args:
            timeout: Maximum time to wait for a wakeup.

        Return `True` when woken and `False` on a timeout.
        """
        if self._woken.wait(timeout):
            # The wake is consumed here, so the next `wait` blocks again.
            # Edge-triggered, like `Subscription.wait`, rather than staying
            # woken forever.
            self._woken.clear()
            return True

        return False

    def wake(self) -> None:
        """
        End the current wait and let the loop re-check its stop event.
        """
        self._woken.set()

    def close(self) -> None:
        """
        Release nothing: there is no subscription behind this wakeup.
        """


def as_work_factory(do_work: DoWork) -> WorkFactory:
    """
    Wrap a plain *do_work* callable into a
    [`WorkFactory`][pgbg.typing.WorkFactory] with no setup or cleanup.

    Args:
        do_work: A callable that performs one bounded work unit.

    Returns:
        A factory that creates a context manager returning *do_work*.
    """
    return lambda: nullcontext(do_work)


@attrs.define
class Service:
    """
    Loop for a per-process background service.

    Every process runs its own work units and waits on
    a [`Wakeup`][pgbg.typing.Wakeup] between loop cycles.

    Users must create it using [`build()`][pgbg.Service.build].
    """

    _interval: float = attrs.field(alias="interval")
    _service_name: str = attrs.field(alias="service_name")
    _wakeup: Wakeup = attrs.field(alias="wakeup")
    _work_factory: WorkFactory = attrs.field(alias="work_factory")
    has_completed_cycle: bool = attrs.field(init=False, default=False)

    @classmethod
    def build(
        cls,
        work_factory: WorkFactory,
        *,
        service_name: str,
        wakeup: Wakeup,
        interval: float = 1.0,
    ) -> Self:
        """
        Validate arguments and build a service, ready to be supervised.

        Hand the result to [`Supervisor.start()`][pgbg.Supervisor.start], to
        run it supervised in a background thread.

        Args:
            work_factory:
                See [`WorkFactory`][pgbg.typing.WorkFactory] and
                [Services](services.md).

            service_name:
                Names the service in logs and metrics. Must not be empty.

            wakeup:
                Ends the wait between loop cycles. See
                [`Wakeup`][pgbg.typing.Wakeup].
                A [`Subscription`][pgbg.Subscription] provides
                notification-driven wakeups. An
                [`IntervalOnlyWakeup`][pgbg.IntervalOnlyWakeup] polls on
                *interval* alone.

                !!! warning
                    Do not share this wakeup with another consumer. The service
                    takes exclusive ownership and closes it when supervision
                    ends.

            interval:
                Maximum seconds to wait for a wakeup. The service runs again
                (performs a *loop cycle*) when this interval expires. Must be
                greater than zero.

        Raises:
            ValueError:
                If *interval* or *service_name* are invalid.
        """
        if interval <= 0 or not math.isfinite(interval):
            msg = "interval must be > 0"
            raise ValueError(msg)

        if not service_name:
            msg = "service_name must not be empty"
            raise ValueError(msg)

        return cls(
            interval=interval,
            service_name=service_name,
            wakeup=wakeup,
            work_factory=work_factory,
        )

    def run(self, stop: threading.Event) -> None:
        """
        Wait for wakeups and work until *stop* is set.

        Enters the work factory first: it creates the loop run's `do_work`, and
        its cleanup runs when the loop run ends, crash or not.

        Any failure propagates and a clean return means *stop* was set.
        A factory that suppresses the loop run's crash raises
        [`SuppressedCrashError`][pgbg.exceptions.SuppressedCrashError] instead.

        Args:
            stop:
                The event the loop exits on.
        """
        self.has_completed_cycle = False
        # Create the series at 0 without clobbering an earlier stamp.
        SERVICE_LAST_WORK_UNIT.labels(service_name=self._service_name)
        log = logger.bind(func="service", service_name=self._service_name)
        log.info("service.started")

        with self._work_factory() as do_work:
            while not stop.is_set():
                # Each loop cycle waits first. The startup work unit is
                # triggered by the wakeup's initial wake like `Subscription`'s
                # LISTEN going live, or `IntervalOnlyWakeup` starting woken.
                self._wait_for_wakeup()
                if stop.is_set():
                    break

                self._run_once(do_work, stop)
                self.has_completed_cycle = True

        if not stop.is_set():
            msg = "the work factory suppressed the loop run's crash"
            raise SuppressedCrashError(msg)

        log.info("service.stopped")

    def wake(self) -> None:
        """
        Wake the loop out of its wait so a set stop takes effect promptly.
        """
        self._wakeup.wake()

    def close(self) -> None:
        """
        Release the wakeup.
        """
        self._wakeup.close()

    def _wait_for_wakeup(self) -> None:
        """
        Wait for a wakeup, falling back to the interval timeout.
        """
        if self._wakeup.wait(self._interval):
            logger.debug("service.notified", service_name=self._service_name)

    def _run_once(self, do_work: DoWork, stop: threading.Event) -> None:
        """
        Run the work units of one loop cycle.
        """
        while True:
            again = do_work()
            self.has_completed_cycle = True

            SERVICE_LAST_WORK_UNIT.labels(
                service_name=self._service_name
            ).set_to_current_time()

            if not again or stop.is_set():
                return


@attrs.frozen
class SupervisedService:
    """
    Handle for a service that runs under supervision.

    Construct via [`start()`][pgbg.SupervisedService.start].

    !!! info "See also"
        [Supervised Service Loops](services.md)
    """

    _service: Service = attrs.field(alias="service")
    _supervisor: Supervisor = attrs.field(alias="supervisor")

    @classmethod
    def start(
        cls,
        work_factory: WorkFactory,
        *,
        name: str,
        wakeup: Wakeup,
        interval: float = 1.0,
        initial_backoff: float = 0.1,
    ) -> Self:
        """
        Build a service for *work_factory* and start running it under a
        [`Supervisor`][pgbg.Supervisor].

        *name* names the service, the supervisor, its thread, and the restart
        metric's label.

        See [`Service.build()`][pgbg.Service.build] for the service arguments and
        [`Supervisor.start()`][pgbg.Supervisor.start] for *initial_backoff*.
        """
        service = Service.build(
            work_factory,
            service_name=name,
            wakeup=wakeup,
            interval=interval,
        )

        return cls(
            service=service,
            supervisor=Supervisor.start(
                service, name=name, initial_backoff=initial_backoff
            ),
        )

    @property
    def is_running(self) -> bool:
        """
        Return whether the supervising thread is still alive.
        """
        return self._supervisor.is_running

    def stop(self, timeout: float | None = None) -> bool:
        """
        Stop the supervision and the service loop.

        See [`Supervisor.stop()`][pgbg.Supervisor.stop].
        """
        return self._supervisor.stop(timeout)

    def __enter__(self) -> Self:
        """
        Return the running service itself.
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


@attrs.define
class ElectedService:
    """
    A [`Service`][pgbg.Service] plus a lease: its work units run only while
    this process holds leadership for the service name.

    Leaders are elected through a caller-supplied lease table and the other
    processes stand by and take over when the lease lapses. See
    [Elections](leader-election.md#elections) for details.

    [`run()`][pgbg.ElectedService.run] itself never retries: it waits, elects,
    works, and lets any failure propagate to the supervisor.

    A keeper thread renews the lease on a fixed cadence for as long as
    [`run()`][pgbg.ElectedService.run] lives (even when a work unit runs) so
    neither a long work unit nor a long backlog can outlive the lease while the
    process is healthy. `lease_ttl` therefore sizes failover time after
    a crash, not the work-unit budget.

    Between wakeups it waits on a [`Wakeup`][pgbg.typing.Wakeup].
    [`Subscription`][pgbg.Subscription] provides prompt notification-driven
    wakeups. [`IntervalOnlyWakeup`][pgbg.IntervalOnlyWakeup] polls on
    *interval* alone.

    !!! warning
        Overlap with a new leader's work units is still possible when renewals
        fail or the whole process stalls, because Python cannot safely stop a
        callback after it starts.

        If overlapping work can damage data, use database locks, fencing
        tokens, or idempotent operations. The service logs
        `service.lease_overrun` and increments
        `pgbg_service_lease_overruns_total` when a work unit ends after its
        lease lapsed or was lost.
    """

    _interval: float = attrs.field(alias="interval")
    _term: LeaderTerm = attrs.field(alias="term")
    _wakeup: Wakeup = attrs.field(alias="wakeup")
    _work_factory: WorkFactory = attrs.field(alias="work_factory")
    has_completed_cycle: bool = attrs.field(init=False, default=False)

    @classmethod
    def build(
        cls,
        work_factory: WorkFactory,
        get_connection: ConnectionProvider,
        *,
        service_name: str,
        worker_id: str,
        wakeup: Wakeup,
        leases: str = "pgbg_leases",
        interval: float = 1.0,
        lease_ttl: float | None = None,
    ) -> Self:
        """
        Validate arguments and build a service, ready to be supervised.

        Hand the result to [`Supervisor.start()`][pgbg.Supervisor.start], which
        owns the thread, its restarts, and the backoff.

        Takes the same arguments as [`Service.build()`][pgbg.Service.build],
        plus the following election-related arguments:

        Args:
            service_name:
                Like in [`Service`][pgbg.Service], but additionally also names
                the leadership lease row.

            get_connection:
                Provides the short-lived connections for elections, renewals,
                and the final resign

            leases:
                The lease table's name, optionally schema-qualified with a dot
                (e.g. `"public.service_leases"`). Each part is quoted as an
                identifier. Parts must not contain dots or be empty.

            worker_id:
                Must be a per-worker-unique, non-empty identity supplied by
                the caller. For example, a hostname, container id, or
                job/allocation id.

            lease_ttl:
                Seconds that a leadership lease remains valid. The default is
                *interval* plus 10 seconds. Must be greater than zero.

        Raises:
            ValueError:
                If *interval*, *service_name*, *leases*, *worker_id*, or
                *lease_ttl* are invalid.
        """
        if interval <= 0 or not math.isfinite(interval):
            msg = "interval must be > 0"
            raise ValueError(msg)

        if not service_name:
            msg = "service_name must not be empty"
            raise ValueError(msg)

        if not worker_id:
            msg = "worker_id must not be empty"
            raise ValueError(msg)

        if not leases or not all(leases.split(".")):
            msg = "leases must be a table name without empty parts"
            raise ValueError(msg)

        if lease_ttl is None:
            lease_ttl = interval + _DEFAULT_LEASE_TTL_GRACE
        if lease_ttl <= 0 or not math.isfinite(lease_ttl):
            msg = "lease_ttl must be > 0"
            raise ValueError(msg)

        return cls(
            interval=interval,
            term=LeaderTerm(
                service_name,
                worker_id,
                lease_ttl,
                get_connection=get_connection,
                leases=psycopg.sql.Identifier(*leases.split(".")),
            ),
            wakeup=wakeup,
            work_factory=work_factory,
        )

    @property
    def is_leader(self) -> bool:
        """
        Return whether this process currently holds the leadership lease.
        """
        return self._term.is_leader

    def run(self, stop: threading.Event) -> None:
        """
        Wait for wakeups, elect, and work until *stop* is set.

        Enters the work factory first. It makes the loop run's `do_work`, and
        its cleanup runs when the loop run ends.

        Any failure propagates and a clean return means *stop* was set.
        A factory that suppresses the loop run's crash raises
        [`SuppressedCrashError`][pgbg.exceptions.SuppressedCrashError] instead.

        A keeper thread renews the lease for as long as this loop run lives
        in the background and dies with it.
        """
        self.has_completed_cycle = False
        # Create the series at 0 without clobbering an earlier stamp.
        SERVICE_LAST_WORK_UNIT.labels(service_name=self._term.service_name)
        if not self._term.is_leader:
            SERVICE_LEADERSHIP_CONFIRMED.labels(
                service_name=self._term.service_name
            ).set(0)
        log = logger.bind(func="service", service_name=self._term.service_name)
        log.info("service.started")

        # The factory is entered before the keeper starts: leadership keeps
        # decaying while the loop run's setup runs, and the keeper is joined
        # before the factory's cleanup on the way out.
        with self._work_factory() as do_work, self._term.keep_alive():
            while not stop.is_set():
                # Each loop cycle waits first: the startup election and work unit
                # are triggered by the wakeup's initial wake (a
                # `Subscription`'s LISTEN going live, or an
                # `IntervalOnlyWakeup` starting woken).
                self._wait_for_wakeup()
                if stop.is_set():
                    break

                self._run_once(do_work, stop)
                self.has_completed_cycle = True

        if not stop.is_set():
            msg = "the work factory suppressed the loop run's crash"
            raise SuppressedCrashError(msg)

        log.info("service.stopped")

    def wake(self) -> None:
        """
        Wake the loop out of its wait so a set stop takes effect promptly.
        """
        self._wakeup.wake()

    def close(self) -> None:
        """
        Release the wakeup and resign leadership.

        The supervisor calls this when it stops for good so a transient crash
        keeps the lease while a graceful stop gives it up promptly. The resign
        asks the term's *get_connection* for one last connection, so stop the
        supervisor before you tear the provider's backing down.
        """
        self._wakeup.close()
        self._term.resign_best_effort()

    def _wait_for_wakeup(self) -> None:
        """
        Wait for a wakeup, falling back to the interval timeout.
        """
        if self._wakeup.wait(self._interval):
            logger.debug(
                "service.notified",
                service_name=self._term.service_name,
            )

    def _run_once(self, do_work: DoWork, stop: threading.Event) -> None:
        """
        Run the work units of one loop cycle.
        """
        while True:
            self._term.ensure()

            is_leader, lease_deadline = self._term.snapshot()
            if not is_leader:
                return
            # snapshot's contract: a True never comes with a None
            # deadline.
            assert lease_deadline is not None

            try:
                again = do_work()
            finally:
                # Safety-check whether we've overrun our leadership term.
                current = self._term.lease_deadline
                deadline = lease_deadline if current is None else current
                overrun = self._term.time_source() - deadline
                if current is None or overrun >= 0:
                    SERVICE_LEASE_OVERRUNS.labels(
                        service_name=self._term.service_name
                    ).inc()
                    logger.warning(
                        "service.lease_overrun",
                        service_name=self._term.service_name,
                        worker_id=self._term.worker_id,
                        overrun_seconds=max(overrun, 0.0),
                        reason="lost" if current is None else "lapsed",
                    )

            self.has_completed_cycle = True

            SERVICE_LAST_WORK_UNIT.labels(
                service_name=self._term.service_name
            ).set_to_current_time()

            if not again or stop.is_set():
                return


@attrs.frozen
class SupervisedElectedService:
    """
    Handle for an elected service that runs under supervision.

    Construct via [`start()`][pgbg.SupervisedElectedService.start] (or
    [`pgbg.sqlalchemy.start_elected_service`][pgbg.sqlalchemy.start_elected_service]
    from an [SQLAlchemy `Engine`][sqlalchemy.engine.Engine]).

    !!! info "See also"
        - [Supervised Service Loops](services.md)
        - [Elections](leader-election.md#elections)
    """

    _service: ElectedService = attrs.field(alias="service")
    _supervisor: Supervisor = attrs.field(alias="supervisor")

    @classmethod
    def start(
        cls,
        work_factory: WorkFactory,
        get_connection: ConnectionProvider,
        *,
        name: str,
        worker_id: str,
        wakeup: Wakeup,
        leases: str = "pgbg_leases",
        interval: float = 1.0,
        lease_ttl: float | None = None,
        initial_backoff: float = 0.1,
    ) -> Self:
        """
        Build an elected service and start running it under a
        [`Supervisor`][pgbg.Supervisor].

        *name* names the lease row, the supervisor, its thread, and the restart
        metric's label alike, and the returned handle is all you need.

        See [`ElectedService.build()`][pgbg.ElectedService.build] for the
        service arguments and [`Supervisor.start()`][pgbg.Supervisor.start]
        for *initial_backoff*.
        """
        service = ElectedService.build(
            work_factory,
            get_connection,
            service_name=name,
            leases=leases,
            worker_id=worker_id,
            wakeup=wakeup,
            interval=interval,
            lease_ttl=lease_ttl,
        )

        return cls(
            service=service,
            supervisor=Supervisor.start(
                service,
                name=name,
                initial_backoff=initial_backoff,
            ),
        )

    @property
    def is_running(self) -> bool:
        """
        Return whether the supervising thread is still alive.
        """
        return self._supervisor.is_running

    @property
    def is_leader(self) -> bool:
        """
        Return whether this process currently holds the leadership lease.
        """
        return self._service.is_leader

    def stop(self, timeout: float | None = None) -> bool:
        """
        Stop the supervision and the service loop.

        See [`Supervisor.stop()`][pgbg.Supervisor.stop].
        """
        return self._supervisor.stop(timeout)

    def __enter__(self) -> Self:
        """
        Return the running service itself.
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
