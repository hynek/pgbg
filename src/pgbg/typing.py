import threading

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol

import psycopg


if TYPE_CHECKING:
    from ._dispatcher import Subscription


type ConnectionProvider = Callable[
    [], AbstractContextManager[psycopg.Connection[Any]]
]
"""
Provides short-lived connections for elections and renewals. It's called
once per database visit, so try to utilize some kind of connection pooling.

Connections must be lent out with **no** transaction in progress. The lease
SQL assumes [`READ COMMITTED`][rc] semantics.

[rc]: https://www.postgresql.org/docs/current/transaction-iso.html#XACT-READ-COMMITTED
"""


class Wakeup(Protocol):
    """
    A source that wakes a service between work units.
    """

    def wait(self, timeout: float) -> bool:
        """
        Wait up to *timeout* seconds for a wake.

        Return `True` when woken and `False` on a timeout. A `True` return
        consumes the pending wake, so the next call blocks again.
        """
        ...

    def wake(self) -> None:
        """
        End the current wait.
        """
        ...

    def close(self) -> None:
        """
        Release the wakeup.
        """
        ...


class Loop(Protocol):
    """
    A loop that a [`Supervisor`][pgbg.Supervisor] keeps alive.

    Only relevant for people implementing their own loops.

    Within *pgbg*, implemented by [`Service`][pgbg.Service],
    [`ElectedService`][pgbg.ElectedService], and the internal adapter that
    makes a [`NotifyDispatcher`][pgbg.NotifyDispatcher] supervisable.
    """

    @property
    def has_completed_cycle(self) -> bool:
        """
        Whether the current loop run has completed at least one full loop
        cycle.
        """
        ...

    def run(self, stop: threading.Event) -> None:
        """
        Run the loop.

        Block until the stop event is set (a clean return) or something
        breaks (an exception the supervisor restarts after).

        **Must** set `has_completed_cycle` to `False` before any fallible work
        and True once a full loop cycle has completed. This way the supervisor
        can tell a crash after real progress (reset the backoff) from one that
        never got going (grow backoff).

        Args:
            stop: Event that signals the loop to stop.
        """
        ...

    def wake(self) -> None:
        """
        Nudge the loop out of any wait so it re-checks the stop event.

        Called by `Supervisor.stop` right after it sets the event, so a healthy
        loop parked in a wait exits immediately instead of sitting out its poll
        interval.

        Also called before a crash restart, so a wait-first loop's next loop
        run starts its first work unit immediately.

        A loop whose wait can't be interrupted from another thread may make
        this a no-op and accept interval-bounded stop latency.
        """
        ...

    def close(self) -> None:
        """
        Release resources once, when the supervisor stops for good.

        Is **not** run on restarts.
        """
        ...


class DoWork(Protocol):
    """
    Callable signature for a service's work.

    Must do **one bounded work unit** (for example, one batch) for responsive
    shutdowns, accurate work-unit metrics, and non-overlapping leases.

    Return `True` to be run again immediately, or `False` to wait for the next
    wakeup.
    """

    def __call__(self) -> bool:
        """
        Perform one bounded work unit.
        """
        ...


type WorkFactory = Callable[[], AbstractContextManager[DoWork]]
"""
Each time a new loop run starts, this factory creates a context manager that
is immediately entered. Once the loop run exits or crashes, the context
manager is exited.

The context manager must produce the work callable that is run by the loop. The
work callable may return `True` if it wants to run again immediately.

The context manager must not suppress the loop run's exception: the service
raises [`SuppressedCrashError`][pgbg.exceptions.SuppressedCrashError] when it
does, because the supervisor would otherwise mistake the crash for a clean
stop.

`as_work_factory` wraps a plain [`DoWork`][pgbg.typing.DoWork] that needs no
setup or cleanup.
"""


class Subscribable(Protocol):
    """
    Anything that hands out channel subscriptions.

    Both `NotifyDispatcher` and `SupervisedDispatcher` satisfy it, so code that
    only needs to subscribe (like a background service wiring up its wakeup)
    can take either.
    """

    def subscribe(self, channel: str) -> Subscription:
        """
        Subscribe to *channel*.
        """
        ...
