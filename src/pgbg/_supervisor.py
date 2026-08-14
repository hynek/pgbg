"""
Supervision for a background loop.
"""

import threading

from collections.abc import Generator
from types import TracebackType
from typing import Self

import attrs
import structlog

from prometheus_client import Counter

from ._backoff import backoff_iter
from .typing import Loop


logger = structlog.stdlib.get_logger("pgbg")

SUPERVISOR_RESTARTS = Counter(
    "pgbg_supervisor_restarts_total",
    "Number of supervised-loop crashes the supervisor restarted after",
    ["name"],
)

_MAX_BACKOFF_SECONDS = 30.0


@attrs.define
class Supervisor:
    """
    Runs a loop in a background thread and restarts it when it dies.

    Owns the loop's thread, its stop event, and the restart policy. Crashes are
    instrumented and retried after an exponential backoff. A persistently
    broken loop crash-loops with backoff and heals as soon as its cause is
    fixed.

    Users **must** create instances with [`start()`][pgbg.Supervisor.start].

    Can be used as a context manager that automatically stops the loop on
    exit.
    """

    name: str
    _loop: Loop = attrs.field(alias="loop")
    _stop_event: threading.Event = attrs.field(
        init=False, factory=threading.Event
    )
    _thread: threading.Thread = attrs.field(init=False)
    _initial_backoff: float = attrs.field(alias="initial_backoff")

    @classmethod
    def start(
        cls, loop: Loop, *, name: str, initial_backoff: float = 0.1
    ) -> Self:
        """
        Build and start the supervisor thread.

        Args:
            loop:
                The loop this supervisor keeps alive.

            name:
                Identifies this supervisor in its thread name, logs, and the
                restart metric's label.

            initial_backoff:
                How long the supervisor waits after a crash before restarting.
        """
        # Validate the backoff range. Otherwise, the error would only
        # bubble up when the first backoff is attempted.
        next(backoff_iter(initial_backoff, _MAX_BACKOFF_SECONDS))

        # Create the restart series at 0, so the very first restart is
        # already visible to rate().
        SUPERVISOR_RESTARTS.labels(name=name)

        instance = cls(name=name, loop=loop, initial_backoff=initial_backoff)
        instance._thread = threading.Thread(
            target=instance._supervise, name=f"supervisor-{name}", daemon=True
        )
        instance._thread.start()

        return instance

    @property
    def is_running(self) -> bool:
        """
        Return whether the supervisor thread is still alive.
        """
        return self._thread.is_alive()

    def stop(self, timeout: float | None = None) -> bool:
        """
        Signal everything to stop and wait for the thread to exit.

        Idempotent.

        Args:
            timeout:
                Maximum time to wait for the thread to exit. None means wait
                indefinitely.

        Returns:
            `True` if the thread exited within the timeout, `False` otherwise.
        """
        self._stop_event.set()
        self._loop.wake()
        self._thread.join(timeout)

        return not self._thread.is_alive()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.stop()

    def _supervise(self) -> None:
        """
        Thread body: run the loop, restarting it on crashes until stopped.
        """
        log = logger.bind(func="supervisor", name=self.name)
        log.info("supervisor.started")

        def make_backoff() -> Generator[float]:
            return backoff_iter(
                start=self._initial_backoff, stop=_MAX_BACKOFF_SECONDS
            )

        backoff = make_backoff()
        reason = "stopped"

        try:
            while not self._stop_event.is_set():
                crashed = False

                try:
                    self._loop.run(self._stop_event)
                except Exception:
                    if self._stop_event.is_set():
                        # Crash happened during the shutdown, so we don't
                        # care. Just log it.
                        log.warning(
                            "supervisor.loop_crashed_during_shutdown",
                            exc_info=True,
                        )
                        break

                    crashed = True
                    log.exception("supervisor.loop_crashed")

                if not crashed:
                    # A clean return means the stop event is set.
                    break

                if self._loop.has_completed_cycle:
                    # The loop run survived at least one full loop cycle, so
                    # this crash is fresh trouble rather than the same one
                    # again: start the backoff ladder over.
                    backoff = make_backoff()

                sleep = next(backoff)
                if self._stop_event.wait(sleep):
                    break

                SUPERVISOR_RESTARTS.labels(name=self.name).inc()
                log.info("supervisor.restarting", backoff=sleep)
                # Pre-trigger the wakeup: the restarted loop run waits first,
                # and the crashed work must be retried immediately instead of
                # sitting out an interval.
                self._loop.wake()
        except BaseException as e:
            if not isinstance(e, KeyboardInterrupt | SystemExit):
                log.exception("supervisor.crashed")
                reason = "crashed"
            else:
                reason = "killed"
        finally:
            try:
                self._loop.close()
            except BaseException:
                # Even a SystemExit from a teardown must not erase the record
                # that supervision ended. `threading.excepthook` would swallow
                # it without a trace.
                log.exception("supervisor.close_failed")
            log.info("supervisor.stopped", reason=reason)
