# Supervised Service Loops

!!! abstract ""
    The core task of *pgbg* is to reliably run a user-supplied callable repeatedly in the background.

[^callable]: A function, a bound method, a class with a `__call__` method…

That callable[^callable] we call `do_work` throughout APIs and a call to it is a **work unit**.
The entity that drives your work unit repeatedly is a **service**.

A *waiting* service always wakes up regularly via a configurable **interval**.
If the work unit returns `True`, it is run immediately again.
This allows for **bounded runtimes** which are important for prompt shutdowns and [leader elections](leader-election.md).
Once the work unit indicates it is done, the service waits again.

A service also takes an object that implements the [`Wakeup`][pgbg.typing.Wakeup] protocol which allows for real-time wakeups in addition to the interval.
*pgbg* ships with [`LISTEN` / `NOTIFY`-based](dispatch.md) wakeups, dispatched over a single database connection per process, no less.


## Supervision

A plain running [`Service`][pgbg.Service] is nothing but a blocking loop.
It's started with its [`run()`][pgbg.Service.run] method which repeatedly calls your work unit.
We call one call to `Service.run(stop)` end-to-end a **loop run**.
A loop run ends when the work unit raises an exception or when the supplied `stop` [`threading.Event`][threading.Event] is set from the outside, which signals a graceful shutdown.

Making such a loop reliable is surprisingly difficult, because you have to entangle your business concerns with error handling and service recovery.
[Crash-only design](glossary.md#crash-only) is a lot more robust and allows for cleaner code in your work units.

For that, you wrap your [`Service`][pgbg.Service] in a [`Supervisor`][pgbg.Supervisor] which runs it in a **background thread** and **restarts** it whenever it dies.
A supervisor owns the thread, retry policy, and final cleanup for a background loop.

The supervisor [reports crashed loop runs](observability.md) and starts a new loop run after an exponential backoff with jitter.
The backoff resets after a loop run completes one healthy loop cycle.

A persistently broken loop crash-loops with backoff and heals as soon as its cause is fixed.
Outside shutdown, every crash is an `ERROR` log, and every restart increments the restart counter (see [Observability](observability.md)).
Alert on the restart rate and on the staleness of the gauges to catch chronic failure.

Only a `BaseException` that is not an `Exception`, such as `SystemExit` or `KeyboardInterrupt`, ends supervision for good.


## Microreboots

Since `Service` takes a *work factory* (a callable that makes a context manager that produces the work callable), you can use it for initialization and cleanup of your work unit's resources.
And since you own the resources in your context manager, they survive for the whole lifetime of the loop run (importantly: *not* per work unit run).

!!! tip
    This allows you to have "loop [microreboots](glossary.md#microreboot)" that in turn allow your work units to be truly [crash-only](glossary.md#crash-only).

*pgbg* comes with the [`SupervisedService.start()`][pgbg.SupervisedService.start] helper that makes the whole process ergonomic:

```python
def do_work() -> bool:
    """
    Crash-only work unit code goes here.
    """
    ...


@contextmanager
def make_work() -> Generator[DoWork]:
    logger.info("work init!")
    try:
        yield do_work
    finally:
        logger.info("work cleanup!")


with pgbg.SupervisedService.start(
    make_work,
    name="example-thread",
    wakeup=pgbg.IntervalOnlyWakeup(),
) as svc:
    # do_work runs in the background until we exit this context manager
    ...
```

!!! tip
    See [Getting Started](tutorial.md) for a complete, runnable example.

If you don't need any setup or cleanup work, you can use [`as_work_factory(do_work)`][pgbg.as_work_factory] to wrap a plain callable into a no-op factory.


## Lifecycle

[`SupervisedService.stop()`][pgbg.SupervisedService.stop] wakes the loop[^via], so a healthy service stops immediately after its current work unit finishes.

[^via]: Via [`Supervisor.stop()`][pgbg.Supervisor.stop]

The supervisor can drive anything that satisfies the small [`Loop`][pgbg.typing.Loop] protocol:
`run(stop)`, `wake()`, `close()`, and a `has_completed_cycle` attribute.

In addition to this chapter's [`Service`][pgbg.Service], `Loop` is also implemented by the headliners from the next chapters: [`ElectedService`][pgbg.ElectedService] and [`NotifyDispatcher`][pgbg.NotifyDispatcher][^indirectly].

[^indirectly]: Pedantically, `pgbg.NotifyDispatcher` does not actually implement `Loop`, but *pgbg* comes with an adapter to make it supervisable.


## Caveats galore: thread cancellation { #thread-cancellation }

Many caveats throughout the project are caused by the unequivocal fact that it's **impossible** to **safely** and **forcibly** terminate threads in Python.

So for example, shutdown is cooperative.
`stop(timeout)` requests shutdown and waits for the current work unit and any [lease operation](leader-election.md) to finish.
It returns `False` if they remain blocked and there's nothing *pgbg* can do about it, except wait some more.

!!! danger
    This is why it's important to keep your work units **bounded** regarding their runtime.
    For bigger workloads, return `True` to be run again immediately, unless the supervisor has been stopped.

    A long-running work unit stalls graceful shutdown, potentially leading to a hard kill by the OS-level supervisor.

---

We are aware of the hacks that make it possible to cancel a thread in Python, but we do not think these hacks have a place in production software.
We do hope, though, that in the age of free-threading, we will get better threading primitives that will improve the situation through a new focus on threads as a viable concurrency primitive.

Also keep in mind that the cancellation in, for example, Go is also strictly cooperative via context cancellation, so this is not a specific Python downside.
