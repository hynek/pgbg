# Glossary

There's a lot of similar terms and proper nouns – let's try to untangle them!


## General concepts

crash-only software { #crash-only }
:   The core idea of crash-only software is that software gets significantly simpler and more robust if you focus on fast recovery after a crash instead of trying to handle all possible errors everywhere.

    With that, **initialization is recovery**.
    It's ["Have You Tried Turning It Off And On Again?"](https://www.youtube.com/watch?v=5UT8RkSmN4k) for applications.

    In *pgbg*, a supervised service can crash anytime, as long as its [work factory][pgbg.typing.WorkFactory] knows how to initialize all necessary resources.

    The concept is closely related to the [*Let It Crash*](https://wiki.c2.com/?LetItCrash) philosophy in the – famously fault-tolerant – [Erlang](https://www.erlang.org) ecosystem.
    The term was coined by George Candea and Armando Fox in their [*Crash-Only Software*](https://www.usenix.org/conference/hotos-ix/crash-only-software) paper.

microreboot { #microreboot }
:   Microreboots make crash-only software more practical in the real world.

    Usually, you don't want your whole application to crash because one of ten HTTP connection pools went bad.
    But recovering a resource pool can be difficult or impossible (for example, with stuck resources).

    So you "reboot" only a part of your application and reinitialize the resource from scratch.

    Supervised services with work factories lend themselves to microreboots.

    As with crash-only software, there's a [paper by Candea et al.](https://www.usenix.org/conference/osdi-04/microreboot%E2%80%94-technique-cheap-recovery) on it.


## Services

service
:   A background thread running a [`Service`][pgbg.Service] or [`ElectedService`][pgbg.ElectedService] in a *loop*.

elected service
:   A service that uses the *lease* table to best-effort ensure that only **one** service loop runs its work units at a time.


## Loops and progress

loop
:   A long-running, blocking [`Loop.run()`][pgbg.typing.Loop.run] that a *supervisor* drives.
    Within *pgbg*: the *dispatch loop* or a *service*.

loop cycle
:   One pass through a loop's body: wait, then act.

    A dispatcher cycle waits for notifications and dispatches them.
    A service cycle waits for its wakeup and runs work units.
    A loop reports its progress through its [`has_completed_cycle`][pgbg.typing.Loop.has_completed_cycle] attribute.

loop run
:   One execution of a loop's `run()`, from start until it returns or crashes.
    The supervisor starts a new loop run after a backoff.

    Therefore, a *loop run* consists of zero (crashed or stopped before first loop cycle) to infinite (never crashes, never exits) *loop cycles*.

interval
:   The upper bound on a loop cycle's wait, if not interrupted by a notification or other wakeup.
    Depending on whether it's a [`Service`][pgbg.Service] or an [`ElectedService`][pgbg.ElectedService], this has different implications.
    For both it's the maximum wait time between *loop cycles*.

    *Elected services* that are currently followers also try to get the *lease*.


## Work

work unit
:   One call of `do_work`, time-bounded by contract.
    While `do_work` returns `True`, the next *work unit* runs back-to-back within the same loop cycle.
    Only *services* have work units.

`do_work`
:   The callable that performs one work unit per call.
    It's your code and the reason why *pgbg* exists.
    Its signature is the [`DoWork`][pgbg.typing.DoWork] protocol.

work factory
:   Makes a loop run's `do_work` and cleans up when the loop run ends.
    It is called at the start of every loop run, so setup is also recovery.
    [`as_work_factory`][pgbg.as_work_factory] wraps a plain `do_work` that needs no setup.


## Waking

wakeup
:   The object a service `wait()`s on between loop cycles.
    It can be anything that satisfies [`Wakeup`][pgbg.typing.Wakeup], but usually a [`Subscription`][pgbg.Subscription] or an [`IntervalOnlyWakeup`][pgbg.IntervalOnlyWakeup].

wake
:   The pending signal that a wakeup's `wait()` consumes by returning `True`.
    [`Wakeup.wake()`][pgbg.typing.Wakeup.wake] delivers one, and any number of pending wakes coalesce into one.


## Supervision

supervisor
:  Owns the thread, the backoff, and the restart policy for one *loop*.
   Usually a [`Supervisor`][pgbg.Supervisor].

crash
:   A loop run that ends with an exception.

backoff
:   The exponentially growing delay between a crash and the next loop run.
    It resets after a loop run's first completed loop cycle.

stop
:   The cooperative shutdown request.
    [`SupervisedService.stop()`][pgbg.SupervisedService.stop] / [`SupervisedElectedService.stop()`][pgbg.SupervisedElectedService.stop] set the loop's stop event and wake it.

handle
:   A batteries-included supervised facade:
    [`SupervisedDispatcher`][pgbg.SupervisedDispatcher], [`SupervisedService`][pgbg.SupervisedService], or [`SupervisedElectedService`][pgbg.SupervisedElectedService].
    It offers `stop()`, `is_running`, and a context manager.


## Leadership

worker
:   The process that runs *pgbg*'s threads and services.

worker id
:   The caller-supplied name of one worker:
    unique among the workers sharing a lease table, and ideally an identity that survives restarts.
    For example: a hostname, a Nomad allocation ID, or a Kubernetes pod name.
    The lease records the worker id of the current leaseholder;
    [Elections](leader-election.md#elections) explains its role.

leader
:   The process that currently holds the lease for a service name.

follower
:   A process that runs the same elected service but does not hold the lease.
    It stands by and takes over when the lease lapses.

lease
:   The row that grants leadership for one service name until `expires_at`.

lease table
:   The caller-supplied table of leases, one row per service name.
    `make_create_leases_table_sql` and `init_db` create the canonical definition.

term
:   One continuous stretch of leadership, identified by its `elected_at`:
    from election until lapse, resignation, or deposition.

election
:   Winning a free or lapsed lease.
    [Elections](leader-election.md#elections) explains how one works.

renewal
:   Extending a held lease before it expires.

resignation
:   Voluntarily deleting the held lease on shutdown.

deposition
:   Accepting that the database says the lease is gone or taken, and dropping leadership locally.

lapse
:   The lease expiring by time because renewals stopped.

lease keeper
:   The thread that renews the lease on a fixed cadence while a *loop run* lives.

lease overrun
:   A work unit that ends after its lease lapsed or was lost.
    This is a problem but should be extremely rare because it implies a loss of a lease which [`ElectedService`][pgbg.ElectedService] is trying to avoid.


## Dispatch

notification
:   One PostgreSQL [`NOTIFY`][notify] on a channel.
    *pgbg* turns it into a *wake* and ignores its payload.

channel
:   The PostgreSQL [`LISTEN`][listen] / [`NOTIFY`][notify] name that notifications travel on.

subscription
:   An in-process handle on one channel's notifications.
    A coalescing signal that `wait()` consumes.

dispatch
:   The in-process fan-out of a notification's wake to all subscriptions of its channel.

live
:   A [`LISTEN`][listen] is live once the current loop run has issued it on its connection.


[notify]: https://www.postgresql.org/docs/current/sql-notify.html
[listen]: https://www.postgresql.org/docs/current/sql-listen.html
