# Glossary

There's a lot of similar terms and proper nouns – let's try to untangle them!

*pgbg* builds on [*bgt*](https://bgt.hynek.me/).
For *bgt*-specific concepts like supervised services, loops, work units, and wakeups, see [its glossary](https://bgt.hynek.me/stable/glossary/).


## General concepts

crash-only software { #crash-only }
:   The core idea of crash-only software is that software gets significantly simpler and more robust if you focus on fast recovery after a crash instead of trying to handle all possible errors everywhere.

    With that, **initialization is recovery**.
    It's ["Have You Tried Turning It Off And On Again?"](https://www.youtube.com/watch?v=5UT8RkSmN4k) for applications.

    In *pgbg*, a supervised service can crash anytime, as long as its [work factory][bgt.typing.WorkFactory] knows how to initialize all necessary resources.

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

elected service
:   A service that uses the *lease* table to best-effort ensure that only **one** service loop runs its work units at a time.


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
