# Leader Election

An [`ElectedService`][pgbg.ElectedService] is a [`Service`][pgbg.Service] whose work units run only while the process holds a leadership lease for that kind of service[^pedant].

Use it for work that must not run on every process, such as queue maintenance, cleanups, or projection updates.
Many processes can run the same service, but a lease row in a caller-supplied table makes sure that only one of them *starts* doing new work.
The others stand by and take over when the lease lapses.

[^pedant]: Technically, a process can start as many elected services as it wants; including of the same kind.
  For simplicity's sake, we're going to assume that is not the case in these docs.

!!! warning

    Under normal operation, the lease keeps one process working at a time **and** it's highly unlikely for a process to lose its lease.
    However, the lease is **not** a hard mutual-exclusion primitive.

    In rare failure scenarios, an already-in-progress work unit *can* overlap with a new work unit from a replacement leader.
    Once *pgbg* detects a lease loss, it won't start *new* leader-only work.
    But it can't stop a work unit already running, because it's impossible to safely cancel threads in Python.

    If even this rare overlap is unacceptable, protect the work with a row lock or another synchronization mechanism.
    You can mitigate the risk by keeping your work units short and ask to run again immediately by returning `True`.

Like the dispatcher, it takes a *connection provider* for its election queries: a callable that lends out one connection per `with provider() as conn:` visit.
*Unlike* the dispatcher, this should be some kind of connection pool, because its election queries are frequent, but very short-lived.

A plain connect factory works, because a Psycopg connection is its own context manager but is a poor choice due to its overhead.
With SQLAlchemy, `pooled_connection_factory_from_engine` borrows the connections from the engine's pool instead of opening a fresh one per visit:

```python
from pgbg import as_work_factory
from pgbg.sqlalchemy import start_elected_service


def process_orders() -> bool:
    """
    Do one bounded work unit. Return True to run again at once.
    """
    ...


service = start_elected_service(
    as_work_factory(process_orders),
    engine,
    name="orders",
    leases="public.service_leases",
    worker_id="worker-01.example.internal",
    wakeup=dispatcher.subscribe("orders"),
)
```

The service handle mirrors the dispatcher's:
`stop()` and `is_running` for lifecycle, `is_leader` for the election state, or a `with` block that stops it for you.

Without SQLAlchemy, [`SupervisedElectedService.start()`][pgbg.SupervisedElectedService.start] directly takes a connection provider instead of an engine.


## Lifecycle

On each wake, the service runs work units:

- it makes sure that it holds the lease,
- calls `do_work`,
- and repeats while `do_work` returns `True`.

A keeper thread renews the lease on a fixed cadence for as long as the service runs, **also while a work unit runs**.
So neither a slow work unit nor a long backlog can outlive the lease while the process is healthy, and `lease_ttl` sizes the failover time after a crash, *not* the work-unit budget.

If a work unit raises an exception, the supervisor [reports the failure](observability.md) and restarts the service after a backoff.
The new loop run acquires the lease again before work continues.


## Lease overruns

When a work unit ends after its lease lapsed or was lost, the service logs `service.lease_overrun` and increments a counter.
That signal means the lease was not kept alive while the work unit ran (renewals failed, the whole process stalled, or the lease was taken away) so another leader might exist.


## Elections

An election is one short transaction against the lease table, and PostgreSQL is the only arbiter.
The worker deletes the service's lease if it has lapsed, then tries to `INSERT` a fresh lease that carries its own worker id and expires one `lease_ttl` later.
The lease table allows only one lease per service name, so the first `INSERT` after the lease becomes free wins.
Every later one conflicts, does nothing, and leaves its worker a follower.
There is no voting and no priority:
winning means your `INSERT` returned a row.

A follower attempts an election at every work-unit boundary:
on each wake, and at least once per `interval`.
A lease becomes free when its worker resigns on shutdown, or when it lapses because renewals stopped.
So after a leader dies without resigning, failover takes up to one `lease_ttl` for the lease to lapse, plus one wake for a follower to attempt the next election.

The worker id plays its role *after* the win:
it does not influence who wins, but the winning `INSERT` records it in the lease as the owner.
From then on, every renewal and the final resignation touch the lease only while it still carries the worker's own id and the term's `elected_at`.
So no worker can ever extend or delete another worker's lease, and a renewal that no longer matches anything is how a worker learns that it was deposed.
Because the lease records the current leaseholder, the lease table always answers who leads – as do the leadership log events.
