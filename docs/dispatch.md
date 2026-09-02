# `NOTIFY` Dispatch

Modern PostgreSQL has great support for real-time signaling with [`LISTEN`](https://www.postgresql.org/docs/current/sql-listen.html) and [`NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html).
However, PostgreSQL connections are expensive and external connection pools like [PgBouncer] are finicky with `LISTEN` / `NOTIFY`.

And so the [notifier pattern](https://brandur.org/notifier) emerged, where each process has only *one* connection that listens on **all** channels and dispatches the notifications to its **local** subscribers.

In *pgbg* it means that you configure your service to be woken up by your local subscriptions in addition to the interval.

---

[`NotifyDispatcher`][pgbg.NotifyDispatcher] is an implementation of that pattern and [`SupervisedDispatcher`][pgbg.SupervisedDispatcher] makes it easy to run one under supervision in the background.

If you use SQLAlchemy, you can use [`start_dispatcher()`][pgbg.sqlalchemy.start_dispatcher] to derive a connection factory from your [`Engine`][sqlalchemy.engine.Engine] – as demonstrated in the [real-time notification section of our tutorial](tutorial.md#rt).


## Subscriptions

Once you've started a dispatcher (either using [`SupervisedDispatcher.start()`][pgbg.SupervisedDispatcher.start] or [`start_dispatcher()`][pgbg.sqlalchemy.start_dispatcher]), you can create subscriptions by calling its [`subscribe("channel")` method][pgbg.SupervisedDispatcher.subscribe].

This subscription implements the [`Wakeup`][pgbg.typing.Wakeup] protocol and therefore you can pass it into services for their *wakeup* argument.

It works as a coalescing, edge-triggered wakeup signal that ignores the `NOTIFY` payloads, because `LISTEN` / `NOTIFY` is best-effort and supporting payloads would coax people into using it for things [it's not intended for](https://blog.ganssle.io/articles/2023/01/attractive-nuisances.html).

!!! success ""
    Treat every wake as a doorbell, and read the data you care about from your tables on each wake.

[`Subscription.close()`][pgbg.Subscription.close] cancels a subscription, and the loop stops the `LISTEN` when the last subscription of a channel is gone.

True to its crash-only principles, each dispatcher run opens a fresh connection and rebuilds its `LISTEN` state from the current subscriptions.
Every time this happens, the dispatcher wakes all subscriptions to account for missed notifications.
Therefore, subscriptions remain valid across these restarts.


## Connection poolers

The dispatcher's `LISTEN` connection is dedicated and lives outside any pool, because transaction-mode poolers such as [PgBouncer] do not support `LISTEN`.
Point the connection factory *directly* at PostgreSQL, while your application traffic goes through the pooler.

If no direct connection is possible, run your services with an `IntervalOnlyWakeup` and skip the dispatcher entirely.
Everything works the same, except your services then only wake up on their interval instead of on notifications.

[PgBouncer]: https://www.pgbouncer.org
