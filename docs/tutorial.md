# Getting Started

## Installation

*pgbg* is available on [PyPI under the `pgbg` name](https://pypi.org/project/pgbg/).
We're also going to use the SQLAlchemy integration later, so let's install both[^binary]:

```console
$ uv pip install 'pgbg[sqlalchemy]'
```

[^binary]: Psycopg also needs a binary database driver.
    You can either make sure your system has a libpq or add `psycopg[binary]` to the `uv pip install`.


## Ground floor: Need a refresher?

But before we dive into *pgbg*'s database-backed features, you should be somewhat comfortable with [*bgt*]'s supervised threads that *pgbg*'s features build on.

Check out its [quick tutorial][bgt-t] if you've never heard of it!


## Mezzanine: Database setup

To augment *bgt*'s supervised threads with database superpowers, we need to create the table that *pgbg* uses for coordination.

In this tutorial, we will assume that you can connect to `postgresql://pgbg@127.0.0.1/pgbg` (no password) and have access to the `pgbg_leases` table in the default schema.

On most systems you can create both by running the following as the `postgres` user:

```sql
CREATE ROLE pgbg LOGIN;
CREATE DATABASE pgbg OWNER pgbg;
```

You can dump the [DDL](https://en.wikipedia.org/wiki/Data_definition_language) for the table by running `python -Im pgbg init-db`, or let *pgbg* execute it by passing it a [Psycopg-compatible connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING).

If you've been following along, the invocation looks like this:

```console
$ python -Im pgbg init-db "postgresql://pgbg@127.0.0.1/pgbg"
```

You can test it using:

```console
$ psql -c 'SELECT 1' -h 127.0.0.1 -U pgbg pgbg
 ?column?
──────────
        1
(1 row)
```

With everything in place, let's take it for a ride!


## Level 1: Elected background threads

In the [*bgt* tutorial][bgt-t]'s example, if you run two instances at the same time, both perform their "work" independently of each other.

That's useful, but often not what you want.
For example, when you have the same application running in multiple Docker containers at the same time and want only *one of them* to take over certain duties like purging caches or reaping sessions.

To solve this, *pgbg* uses the `pgbg_leases` table we've just created to offer the concept of **elected leaders**.

The next example looks similar, except we now start the loop using the SQLAlchemy helper [`start_elected_service()`][pgbg.sqlalchemy.start_elected_service].
Pass the wrapped `do_work`, an [`Engine`][sqlalchemy.engine.Engine], and a unique worker ID that identifies the worker process (for example, Docker container).
The helper returns a [`SupervisedElectedService`][pgbg.SupervisedElectedService] handle.

```python title="elected_thread.py" hl_lines="15-18 21-29"
--8<-- "docs/examples/elected_thread.py"
```

This time, if you start two workers, you'll see that only one actually works and the other stands by.
If you interrupt the active one, the other takes over within an interval (1 second by default, 2 seconds in our example).

You can try what happens if you "forget" to close the elected service by commenting out `svc.stop()`!

Spoiler: the takeover takes longer, roughly bounded by the configurable lease time-to-live.


### Cooperation is necessary

And this is where the return value of `do_work` comes into play:
in Python, it's impossible to safely kill a thread (see *bgt's* [thread cancellation explanation](https://bgt.hynek.me/stable/services/#thread-cancellation)).
This can become a problem if your worker loses its leadership lease while your work unit is busy (or when you want prompt application shutdowns).

*pgbg* tries to keep the lease alive in a background thread, but there are rare situations where you can lose it anyway.
This is why it's useful to split your work into bounded work units whose runtimes are well under the configured lease time-to-live and ask to be allowed to run again by returning `True`.
If your worker still has the lease (and the loop hasn't been asked to exit), it will oblige immediately.
Otherwise, the new worker has to take over – or the loop exits.

!!! tip
    Since [`start_elected_service()`][pgbg.sqlalchemy.start_elected_service] returns a handle that also works as a context manager, you generally should use that shape if possible like we will in the next example.


## Penthouse: real-time notifications over one connection { #rt }

So far, our background work has only been triggered using a fixed interval timer.
Let's make it real-time and add `NOTIFY` dispatchers to the workers.

```python title="dispatch.py" hl_lines="15-17 20-22 29-30 33-48"
--8<-- "docs/examples/dispatch.py"
```

1. Very long interval to ensure the threads only react to event notifications.

Go on and run two or more!

To compensate for possible race conditions before the `LISTEN` is live on the relevant channel, the leader runs `do_foo()` and `do_bar()` on startup[^possible].
Afterwards you can trigger `foo` and `bar` in real time:

[^possible]: Since we're running two elected services that compete for two leases (`foo-worker` and `bar-worker`), it's technically possible that you end up with each process getting one lease.

```console
$ psql -c 'NOTIFY foo; NOTIFY bar;' -h 127.0.0.1 -U pgbg pgbg
```

!!! note

    *pgbg* ignores `NOTIFY` payloads, since PostgreSQL notification events aren't a reliable communication channel.

    A notification only pokes workers that look for their work on their own.

Since we use *elected* services, only one worker executes the work units.
If you kill it, the other takes over.

You can also inspect the lease table any time:

```console
$ psql -c 'SELECT * from pgbg_leases' -h 127.0.0.1 -U pgbg pgbg

          elected_at           │          expires_at           │ worker_id │    name
───────────────────────────────┼───────────────────────────────┼───────────┼────────────
 2026-08-18 19:25:33.764684+02 │ 2026-08-18 21:05:43.764684+02 │ 2         │ foo-worker
 2026-08-18 19:25:33.76464+02  │ 2026-08-18 21:05:43.76464+02  │ 2         │ bar-worker
(2 rows)
```

---

And that concludes our quick tour of *pgbg*!
Keep reading our topical guides to learn more details of how it works and if there's a proper noun overflow, don't hesitate to check our [glossary](glossary.md).

[crash-only]: glossary.md#crash-only
[*bgt*]: https://bgt.hynek.me/
[bgt-t]: https://bgt.hynek.me/stable/tutorial/
