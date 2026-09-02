# Getting Started

## Installation

*pgbg* is available on [PyPI under the `pgbg` name](https://pypi.org/project/pgbg/).
We're also going to use the SQLAlchemy integration later, so let's install both[^binary]:

```console
$ uv pip install 'pgbg[sqlalchemy]'
```

But for the first use case we don't need a database!

[^binary]: Psycopg also needs a binary database driver.
    You can either make sure your system has a libpq or add `psycopg[binary]` to the `uv pip install`.


## Level 1: Independent background threads

In its simplest configuration, *pgbg* allows you to start a thread that runs a function at a fixed interval in the background.
Since real-world software crashes eventually, *pgbg* is designed around **failure recovery** and does its best to keep everything running, even if your function raises an exception.

So, the following script starts a [`SupervisedService`][pgbg.SupervisedService] (a background thread) and runs for 10 seconds or until you interrupt it.
It runs a **loop** that calls the `do_work` function every 2 seconds (ignore the call to `as_work_factory()` for a second).
A single call of `do_work` is a **work unit** and the process that runs *pgbg* threads is a **worker**.

In this case, there's a 25% chance for the function to crash and a 50% chance to return `True`:

```python title="indie_thread.py"  hl_lines="16-22 26-31"
--8<-- "docs/examples/indie_thread.py"
```

You should see occasional crashes, but also multiple "did some work!" in quick succession.

This demonstrates two features:

1. Crashes are reported, but the *pgbg*-provided background thread with the loop is kept alive.
2. If `do_work` returns `True`, it's run again *immediately*.

It's easy to get excited by fault-tolerance, but the second feature only becomes important in a later chapter.


### Crash better

But before we go there, we can do a little better without a database still:
since *pgbg* is about failure recovery, it tries to encourage you to write [crash-only] software.

And that's why the default shape of work is not a callable, but a factory of [context managers](https://docs.python.org/3/library/stdtypes.html#context-manager-types).
The previous example used [`as_work_factory()`][pgbg.as_work_factory] to adapt a plain function to it, but if you write a function that returns a context manager, that context manager is entered at the start of a loop run – and after each crash.

This gives you the ability to "[microreboot](glossary.md#microreboot)" your loop on failures.
That simplifies the logic a lot, since you don't have to write error-prone recovery code.

Same example, except with an init and cleanup:

```python title="indie_with_init.py"  hl_lines="30-36"
--8<-- "docs/examples/indie_with_init.py"
```

Now, you can see a "work init!" at the start of each loop run and a "work cleanup!" after each crash and on application exit.
Your loop can have its own independent lifecycle.


## Mezzanine: Database setup

For the next example we need to create the table that *pgbg* uses for coordination.

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


## Level 2: Elected background threads

With our first example, if you run two instances at the same time, both perform their "work" independently of each other.

That's useful, but often not what you want.
For example, when you have the same application running in multiple Docker containers at the same time and want only *one of them* to take over certain duties like purging caches or reaping sessions.

To solve this, *pgbg* uses the `pgbg_leases` table we've just created to offer the concept of **elected leaders**.

The next example looks similar, except we now start the loop using the SQLAlchemy helper [`start_elected_service()`][pgbg.sqlalchemy.start_elected_service].
It takes the wrapped `do_work`, an [`Engine`][sqlalchemy.engine.Engine], and a `worker_id` string that identifies the worker process (for example, Docker container) uniquely and returns a [`SupervisedElectedService`][pgbg.SupervisedElectedService] handle.

```python title="elected_thread.py" hl_lines="16-19 22-30"
--8<-- "docs/examples/elected_thread.py"
```

This time, if you start two workers **with different IDs**, you'll see that only one actually works and the other stands by.
If you interrupt the active one, the other takes over within an interval (1 second by default, 2 seconds in our example).

You can try what happens if you "forget" to close the elected service by commenting out `svc.stop()`!

Spoiler: the takeover takes longer, roughly bounded by the configurable lease time-to-live.


### Cooperation is necessary

And this is where the return value of `do_work` comes into play:
in Python, it's impossible to safely kill a thread (see [thread cancellation](services.md#thread-cancellation)).
This can become a problem if your worker loses its leadership lease while your work unit is busy (or when you want prompt application shutdowns).

*pgbg* tries to keep the lease alive in a background thread, but there are rare situations where you can lose it anyway.
This is why it's useful to split your work into bounded work units whose runtimes are well under the configured lease time-to-live and ask to be allowed to run again by returning `True`.
If your worker still has the lease (and the loop hasn't been asked to exit), it will oblige immediately.
Otherwise, the new worker has to take over – or the loop exits.

---

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
