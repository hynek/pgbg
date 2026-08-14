# Core API

## Batteries-included

Reach for these first:
every handle composes the necessary buildings blocks, starts its loop under a supervisor and offers `stop()`, `is_running`, and a context manager.

If you're using SQLAlchemy, have also a look at [our helpers](api-sqlalchemy.md) with even more batteries included.

::: pgbg
    options:
      members:
        - SupervisedService
        - SupervisedElectedService
        - SupervisedDispatcher
        - Subscription
        - IntervalOnlyWakeup
        - as_work_factory


## Escape hatches

You do not have to use *pgbg*'s supervision:
[`Service`][pgbg.Service], [`ElectedService`][pgbg.ElectedService], and [`NotifyDispatcher`][pgbg.NotifyDispatcher] are plain blocking loops that you can run on a thread you own, for example your main thread, under a process supervisor.

You can also implement a [`Loop`][pgbg.typing.Loop] of your own.

::: pgbg
    options:
      members:
        - Service
        - ElectedService
        - NotifyDispatcher
        - Supervisor


## Database bootstrap

::: pgbg
    options:
      members:
        - init_db
        - make_create_leases_table_sql


## Exceptions

::: pgbg.exceptions
    options:
      members:
        - SuppressedCrashError
