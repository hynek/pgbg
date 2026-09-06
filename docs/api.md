# Core API

For the non-database building blocks (like supervised threads) that *pgbg* builds on, check out [its API documentation](https://bgt.hynek.me/stable/api/).


## Batteries-included

Reach for these first:
every handle composes the necessary building blocks, starts its loop under a supervisor and offers `stop()`, `is_running`, and a context manager.

If you're using SQLAlchemy, have also a look at [our helpers](api-sqlalchemy.md) with even more batteries included.

::: pgbg
    options:
      members:
        - SupervisedElectedService
        - SupervisedDispatcher
        - Subscription


## Escape hatches

You do not have to use *bgt*'s supervision:
[`ElectedService`][pgbg.ElectedService] and [`NotifyDispatcher`][pgbg.NotifyDispatcher] are plain blocking loops that you can run on a thread you own, for example your main thread, under a process supervisor.

[`ElectedService`][pgbg.ElectedService] implements [`bgt.typing.Loop`][bgt.typing.Loop], so you can pass it directly to [`bgt.Supervisor.start()`][bgt.Supervisor.start].
[`SupervisedDispatcher`][pgbg.SupervisedDispatcher] uses an internal adapter to connect [`NotifyDispatcher`][pgbg.NotifyDispatcher] to that protocol.

::: pgbg
    options:
      members:
        - ElectedService
        - NotifyDispatcher


## Database bootstrap

::: pgbg
    options:
      members:
        - init_db
        - make_create_leases_table_sql
