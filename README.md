<!-- --8<-- [start:header] -->
# *pgbg*

*PostgreSQL-orchestrated background threads for Python*
<!-- --8<-- [end:header] -->

[![Documentation at ReadTheDocs](https://img.shields.io/badge/Docs-Read%20Them!-black)](https://pgbg.hynek.me)
[![License: MIT](https://img.shields.io/badge/license-MIT-C06524)](https://github.com/hynek/pgbg/blob/main/LICENSE)
[![PyPI version](https://img.shields.io/pypi/v/pgbg)](https://pypi.org/project/pgbg/)
[![No AI slop inside.](https://img.shields.io/badge/no-slop-purple)](https://github.com/hynek/pgbg/blob/main/.github/AI_POLICY.md)


<!-- --8<-- [start:spiel] -->

*pgbg* builds on [*bgt*](https://bgt.hynek.me/) (a great way to reliably run background threads) and adds PostgreSQL integration:

- A [**`NOTIFY` dispatcher**](https://brandur.org/notifier) that takes **one** database connection per process and wakes up an arbitrary number of background threads.

- A table-based **leader election with automatic failover**.
  Make sure only one process runs work at a time.

Even with database backing, background tasks are **not** a job queue like Celery or RQ[^but].
Common use cases include:

- Periodic cleanup duties for [expired caches](https://psycache.hynek.me/en/latest/cleanup/#pgbg) or sessions.
- Maintenance of eventually consistent read models.
- Lightweight background tasks with the [transactional outbox pattern](https://en.wikipedia.org/wiki/Inbox_and_outbox_pattern#The_outbox_pattern).

[^but]: But it's useful for *implementing* worker queues _\~ominous foreshadowing\~_.

---

The core needs and supports only [Psycopg 3](https://www.psycopg.org/psycopg3/docs/) for database access.
*pgbg* comes with optional support for [SQLAlchemy](https://www.sqlalchemy.org) in the `pgbg.sqlalchemy` module that connects everything to a SQLAlchemy `Engine`.
If you use [*psycopg-pool*](https://www.psycopg.org/psycopg3/docs/api/pool.html), you don't need an adapter at all:
pass your pool's `connection` method wherever *pgbg* asks for a connection provider.

<!-- --8<-- [end:spiel] -->

Check out our [step-by-step tutorial](https://pgbg.hynek.me/stable/tutorial/) to get an instant feel for the features!


## Installation

The package is available on [PyPI under the `pgbg` name](https://pypi.org/project/pgbg/).
It comes with two optional extras:

- `sqlalchemy` (`uv pip install 'pgbg[sqlalchemy]'`) currently only adds a `SQLAlchemy>=2` dependency.
- `pool` (`uv pip install 'pgbg[pool]'`) installs [*psycopg-pool*](https://www.psycopg.org/psycopg3/docs/api/pool.html).


## Documentation

Full documentation lives at **<https://pgbg.hynek.me/>**.


<!-- --8<-- [start:credits] -->
## Credits

*pgbg* is written by [Hynek Schlawack](https://hynek.me/) and distributed under the terms of the [MIT license](https://choosealicense.com/licenses/mit/).

The development is kindly supported by my employer [Variomedia AG](https://www.variomedia.de/) and all my fabulous [GitHub Sponsors](https://github.com/sponsors/hynek).
<!-- --8<-- [end:credits] -->
