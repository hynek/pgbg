# *pgbg*: PostgreSQL-orchestrated background threads for Python

[![Documentation at ReadTheDocs](https://img.shields.io/badge/Docs-Read%20Them!-black)](https://pgbg.hynek.me)
[![License: MIT](https://img.shields.io/badge/license-MIT-C06524)](https://github.com/hynek/pgbg/blob/main/LICENSE)
[![PyPI version](https://img.shields.io/pypi/v/pgbg)](https://pypi.org/project/pgbg/)
[![No AI slop inside.](https://img.shields.io/badge/no-slop-purple)](https://github.com/hynek/pgbg/blob/main/.github/AI_POLICY.md)


<!-- --8<-- [start:spiel] -->
POV: you want a framework-agnostic way to reliably run a plain[^non-async] function or method in the background, repeatedly, but not all the time.

[^non-async]: No `async`.

*pgbg* comes to the rescue with:

- A [**`NOTIFY` dispatcher**](https://brandur.org/notifier) that takes **one** database connection per process and wakes up an arbitrary number of subscribers.

- A **supervisor** that runs your code as a *service*: in a loop, in a background thread.
  If your code crashes, the supervisor restarts the loop.
  Write [crash-only](https://pgbg.hynek.me/stable/glossary/#crash-only) code, *pgbg* takes care of the rest.

    Your services can wake up on `NOTIFY`s, fixed time intervals, or both.

- PostgreSQL-based **leader election with automatic failover**.
  Make sure only one process runs work at a time.

- Framework and platform independence.

Background tasks are **not** a traditional worker queue[^but].
Common use cases include:

- Periodic cleanup duties for [expired caches](https://psycache.hynek.me/en/latest/cleanup/#pgbg) or sessions.
- Maintenance of eventually consistent read models.
- Lightweight transactional background tasks with the [outbox pattern](https://en.wikipedia.org/wiki/Inbox_and_outbox_pattern).

[^but]: But it's useful for *implementing* worker queues _\~ominous foreshadowing\~_.

---

The core needs and supports only [Psycopg 3](https://www.psycopg.org/psycopg3/docs/) for database access.
*pgbg* comes with optional support for [SQLAlchemy](https://www.sqlalchemy.org) in the `pgbg.sqlalchemy` module that connects everything to a SQLAlchemy `Engine`.

<!-- --8<-- [end:spiel] -->

Check out our [step-by-step tutorial](https://pgbg.hynek.me/stable/tutorial/) to get an instant feel for the features!


## Installation

The package is available on [PyPI under the `pgbg` name](https://pypi.org/project/pgbg/).
It comes with an optional extra `sqlalchemy` (`uv pip install 'pgbg[sqlalchemy]'`), but that currently only adds a `SQLAlchemy>2` lower pin.


## Documentation

Full documentation lives at **<https://pgbg.hynek.me/>**.


<!-- --8<-- [start:credits] -->
## Credits

*pgbg* is written by [Hynek Schlawack](https://hynek.me/) and distributed under the terms of the [MIT license](https://choosealicense.com/licenses/mit/).

The development is kindly supported by my employer [Variomedia AG](https://www.variomedia.de/) and all my fabulous [GitHub Sponsors](https://github.com/sponsors/hynek).
<!-- --8<-- [end:credits] -->
