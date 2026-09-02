"""
DDL for the lease table that leader election needs.

`"pgbg_leases"` is hard-coded for nicer APIs. If it ever changes, it's an easy
global search and replace.
"""

from typing import Any

import psycopg

from psycopg import sql


def leases_identifier(name: str) -> sql.Identifier:
    """
    Build the quoted identifier for the lease-table *name*.

    *name* is optionally schema-qualified with a dot (for example,
    `"public.pgbg_leases"`).
    """
    if not name or not all(name.split(".")):
        msg = "the lease-table name must not be empty or have empty parts"
        raise ValueError(msg)

    return sql.Identifier(*name.split("."))


def make_create_leases_table_sql(name: str = "pgbg_leases") -> sql.Composed:
    """
    Return the `CREATE TABLE` statement for a lease table called *name*.

    *name* is optionally schema-qualified with a dot (for example,
    `"public.pgbg_leases"`).
    """
    table = name.rsplit(".", 1)[-1]

    # UNLOGGED is fine b/c a lease is worthless after a server restart anyway.
    return sql.SQL("""\
CREATE UNLOGGED TABLE IF NOT EXISTS {} (
    elected_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    worker_id text NOT NULL,
    name text NOT NULL PRIMARY KEY,
    CONSTRAINT {} CHECK (name != ''),
    CONSTRAINT {} CHECK (worker_id != '')
)
""").format(
        leases_identifier(name),
        sql.Identifier(f"{table}_name_not_empty"),
        sql.Identifier(f"{table}_worker_id_not_empty"),
    )


def init_db(conn: psycopg.Connection[Any], name: str = "pgbg_leases") -> None:
    """
    Create the lease table called *name* if it doesn't exist.

    Args:
        conn:
            A psycopg connection.

        name:
            The name of the lease table, optionally schema-qualified with
            a dot (for example, `"public.pgbg_leases"`).
    """
    conn.execute(make_create_leases_table_sql(name))
