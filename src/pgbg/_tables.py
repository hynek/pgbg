"""
DDL for the lease table that leader election needs.

`"pgbg_leases"` is hard-coded for nicer APIs. If it ever changes, it's an easy
global search and replace.
"""

from typing import Any

import psycopg

from psycopg import sql


def _leases_table(name: str, schema: str | None) -> sql.Identifier:
    """
    Build the (optionally schema-qualified) table identifier.
    """
    if not name:
        msg = "name must not be empty"
        raise ValueError(msg)

    if "." in name:
        msg = "name must not contain dots; pass a schema instead"
        raise ValueError(msg)

    if schema == "":
        msg = "schema must not be empty"
        raise ValueError(msg)

    if schema is None:
        return sql.Identifier(name)

    return sql.Identifier(schema, name)


def make_create_leases_table_sql(
    name: str = "pgbg_leases", schema: str | None = None
) -> sql.Composed:
    """
    Return the CREATE TABLE statement for a lease table.

    The table is UNLOGGED because a lease is worthless after a server restart
    anyway.
    """
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
        _leases_table(name, schema),
        sql.Identifier(f"{name}_name_not_empty"),
        sql.Identifier(f"{name}_worker_id_not_empty"),
    )


def init_db(
    conn: psycopg.Connection[Any],
    *,
    name: str = "pgbg_leases",
    schema: str | None = None,
) -> None:
    """
    Create the lease table if it doesn't exist.

    Args:
        conn:
            A psycopg connection.

        name:
            The name of the lease table.

        schema:
            The PostgreSQL schema in which to create the table. If
            `None`, the table is created in the connection's current
            default schema.
    """
    conn.execute(make_create_leases_table_sql(name=name, schema=schema))
