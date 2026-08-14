"""
The ``python -m pgbg`` maintenance CLI.
"""

import argparse
import sys

from collections.abc import Sequence

import psycopg

from ._tables import init_db, make_create_leases_table_sql


def _dump_init_db_sql(name: str, schema: str | None = None) -> None:
    """
    Print the leases-table DDL to stdout.
    """
    statement = make_create_leases_table_sql(name=name, schema=schema)
    print(f"{statement.as_string().rstrip()};")


def _do_init_db(
    dsn: str | None, *, name: str, schema: str | None = None
) -> int:
    """
    Create the lease table, or print its SQL if *dsn* is None.
    """
    try:
        if dsn is None:
            _dump_init_db_sql(name, schema)
            return 0

        with psycopg.connect(dsn, autocommit=True) as conn:
            init_db(conn, name=name, schema=schema)
    except (ValueError, psycopg.Error) as e:
        print(f"pgbg: init-db failed: {e}", file=sys.stderr)
        return 1

    print("pgbg: initialized the lease table.")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    """
    Build the argument parser for all maintenance commands.
    """
    parser = argparse.ArgumentParser(
        prog="python -m pgbg",
        description="Maintenance commands for pgbg.",
    )
    subparsers = parser.add_subparsers(required=True)

    init_db_parser = subparsers.add_parser(
        "init-db",
        help="Create the pgbg lease table.",
        description="Create the lease table in the database identified "
        "by DSN, or print the SQL to stdout if DSN is omitted.",
    )
    init_db_parser.add_argument(
        "--name",
        default="pgbg_leases",
        help="Name of the lease table to create.",
    )
    init_db_parser.add_argument(
        "--schema",
        help="PostgreSQL schema in which to create the lease table.",
    )
    init_db_parser.add_argument(
        "dsn",
        nargs="?",
        metavar="DSN",
        help="A libpq connection string, e.g. postgresql://user@host/db. "
        "If omitted, print the SQL to stdout.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """
    Parse *argv* and run the selected command.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    return _do_init_db(args.dsn, name=args.name, schema=args.schema)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
