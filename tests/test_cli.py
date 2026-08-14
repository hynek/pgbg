"""
Tests for the `python -m pgbg` CLI.
"""

import psycopg
import pytest

from pgbg.__main__ import main


@pytest.fixture(name="fresh_leases")
def _fresh_leases(pgbg_dsn):
    """
    Make sure the default lease table does not exist yet.
    """
    with psycopg.connect(pgbg_dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS pgbg_leases")


class TestInitDB:
    def test_creates_table(self, pgbg_dsn, fresh_leases):
        """
        `init-db` creates the pgbg_lease table and is idempotent.
        """
        assert 0 == main(["init-db", pgbg_dsn])

        with psycopg.connect(pgbg_dsn, autocommit=True) as conn:
            regclass = conn.execute(
                "SELECT to_regclass('pgbg_leases')"
            ).fetchone()[0]

        assert "pgbg_leases" == regclass

        # Running it again against an existing table is a no-op.
        assert 0 == main(["init-db", pgbg_dsn])

    def test_creates_named_table(self, pgbg_dsn):
        """
        `init-db --name` creates a table under that name instead.
        """
        with psycopg.connect(pgbg_dsn, autocommit=True) as conn:
            conn.execute("DROP TABLE IF EXISTS cli_leases")

        assert 0 == main(["init-db", "--name", "cli_leases", pgbg_dsn])

        with psycopg.connect(pgbg_dsn, autocommit=True) as conn:
            regclass = conn.execute(
                "SELECT to_regclass('cli_leases')"
            ).fetchone()[0]

        assert "cli_leases" == regclass

    def test_creates_table_in_schema(self, pgbg_dsn):
        """
        `init-db --schema` creates the lease table in that schema.
        """
        schema = "pgbg_cli_test"

        with psycopg.connect(pgbg_dsn, autocommit=True) as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            conn.execute(f"CREATE SCHEMA {schema}")

        assert 0 == main(["init-db", "--schema", schema, pgbg_dsn])

        with psycopg.connect(pgbg_dsn, autocommit=True) as conn:
            regclass = conn.execute(
                "SELECT to_regclass('pgbg_cli_test.pgbg_leases')"
            ).fetchone()[0]

        assert "pgbg_cli_test.pgbg_leases" == regclass

    def test_without_dsn_prints_sql(self, capsys):
        """
        `init-db` prints the SQL to create the table if no DSN is passed.
        """
        assert 0 == main(["init-db"])

        out = capsys.readouterr().out

        assert 'CREATE UNLOGGED TABLE IF NOT EXISTS "pgbg_leases" (' in out
        assert 'CONSTRAINT "pgbg_leases_name_not_empty"' in out
        assert out.rstrip().endswith(");")

    def test_without_dsn_prints_named_schema_sql(self, capsys):
        """
        `init-db --name --schema` prints qualified SQL if no DSN is
        passed, with the constraint names following the table name.
        """
        assert 0 == main(["init-db", "--name", "leases", "--schema", "app"])

        out = capsys.readouterr().out

        assert 'CREATE UNLOGGED TABLE IF NOT EXISTS "app"."leases" (' in out
        assert 'CONSTRAINT "leases_name_not_empty"' in out
        assert 'CONSTRAINT "leases_worker_id_not_empty"' in out

    def test_reports_connection_failure(self, capsys):
        """
        A connection error is reported on stderr and exits non-zero.
        """
        assert 1 == main(["init-db", "postgresql://nope@127.0.0.1:1/nope"])

        assert "init-db failed" in capsys.readouterr().err

    def test_reports_empty_name(self, pgbg_dsn, capsys):
        """
        An empty table name is reported on stderr and exits non-zero.
        """
        assert 1 == main(["init-db", "--name", "", pgbg_dsn])

        assert "name must not be empty" in capsys.readouterr().err

    def test_reports_dotted_name(self, pgbg_dsn, capsys):
        """
        A dotted table name is rejected in favor of --schema.
        """
        assert 1 == main(["init-db", "--name", "public.leases", pgbg_dsn])

        assert "must not contain dots" in capsys.readouterr().err

    def test_reports_empty_schema(self, pgbg_dsn, capsys):
        """
        An empty schema name is reported on stderr and exits non-zero.
        """
        assert 1 == main(["init-db", "--schema", "", pgbg_dsn])

        assert "schema must not be empty" in capsys.readouterr().err


def test_requires_a_command():
    """
    Invoking without a subcommand is an argparse usage error.
    """
    with pytest.raises(SystemExit):
        main([])
