from __future__ import annotations

import os
from contextlib import contextmanager
from uuid import uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from psycopg import sql
from sqlalchemy.engine import make_url

from tests.conftest import TEST_DATABASE_URL


pytestmark = pytest.mark.integration


def _migration_url(database_name: str) -> str:
    return make_url(TEST_DATABASE_URL).set(database=database_name).render_as_string(
        hide_password=False
    )


def _psycopg_url(database_name: str) -> str:
    return _migration_url(database_name).replace(
        "postgresql+asyncpg://", "postgresql://", 1
    )


@contextmanager
def _database_at_previous_revision():
    database_name = f"load_number_{uuid4().hex}"
    admin_url = _psycopg_url("postgres")
    previous_database_url = os.environ.get("DATABASE_URL")
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name)))

    try:
        os.environ["DATABASE_URL"] = _migration_url(database_name)
        alembic_config = Config("alembic.ini")
        command.upgrade(alembic_config, "202607190002")
        yield database_name, alembic_config
    finally:
        if previous_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_database_url
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(database_name)
                )
            )


def test_load_number_migration_normalizes_existing_values_and_enforces_contract():
    with _database_at_previous_revision() as (database_name, alembic_config):
        with psycopg.connect(_psycopg_url(database_name)) as connection:
            connection.execute(
                """
                INSERT INTO shipments (id, load_number, reconciliation_status)
                VALUES (%s, E' \tshipment-1\t ', 'pending')
                """,
                (uuid4(),),
            )
            connection.execute(
                """
                INSERT INTO rate_confirmations
                    (id, load_number, carrier_name, origin, destination,
                     agreed_rate, currency, shipment_date)
                VALUES (%s, '  rate-1  ', 'ACME', 'A', 'B', 1, 'USD', '2026-07-19')
                """,
                (uuid4(),),
            )
            connection.commit()

        command.upgrade(alembic_config, "head")

        with psycopg.connect(_psycopg_url(database_name)) as connection:
            assert connection.execute(
                "SELECT load_number FROM shipments"
            ).fetchone()[0] == "SHIPMENT-1"
            assert connection.execute(
                "SELECT load_number FROM rate_confirmations"
            ).fetchone()[0] == "RATE-1"
            with pytest.raises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    INSERT INTO shipments (id, load_number, reconciliation_status)
                    VALUES (%s, 'not-normalized', 'pending')
                    """,
                    (uuid4(),),
                )


def test_load_number_migration_rejects_normalization_collisions():
    with _database_at_previous_revision() as (database_name, alembic_config):
        with psycopg.connect(_psycopg_url(database_name)) as connection:
            connection.execute(
                """
                INSERT INTO shipments (id, load_number, reconciliation_status)
                VALUES (%s, 'collision-1', 'pending'),
                       (%s, ' COLLISION-1 ', 'pending')
                """,
                (uuid4(), uuid4()),
            )
            connection.commit()

        with pytest.raises(RuntimeError, match="collisions: COLLISION-1"):
            command.upgrade(alembic_config, "head")
