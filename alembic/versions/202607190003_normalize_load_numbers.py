"""normalize load-number identity

Revision ID: 202607190003
Revises: 202607190002
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa


revision: str = "202607190003"
down_revision: str | None = "202607190002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NORMALIZED_LOAD_NUMBER_SQL = (
    "upper(regexp_replace(load_number, "
    "'^[[:space:]]+|[[:space:]]+$', '', 'g'))"
)


def _reject_invalid_load_numbers(table_name: str) -> None:
    bind = op.get_bind()
    empty_count = bind.scalar(
        sa.text(
            f"SELECT count(*) FROM {table_name} "
            f"WHERE {NORMALIZED_LOAD_NUMBER_SQL} = ''"
        )
    )
    if empty_count:
        raise RuntimeError(
            f"Cannot normalize {table_name}.load_number: {empty_count} empty value(s)"
        )

    collisions = bind.scalar(
        sa.text(
            "SELECT string_agg(normalized_load_number, ', ' ORDER BY normalized_load_number) "
            "FROM ("
            f"SELECT {NORMALIZED_LOAD_NUMBER_SQL} AS normalized_load_number "
            f"FROM {table_name} GROUP BY {NORMALIZED_LOAD_NUMBER_SQL} HAVING count(*) > 1"
            ") AS duplicate_load_numbers"
        )
    )
    if collisions:
        raise RuntimeError(
            f"Cannot normalize {table_name}.load_number; collisions: {collisions}"
        )


def upgrade() -> None:
    for table_name in ("shipments", "rate_confirmations"):
        _reject_invalid_load_numbers(table_name)
        op.execute(
            sa.text(
                f"UPDATE {table_name} "
                f"SET load_number = {NORMALIZED_LOAD_NUMBER_SQL} "
                f"WHERE load_number <> {NORMALIZED_LOAD_NUMBER_SQL}"
            )
        )

    op.create_check_constraint(
        "ck_shipments_load_number_normalized",
        "shipments",
        f"load_number = {NORMALIZED_LOAD_NUMBER_SQL} AND load_number <> ''",
    )
    op.create_check_constraint(
        "ck_rate_confirmations_load_number_normalized",
        "rate_confirmations",
        f"load_number = {NORMALIZED_LOAD_NUMBER_SQL} AND load_number <> ''",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_rate_confirmations_load_number_normalized",
        "rate_confirmations",
        type_="check",
    )
    op.drop_constraint(
        "ck_shipments_load_number_normalized",
        "shipments",
        type_="check",
    )
