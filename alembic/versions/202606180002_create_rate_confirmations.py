"""create rate confirmations

Revision ID: 202606180002
Revises: 202606180001
Create Date: 2026-06-18 12:00:00.000000

"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "202606180002"
down_revision: str | None = "202606180001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "rate_confirmations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("load_number", sa.String(), nullable=False),
        sa.Column("carrier_name", sa.String(), nullable=False),
        sa.Column("origin", sa.String(), nullable=False),
        sa.Column("destination", sa.String(), nullable=False),
        sa.Column("agreed_rate", sa.Float(), nullable=False),
        sa.Column("currency", sa.String(), server_default="USD", nullable=False),
        sa.Column("shipment_date", sa.Date(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_rate_confirmations_load_number",
        "rate_confirmations",
        ["load_number"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_rate_confirmations_load_number", table_name="rate_confirmations")
    op.drop_table("rate_confirmations")
