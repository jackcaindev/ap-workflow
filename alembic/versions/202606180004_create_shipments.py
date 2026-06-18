"""create shipments

Revision ID: 202606180004
Revises: 202606180003
Create Date: 2026-06-18 13:00:00.000000

"""
from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "202606180004"
down_revision: str | None = "202606180003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "shipments",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("load_number", sa.String(), nullable=False),
        sa.Column("carrier_name", sa.String(), nullable=True),
        sa.Column("has_invoice", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("has_rate_con", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("has_bol", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("has_pod", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("invoice_id", sa.Integer(), nullable=True),
        sa.Column("rate_con_id", sa.Integer(), nullable=True),
        sa.Column("bol_id", sa.Integer(), nullable=True),
        sa.Column("pod_id", sa.Integer(), nullable=True),
        sa.Column("reconciliation_status", sa.String(), server_default="pending", nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["invoice_id"], ["documents.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["rate_con_id"], ["documents.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["bol_id"], ["documents.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["pod_id"], ["documents.id"], ondelete="SET NULL"),
    )
    op.create_index("ix_shipments_load_number", "shipments", ["load_number"], unique=True)
    op.add_column(
        "documents",
        sa.Column("shipment_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_documents_shipment_id_shipments",
        "documents",
        "shipments",
        ["shipment_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_documents_shipment_id", "documents", ["shipment_id"])


def downgrade() -> None:
    op.drop_index("ix_documents_shipment_id", table_name="documents")
    op.drop_constraint("fk_documents_shipment_id_shipments", "documents", type_="foreignkey")
    op.drop_column("documents", "shipment_id")
    op.drop_index("ix_shipments_load_number", table_name="shipments")
    op.drop_table("shipments")
