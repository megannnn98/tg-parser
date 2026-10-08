"""Position analysis cache.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-08
"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "analysis_cache",
        sa.Column("namespace", sa.Text, nullable=False),
        sa.Column("key", sa.Text, nullable=False),
        sa.Column("value", postgresql.JSONB, nullable=False),
        sa.PrimaryKeyConstraint("namespace", "key", name="pk_analysis_cache"),
    )


def downgrade() -> None:
    op.drop_table("analysis_cache")
