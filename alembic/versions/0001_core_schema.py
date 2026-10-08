"""Core schema: users, channels, messages, legacy archive, pgvector extension.

Revision ID: 0001
Revises:
Create Date: 2026-10-08
"""
import sqlalchemy as sa

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def _timestamp(name: str) -> sa.Column:
    return sa.Column(
        name, sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    )


def upgrade() -> None:
    # Vector columns arrive with the embedding tables, once the benchmark has
    # fixed the model and so the dimensionality.
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "users",
        sa.Column("id", sa.BigInteger, sa.Identity(), nullable=False),
        sa.Column("tg_id", sa.BigInteger, nullable=False),
        sa.Column("username", sa.Text),
        sa.Column("first_name", sa.Text),
        sa.Column("last_name", sa.Text),
        sa.Column("profile_collected_at", sa.DateTime(timezone=True)),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.PrimaryKeyConstraint("id", name="pk_users"),
        sa.UniqueConstraint("tg_id", name="uq_users_tg_id"),
    )

    op.create_table(
        "channels",
        sa.Column("id", sa.BigInteger, sa.Identity(), nullable=False),
        sa.Column("username", sa.Text, nullable=False),
        sa.Column("telegram_chat_id", sa.BigInteger),
        sa.Column("linked_chat_id", sa.BigInteger),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        sa.PrimaryKeyConstraint("id", name="pk_channels"),
        sa.UniqueConstraint("username", name="uq_channels_username"),
    )

    op.create_table(
        "messages",
        sa.Column("id", sa.BigInteger, sa.Identity(), nullable=False),
        sa.Column("tg_message_id", sa.BigInteger, nullable=False),
        sa.Column("user_id", sa.BigInteger, nullable=False),
        sa.Column("channel_id", sa.BigInteger, nullable=False),
        sa.Column("text", sa.Text, nullable=False),
        sa.Column("date", sa.DateTime(timezone=True), nullable=False),
        _timestamp("created_at"),
        sa.PrimaryKeyConstraint("id", name="pk_messages"),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name="fk_messages_user_id_users"
        ),
        sa.ForeignKeyConstraint(
            ["channel_id"], ["channels.id"], name="fk_messages_channel_id_channels"
        ),
        sa.UniqueConstraint(
            "channel_id", "tg_message_id", name="uq_messages_channel_id_tg_message_id"
        ),
    )
    op.create_index("ix_messages_user_id_date", "messages", ["user_id", "date"])
    op.create_index(
        "ix_messages_user_id_channel_id", "messages", ["user_id", "channel_id"]
    )

    op.create_table(
        "legacy_messages",
        sa.Column("id", sa.BigInteger, sa.Identity(), nullable=False),
        sa.Column("source", sa.Text, nullable=False),
        sa.Column("source_row_id", sa.BigInteger, nullable=False),
        sa.Column("user_tg_id", sa.BigInteger),
        sa.Column("channel", sa.Text),
        sa.Column("text", sa.Text),
        sa.Column("date_raw", sa.Text),
        sa.PrimaryKeyConstraint("id", name="pk_legacy_messages"),
        sa.UniqueConstraint(
            "source", "source_row_id", name="uq_legacy_messages_source_source_row_id"
        ),
    )


def downgrade() -> None:
    op.drop_table("legacy_messages")
    op.drop_table("messages")
    op.drop_table("channels")
    op.drop_table("users")
    op.execute("DROP EXTENSION IF EXISTS vector")
