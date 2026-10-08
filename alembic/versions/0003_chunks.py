"""Chunk sets, chunks and their messages.

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-08
"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def _created_at() -> sa.Column:
    return sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    )


def upgrade() -> None:
    op.create_table(
        "chunk_sets",
        sa.Column("id", sa.BigInteger, sa.Identity(), nullable=False),
        sa.Column("strategy", sa.Text, nullable=False),
        sa.Column("strategy_version", sa.Integer, nullable=False),
        sa.Column("parameters", postgresql.JSONB, nullable=False),
        sa.Column("parameters_hash", sa.Text, nullable=False),
        sa.Column("tokenizer", sa.Text, nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id", name="pk_chunk_sets"),
        sa.UniqueConstraint(
            "strategy",
            "strategy_version",
            "parameters_hash",
            "tokenizer",
            name="uq_chunk_sets_identity",
        ),
    )

    op.create_table(
        "chunks",
        sa.Column("id", sa.BigInteger, sa.Identity(), nullable=False),
        sa.Column("chunk_set_id", sa.BigInteger, nullable=False),
        sa.Column("user_id", sa.BigInteger, nullable=False),
        sa.Column("channel_id", sa.BigInteger),
        sa.Column("date_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("date_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("text", sa.Text, nullable=False),
        sa.Column("token_count", sa.Integer, nullable=False),
        sa.Column("message_count", sa.Integer, nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id", name="pk_chunks"),
        sa.ForeignKeyConstraint(
            ["chunk_set_id"],
            ["chunk_sets.id"],
            name="fk_chunks_chunk_set_id_chunk_sets",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name="fk_chunks_user_id_users"
        ),
        sa.ForeignKeyConstraint(
            ["channel_id"], ["channels.id"], name="fk_chunks_channel_id_channels"
        ),
    )
    op.create_index(
        "ix_chunks_chunk_set_id_user_id", "chunks", ["chunk_set_id", "user_id"]
    )

    op.create_table(
        "chunk_messages",
        sa.Column("chunk_id", sa.BigInteger, nullable=False),
        sa.Column("message_id", sa.BigInteger, nullable=False),
        sa.Column("position", sa.Integer, nullable=False),
        sa.PrimaryKeyConstraint("chunk_id", "message_id", name="pk_chunk_messages"),
        sa.ForeignKeyConstraint(
            ["chunk_id"],
            ["chunks.id"],
            name="fk_chunk_messages_chunk_id_chunks",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["message_id"],
            ["messages.id"],
            name="fk_chunk_messages_message_id_messages",
        ),
    )
    op.create_index("ix_chunk_messages_message_id", "chunk_messages", ["message_id"])


def downgrade() -> None:
    op.drop_table("chunk_messages")
    op.drop_table("chunks")
    op.drop_table("chunk_sets")
