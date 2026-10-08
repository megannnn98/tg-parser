"""Embedding models and the vectors of messages and chunks.

The dimensionality is that of intfloat/multilingual-e5-base, chosen by the
benchmark in docs/embedding-chunking-research.md. No vector index is created:
search is always within one user's vectors, where an exact scan is faster
than an approximate index and returns the true nearest neighbours.

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-08
"""
import sqlalchemy as sa
from pgvector.sqlalchemy import Vector

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

DIMENSIONS = 768


def _created_at() -> sa.Column:
    return sa.Column(
        "created_at",
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    )


def upgrade() -> None:
    op.create_table(
        "embedding_models",
        sa.Column("id", sa.BigInteger, sa.Identity(), nullable=False),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("revision", sa.Text, nullable=False),
        sa.Column("dimensions", sa.Integer, nullable=False),
        sa.Column("pooling", sa.Text, nullable=False),
        sa.Column("normalized", sa.Boolean, nullable=False),
        sa.Column("max_tokens", sa.Integer, nullable=False),
        sa.Column("input_prefix", sa.Text, nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("id", name="pk_embedding_models"),
        sa.UniqueConstraint(
            "name",
            "revision",
            "pooling",
            "normalized",
            "input_prefix",
            name="uq_embedding_models_identity",
        ),
    )

    op.create_table(
        "message_embeddings",
        sa.Column("message_id", sa.BigInteger, nullable=False),
        sa.Column("model_id", sa.BigInteger, nullable=False),
        sa.Column("embedding", Vector(DIMENSIONS), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint(
            "message_id", "model_id", name="pk_message_embeddings"
        ),
        sa.ForeignKeyConstraint(
            ["message_id"],
            ["messages.id"],
            name="fk_message_embeddings_message_id_messages",
        ),
        sa.ForeignKeyConstraint(
            ["model_id"],
            ["embedding_models.id"],
            name="fk_message_embeddings_model_id_embedding_models",
        ),
    )

    op.create_table(
        "chunk_embeddings",
        sa.Column("chunk_id", sa.BigInteger, nullable=False),
        sa.Column("model_id", sa.BigInteger, nullable=False),
        sa.Column("embedding", Vector(DIMENSIONS), nullable=False),
        _created_at(),
        sa.PrimaryKeyConstraint("chunk_id", "model_id", name="pk_chunk_embeddings"),
        sa.ForeignKeyConstraint(
            ["chunk_id"],
            ["chunks.id"],
            name="fk_chunk_embeddings_chunk_id_chunks",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["model_id"],
            ["embedding_models.id"],
            name="fk_chunk_embeddings_model_id_embedding_models",
        ),
    )


def downgrade() -> None:
    op.drop_table("chunk_embeddings")
    op.drop_table("message_embeddings")
    op.drop_table("embedding_models")
