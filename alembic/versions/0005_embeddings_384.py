"""Embedding columns for intfloat/multilingual-e5-small: vector(768) -> vector(384).

The model comparison in docs/embedding-model-benchmark.md found no model
better than another beyond noise, so the smallest and fastest was chosen.

Vectors of one dimensionality cannot be turned into another: the rows already
stored are deleted, with the models that produced them. They were trial
vectors of a single user and are recomputed from the messages by
`python -m scripts.embed`. Messages and chunks are not touched.

Revision ID: 0005
Revises: 0004
Create Date: 2026-10-09
"""
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def _resize(dimensions: int) -> None:
    # Derived data only: embeddings are recomputed from the stored texts.
    op.execute("DELETE FROM chunk_embeddings")
    op.execute("DELETE FROM message_embeddings")
    op.execute("DELETE FROM embedding_models")
    for table in ("message_embeddings", "chunk_embeddings"):
        op.execute(
            f"ALTER TABLE {table} ALTER COLUMN embedding TYPE vector({dimensions})"
        )


def upgrade() -> None:
    _resize(384)


def downgrade() -> None:
    _resize(768)
