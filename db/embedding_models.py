"""Embedding tables. Kept apart from db/models.py: pgvector's type needs numpy,
which the web application must be able to start without."""
from __future__ import annotations

from datetime import datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Identity,
    Integer,
    PrimaryKeyConstraint,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from db.models import Base

# The dimensionality of the vector columns, fixed by the chosen model
# (intfloat/multilingual-e5-base). A model of another size needs a migration
# with tables of its own.
EMBEDDING_DIMENSIONS = 768


class EmbeddingModel(Base):
    """What produced a vector: two rows that differ in any field are not comparable."""

    __tablename__ = "embedding_models"
    __table_args__ = (
        UniqueConstraint(
            "name",
            "revision",
            "pooling",
            "normalized",
            "input_prefix",
            name="uq_embedding_models_identity",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    # A commit of the model repository, never empty: a nullable column would
    # let the unique constraint accept the same model twice.
    revision: Mapped[str] = mapped_column(Text)
    dimensions: Mapped[int] = mapped_column(Integer)
    pooling: Mapped[str] = mapped_column(Text)
    normalized: Mapped[bool] = mapped_column(Boolean)
    max_tokens: Mapped[int] = mapped_column(Integer)
    # What is put before a stored text, "passage: " for E5.
    input_prefix: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class MessageEmbedding(Base):
    __tablename__ = "message_embeddings"
    __table_args__ = (PrimaryKeyConstraint("message_id", "model_id"),)

    message_id: Mapped[int] = mapped_column(ForeignKey("messages.id"))
    model_id: Mapped[int] = mapped_column(ForeignKey("embedding_models.id"))
    embedding: Mapped[list[float]] = mapped_column(Vector(EMBEDDING_DIMENSIONS))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ChunkEmbedding(Base):
    __tablename__ = "chunk_embeddings"
    __table_args__ = (PrimaryKeyConstraint("chunk_id", "model_id"),)

    # A rebuilt chunk is a new row; the vector of the old one goes with it.
    chunk_id: Mapped[int] = mapped_column(ForeignKey("chunks.id", ondelete="CASCADE"))
    model_id: Mapped[int] = mapped_column(ForeignKey("embedding_models.id"))
    embedding: Mapped[list[float]] = mapped_column(Vector(EMBEDDING_DIMENSIONS))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
