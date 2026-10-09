"""PostgreSQL schema. Alembic owns the DDL; these models are its source."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Stable constraint names, so migrations can refer to them.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    # The stable identity: a username can change or disappear.
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    username: Mapped[str | None] = mapped_column(Text)
    first_name: Mapped[str | None] = mapped_column(Text)
    last_name: Mapped[str | None] = mapped_column(Text)
    # Set by user-comments: the user's whole comment history was requested, so
    # they have a profile. Unset for authors only seen while collecting channels.
    profile_collected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Channel(Base):
    __tablename__ = "channels"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    username: Mapped[str] = mapped_column(Text, unique=True)
    telegram_chat_id: Mapped[int | None] = mapped_column(BigInteger)
    # Comments live in the channel's discussion chat, not in the channel.
    linked_chat_id: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Message(Base):
    """One Telegram comment, never rewritten: chunks and embeddings derive from it."""

    __tablename__ = "messages"
    __table_args__ = (
        # tg_message_id is the id inside the channel's discussion chat. A channel
        # has one discussion chat, so the pair identifies the comment.
        UniqueConstraint("channel_id", "tg_message_id"),
        Index("ix_messages_user_id_date", "user_id", "date"),
        Index("ix_messages_user_id_channel_id", "user_id", "channel_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    tg_message_id: Mapped[int] = mapped_column(BigInteger)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id"))
    text: Mapped[str] = mapped_column(Text)
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class LegacyMessage(Base):
    """Archive of app.db rows that carry no Telegram message id.

    They cannot be identified as comments, so nothing in the application reads
    them; a fresh collect refills `messages` with proper ids.
    """

    __tablename__ = "legacy_messages"
    __table_args__ = (UniqueConstraint("source", "source_row_id"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    source: Mapped[str] = mapped_column(Text)
    source_row_id: Mapped[int] = mapped_column(BigInteger)
    user_tg_id: Mapped[int | None] = mapped_column(BigInteger)
    channel: Mapped[str | None] = mapped_column(Text)
    text: Mapped[str | None] = mapped_column(Text)
    # As stored by SQLite: a naive string in the collecting process's local time.
    date_raw: Mapped[str | None] = mapped_column(Text)


class AnalysisCache(Base):
    """Checkpoints and cached inference of the position analysis.

    Derived data, kept apart from the comments: it can be dropped and recomputed.
    """

    __tablename__ = "analysis_cache"
    __table_args__ = (PrimaryKeyConstraint("namespace", "key"),)

    namespace: Mapped[str] = mapped_column(Text)
    key: Mapped[str] = mapped_column(Text)
    value: Mapped[object] = mapped_column(JSONB)


class ChunkSet(Base):
    """One way of chunking: a strategy with its parameters.

    Several sets live side by side, so experiments can be compared. A chunk
    is reproducible from its set and its messages.
    """

    __tablename__ = "chunk_sets"
    __table_args__ = (
        # Named by hand: the generated name exceeds PostgreSQL's 63 characters.
        UniqueConstraint(
            "strategy",
            "strategy_version",
            "parameters_hash",
            "tokenizer",
            name="uq_chunk_sets_identity",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    strategy: Mapped[str] = mapped_column(Text)
    strategy_version: Mapped[int] = mapped_column(Integer)
    parameters: Mapped[dict] = mapped_column(JSONB)
    # sha256 of the canonical JSON of `parameters`: JSONB cannot be unique by itself.
    parameters_hash: Mapped[str] = mapped_column(Text)
    # Token budgets are counted with this tokenizer.
    tokenizer: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Chunk(Base):
    """Derived from messages; the messages themselves are never replaced by it."""

    __tablename__ = "chunks"
    __table_args__ = (Index("ix_chunks_chunk_set_id_user_id", "chunk_set_id", "user_id"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    chunk_set_id: Mapped[int] = mapped_column(
        ForeignKey("chunk_sets.id", ondelete="CASCADE")
    )
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    # Unset when the chunk spans several channels.
    channel_id: Mapped[int | None] = mapped_column(ForeignKey("channels.id"))
    date_from: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    date_to: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    text: Mapped[str] = mapped_column(Text)
    token_count: Mapped[int] = mapped_column(Integer)
    message_count: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ChunkMessageLink(Base):
    """Which messages a chunk holds, in order. With overlap a message is in several."""

    __tablename__ = "chunk_messages"
    __table_args__ = (
        PrimaryKeyConstraint("chunk_id", "message_id"),
        Index("ix_chunk_messages_message_id", "message_id"),
    )

    chunk_id: Mapped[int] = mapped_column(ForeignKey("chunks.id", ondelete="CASCADE"))
    message_id: Mapped[int] = mapped_column(ForeignKey("messages.id"))
    position: Mapped[int] = mapped_column(Integer)
