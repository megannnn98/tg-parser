"""PostgreSQL schema. Alembic owns the DDL; these models are its source."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    MetaData,
    Text,
    UniqueConstraint,
    func,
)
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
