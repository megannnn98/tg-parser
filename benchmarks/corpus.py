"""Read-only access to the stored comments for the benchmarks."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine, make_url

from db.engine import database_url
from db.models import Channel, Message, User


@dataclass(frozen=True)
class Comment:
    id: int
    tg_id: int
    channel_id: int
    channel: str
    tg_message_id: int
    date: datetime
    text: str


def sync_engine(url: str | None = None) -> Engine:
    return create_engine(
        make_url(url or database_url()).set(drivername="postgresql+psycopg")
    )


def load_comments(engine: Engine, profiles_only: bool = True) -> list[Comment]:
    """Comments ordered by author, then time. Profiles are the users whose
    whole comment history was collected; other authors have a few messages."""
    stmt = (
        select(
            Message.id,
            User.tg_id,
            Message.channel_id,
            Channel.username,
            Message.tg_message_id,
            Message.date,
            Message.text,
        )
        .join(User, User.id == Message.user_id)
        .join(Channel, Channel.id == Message.channel_id)
        .order_by(User.tg_id, Message.date, Message.channel_id, Message.tg_message_id)
    )
    if profiles_only:
        stmt = stmt.where(User.profile_collected_at.is_not(None))
    with engine.connect() as connection:
        return [Comment(*row) for row in connection.execute(stmt)]
