"""All SQL of the application. Callers own the transaction (session.begin())."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Channel, Message, User

# asyncpg allows 32767 bind parameters per statement; a message row takes five.
_BATCH_ROWS = 2000


@dataclass(frozen=True)
class UserSummary:
    id: int
    tg_id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    total_messages: int
    channel_count: int


@dataclass(frozen=True)
class StoredComment:
    channel: str
    tg_message_id: int
    date: datetime
    text: str


async def upsert_user(
    session: AsyncSession,
    tg_id: int,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> int:
    stmt = insert(User).values(
        tg_id=tg_id, username=username, first_name=first_name, last_name=last_name
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[User.tg_id],
        set_={
            "username": stmt.excluded.username,
            # Comment authors come without names; a known name is not erased.
            "first_name": func.coalesce(stmt.excluded.first_name, User.first_name),
            "last_name": func.coalesce(stmt.excluded.last_name, User.last_name),
            "updated_at": func.now(),
        },
    ).returning(User.id)
    return (await session.execute(stmt)).scalar_one()


async def upsert_users(
    session: AsyncSession, usernames: dict[int, str | None]
) -> dict[int, int]:
    """Upserts {tg_id: username} and returns {tg_id: users.id}."""
    ids: dict[int, int] = {}
    # Sorted, so concurrent batches lock rows in one order and cannot deadlock.
    rows = [
        {"tg_id": tg_id, "username": usernames[tg_id]} for tg_id in sorted(usernames)
    ]
    for start in range(0, len(rows), _BATCH_ROWS):
        stmt = insert(User).values(rows[start : start + _BATCH_ROWS])
        stmt = stmt.on_conflict_do_update(
            index_elements=[User.tg_id],
            set_={"username": stmt.excluded.username, "updated_at": func.now()},
        ).returning(User.tg_id, User.id)
        ids.update((await session.execute(stmt)).tuples().all())
    return ids


async def upsert_channel(
    session: AsyncSession,
    username: str,
    telegram_chat_id: int | None = None,
    linked_chat_id: int | None = None,
) -> int:
    stmt = insert(Channel).values(
        username=username,
        telegram_chat_id=telegram_chat_id,
        linked_chat_id=linked_chat_id,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[Channel.username],
        set_={
            "telegram_chat_id": func.coalesce(
                stmt.excluded.telegram_chat_id, Channel.telegram_chat_id
            ),
            "linked_chat_id": func.coalesce(
                stmt.excluded.linked_chat_id, Channel.linked_chat_id
            ),
            "updated_at": func.now(),
        },
    ).returning(Channel.id)
    return (await session.execute(stmt)).scalar_one()


async def insert_messages(session: AsyncSession, rows: list[dict]) -> int:
    """Inserts rows of tg_message_id, user_id, channel_id, text, date.

    A comment already stored is left as it is. Returns the number of new rows.
    """
    rows = sorted(rows, key=lambda row: (row["channel_id"], row["tg_message_id"]))
    inserted = 0
    for start in range(0, len(rows), _BATCH_ROWS):
        stmt = (
            insert(Message)
            .values(rows[start : start + _BATCH_ROWS])
            .on_conflict_do_nothing(
                index_elements=[Message.channel_id, Message.tg_message_id]
            )
            .returning(Message.id)
        )
        inserted += len((await session.execute(stmt)).all())
    return inserted


async def get_user_by_tg_id(session: AsyncSession, tg_id: int) -> User | None:
    return await session.scalar(select(User).where(User.tg_id == tg_id))


async def list_user_summaries(session: AsyncSession) -> list[UserSummary]:
    stmt = (
        select(
            User.id,
            User.tg_id,
            User.username,
            User.first_name,
            User.last_name,
            func.count(Message.id),
            func.count(Message.channel_id.distinct()),
        )
        .outerjoin(Message, Message.user_id == User.id)
        .group_by(User.id)
        .order_by(User.tg_id)
    )
    return [UserSummary(*row) for row in await session.execute(stmt)]


async def channel_counts(session: AsyncSession, user_id: int) -> list[tuple[str, int]]:
    count = func.count(Message.id)
    stmt = (
        select(Channel.username, count)
        .join(Message, Message.channel_id == Channel.id)
        .where(Message.user_id == user_id)
        .group_by(Channel.username)
        .order_by(count.desc(), Channel.username)
    )
    return [tuple(row) for row in await session.execute(stmt)]


async def user_messages(session: AsyncSession, user_id: int) -> list[StoredComment]:
    stmt = (
        select(Channel.username, Message.tg_message_id, Message.date, Message.text)
        .join(Channel, Channel.id == Message.channel_id)
        .where(Message.user_id == user_id)
        .order_by(Message.date, Channel.username, Message.tg_message_id)
    )
    return [StoredComment(*row) for row in await session.execute(stmt)]


def _local(tz: str):
    # timestamptz AT TIME ZONE gives the wall-clock time in that zone.
    return func.timezone(tz, Message.date)


async def hourly_activity(
    session: AsyncSession, user_id: int, tz: str
) -> dict[int, int]:
    hour = func.extract("hour", _local(tz))
    stmt = (
        select(hour, func.count()).where(Message.user_id == user_id).group_by(hour)
    )
    return {int(h): count for h, count in await session.execute(stmt)}


async def daily_activity(
    session: AsyncSession, user_id: int, tz: str
) -> list[tuple[date, int]]:
    day = func.date(_local(tz))
    stmt = (
        select(day, func.count())
        .where(Message.user_id == user_id)
        .group_by(day)
        .order_by(day)
    )
    return [tuple(row) for row in await session.execute(stmt)]


async def weekly_activity(
    session: AsyncSession, user_id: int, tz: str
) -> dict[tuple[int, int], int]:
    """Counts by (weekday, hour), Monday being 0."""
    local = _local(tz)
    weekday = func.extract("isodow", local)
    hour = func.extract("hour", local)
    stmt = (
        select(weekday, hour, func.count())
        .where(Message.user_id == user_id)
        .group_by(weekday, hour)
    )
    return {
        (int(d) - 1, int(h)): count for d, h, count in await session.execute(stmt)
    }
