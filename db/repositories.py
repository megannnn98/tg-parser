"""All SQL of the application. Callers own the transaction (session.begin())."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import func, literal_column, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Channel, LegacyMessage, Message, User

# asyncpg allows 32767 bind parameters per statement; a message row takes five.
_BATCH_ROWS = 2000


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


async def insert_messages(
    session: AsyncSession, rows: list[dict], refresh_text: bool = False
) -> int:
    """Inserts rows of tg_message_id, user_id, channel_id, text, date.

    A comment already stored is left as it is, unless `refresh_text` asks to
    take its text from Telegram again. A comment whose text did change loses
    what was derived from the old text: its embeddings and the chunks holding
    it, which the chunk builder and the embedding pipeline then compute again.
    Returns the number of new rows.
    """
    key = [Message.channel_id, Message.tg_message_id]
    # Sorted, so concurrent batches lock rows in one order and cannot deadlock.
    rows = sorted(rows, key=lambda row: (row["channel_id"], row["tg_message_id"]))
    inserted = 0
    changed: list[int] = []
    for start in range(0, len(rows), _BATCH_ROWS):
        stmt = insert(Message).values(rows[start : start + _BATCH_ROWS])
        if refresh_text:
            stmt = stmt.on_conflict_do_update(
                index_elements=key,
                set_={"text": stmt.excluded.text},
                where=Message.text.is_distinct_from(stmt.excluded.text),
                # xmax is 0 only for a row this statement created.
            ).returning(Message.id, literal_column("xmax = 0"))
            for message_id, is_new in await session.execute(stmt):
                if is_new:
                    inserted += 1
                else:
                    changed.append(message_id)
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=key).returning(
                Message.id
            )
            inserted += len((await session.execute(stmt)).all())
    if changed:
        await _drop_derived(session, changed)
    return inserted


async def _drop_derived(session: AsyncSession, message_ids: list[int]) -> None:
    """Removes embeddings and chunks made from texts that have just changed.

    Plain SQL: the embedding tables are mapped in db/embedding_models.py, which
    needs numpy, and this module must load without it.
    """
    ids = {"ids": message_ids}
    await session.execute(
        text("DELETE FROM message_embeddings WHERE message_id = ANY(:ids)"), ids
    )
    # Their embeddings and message links go with the chunks (ON DELETE CASCADE).
    await session.execute(
        text(
            "DELETE FROM chunks WHERE id IN "
            "(SELECT chunk_id FROM chunk_messages WHERE message_id = ANY(:ids))"
        ),
        ids,
    )


async def get_user_by_tg_id(session: AsyncSession, tg_id: int) -> User | None:
    return await session.scalar(select(User).where(User.tg_id == tg_id))


async def count_user_messages(session: AsyncSession, user_id: int) -> int:
    return await session.scalar(
        select(func.count()).select_from(Message).where(Message.user_id == user_id)
    )


async def find_profile(session: AsyncSession, user_ref: int | str) -> User | None:
    """A user whose comments were collected before, by tg_id or by username."""
    stmt = select(User).where(User.profile_collected_at.is_not(None))
    if isinstance(user_ref, int):
        stmt = stmt.where(User.tg_id == user_ref)
    else:
        stmt = stmt.where(func.lower(User.username) == user_ref.lower())
    # Two users may have held one username at different times.
    return await session.scalar(stmt.order_by(User.updated_at.desc()).limit(1))


async def mark_profiles_collected(
    session: AsyncSession, collected_at: dict[int, datetime], overwrite: bool = True
) -> None:
    """Takes {users.id: time of the user-comments run}."""
    for user_id, when in collected_at.items():
        stmt = update(User).where(User.id == user_id)
        if not overwrite:
            stmt = stmt.where(User.profile_collected_at.is_(None))
        await session.execute(stmt.values(profile_collected_at=when))


async def list_profile_users(session: AsyncSession) -> list[User]:
    """Users whose comments were collected, including those with none found."""
    stmt = select(User).where(User.profile_collected_at.is_not(None))
    return list(await session.scalars(stmt.order_by(User.tg_id)))


async def channel_counts_of_profiles(
    session: AsyncSession,
) -> dict[int, list[tuple[str, int]]]:
    """{users.id: [(channel, messages)]} for collected users, largest channel first."""
    count = func.count(Message.id)
    stmt = (
        select(Message.user_id, Channel.username, count)
        .join(Channel, Channel.id == Message.channel_id)
        .join(User, User.id == Message.user_id)
        .where(User.profile_collected_at.is_not(None))
        .group_by(Message.user_id, Channel.username)
        .order_by(Message.user_id, count.desc(), Channel.username)
    )
    result: dict[int, list[tuple[str, int]]] = {}
    for user_id, channel, messages in await session.execute(stmt):
        result.setdefault(user_id, []).append((channel, messages))
    return result


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


# The SQLite importer: it fills gaps and never overwrites what is already stored.


async def insert_users_if_absent(
    session: AsyncSession, users: dict[int, tuple[str | None, str | None]]
) -> tuple[dict[int, int], int]:
    """Takes {tg_id: (username, first_name)}; returns ({tg_id: users.id}, new rows)."""
    ids: dict[int, int] = {}
    inserted = 0
    rows = [
        {"tg_id": tg_id, "username": users[tg_id][0], "first_name": users[tg_id][1]}
        for tg_id in sorted(users)
    ]
    for start in range(0, len(rows), _BATCH_ROWS):
        stmt = insert(User).values(rows[start : start + _BATCH_ROWS])
        stmt = stmt.on_conflict_do_update(
            index_elements=[User.tg_id],
            set_={
                "username": func.coalesce(User.username, stmt.excluded.username),
                "first_name": func.coalesce(User.first_name, stmt.excluded.first_name),
            },
            # xmax is 0 only for a row this statement created.
        ).returning(User.tg_id, User.id, literal_column("xmax = 0"))
        for tg_id, user_id, is_new in await session.execute(stmt):
            ids[tg_id] = user_id
            inserted += is_new
    return ids, inserted


async def ensure_channels(
    session: AsyncSession, usernames: set[str]
) -> tuple[dict[str, int], int]:
    """Returns ({username: channels.id}, new rows)."""
    ids: dict[str, int] = {}
    inserted = 0
    rows = [{"username": username} for username in sorted(usernames)]
    for start in range(0, len(rows), _BATCH_ROWS):
        stmt = insert(Channel).values(rows[start : start + _BATCH_ROWS])
        stmt = stmt.on_conflict_do_update(
            index_elements=[Channel.username],
            # A no-op update, so that RETURNING also yields existing rows.
            set_={"username": stmt.excluded.username},
        ).returning(Channel.username, Channel.id, literal_column("xmax = 0"))
        for username, channel_id, is_new in await session.execute(stmt):
            ids[username] = channel_id
            inserted += is_new
    return ids, inserted


async def insert_legacy_messages(session: AsyncSession, rows: list[dict]) -> int:
    inserted = 0
    for start in range(0, len(rows), _BATCH_ROWS):
        stmt = (
            insert(LegacyMessage)
            .values(rows[start : start + _BATCH_ROWS])
            .on_conflict_do_nothing(
                index_elements=[LegacyMessage.source, LegacyMessage.source_row_id]
            )
            .returning(LegacyMessage.id)
        )
        inserted += len((await session.execute(stmt)).all())
    return inserted
