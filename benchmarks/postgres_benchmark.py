"""Write and read timings of the PostgreSQL storage on a copy of the real comments.

    SOURCE_DATABASE_URL=...  TEST_DATABASE_URL=..._test \\
        python -m benchmarks.postgres_benchmark

Reads the comments from the source database and replays them into the test
database, whose tables are emptied first; the name must end in `_test`. The
aim is to know the cost of the application's own queries, not to rank
PostgreSQL against SQLite.
"""
from __future__ import annotations

import asyncio
import os
import statistics
import time
from datetime import timedelta

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url

from db import repositories as repo
from db.engine import create_engine, session_factory
from db.models import Base, Channel, Message, User

BATCH = 500
WRITERS = 8


async def _load_source(url: str) -> list[tuple]:
    engine = create_engine(url)
    try:
        async with engine.connect() as connection:
            rows = await connection.execute(
                select(
                    User.tg_id,
                    Channel.username,
                    Message.tg_message_id,
                    Message.text,
                    Message.date,
                )
                .join(User, User.id == Message.user_id)
                .join(Channel, Channel.id == Message.channel_id)
                .order_by(Message.id)
            )
            return [tuple(row) for row in rows]
    finally:
        await engine.dispose()


async def _empty(engine) -> None:
    tables = ", ".join(table.name for table in Base.metadata.sorted_tables)
    async with engine.begin() as connection:
        await connection.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))


async def _prepare(sessions, source) -> list[dict]:
    """Users and channels in place; returns the message rows ready to insert."""
    async with sessions.begin() as session:
        user_ids = await repo.upsert_users(session, {row[0]: None for row in source})
        channel_ids, _ = await repo.ensure_channels(session, {row[1] for row in source})
    return [
        {
            "tg_message_id": tg_message_id,
            "user_id": user_ids[tg_id],
            "channel_id": channel_ids[channel],
            "text": body,
            "date": date,
        }
        for tg_id, channel, tg_message_id, body, date in source
    ]


async def _insert_once(sessions, rows) -> float:
    started = time.perf_counter()
    async with sessions.begin() as session:
        await repo.insert_messages(session, rows)
    return time.perf_counter() - started


async def _insert_batches(sessions, rows, writers: int) -> float:
    """Transactions of BATCH rows, written by `writers` tasks at once."""
    batches = [rows[i : i + BATCH] for i in range(0, len(rows), BATCH)]
    queue: asyncio.Queue = asyncio.Queue()
    for batch in batches:
        queue.put_nowait(batch)

    async def writer():
        while not queue.empty():
            batch = queue.get_nowait()
            async with sessions.begin() as session:
                await repo.insert_messages(session, batch)

    started = time.perf_counter()
    await asyncio.gather(*(writer() for _ in range(writers)))
    return time.perf_counter() - started


async def _timed(sessions, query, repeats: int = 30) -> tuple[float, float, int]:
    times, size = [], 0
    for _ in range(repeats):
        async with sessions() as session:
            started = time.perf_counter()
            result = await query(session)
            times.append((time.perf_counter() - started) * 1000)
            size = len(result)
    times.sort()
    return statistics.median(times), times[int(len(times) * 0.95) - 1], size


async def main() -> None:
    target_url = os.environ["TEST_DATABASE_URL"]
    if not make_url(target_url).database.endswith("_test"):
        raise SystemExit("TEST_DATABASE_URL must name a database ending in _test")
    source = await _load_source(os.environ["SOURCE_DATABASE_URL"])
    engine = create_engine(target_url, pool_size=WRITERS)
    sessions = session_factory(engine)
    try:
        await _empty(engine)
        rows = await _prepare(sessions, source)
        print(f"Source: {len(rows)} comments.\n")
        print("| write | rows | seconds | rows/s |")
        print("|---|---|---|---|")

        async def report(name, count, seconds):
            print(f"| {name} | {count} | {seconds:.2f} | {count / seconds:.0f} |")

        for count in (1_000, 10_000, len(rows)):
            async with engine.begin() as connection:
                await connection.execute(text("TRUNCATE messages RESTART IDENTITY CASCADE"))
            await report("one transaction", count, await _insert_once(sessions, rows[:count]))
        await report(
            "the same rows again (all duplicates)",
            len(rows),
            await _insert_once(sessions, rows),
        )
        for name, writers in (
            (f"batches of {BATCH}, one writer", 1),
            (f"batches of {BATCH}, {WRITERS} concurrent writers", WRITERS),
        ):
            async with engine.begin() as connection:
                await connection.execute(text("TRUNCATE messages RESTART IDENTITY CASCADE"))
            await report(name, len(rows), await _insert_batches(sessions, rows, writers))
        async with engine.begin() as connection:
            await connection.execute(text("ANALYZE"))

        async with sessions() as session:
            user_id, total = (
                await session.execute(
                    select(Message.user_id, func.count())
                    .group_by(Message.user_id)
                    .order_by(func.count().desc())
                    .limit(1)
                )
            ).one()
            newest = await session.scalar(
                select(func.max(Message.date)).where(Message.user_id == user_id)
            )
        month_ago = newest - timedelta(days=30)

        async def latest(session):
            return (
                await session.execute(
                    select(Message.id, Message.text)
                    .where(Message.user_id == user_id)
                    .order_by(Message.date.desc())
                    .limit(50)
                )
            ).all()

        async def date_range(session):
            return (
                await session.execute(
                    select(Message.id, Message.text).where(
                        Message.user_id == user_id, Message.date >= month_ago
                    )
                )
            ).all()

        print(f"\nQueries for the user with the most comments ({total}):\n")
        print("| read | rows | p50 ms | p95 ms |")
        print("|---|---|---|---|")
        for name, query in (
            ("all comments of the user", lambda s: repo.user_messages(s, user_id)),
            ("latest 50", latest),
            ("last 30 days", date_range),
            ("comments by channel", lambda s: repo.channel_counts(s, user_id)),
            ("activity by hour", lambda s: repo.hourly_activity(s, user_id, "Asia/Almaty")),
            ("activity by weekday and hour",
             lambda s: repo.weekly_activity(s, user_id, "Asia/Almaty")),
        ):
            p50, p95, size = await _timed(sessions, query)
            print(f"| {name} | {size} | {p50:.1f} | {p95:.1f} |")
    finally:
        await _empty(engine)
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
