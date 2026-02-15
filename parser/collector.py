# parser/collector.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable, Iterable, Protocol

import aiosqlite

from parser.logger import get_logger
from parser.measure_time import measure_time
from parser.storage import (
    get_db,
    init_db,
    save_messages_many,
    upsert_channels_many,
    upsert_users_many,
)
from parser.telegram import fetch_messages, get_client


_QUEUE_CHANNEL = "channel"
_QUEUE_USER = "user"
_QUEUE_MESSAGE = "message"


class TgClient(Protocol):
    async def __aenter__(self): ...
    async def __aexit__(self, exc_type, exc, tb): ...
    async def get_chat(self, channel_username: str): ...


@dataclass(frozen=True)
class CollectorConfig:
    channels: list[str]
    batch_size: int = 500
    queue_size: int = 5000
    concurrency: int = 8


@dataclass(frozen=True)
class CollectorDeps:
    tg_client_factory: Callable[[], TgClient] = get_client
    fetch_messages_fn: Callable = fetch_messages
    logger_factory: Callable[[str], object] = get_logger


async def _db_writer(db_path: Path, queue: asyncio.Queue, batch_size: int):
    db = await get_db(db_path)
    try:
        await init_db(db)

        channel_buf: set[str] = set()
        user_buf: dict[int, str | None] = {}
        msg_buf: list[tuple[int, str, str, str]] = []

        async def flush():
            if not channel_buf and not user_buf and not msg_buf:
                return
            await db.execute("BEGIN")
            try:
                if channel_buf:
                    await upsert_channels_many(db, [(c,) for c in channel_buf])
                if user_buf:
                    await upsert_users_many(db, list(user_buf.items()))
                if msg_buf:
                    await save_messages_many(db, msg_buf)
                await db.commit()
            except Exception:
                await db.rollback()
                raise
            finally:
                channel_buf.clear()
                user_buf.clear()
                msg_buf.clear()

        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break

            kind = item[0]
            if kind == _QUEUE_CHANNEL:
                channel_buf.add(item[1])
            elif kind == _QUEUE_USER:
                user_buf[item[1]] = item[2]
            elif kind == _QUEUE_MESSAGE:
                msg_buf.append(item[1])

            if (
                len(channel_buf) >= batch_size
                or len(user_buf) >= batch_size
                or len(msg_buf) >= batch_size
            ):
                await flush()

            queue.task_done()

        await flush()
    finally:
        await db.close()


async def collect_channel(
    tg_client: TgClient,
    queue: asyncio.Queue,
    channel_username: str,
    fetch_messages_fn: Callable,
    logger,
):
    channel = await tg_client.get_chat(channel_username)
    if not channel.linked_chat:
        logger.warning(f"Channel {channel_username} has no linked discussion")
        return

    await queue.put((_QUEUE_CHANNEL, channel_username))
    seen_users: dict[int, str | None] = {}

    async for msg in fetch_messages_fn(tg_client, channel.linked_chat.id):
        tg_id = msg["tg_id"]
        username = msg["username"]
        if seen_users.get(tg_id) != username:
            await queue.put((_QUEUE_USER, tg_id, username))
            seen_users[tg_id] = username

        await queue.put((_QUEUE_MESSAGE, (tg_id, channel_username, msg["text"], msg["date"])))


@measure_time(name="collect_db")
async def collect_db(db_path: Path, cfg: CollectorConfig, deps: CollectorDeps = CollectorDeps()):
    if not cfg.channels:
        raise RuntimeError("CHANNELS are empty")

    logger = deps.logger_factory("collector")
    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.queue_size)
    writer_task = asyncio.create_task(_db_writer(db_path, queue, cfg.batch_size))
    sem = asyncio.Semaphore(cfg.concurrency)

    tg_client = deps.tg_client_factory()
    try:
        async with tg_client:
            async def one(channel: str):
                async with sem:
                    logger.info(f"[{channel}] start")
                    await collect_channel(
                        tg_client=tg_client,
                        queue=queue,
                        channel_username=channel,
                        fetch_messages_fn=deps.fetch_messages_fn,
                        logger=logger,
                    )
                    logger.info(f"[{channel}] done")

            await asyncio.gather(*(one(ch) for ch in cfg.channels))
    finally:
        await queue.put(None)
        await writer_task
