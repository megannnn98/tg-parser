# parser/collector.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Callable, Protocol

from parser.channel_sweep import resolve_channel_context
from parser.logger import get_logger
from parser.measure_time import measure_time
from parser.telegram import FATAL_TG_ERRORS, fetch_messages, get_client
from services.ingest import CommentIngest


class TgClient(Protocol):
    async def __aenter__(self): ...
    async def __aexit__(self, exc_type, exc, tb): ...
    async def get_chat(self, channel_username: str): ...


@dataclass(frozen=True)
class CollectorConfig:
    channels: list[str]
    batch_size: int = 500
    # Channels read at once; each writes its own batches, so the database
    # needs this many connections.
    concurrency: int = 8


@dataclass(frozen=True)
class CollectorDeps:
    tg_client_factory: Callable[[], TgClient] = get_client
    fetch_messages_fn: Callable = fetch_messages
    logger_factory: Callable[[str], object] = get_logger


async def collect_channel(
    tg_client: TgClient,
    ingest: CommentIngest,
    channel_username: str,
    batch_size: int,
    fetch_messages_fn: Callable,
    logger,
) -> int:
    channel = await resolve_channel_context(tg_client, channel_username, logger)
    if channel is None:
        return 0

    channel_id = await ingest.save_channel(
        channel_username, getattr(channel.chat, "id", None), channel.linked_chat_id
    )

    # Each batch is a transaction of its own: what is stored stays stored when
    # this channel fails later, and other channels are never rolled back.
    new_rows = 0
    batch = []
    async for msg in fetch_messages_fn(tg_client, channel.linked_chat_id):
        batch.append(msg)
        if len(batch) >= batch_size:
            new_rows += await ingest.save_history(channel_id, batch)
            batch = []
    new_rows += await ingest.save_history(channel_id, batch)
    return new_rows


@measure_time(name="collect_db")
async def collect_db(
    sessions, cfg: CollectorConfig, deps: CollectorDeps = CollectorDeps()
) -> int:
    if not cfg.channels:
        raise RuntimeError("CHANNELS are empty")

    logger = deps.logger_factory("collector")
    ingest = CommentIngest(sessions)
    sem = asyncio.Semaphore(cfg.concurrency)
    failed = 0
    new_rows = 0

    tg_client = deps.tg_client_factory()
    async with tg_client:

        async def one(channel: str):
            nonlocal failed, new_rows
            async with sem:
                logger.info(f"[{channel}] start")
                try:
                    added = await collect_channel(
                        tg_client=tg_client,
                        ingest=ingest,
                        channel_username=channel,
                        batch_size=cfg.batch_size,
                        fetch_messages_fn=deps.fetch_messages_fn,
                        logger=logger,
                    )
                except FATAL_TG_ERRORS:
                    raise
                except Exception:
                    failed += 1
                    logger.exception(f"[{channel}] failed, skipped")
                    return
                new_rows += added
                logger.info(f"[{channel}] done, new {added}")

        await asyncio.gather(*(one(ch) for ch in cfg.channels))
        if failed:
            logger.warning(f"{failed} of {len(cfg.channels)} channels failed")

    return new_rows
