"""Takes the comments of every collected profile from Telegram again.

    python -m scripts.refresh_texts

The SQLite importer brings comments as the old collector stored them:
lowercased. This restores the original text and adds comments written since.
It walks the channels once and asks each for every profile, which costs one
channel lookup per channel instead of one per channel and profile.
"""
from __future__ import annotations

import asyncio

from config import CHANNELS
from db import repositories as repo
from db.engine import create_engine, session_factory
from parser.channel_sweep import resolve_channel_context, sweep_channels
from parser.logger import get_logger
from parser.telegram import fetch_user_messages, get_client
from services.ingest import CommentIngest


async def refresh_texts(
    sessions,
    channels: list[str],
    tg_client_factory=get_client,
    fetch_user_messages_fn=fetch_user_messages,
    logger=None,
) -> tuple[int, int]:
    """Returns (comments fetched, new rows)."""
    logger = logger or get_logger("refresh_texts")
    ingest = CommentIngest(sessions)
    async with sessions() as session:
        profiles = [(u.id, u.tg_id) for u in await repo.list_profile_users(session)]
    fetched = new = 0

    tg_client = tg_client_factory()
    async with tg_client:

        async def one(channel_username: str) -> None:
            nonlocal fetched, new
            channel = await resolve_channel_context(tg_client, channel_username, logger)
            if channel is None:
                return
            channel_id = await ingest.save_channel(
                channel_username,
                getattr(channel.chat, "id", None),
                channel.linked_chat_id,
            )
            found = added = 0
            for user_id, tg_id in profiles:
                comments = [
                    msg
                    async for msg in fetch_user_messages_fn(
                        tg_client, channel.linked_chat_id, tg_id
                    )
                ]
                found += len(comments)
                added += await ingest.save_user_comments(
                    user_id, channel_id, comments, refresh_text=True
                )
            fetched += found
            new += added
            logger.info(f"[{channel_username}] fetched {found}, new {added}")

        await sweep_channels(
            channels,
            logger,
            one,
            fatal_message=lambda channel_username: (
                f"[{channel_username}] fatal session error, aborting"
            ),
        )

    return fetched, new


async def _main() -> None:
    engine = create_engine()
    try:
        fetched, new = await refresh_texts(session_factory(engine), CHANNELS)
    finally:
        await engine.dispose()
    print(f"messages fetched: {fetched}\nnew messages: {new}")


if __name__ == "__main__":
    asyncio.run(_main())
