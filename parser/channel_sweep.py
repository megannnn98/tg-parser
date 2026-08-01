from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

from parser.telegram import FATAL_TG_ERRORS, get_chat_with_retry


@dataclass(frozen=True)
class ChannelContext:
    username: str
    chat: object
    linked_chat_id: int | None


async def resolve_channel_context(
    tg_client,
    channel_username: str,
    logger,
    *,
    require_discussion: bool = True,
    no_discussion_message: str | None = None,
) -> ChannelContext | None:
    channel = await get_chat_with_retry(tg_client, channel_username, logger)
    linked_chat_id = channel.linked_chat.id if channel.linked_chat else None

    if linked_chat_id is None:
        logger.warning(
            no_discussion_message
            or f"Channel {channel_username} has no linked discussion"
        )
        if require_discussion:
            return None

    return ChannelContext(
        username=channel_username,
        chat=channel,
        linked_chat_id=linked_chat_id,
    )


async def sweep_channels(
    channels: list[str],
    logger,
    handler: Callable[[str], Awaitable[None]],
    *,
    fatal_message: Callable[[str], str],
) -> int:
    failed = 0
    for channel_username in channels:
        try:
            await handler(channel_username)
        except FATAL_TG_ERRORS:
            logger.error(fatal_message(channel_username))
            raise
        except Exception:
            failed += 1
            logger.exception(f"[{channel_username}] failed, skipped")

    if failed:
        logger.warning(f"{failed} of {len(channels)} channels failed")

    return failed
