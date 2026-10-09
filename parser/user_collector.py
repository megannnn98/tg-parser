# parser/user_collector.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from parser.channel_sweep import resolve_channel_context, sweep_channels
from parser.logger import get_logger
from parser.measure_time import measure_time
from parser.telegram import (
    TelegramUser,
    fetch_user_messages,
    get_client,
    resolve_user,
)
from parser.utils import join_name
from services.ingest import CommentIngest


@dataclass(frozen=True)
class UserCollectorConfig:
    channels: list[str]
    # Take the text of already stored comments from Telegram again.
    refresh_text: bool = False


@dataclass(frozen=True)
class ChannelProgress:
    channel: str
    status: str  # "started" | "done" | "failed"
    saved: int = 0
    error: str | None = None


@dataclass(frozen=True)
class UserCollectResult:
    tg_id: int
    username: str | None
    channels_scanned: int
    channels_failed: int
    fetched: int
    new: int

    @property
    def duplicates(self) -> int:
        return self.fetched - self.new

    def render(self) -> str:
        return (
            "Resolved user:\n"
            f"tg_id={self.tg_id}\n"
            f"username={self.username}\n"
            "\n"
            f"channels scanned: {self.channels_scanned}\n"
            f"channels failed: {self.channels_failed}\n"
            f"messages fetched: {self.fetched}\n"
            f"new messages: {self.new}\n"
            f"duplicates: {self.duplicates}"
        )


@dataclass(frozen=True)
class UserCollectorDeps:
    tg_client_factory: Callable[[], object] = get_client
    fetch_user_messages_fn: Callable = fetch_user_messages
    resolve_user_fn: Callable = resolve_user
    logger_factory: Callable[[str], object] = get_logger
    on_user_resolved: Callable[[TelegramUser], None] = lambda _user: None
    on_channel_progress: Callable[[ChannelProgress], None] = lambda _progress: None


async def collect_user_channel(
    ingest: CommentIngest,
    tg_client,
    channel_username: str,
    user_id: int,
    tg_id: int,
    refresh_text: bool,
    fetch_user_messages_fn: Callable,
    logger,
) -> tuple[int, int]:
    """Returns (comments fetched, new rows)."""
    channel = await resolve_channel_context(tg_client, channel_username, logger)
    if channel is None:
        return 0, 0

    channel_id = await ingest.save_channel(
        channel_username, getattr(channel.chat, "id", None), channel.linked_chat_id
    )
    comments = [
        msg
        async for msg in fetch_user_messages_fn(
            tg_client, channel.linked_chat_id, tg_id
        )
    ]
    new_rows = await ingest.save_user_comments(
        user_id, channel_id, comments, refresh_text
    )
    logger.info(f"[{channel_username}] fetched {len(comments)}, new {new_rows}")
    return len(comments), new_rows


@measure_time(name="collect_user_comments")
async def collect_user_comments(
    sessions,
    cfg: UserCollectorConfig,
    user_ref: int | str,
    deps: UserCollectorDeps = UserCollectorDeps(),
) -> UserCollectResult:
    if not cfg.channels:
        raise RuntimeError("CHANNELS are empty")

    logger = deps.logger_factory("user_collector")
    ingest = CommentIngest(sessions)
    fetched = 0
    saved = 0

    tg_client = deps.tg_client_factory()
    async with tg_client:
        # A user collected before is not resolved again: Telegram rate-limits
        # username lookups hard (FLOOD_WAIT of hours).
        existing = await ingest.find_profile(user_ref)
        if existing is not None:
            user_id, resolved = existing
            await ingest.touch_profile(user_id)
            logger.info(
                f"Reusing stored profile for {user_ref}: "
                f"tg_id={resolved.tg_id}, username={resolved.username}"
            )
        else:
            resolved = await deps.resolve_user_fn(tg_client, user_ref)
            user_id = await ingest.save_profile(resolved)
            logger.info(
                f"Resolved {user_ref} -> tg_id={resolved.tg_id}, "
                f"username={resolved.username}, "
                f"name={join_name(resolved.first_name, resolved.last_name)!r}"
            )
        deps.on_user_resolved(resolved)

        async def one(channel_username: str) -> None:
            nonlocal fetched, saved
            deps.on_channel_progress(
                ChannelProgress(channel=channel_username, status="started")
            )
            try:
                found, added = await collect_user_channel(
                    ingest=ingest,
                    tg_client=tg_client,
                    channel_username=channel_username,
                    user_id=user_id,
                    tg_id=resolved.tg_id,
                    refresh_text=cfg.refresh_text,
                    fetch_user_messages_fn=deps.fetch_user_messages_fn,
                    logger=logger,
                )
            except Exception as exc:
                deps.on_channel_progress(
                    ChannelProgress(
                        channel=channel_username, status="failed", error=str(exc)
                    )
                )
                raise

            fetched += found
            saved += added
            deps.on_channel_progress(
                ChannelProgress(channel=channel_username, status="done", saved=added)
            )

        failed = await sweep_channels(
            cfg.channels,
            logger,
            one,
            fatal_message=lambda channel_username: (
                f"[{channel_username}] fatal session error after saving "
                f"{saved} rows, aborting sweep"
            ),
        )

    return UserCollectResult(
        tg_id=resolved.tg_id,
        username=resolved.username,
        channels_scanned=len(cfg.channels),
        channels_failed=failed,
        fetched=fetched,
        new=saved,
    )
