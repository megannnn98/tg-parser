# parser/user_collector.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from parser.channel_sweep import resolve_channel_context, sweep_channels
from parser.logger import get_logger
from parser.measure_time import measure_time
from parser.storage import get_db
from parser.telegram import (
    TelegramUser,
    fetch_user_messages,
    get_client,
    resolve_user,
)
from parser.user_storage import init_user_db, save_user_messages_many
from parser.utils import join_name, user_db_filename


@dataclass(frozen=True)
class UserCollectorConfig:
    channels: list[str]


@dataclass(frozen=True)
class ChannelProgress:
    channel: str
    status: str  # "started" | "done" | "failed"
    saved: int = 0
    error: str | None = None


@dataclass(frozen=True)
class UserCollectorDeps:
    tg_client_factory: Callable[[], object] = get_client
    fetch_user_messages_fn: Callable = fetch_user_messages
    resolve_user_fn: Callable = resolve_user
    logger_factory: Callable[[str], object] = get_logger
    on_user_resolved: Callable[[TelegramUser], None] = lambda _user: None
    on_channel_progress: Callable[[ChannelProgress], None] = lambda _progress: None


async def collect_user_channel(
    db,
    tg_client,
    channel_username: str,
    tg_id: int,
    username: str | None,
    fetch_user_messages_fn: Callable,
    logger,
) -> int:
    channel = await resolve_channel_context(tg_client, channel_username, logger)
    if channel is None:
        return 0

    rows = [
        (
            tg_id,
            username,
            channel_username,
            msg.message_id,
            msg.text,
            msg.date,
        )
        async for msg in fetch_user_messages_fn(
            tg_client, channel.linked_chat_id, tg_id
        )
    ]

    if not rows:
        logger.info(f"[{channel_username}] fetched 0, new 0")
        return 0

    changes_before = db.total_changes
    await db.execute("BEGIN")
    try:
        await save_user_messages_many(db, rows)
        await db.commit()
    except Exception:
        await db.rollback()
        raise

    new_rows = db.total_changes - changes_before
    logger.info(f"[{channel_username}] fetched {len(rows)}, new {new_rows}")
    return new_rows


@measure_time(name="collect_user_comments")
async def collect_user_comments(
    data_dir: Path,
    cfg: UserCollectorConfig,
    user_ref: int | str,
    deps: UserCollectorDeps = UserCollectorDeps(),
    db_path_override: Path | None = None,
) -> tuple[Path, int]:
    if not cfg.channels:
        raise RuntimeError("CHANNELS are empty")

    logger = deps.logger_factory("user_collector")
    saved = 0

    tg_client = deps.tg_client_factory()
    async with tg_client:
        # Resolve first: the db file is named after the user, and an unresolvable
        # user must not leave an empty database behind.
        resolved = await deps.resolve_user_fn(tg_client, user_ref)
        tg_id = resolved.tg_id
        username = resolved.username
        logger.info(
            f"Resolved {user_ref} -> tg_id={tg_id}, username={username}, "
            f"name={join_name(resolved.first_name, resolved.last_name)!r}"
        )
        deps.on_user_resolved(resolved)

        db_path = db_path_override or data_dir / user_db_filename(
            tg_id,
            username,
            resolved.first_name,
            resolved.last_name,
        )
        db = await get_db(db_path)
        try:
            await init_user_db(db)

            async def one(channel_username: str) -> None:
                nonlocal saved
                deps.on_channel_progress(
                    ChannelProgress(channel=channel_username, status="started")
                )
                try:
                    added = await collect_user_channel(
                        db=db,
                        tg_client=tg_client,
                        channel_username=channel_username,
                        tg_id=tg_id,
                        username=username,
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

                saved += added
                deps.on_channel_progress(
                    ChannelProgress(
                        channel=channel_username, status="done", saved=added
                    )
                )

            await sweep_channels(
                cfg.channels,
                logger,
                one,
                fatal_message=lambda channel_username: (
                    f"[{channel_username}] fatal session error after saving "
                    f"{saved} rows, aborting sweep"
                ),
            )
        finally:
            await db.close()

    return db_path, saved
