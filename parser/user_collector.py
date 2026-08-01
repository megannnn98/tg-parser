# parser/user_collector.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from parser.logger import get_logger
from parser.measure_time import measure_time
from parser.storage import get_db
from parser.telegram import (
    FATAL_TG_ERRORS,
    fetch_user_messages,
    get_chat_with_retry,
    get_client,
    resolve_user,
)
from parser.user_storage import init_user_db, save_user_messages_many
from parser.utils import join_name, user_db_filename


@dataclass(frozen=True)
class UserCollectorConfig:
    channels: list[str]


@dataclass(frozen=True)
class UserCollectorDeps:
    tg_client_factory: Callable[[], object] = get_client
    fetch_user_messages_fn: Callable = fetch_user_messages
    resolve_user_fn: Callable = resolve_user
    logger_factory: Callable[[str], object] = get_logger


async def collect_user_channel(
    db,
    tg_client,
    channel_username: str,
    tg_id: int,
    username: str | None,
    fetch_user_messages_fn: Callable,
    logger,
) -> int:
    channel = await get_chat_with_retry(tg_client, channel_username, logger)
    if not channel.linked_chat:
        logger.warning(f"Channel {channel_username} has no linked discussion")
        return 0

    rows = [
        (
            tg_id,
            username,
            channel_username,
            msg["message_id"],
            msg["text"],
            msg["date"],
        )
        async for msg in fetch_user_messages_fn(
            tg_client, channel.linked_chat.id, tg_id
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
    failed = 0

    tg_client = deps.tg_client_factory()
    async with tg_client:
        # Resolve first: the db file is named after the user, and an unresolvable
        # user must not leave an empty database behind.
        resolved = await deps.resolve_user_fn(tg_client, user_ref)
        tg_id = resolved["tg_id"]
        username = resolved["username"]
        logger.info(
            f"Resolved {user_ref} -> tg_id={tg_id}, username={username}, "
            f"name={join_name(resolved['first_name'], resolved['last_name'])!r}"
        )

        db_path = db_path_override or data_dir / user_db_filename(
            tg_id,
            username,
            resolved["first_name"],
            resolved["last_name"],
        )
        db = await get_db(db_path)
        try:
            await init_user_db(db)

            for channel_username in cfg.channels:
                try:
                    saved += await collect_user_channel(
                        db=db,
                        tg_client=tg_client,
                        channel_username=channel_username,
                        tg_id=tg_id,
                        username=username,
                        fetch_user_messages_fn=deps.fetch_user_messages_fn,
                        logger=logger,
                    )
                except FATAL_TG_ERRORS:
                    logger.error(
                        f"[{channel_username}] fatal session error after saving "
                        f"{saved} rows, aborting sweep"
                    )
                    raise
                except Exception:
                    failed += 1
                    logger.exception(f"[{channel_username}] failed, skipped")
        finally:
            await db.close()

    if failed:
        logger.warning(f"{failed} of {len(cfg.channels)} channels failed")

    return db_path, saved
