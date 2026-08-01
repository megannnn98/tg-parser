# parser/user_finder.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from parser.logger import get_logger
from parser.measure_time import measure_time
from parser.telegram import (
    FATAL_TG_ERRORS,
    find_chat_members,
    find_history_authors,
    get_chat_with_retry,
    get_client,
)
from parser.utils import join_name


@dataclass(frozen=True)
class UserFinderConfig:
    channels: list[str]


@dataclass(frozen=True)
class UserFinderDeps:
    tg_client_factory: Callable[[], object] = get_client
    find_chat_members_fn: Callable = find_chat_members
    find_history_authors_fn: Callable = find_history_authors
    logger_factory: Callable[[str], object] = get_logger


@dataclass
class FoundUser:
    tg_id: int
    username: str | None
    first_name: str | None
    last_name: str | None
    channels: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)

    @property
    def display_name(self) -> str:
        return join_name(self.first_name, self.last_name) or "—"


_TABLE_HEADER = ("tg_id", "username", "name", "found in", "channels")


def format_found_users(found: list[FoundUser]) -> str:
    rows = [_TABLE_HEADER]
    for user in found:
        rows.append(
            (
                str(user.tg_id),
                f"@{user.username}" if user.username else "—",
                user.display_name,
                ", ".join(user.sources),
                ", ".join(user.channels),
            )
        )

    widths = [max(len(row[i]) for row in rows) for i in range(len(_TABLE_HEADER))]
    table = [
        " | ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip()
        for row in rows
    ]

    commands = [f"    ./scripts/run.sh user-comments {user.tg_id}" for user in found]
    return "\n".join([*table, "", "Собрать комментарии:", *commands])


def _merge(found: dict[int, FoundUser], row: dict, channel: str, source: str):
    user = found.get(row["tg_id"])
    if user is None:
        user = FoundUser(
            tg_id=row["tg_id"],
            username=row["username"],
            first_name=row["first_name"],
            last_name=row["last_name"],
        )
        found[user.tg_id] = user

    if channel not in user.channels:
        user.channels.append(channel)
    if source not in user.sources:
        user.sources.append(source)


async def search_channel(
    tg_client,
    channel_username: str,
    query: str,
    found: dict[int, FoundUser],
    find_chat_members_fn: Callable,
    find_history_authors_fn: Callable,
    logger,
) -> int:
    channel = await get_chat_with_retry(tg_client, channel_username, logger)
    if not channel.linked_chat:
        logger.warning(f"Channel {channel_username} has no linked discussion")
        return 0

    chat_id = channel.linked_chat.id
    hits = 0

    try:
        async for row in find_chat_members_fn(tg_client, chat_id, query):
            _merge(found, row, channel_username, "members")
            hits += 1
    except FATAL_TG_ERRORS:
        raise
    except Exception as exc:
        # Listing members needs membership in the discussion chat, reading its
        # history does not — fall through instead of losing the channel.
        logger.warning(
            f"[{channel_username}] member search unavailable ({exc}), "
            f"falling back to history"
        )

    if hits:
        logger.info(f"[{channel_username}] {hits} match(es) among members")
        return hits

    # Nobody among current members: the person may have left, so look at who
    # actually wrote in the chat. Bounded by LIMIT, hence only the fallback.
    async for row in find_history_authors_fn(tg_client, chat_id, query):
        _merge(found, row, channel_username, "history")
        hits += 1

    logger.info(f"[{channel_username}] {hits} match(es) in history")
    return hits


@measure_time(name="find_users")
async def find_users(
    cfg: UserFinderConfig,
    query: str,
    deps: UserFinderDeps = UserFinderDeps(),
) -> list[FoundUser]:
    if not cfg.channels:
        raise RuntimeError("CHANNELS are empty")
    if not query.strip():
        raise ValueError("Search query is empty")

    logger = deps.logger_factory("user_finder")
    found: dict[int, FoundUser] = {}
    failed = 0

    tg_client = deps.tg_client_factory()
    async with tg_client:
        for channel_username in cfg.channels:
            try:
                await search_channel(
                    tg_client=tg_client,
                    channel_username=channel_username,
                    query=query,
                    found=found,
                    find_chat_members_fn=deps.find_chat_members_fn,
                    find_history_authors_fn=deps.find_history_authors_fn,
                    logger=logger,
                )
            except FATAL_TG_ERRORS:
                logger.error(
                    f"[{channel_username}] fatal session error, aborting search"
                )
                raise
            except Exception:
                failed += 1
                logger.exception(f"[{channel_username}] failed, skipped")

    if failed:
        logger.warning(f"{failed} of {len(cfg.channels)} channels failed")

    return sorted(found.values(), key=lambda user: user.tg_id)
