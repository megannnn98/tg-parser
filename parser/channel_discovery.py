# parser/channel_discovery.py
from __future__ import annotations

import asyncio
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from parser.channel_sweep import resolve_channel_context, sweep_channels
from parser.logger import get_logger
from parser.measure_time import measure_time
from parser.telegram import (
    ChannelInfo,
    FATAL_TG_ERRORS,
    FloodWait,
    describe_channel,
    fetch_mentions,
    get_client,
)

# Telegram usernames: a letter first, then letters/digits/underscores, 5-32 total.
_USERNAME = r"[a-zA-Z][a-zA-Z0-9_]{4,31}"

# t.me/<name>, with or without scheme; a trailing /123 (link to a post) is fine.
_LINK_RE = re.compile(rf"(?:https?://)?t\.me/({_USERNAME})\b", re.IGNORECASE)

# @<name>, but not the tail of an email address.
_MENTION_RE = re.compile(rf"(?<![\w@])@({_USERNAME})\b")

# t.me paths that are not channels.
_RESERVED_PATHS = frozenset(
    {
        "joinchat",
        "addstickers",
        "addemoji",
        "addlist",
        "addtheme",
        "setlanguage",
        "confirmphone",
        "contact",
        "invoice",
        "giftcode",
        "boost",
        "proxy",
        "socks",
        "share",
        "login",
    }
)


@dataclass(frozen=True)
class DiscoveryConfig:
    channels: list[str]
    target: int = 200


@dataclass(frozen=True)
class DiscoveryDeps:
    tg_client_factory: Callable[[], object] = get_client
    fetch_mentions_fn: Callable = fetch_mentions
    describe_channel_fn: Callable = describe_channel
    logger_factory: Callable[[str], object] = get_logger
    sleep_fn: Callable = asyncio.sleep


def extract_usernames(text: str | None) -> set[str]:
    if not text:
        return set()

    found = {match.casefold() for match in _LINK_RE.findall(text)}
    found |= {match.casefold() for match in _MENTION_RE.findall(text)}
    return found - _RESERVED_PATHS


def append_channels(path: Path, new_channels: list[str]) -> int:
    current = json.loads(path.read_text())
    known = {channel.casefold() for channel in current}

    added = []
    for channel in new_channels:
        if channel.casefold() in known:
            continue
        known.add(channel.casefold())
        added.append(channel)

    if not added:
        return 0

    # Write through a temp file: a crash must not leave a truncated list.
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(current + added, ensure_ascii=False, indent=2) + "\n"
    )
    tmp.replace(path)
    return len(added)


async def scan_channel(
    tg_client,
    channel_username: str,
    fetch_mentions_fn: Callable,
    logger,
) -> Counter[str]:
    channel = await resolve_channel_context(
        tg_client,
        channel_username,
        logger,
        require_discussion=False,
        no_discussion_message=(
            f"Channel {channel_username} has no linked discussion, posts only"
        ),
    )

    sources = [channel.chat.id]
    if channel.linked_chat_id is not None:
        sources.append(channel.linked_chat_id)

    mentions: Counter[str] = Counter()
    for chat_id in sources:
        async for row in fetch_mentions_fn(tg_client, chat_id):
            if row.forward_channel:
                mentions[row.forward_channel.casefold()] += 1
            for username in extract_usernames(row.text):
                mentions[username] += 1

    logger.info(f"[{channel_username}] {len(mentions)} candidate(s) mentioned")
    return mentions


async def check_candidate(
    deps: DiscoveryDeps,
    tg_client,
    username: str,
    logger,
) -> ChannelInfo | None:
    try:
        return await deps.describe_channel_fn(tg_client, username)
    except FloodWait as exc:
        # Telegram throttles bursts of username resolves; pyrogram only sleeps
        # through short waits on its own, so honour the longer ones here rather
        # than lose the candidate.
        wait = exc.value + 1
        logger.warning(f"FloodWait {wait}s on @{username}, waiting it out")
        await deps.sleep_fn(wait)

    try:
        return await deps.describe_channel_fn(tg_client, username)
    except FloodWait:
        logger.warning(f"@{username} still rate-limited after the wait, skipped")
        return None


@measure_time(name="discover_channels")
async def discover_channels(
    cfg: DiscoveryConfig,
    deps: DiscoveryDeps = DiscoveryDeps(),
) -> list[str]:
    if not cfg.channels:
        raise RuntimeError("CHANNELS are empty")

    logger = deps.logger_factory("channel_discovery")
    known = {channel.casefold() for channel in cfg.channels}
    mentions: Counter[str] = Counter()

    tg_client = deps.tg_client_factory()
    async with tg_client:
        async def one(channel_username: str) -> None:
            mentions.update(
                await scan_channel(
                    tg_client=tg_client,
                    channel_username=channel_username,
                    fetch_mentions_fn=deps.fetch_mentions_fn,
                    logger=logger,
                )
            )

        await sweep_channels(
            cfg.channels,
            logger,
            one,
            fatal_message=lambda channel_username: (
                f"[{channel_username}] fatal session error, aborting discovery"
            ),
        )

        # Known channels are not candidates: this also drops the self-reposts
        # every discussion carries from its own channel.
        for username in known:
            del mentions[username]

        logger.info(f"{len(mentions)} unique candidate(s) to check")

        # Resolving costs a request each, so spend them on the most mentioned
        # candidates first and stop as soon as the list is big enough.
        added: list[str] = []
        for username, count in mentions.most_common():
            if len(known) + len(added) >= cfg.target:
                logger.info(f"Reached the target of {cfg.target} channels")
                break

            try:
                info = await check_candidate(deps, tg_client, username, logger)
            except FATAL_TG_ERRORS:
                # The caller never receives `added`, so name the survivors here:
                # the scan that produced them can take tens of minutes.
                logger.error(
                    f"Fatal session error while checking @{username}, "
                    f"aborting. Found so far: {', '.join(added) or 'nothing'}"
                )
                raise

            if info is None:
                continue

            added.append(username)
            logger.info(
                f"+ @{username} — {info.title!r}, "
                f"{info.members} members, {count} mention(s)"
            )

    return added
