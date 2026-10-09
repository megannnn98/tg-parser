import asyncio
import os
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager

from db.engine import create_engine, session_factory

from parser.collector import collect_db, CollectorConfig
from parser.user_collector import collect_user_comments, UserCollectorConfig
from parser.user_finder import find_users, format_found_users, UserFinderConfig
from parser.channel_discovery import (
    append_channels,
    discover_channels,
    DiscoveryConfig,
)
from parser.utils import parse_args, parse_user_ref
from parser.logger import get_logger
from config import (
    CHANNELS,
    CHANNELS_PATH,
    DISCOVER_TARGET,
)


@asynccontextmanager
async def database(pool_size: int = 5):
    engine = create_engine(pool_size=pool_size)
    try:
        yield session_factory(engine)
    finally:
        await engine.dispose()


async def run_collect(_args, logger) -> None:
    cfg = CollectorConfig(channels=CHANNELS)
    async with database(pool_size=cfg.concurrency) as sessions:
        new_rows = await collect_db(sessions, cfg)
    logger.info(f"Saved {new_rows} new comments")


async def run_user_comments(args, _logger) -> None:
    user_ref = parse_user_ref(args.user)
    cfg = UserCollectorConfig(channels=CHANNELS, refresh_text=args.refresh_text)
    async with database() as sessions:
        result = await collect_user_comments(sessions, cfg, user_ref)
    print(result.render())


async def run_find_user(args, logger) -> None:
    found = await find_users(UserFinderConfig(channels=CHANNELS), args.user)
    if not found:
        logger.info(f"No user matching {args.user!r} found")
        return

    print(format_found_users(found))


async def run_discover_channels(_args, logger) -> None:
    cfg = DiscoveryConfig(channels=CHANNELS, target=DISCOVER_TARGET)
    found = await discover_channels(cfg)
    if not found:
        logger.info("No new channels found")
        return

    added = append_channels(CHANNELS_PATH, found)
    logger.info(
        f"Added {added} channel(s) to {CHANNELS_PATH}: "
        f"{len(CHANNELS)} -> {len(CHANNELS) + added}"
    )


async def run_web(_args, _logger) -> None:
    import uvicorn

    from web.app import create_app

    await asyncio.to_thread(
        uvicorn.run,
        create_app(),
        host=os.getenv("WEB_HOST", "0.0.0.0"),
        port=int(os.getenv("WEB_PORT", "8000")),
    )


CommandHandler = Callable[[object, object], Awaitable[None]]

COMMANDS: dict[str, CommandHandler] = {
    "collect": run_collect,
    "user-comments": run_user_comments,
    "find-user": run_find_user,
    "discover-channels": run_discover_channels,
    "web": run_web,
}


async def main():
    args = parse_args()
    logger = get_logger("main")
    await COMMANDS[args.mode](args, logger)


if __name__ == "__main__":
    asyncio.run(main())
