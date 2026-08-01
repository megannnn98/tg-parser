import asyncio
import aiosqlite
import os
from parser.collector import collect_db, CollectorConfig
from parser.user_collector import collect_user_comments, UserCollectorConfig
from parser.user_finder import find_users, format_found_users, UserFinderConfig
from parser.channel_discovery import (
    append_channels,
    discover_channels,
    DiscoveryConfig,
)
from parser.analytics import get_haters
from parser.utils import parse_args, parse_user_ref
from parser.logger import get_logger
from config import (
    CHANNELS,
    CHANNELS_PATH,
    DATA_DIR,
    DB_PATH,
    DISCOVER_TARGET,
    USER_DB_PATH,
)
from pathlib import Path

async def main():
    args = parse_args()
    logger = get_logger("main")
    db_path = Path(DB_PATH)

    if args.mode == "collect":
        cfg = CollectorConfig(channels=CHANNELS)
        await collect_db(db_path, cfg)
        return

    if args.mode == "user-comments":
        user_ref = parse_user_ref(args.user)
        cfg = UserCollectorConfig(channels=CHANNELS)
        user_db_path, saved = await collect_user_comments(
            Path(DATA_DIR),
            cfg,
            user_ref,
            db_path_override=Path(USER_DB_PATH) if USER_DB_PATH else None,
        )
        logger.info(f"Saved {saved} new comments of {user_ref} to {user_db_path}")
        return

    if args.mode == "find-user":
        found = await find_users(UserFinderConfig(channels=CHANNELS), args.user)
        if not found:
            logger.info(f"No user matching {args.user!r} found")
            return

        print(format_found_users(found))
        return

    if args.mode == "discover-channels":
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
        return

    if args.mode == "web":
        import uvicorn

        from web.app import create_app

        await asyncio.to_thread(
            uvicorn.run,
            create_app(Path(DATA_DIR)),
            host=os.getenv("WEB_HOST", "0.0.0.0"),
            port=int(os.getenv("WEB_PORT", "8000")),
        )
        return

    if args.mode == "haters":

        channels = CHANNELS
        hate_words = ["путин"]

        for channel in channels:


            logger.info(f"Processing {channel} ({db_path})")

            async with aiosqlite.connect(db_path) as db:
                haters = await get_haters(db, hate_words, channel)

            if not haters:
                continue

            print(f"Канал {channel}")
            for username, tg_id, hate_count, total_count, hate_percent in haters:
                name = username or f"id:{tg_id}"
                print(
                    f"    {name}: {hate_count} "
                    f"({hate_percent}% из {total_count} сообщений)"
                )

    return

if __name__ == "__main__":
    asyncio.run(main())
