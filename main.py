import asyncio
import itertools
from datetime import datetime
import aiosqlite
from parser.collector import collect_db, CollectorConfig
from parser.analytics import get_haters, get_user_comments
from parser.export import write_comments_csv
from parser.utils import parse_args
from parser.logger import get_logger
from config import CHANNELS, DATA_DIR, DB_PATH
from pathlib import Path

async def main():
    args = parse_args()
    logger = get_logger("main")
    db_path = Path(DB_PATH)

    if args.mode == "collect":
        cfg = CollectorConfig(channels=CHANNELS)
        await collect_db(db_path, cfg)
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

    if args.mode == "comments":
        if not args.username:
            raise SystemExit("--username is required for comments mode")

        async with aiosqlite.connect(db_path) as db:
            comments = await get_user_comments(db, args.username)

        if not comments:
            logger.info(f"No comments found for username={args.username}")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        data_dir = Path(DATA_DIR)

        for channel, group in itertools.groupby(comments, key=lambda row: row[0]):
            rows = [(date, text) for _, date, text in group]
            path = write_comments_csv(data_dir, channel, timestamp, rows)
            logger.info(f"Wrote {len(rows)} comments for {channel} to {path}")

        return

if __name__ == "__main__":
    asyncio.run(main())
