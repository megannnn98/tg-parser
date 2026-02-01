import asyncio
import aiosqlite
from parser.collector import collect_db
from parser.analytics import get_haters
from parser.utils import parse_args, db_path_for_channel
from parser.logger import get_logger
from config import CHANNELS
from parser.storage import get_db

async def main():
    args = parse_args()
    logger = get_logger("main")

    if args.mode == "collect":
        await collect_db()
        return

    if args.mode == "haters":

        channels = CHANNELS
        hate_words = ["рудуа"]

        for channel in channels:
            db_path = db_path_for_channel(channel)

            logger.info(f"Processing {channel} ({db_path})")

            async with aiosqlite.connect(db_path) as db:
                haters = await get_haters(db, hate_words)

            if not haters:
                continue

            print(f"Канал {channel}")
            for username, id, count in haters:
                name = username or f"id:{id}"
                print(f"    {name}: {count}")

    return

if __name__ == "__main__":
    asyncio.run(main())
