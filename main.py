import asyncio
from parser.collector import collect_db
from parser.analytics import get_haters_from_db, print_user_messages
from parser.utils import parse_channels, parse_args
from parser.logger import get_logger
from config import CHANNELS

async def main():
    args = parse_args()
    logger = get_logger("main")

    if args.mode == "collect":
        await collect_db()
        return

    if args.mode == "haters":
        channels = parse_channels(CHANNELS)
        hate_words = ["рудуа"]

        for channel in channels:
            db_path = f"data/{channel}.db"

            haters = await get_haters_from_db(db_path, hate_words)

            if not haters:
                continue

            print("****************************************")
            print(f"Канал {channel}")
            print("")
            print("")

            logger.info(f"Processing {channel} ({db_path})")


            for hater in haters:
                await print_user_messages(db_path, hater)

if __name__ == "__main__":
    asyncio.run(main())
