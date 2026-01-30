import asyncio
from parser.collector import collect_db
from parser.analytics import get_haters_from_db, print_user_messages, get_user_messages_from_db
from parser.utils import parse_args
from parser.logger import get_logger
from config import CHANNELS
from parser import analytics

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
            db_path = f"data/{channel}.db"

            haters = await get_haters_from_db(db_path, hate_words)

            if not haters:
                continue

            print(f"Канал {channel}")
            logger.info(f"Processing {channel} ({db_path})")

            haters = await get_haters_from_db(db_path, hate_words)

            for tg_id, username, count in haters:
                name = username or f"id:{tg_id}"
                all_msgs = await get_user_messages_from_db(db_path, tg_id)
                num = len(all_msgs)
                print(f"    {name}: {count}({num}) -> {int(round(count / num, 2) * 100)}%")
            print("")

    return

if __name__ == "__main__":
    asyncio.run(main())
