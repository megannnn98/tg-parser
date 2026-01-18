import asyncio
from parser.telegram import get_client
from parser.database import get_db, init_db
from parser.collector import collect_channel
from parser.utils import parse_channels, db_path_for_channel
from config import CHANNELS
from parser.logger import get_logger

async def main():
    channels = parse_channels(CHANNELS)
    if not channels:
        raise RuntimeError("CHANNELS is empty")

    tg_client = get_client()
    logger = get_logger("main")

    async with tg_client:
        for channel in channels:
            db_path = db_path_for_channel(channel)
            db = await get_db(db_path)

            try:
                logger.info(f"Collecting channel {channel}")
                await init_db(db)
                await collect_channel(tg_client, db, channel)
                logger.info(f"Finished collecting channel {channel}")
            finally:
                await db.close()
        logger.info("Done")

if __name__ == "__main__":
    asyncio.run(main())
