import asyncio
from parser.telegram import get_client
from parser.database import get_db, init_db
from parser.collector import collect_channel
from parser.utils import parse_channels, db_path_for_channel
from config import CHANNELS


async def main():
    channels = parse_channels(CHANNELS)
    if not channels:
        raise RuntimeError("CHANNELS is empty")

    app = get_client()

    async with app:
        for channel in channels:
            db_path = db_path_for_channel(channel)
            db = await get_db(db_path)

            try:
                await init_db(db)
                await collect_channel(app, db, channel)
            finally:
                await db.close()

if __name__ == "__main__":
    asyncio.run(main())
