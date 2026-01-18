import asyncio
from parser.database import get_db, init_db
from parser.telegram import get_client
from parser.collector import collect_data
from parser.logger import get_logger

logger = get_logger(__name__)

async def main():
    logger.info("Application started")

    db = await get_db()
    await init_db(db)
    logger.info("Database connected")

    app = get_client()

    async with app:
        logger.info("Telegram client started")
        await collect_data(app, db)

    await db.commit()
    await db.close()
    logger.info("Application finished")


if __name__ == "__main__":
    asyncio.run(main())
