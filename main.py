import asyncio
from parser.database import get_db, init_db
from parser.telegram import get_client
from parser.collector import collect_data


async def main():
    db = await get_db()
    await init_db(db)

    app = get_client()

    async with app:
        await collect_data(app, db)

    await db.commit()
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
