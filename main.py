import asyncio
from parser.telegram import get_client
from parser.database import get_db, init_db
from parser.collector import collect_channel
from parser.utils import parse_channels, db_path_for_channel
from config import CHANNELS
from parser.logger import get_logger
import aiosqlite
import re

async def get_user_messages_from_db(
    db_path: str,
    tg_id: int
) -> list[str]:
    """
    Возвращает все сообщения пользователя (по tg_id)
    из конкретной базы данных.
    """

    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            """
            SELECT m.text
            FROM messages m
            JOIN users u ON u.id = m.user_id
            WHERE u.tg_id = ?
            ORDER BY m.id
            """,
            (tg_id,)
        ) as cursor:
            return [row[0] async for row in cursor]
    return []

async def get_users_from_db(db_path: str) -> list[int]:
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            "SELECT tg_id FROM users"
        ) as cursor:
            return [row[0] async for row in cursor]
    return []

async def get_haters_from_db(db_path: str) -> list[int]:
    haters = []
    users = await get_users_from_db(db_path="data/ru_doy.db")

    for user in users:
        messages = await get_user_messages_from_db(
            db_path="data/ru_doy.db",
            tg_id=user
        )

        for msg in messages:
            word = "рудуа"
            found = bool(re.search(r'\b{}\b'.format(re.escape(word)), msg, re.IGNORECASE))
            if found:
                haters.append(user)
                break

    return haters


async def main():
    channels = parse_channels(CHANNELS)
    if not channels:
        raise RuntimeError("CHANNELS is empty")

    tg_client = get_client()
    logger = get_logger("main")

    haters = await get_haters_from_db(db_path="data/ru_doy.db")
    print(haters)







    # async with tg_client:
    #     for channel in channels:
    #         db_path = db_path_for_channel(channel)
    #         db = await get_db(db_path)

    #         try:
    #             logger.info(f"Collecting channel {channel}")
    #             await init_db(db)
    #             await collect_channel(tg_client, db, channel)
    #             logger.info(f"Finished collecting channel {channel}")
    #         finally:
    #             await db.close()
    #     logger.info("Done")

    # USER_ID = 428434968

    # messages = await get_user_messages_from_db(
    #     db_path="data/ru_doy.db",
    #     tg_id=USER_ID
    # )

    # print("=" * 40)
    # print(f"Сообщения пользователя {USER_ID}")
    # print("=" * 40)

    # for msg in messages:
    #     print("-", msg)





if __name__ == "__main__":
    asyncio.run(main())
