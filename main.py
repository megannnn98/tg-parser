import asyncio
from parser.collector import collect_db
import aiosqlite
import re
from parser.utils import parse_channels, parse_args
from parser.logger import get_logger
from config import CHANNELS
from typing import Optional

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

async def get_username_by_tg_id(
    db_path: str,
    tg_id: int
) -> Optional[str]:
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            """
            SELECT username
            FROM users
            WHERE tg_id = ?
            """,
            (tg_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else None

async def get_haters_from_db(db_path: str, hate_words: list[str]) -> list[int]:
    haters = []
    users = await get_users_from_db(db_path)

    for user in users:
        messages = await get_user_messages_from_db(
            db_path,
            tg_id=user
        )

        for msg in messages:
            found = bool(re.search(r'\b{}\b'.format(re.escape(hate_words[0])), msg, re.IGNORECASE))
            if found:
                haters.append(user)
                break

    return haters

async def get_haters(db_path: str):
    hate_words = ["Ленин"]
    haters = await get_haters_from_db(db_path, hate_words=hate_words)
    return haters

async def print_user_messages(db_path: str, tg_id: int):
    messages = await get_user_messages_from_db(
        db_path,
        tg_id=tg_id
    )

    username = await get_username_by_tg_id(db_path, tg_id=tg_id)

    print("=" * 40)
    print(f"Сообщения пользователя {username}")
    print("=" * 40)

    for msg in messages:
        print("-", msg)

async def main():
    args = parse_args()
    logger = get_logger("main")

    try :
        if args.mode == "collect":
            await collect_db()
        elif args.mode == "haters":
            channels = parse_channels(CHANNELS)

            for channel in channels:

                print("*" * 40)
                print(f"Канал {channel}")
                print("*" * 40)

                db_path = "data/" + channel + ".db"
                logger.info(f"Processing {channel}({db_path})")
                haters = await get_haters(db_path)
                for hater in haters:
                    await print_user_messages(db_path, tg_id=hater)

    except Exception as e:
        logger.error(e)

if __name__ == "__main__":
    asyncio.run(main())
