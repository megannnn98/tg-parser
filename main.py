import asyncio
from parser.collector import collect_db
import aiosqlite
import re
import argparse

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
    hate_words = ["рудуа"]
    haters = await get_haters_from_db(db_path, hate_words=hate_words)
    return haters

async def print_user_messages(db_path: str, tg_id: int):
    messages = await get_user_messages_from_db(
        db_path,
        tg_id=tg_id
    )

    print("=" * 40)
    print(f"Сообщения пользователя {tg_id}")
    print("=" * 40)

    for msg in messages:
        print("-", msg)




def parse_args():
    parser = argparse.ArgumentParser(
        description="Telegram parser"
    )

    parser.add_argument(
        "mode",
        nargs="?",
        default="collect",
        choices=["collect", "haters"],
        help="Run mode"
    )

    return parser.parse_args()


async def main():
    args = parse_args()
    db_path = "data/ru_doy.db"
    # db_path = "data/vihod_est.db"

    if args.mode == "collect":
        await collect_db()
    elif args.mode == "haters":
        haters = await get_haters(db_path)
        for hater in haters:
            await print_user_messages(db_path, tg_id=hater)

if __name__ == "__main__":
    asyncio.run(main())
