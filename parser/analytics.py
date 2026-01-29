import aiosqlite
import re
from typing import Optional

async def get_user_messages_from_db(
    db_path: str,
    tg_id: int
) -> list[str]:
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


async def get_users_from_db(db_path: str) -> list[int]:
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            "SELECT tg_id FROM users"
        ) as cursor:
            return [row[0] async for row in cursor]


async def get_username_by_tg_id(
    db_path: str,
    tg_id: int
) -> Optional[str]:
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            "SELECT username FROM users WHERE tg_id = ?",
            (tg_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else None


async def get_haters_from_db(
    db_path: str,
    hate_words: list[str]
) -> list[tuple[int, str | None, int]]:
    users = await get_users_from_db(db_path)

    result: list[tuple[int, str | None, int]] = []

    pattern = re.compile(
        r"\b(" + "|".join(map(re.escape, hate_words)) + r")\b",
        re.IGNORECASE
    )

    for tg_id in users:
        messages = await get_user_messages_from_db(db_path, tg_id)

        count = 0
        for msg in messages:
            count += len(pattern.findall(msg))

        if count == 0:
            continue

        username = await get_username_by_tg_id(db_path, tg_id)

        result.append((tg_id, username, count))

    return result




async def print_user_messages(db_path: str, tg_id: int):
    messages = await get_user_messages_from_db(db_path, tg_id)
    username = await get_username_by_tg_id(db_path, tg_id)

    print("=" * 40)
    print(f"Сообщения пользователя {username or tg_id}")
    print("=" * 40)

    for msg in messages:
        print("-", msg)
