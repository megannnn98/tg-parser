import asyncio
from pathlib import Path

from parser.storage import get_db
from parser.user_storage import init_user_db, save_user_messages_many


def test_init_user_db_creates_table_and_indexes(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "user_comments.db")
        try:
            await init_user_db(db)

            async with db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ) as cur:
                tables = {row[0] for row in await cur.fetchall()}

            async with db.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ) as cur:
                indexes = {row[0] for row in await cur.fetchall()}
        finally:
            await db.close()

        return tables, indexes

    tables, indexes = asyncio.run(_run())
    assert "user_messages" in tables
    assert "users" not in tables
    assert "messages" not in tables
    assert "idx_user_messages_tg_id" in indexes
    assert "idx_user_messages_channel" in indexes
    assert "idx_user_messages_date" in indexes


def test_save_user_messages_many_normalizes_text(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "user_comments.db")
        try:
            await init_user_db(db)
            await save_user_messages_many(
                db,
                [
                    (7, "vasya", "chan_a", 100, "HeLLo Ｗｏｒｌｄ", "2025-02-01"),
                    (7, "vasya", "chan_b", 200, "Второй", "2025-02-02"),
                ],
            )

            async with db.execute(
                """
                SELECT tg_id, username, channel, message_id, text, date
                FROM user_messages
                ORDER BY message_id
                """
            ) as cur:
                rows = await cur.fetchall()
        finally:
            await db.close()

        return rows

    rows = asyncio.run(_run())
    assert rows == [
        (7, "vasya", "chan_a", 100, "hello world", "2025-02-01"),
        (7, "vasya", "chan_b", 200, "второй", "2025-02-02"),
    ]


def test_save_user_messages_many_is_idempotent(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "user_comments.db")
        try:
            await init_user_db(db)
            rows = [
                (7, "vasya", "chan_a", 100, "first", "2025-02-01"),
                (7, "vasya", "chan_a", 101, "second", "2025-02-02"),
            ]
            await save_user_messages_many(db, rows)
            await save_user_messages_many(db, rows)

            async with db.execute("SELECT COUNT(*) FROM user_messages") as cur:
                count = (await cur.fetchone())[0]
        finally:
            await db.close()

        return count

    assert asyncio.run(_run()) == 2


def test_save_user_messages_many_keeps_same_message_id_per_channel(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "user_comments.db")
        try:
            await init_user_db(db)
            await save_user_messages_many(
                db,
                [
                    (7, "vasya", "chan_a", 100, "in a", "2025-02-01"),
                    (7, "vasya", "chan_b", 100, "in b", "2025-02-02"),
                ],
            )

            async with db.execute("SELECT COUNT(*) FROM user_messages") as cur:
                count = (await cur.fetchone())[0]
        finally:
            await db.close()

        return count

    assert asyncio.run(_run()) == 2


def test_save_user_messages_many_empty_batch_is_noop(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "user_comments.db")
        try:
            await init_user_db(db)
            await save_user_messages_many(db, [])

            async with db.execute("SELECT COUNT(*) FROM user_messages") as cur:
                count = (await cur.fetchone())[0]
        finally:
            await db.close()

        return count

    assert asyncio.run(_run()) == 0
