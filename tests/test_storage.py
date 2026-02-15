import asyncio
from pathlib import Path

from parser.storage import (
    get_db,
    init_db,
    save_message,
    save_messages_many,
    upsert_channel,
    upsert_channels_many,
    upsert_user,
    upsert_users_many,
)


def test_get_db_creates_file_and_applies_pragmas(tmp_path: Path):
    async def _run():
        db_path = tmp_path / "nested" / "app.db"
        db = await get_db(db_path)
        try:
            async with db.execute("PRAGMA foreign_keys") as cur:
                foreign_keys = (await cur.fetchone())[0]
            async with db.execute("PRAGMA journal_mode") as cur:
                journal_mode = (await cur.fetchone())[0]
            async with db.execute("PRAGMA synchronous") as cur:
                synchronous = (await cur.fetchone())[0]
        finally:
            await db.close()
        return db_path, foreign_keys, journal_mode, synchronous

    db_path, foreign_keys, journal_mode, synchronous = asyncio.run(_run())
    assert db_path.parent.exists()
    assert db_path.exists()
    assert foreign_keys == 1
    assert journal_mode == "wal"
    assert synchronous == 1


def test_init_db_creates_tables_and_indexes(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)

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
    assert {"users", "channels", "messages"}.issubset(tables)
    assert "idx_messages_channel" in indexes
    assert "idx_messages_user_channel" in indexes
    assert "idx_messages_date" in indexes


def test_upsert_user_inserts_and_updates_username(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)

            first = await upsert_user(db, 1001, "Alice")
            second = await upsert_user(db, 1001, "Bob")

            async with db.execute(
                "SELECT username FROM users WHERE tg_id = ?",
                (1001,),
            ) as cur:
                stored = (await cur.fetchone())[0]
        finally:
            await db.close()

        return first, second, stored

    first, second, stored = asyncio.run(_run())
    assert first == "Alice"
    assert second == "Bob"
    assert stored == "Bob"


def test_upsert_channel_returns_name_and_inserts_once(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)
            result = await upsert_channel(db, "chan_a")
            await upsert_channel(db, "chan_a")

            async with db.execute(
                "SELECT COUNT(*) FROM channels WHERE name = ?",
                ("chan_a",),
            ) as cur:
                count = (await cur.fetchone())[0]
        finally:
            await db.close()

        return result, count

    result, count = asyncio.run(_run())
    assert result == "chan_a"
    assert count == 1


def test_save_message_normalizes_text(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)
            await upsert_user(db, 42, "user42")
            await upsert_channel(db, "chan_a")

            await save_message(
                db,
                42,
                "chan_a",
                {"text": "HeLLo Ｗｏｒｌｄ", "date": "2025-02-01"},
            )

            async with db.execute("SELECT user, channel, text, date FROM messages") as cur:
                row = await cur.fetchone()
        finally:
            await db.close()

        return row

    row = asyncio.run(_run())
    assert row == (42, "chan_a", "hello world", "2025-02-01")


def test_bulk_operations_insert_and_update_rows(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)

            await upsert_channels_many(db, [("chan_a",), ("chan_b",), ("chan_a",)])
            await upsert_users_many(db, [(1, "User1"), (2, "User2"), (1, "Renamed")])

            await save_messages_many(
                db,
                [
                    (1, "chan_a", "TeXT", "2025-02-01"),
                    (2, "chan_b", "Ｆｕｌｌ", "2025-02-02"),
                ],
            )

            async with db.execute("SELECT name FROM channels ORDER BY name") as cur:
                channels = [row[0] for row in await cur.fetchall()]

            async with db.execute(
                "SELECT tg_id, username FROM users ORDER BY tg_id"
            ) as cur:
                users = await cur.fetchall()

            async with db.execute(
                "SELECT user, channel, text, date FROM messages ORDER BY id"
            ) as cur:
                messages = await cur.fetchall()
        finally:
            await db.close()

        return channels, users, messages

    channels, users, messages = asyncio.run(_run())
    assert channels == ["chan_a", "chan_b"]
    assert users == [(1, "Renamed"), (2, "User2")]
    assert messages == [
        (1, "chan_a", "text", "2025-02-01"),
        (2, "chan_b", "full", "2025-02-02"),
    ]


def test_empty_batch_operations_are_noop(tmp_path: Path):
    async def _run():
        db = await get_db(tmp_path / "app.db")
        try:
            await init_db(db)

            await upsert_channels_many(db, [])
            await upsert_users_many(db, [])
            await save_messages_many(db, [])

            async with db.execute("SELECT COUNT(*) FROM channels") as cur:
                channels_count = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM users") as cur:
                users_count = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM messages") as cur:
                messages_count = (await cur.fetchone())[0]
        finally:
            await db.close()

        return channels_count, users_count, messages_count

    channels_count, users_count, messages_count = asyncio.run(_run())
    assert channels_count == 0
    assert users_count == 0
    assert messages_count == 0
