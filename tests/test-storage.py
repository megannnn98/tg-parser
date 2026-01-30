# tests/test-storage.py
import pytest
from parser.storage import get_user_id, save_message, upsert_user



@pytest.mark.asyncio
async def test_save_message(db):
    await db.execute("""
        INSERT INTO users (tg_id, username)
        VALUES (123, 'username')
    """)
    await db.commit()

    await save_message(db, 1, {"message_id": 1, "date": "2022-01-01", "text": "Hello"}, 1, "channel1")

    async with db.execute("SELECT * FROM messages") as cursor:
        row = await cursor.fetchone()
        assert row
        assert row[3] == 1
        assert row[4] == "2022-01-01"
        assert row[5] == "Hello"
        assert row[6] == "channel1"


@pytest.mark.asyncio
async def test_save_message_user_not_exists(db):
    await save_message(db, 1, {"message_id": 1, "date": "2022-01-01", "text": "Hello"}, 1, "channel1")

    async with db.execute("SELECT * FROM messages") as cursor:
        row = await cursor.fetchone()
        assert row
        assert row[3] == 1
        assert row[4] == "2022-01-01"
        assert row[5] == "Hello"
        assert row[6] == "channel1"


@pytest.mark.asyncio
async def test_save_message_2(db):
    await db.execute("""
        INSERT INTO users (tg_id, username)
        VALUES (123, 'username')
    """)
    await db.commit()

    await save_message(db, 1, {"message_id": 1, "date": "2022-01-01", "text": "Hello"}, 1, "channel1")
    await save_message(db, 1, {"message_id": 2, "date": "2022-01-01", "text": "Hello"}, 1, "channel1")

    async with db.execute("SELECT * FROM messages") as cursor:
        rows = await cursor.fetchall()
        assert len(rows) == 2
        assert rows[0][3] == 1
        assert rows[1][3] == 1
        assert rows[0][0] == 1
        assert rows[1][0] == 2
        assert rows[0][4] == "2022-01-01"
        assert rows[1][4] == "2022-01-01"
        assert rows[0][5] == "Hello"
        assert rows[1][5] == "Hello"


@pytest.mark.asyncio
async def test_get_user_id_user_not_exists(db):
    user_id = await get_user_id(db, 123)
    assert user_id is None

@pytest.mark.asyncio
async def test_get_user_id_user_exists(db):
    await db.execute("""
        INSERT INTO users (tg_id, username)
        VALUES (123, 'username')
    """)
    await db.commit()

    user_id = await get_user_id(db, 123)
    assert user_id == 1

@pytest.mark.asyncio
async def test_get_user_id_user_exists_with_different_tg_id(db):
    await db.execute("""
        INSERT INTO users (tg_id, username)
        VALUES (456, 'username')
    """)
    await db.commit()

    user_id = await get_user_id(db, 123)
    assert user_id is None

@pytest.mark.asyncio
async def test_get_user_id_user_exists_ignore_username(db):
    await db.execute(
        "INSERT INTO users (tg_id, username) VALUES (?, ?)",
        (123, "different_username")
    )
    await db.commit()

    user_id = await get_user_id(db, 123)
    assert user_id == 1

@pytest.mark.asyncio
async def test_get_user_id_user_exists_ignore_username(db):
    await db.execute(
        "INSERT INTO users (tg_id, username) VALUES (?, ?)",
        (123, "different_username")
    )
    await db.commit()

    user_id = await get_user_id(db, 123)
    assert user_id == 1


@pytest.mark.asyncio
async def test_upsert_user(db):

    await upsert_user(db, 123, "username")
    async with db.execute("SELECT * FROM users") as cursor:
        row = await cursor.fetchone()
        assert row
        assert row[0] == 1
        assert row[1] == 123
        assert row[2] == "username"
