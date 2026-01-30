# tests/conftest.py
import aiosqlite
import pytest_asyncio

@pytest_asyncio.fixture
async def db(tmp_path):
    db_path = tmp_path / "test.db"
    db = await aiosqlite.connect(db_path)

    await db.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE NOT NULL,
            username TEXT
        )
    """)

    await db.execute("""
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            message_id INTEGER,
            user_id INTEGER,
            date TEXT,
            text TEXT,
            channel_id TEXT,
            UNIQUE(chat_id, message_id)
        )
    """)

    await db.commit()
    yield db
    await db.close()
