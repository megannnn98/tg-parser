import aiosqlite
from pathlib import Path

async def get_db(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return await aiosqlite.connect(str(db_path))

async def init_db(db_handle, channels: list[str]):
    await db_handle.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE NOT NULL,
            username TEXT
        )
    """)

    await db_handle.execute("""
        CREATE TABLE IF NOT EXISTS channel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    """)

    await db_handle.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            message_id INTEGER,
            user_id INTEGER,
            date TEXT,
            text TEXT,
            channel_id INTEGER,

            UNIQUE (chat_id, message_id),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(chat_id) REFERENCES channel(id)
        )
    """)

    await db_handle.execute("""
        INSERT OR IGNORE INTO channel (name)
        VALUES (?)
        """,
        channels
    )

    await db_handle.commit()
