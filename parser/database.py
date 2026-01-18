import aiosqlite
from pathlib import Path

async def get_db(db_path: Path):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return await aiosqlite.connect(str(db_path))

async def init_db(db):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE NOT NULL,
            username TEXT
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            discussion_id INTEGER,
            message_id INTEGER,
            user_id INTEGER,
            date TEXT,
            text TEXT,

            UNIQUE (discussion_id, message_id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)
    await db.commit()
