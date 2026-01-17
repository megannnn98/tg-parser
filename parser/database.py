# parser/database.py
import aiosqlite
from config import DB_PATH

async def get_db():
    return await aiosqlite.connect(DB_PATH)

async def init_db(db):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_user_id INTEGER UNIQUE,
            username TEXT
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_message_id INTEGER,
            discussion_id INTEGER,
            user_id INTEGER,
            date TEXT,
            text TEXT,
            UNIQUE (tg_message_id, discussion_id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    await db.commit()
