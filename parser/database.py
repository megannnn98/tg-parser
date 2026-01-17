# parser/database.py
import aiosqlite
from config import DB_PATH

async def get_db():
    return await aiosqlite.connect(DB_PATH)

async def init_db(db):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user TEXT UNIQUE
        )
    """)
    await db.commit()
