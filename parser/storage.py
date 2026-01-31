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
            chat_id INTEGER,
            message_id INTEGER,
            user_id INTEGER,
            date TEXT,
            text TEXT,

            UNIQUE (chat_id, message_id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)

    await db.commit()

async def save_message(db, chat_id, msg, user_id):
    await db.execute(
        """
        INSERT OR IGNORE INTO messages
        (chat_id, message_id, user_id, date, text)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            chat_id,
            msg["message_id"],
            user_id,
            msg["date"],
            msg["text"],
        )
    )
async def upsert_user(db, tg_id: int, username: str | None) -> int:
    async with db.execute(
        """
        INSERT INTO users (tg_id, username)
        VALUES (?, ?)
        ON CONFLICT(tg_id) DO UPDATE SET
            username = excluded.username
        RETURNING id
        """,
        (tg_id, username)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0]

async def get_user_id(db, tg_id: int) -> int | None:
    async with db.execute(
        "SELECT id FROM users WHERE tg_id = ?",
        (tg_id,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None
