import aiosqlite
from pathlib import Path
from parser.utils import normalize


async def get_db(db_path: Path) -> aiosqlite.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    db = await aiosqlite.connect(
        str(db_path),
        isolation_level=None,
    )

    await db.execute("PRAGMA foreign_keys = ON")
    await db.execute("PRAGMA journal_mode = WAL")
    await db.execute("PRAGMA synchronous = NORMAL")

    return db


async def init_db(db):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id INTEGER PRIMARY KEY NOT NULL,
            username TEXT
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            name TEXT PRIMARY KEY NOT NULL
        )
    """)

    await db.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user INTEGER,
            channel TEXT,
            text TEXT,
            date TEXT,

            FOREIGN KEY(user) REFERENCES users(tg_id),
            FOREIGN KEY(channel) REFERENCES channels(name)
        )
    """)

    await db.commit()

async def save_message(db, user, channel, msg):
    await db.execute(
        """
        INSERT OR IGNORE INTO messages
        (user, channel, text, date)
        VALUES (?, ?, ?, ?)
        """,
        (
            user,
            channel,
            normalize(msg["text"]),
            msg["date"],
        )
    )
async def upsert_user(db, tg_id: int, username: str | None) -> int:
    async with db.execute(
        """
        INSERT INTO users (tg_id, username)
        VALUES (?, ?)
        ON CONFLICT(tg_id) DO UPDATE SET
            username = excluded.username
        RETURNING username
        """,
        (tg_id, username)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0]

async def upsert_channel(
    db: aiosqlite.Connection,
    name: str,
) -> str:
    await db.execute(
        """
        INSERT OR IGNORE INTO channels (name)
        VALUES (?)
        """,
        (name,),
    )
    await db.commit()
    return name

async def get_user_id(db, tg_id: int) -> int | None:
    async with db.execute(
        "SELECT id FROM users WHERE tg_id = ?",
        (tg_id,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else None
