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

    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_channel
        ON messages(channel)
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_user_channel
        ON messages(user, channel)
    """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_date
        ON messages(date)
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

async def save_messages_many(db, rows: list[tuple[int, str, str, str]]):
    if not rows:
        return
    normalized_rows = []
    for user, channel, text, date in rows:
        normalized_rows.append((user, channel, normalize(text), date))
    await db.executemany(
        """
        INSERT OR IGNORE INTO messages
        (user, channel, text, date)
        VALUES (?, ?, ?, ?)
        """,
        normalized_rows,
    )

async def upsert_users_many(db, rows: list[tuple[int, str | None]]):
    if not rows:
        return
    await db.executemany(
        """
        INSERT INTO users (tg_id, username)
        VALUES (?, ?)
        ON CONFLICT(tg_id) DO UPDATE SET
            username = excluded.username
        """,
        rows,
    )

async def upsert_channels_many(db, rows: list[tuple[str]]):
    if not rows:
        return
    await db.executemany(
        """
        INSERT OR IGNORE INTO channels (name)
        VALUES (?)
        """,
        rows,
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
    return name
